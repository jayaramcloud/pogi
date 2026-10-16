// Package sqs emulates Amazon SQS over JSON 1.0 and the legacy Query
// protocol: standard and FIFO queues, visibility timeouts, long polling,
// delays, dead-letter queues and redrive, batches and tags.
package sqs

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the SQS emulator.
type Service struct {
	hub *core.Hub
	rpc *core.RPC
	mu  sync.Mutex
	st  state
	j   *core.Journal // wal mode
}

type state struct {
	Queues map[string]*Queue `json:"queues"` // account/region/name
}

// Queue is one SQS queue.
type Queue struct {
	Name     string               `json:"name"`
	Account  string               `json:"account"`
	Region   string               `json:"region"`
	Created  time.Time            `json:"created"`
	Modified time.Time            `json:"modified"`
	Attrs    map[string]string    `json:"attrs"`
	Tags     map[string]string    `json:"tags,omitempty"`
	Messages []*Message           `json:"messages"`
	Dedup    map[string]string    `json:"dedup,omitempty"` // dedup id → message id (5 min window via DedupAt)
	DedupAt  map[string]time.Time `json:"dedupAt,omitempty"`
	Seq      int64                `json:"seq"`
	wake     chan struct{}
}

// Message is one stored message.
type Message struct {
	ID           string                           `json:"id"`
	Body         string                           `json:"body"`
	Attrs        map[string]core.MessageAttribute `json:"attrs,omitempty"`
	SysAttrs     map[string]core.MessageAttribute `json:"sysAttrs,omitempty"`
	Sent         time.Time                        `json:"sent"`
	VisibleAt    time.Time                        `json:"visibleAt"`
	Receives     int                              `json:"receives"`
	FirstReceive time.Time                        `json:"firstReceive,omitempty"`
	Receipt      string                           `json:"receipt,omitempty"`
	GroupID      string                           `json:"groupId,omitempty"`
	DedupID      string                           `json:"dedupId,omitempty"`
	Seq          string                           `json:"seq,omitempty"`
	Source       string                           `json:"source,omitempty"` // original queue ARN after a DLQ move
	Trace        string                           `json:"trace,omitempty"`  // the flow that sent it
	Sender       string                           `json:"sender,omitempty"` // the sender's user id, when it is an STS or IAM identity
}

var defaults = map[string]string{
	"VisibilityTimeout": "30", "MaximumMessageSize": "1048576", "MessageRetentionPeriod": "345600",
	"DelaySeconds": "0", "ReceiveMessageWaitTimeSeconds": "0", "SqsManagedSseEnabled": "true",
}

// New creates the SQS service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.0", XMLNS: "http://queue.amazonaws.com/doc/2012-11-05/", QueryCodes: queryCodes, Ops: map[string]core.Handler{
		"CreateQueue":                  core.H(s.createQueue),
		"GetQueueUrl":                  core.H(s.getQueueURL),
		"ListQueues":                   core.H(s.listQueues),
		"DeleteQueue":                  core.H(s.deleteQueue),
		"GetQueueAttributes":           core.H(s.getQueueAttributes),
		"SetQueueAttributes":           core.H(s.setQueueAttributes),
		"PurgeQueue":                   core.H(s.purgeQueue),
		"SendMessage":                  core.H(s.sendMessage),
		"SendMessageBatch":             core.H(s.sendMessageBatch),
		"ReceiveMessage":               core.H(s.receiveMessage),
		"DeleteMessage":                core.H(s.deleteMessage),
		"DeleteMessageBatch":           core.H(s.deleteMessageBatch),
		"ChangeMessageVisibility":      core.H(s.changeVisibility),
		"ChangeMessageVisibilityBatch": core.H(s.changeVisibilityBatch),
		"TagQueue":                     core.H(s.tagQueue),
		"UntagQueue":                   core.H(s.untagQueue),
		"ListQueueTags":                core.H(s.listQueueTags),
		"ListDeadLetterSourceQueues":   core.H(s.listDLQSources),
		"StartMessageMoveTask":         core.H(s.startMoveTask),
		"ListMessageMoveTasks":         core.H(s.listMoveTasks),
		"CancelMessageMoveTask":        core.H(s.cancelMoveTask),
		"AddPermission":                core.H(s.addPermission),
		"RemovePermission":             core.H(s.removePermission),
	}}
	if hub != nil {
		hub.OnDeliver("sqs", s.deliver)
		hub.Mount("/_aws/sqs/messages", http.HandlerFunc(s.peekHandler))
	}
	return s
}

// queryCodes maps JSON error codes to the legacy Query codes SDKs expect.
var queryCodes = map[string]string{
	"QueueDoesNotExist":              "AWS.SimpleQueueService.NonExistentQueue;Sender",
	"QueueNameExists":                "QueueAlreadyExists;Sender",
	"ReceiptHandleIsInvalid":         "ReceiptHandleIsInvalid;Sender",
	"PurgeQueueInProgress":           "AWS.SimpleQueueService.PurgeQueueInProgress;Sender",
	"BatchEntryIdsNotDistinct":       "AWS.SimpleQueueService.BatchEntryIdsNotDistinct;Sender",
	"EmptyBatchRequest":              "AWS.SimpleQueueService.EmptyBatchRequest;Sender",
	"TooManyEntriesInBatchRequest":   "AWS.SimpleQueueService.TooManyEntriesInBatchRequest;Sender",
	"BatchRequestTooLong":            "AWS.SimpleQueueService.BatchRequestTooLong;Sender",
	"InvalidAttributeName":           "InvalidAttributeName;Sender",
	"InvalidAttributeValue":          "InvalidAttributeValue;Sender",
	"MessageNotInflight":             "AWS.SimpleQueueService.MessageNotInflight;Sender",
	"InvalidParameterValueException": "InvalidParameterValue;Sender",
	"MissingParameter":               "MissingParameter;Sender",
	"ResourceNotFoundException":      "ResourceNotFoundException;Sender",
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "sqs", Cloud: core.AWS, Title: "SQS", Targets: []string{"AmazonSQS"}, Signing: []string{"sqs"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Query clients POST to the queue URL itself instead of passing QueueUrl.
	if p := strings.Split(strings.Trim(r.URL.Path, "/"), "/"); len(p) == 2 && r.Header.Get("X-Amz-Target") == "" {
		q := r.URL.Query()
		if q.Get("QueueUrl") == "" {
			q.Set("QueueUrl", "http://"+r.Host+r.URL.Path)
			r.URL.RawQuery = q.Encode()
		}
	}
	s.rpc.ServeHTTP(w, r)
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Queues: map[string]*Queue{}}
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
	if st.Queues == nil {
		st.Queues = map[string]*Queue{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// SetJournal logs every change of queues and messages (wal mode).
func (s *Service) SetJournal(j *core.Journal) {
	s.mu.Lock()
	s.j = j
	s.mu.Unlock()
}

// fifoSend is the journal record of a FIFO send: deduplication and sequence.
type fifoSend struct {
	ID  string    `json:"id"`
	At  time.Time `json:"at"`
	Seq int64     `json:"seq"`
}

// Apply replays a journal record: q/ queues (without messages), m/ messages
// and f/ FIFO sends. A message it has not seen goes to the end of its queue.
// ponytail: messages are found by a linear scan, so index them if replays of
// deep queues get slow; and a message moved out and back while a compaction
// saves can replay behind later ones. Add a per-queue sequence if that matters.
func (s *Service) Apply(jk string, v []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := strings.SplitN(jk, "/", 5) // kind, account, region, queue, message or deduplication id
	if len(p) < 4 {
		return fmt.Errorf("bad key %q", jk)
	}
	k := key(p[1], p[2], p[3])
	q := s.st.Queues[k]
	switch {
	case p[0] == "q":
		if err := core.Replay(s.st.Queues, k, v); err != nil || v == nil || q == nil {
			return err
		}
		s.st.Queues[k].Messages = q.Messages
	case q == nil || len(p) < 5: // the queue went later
	case p[0] == "f":
		var f fifoSend
		if err := json.Unmarshal(v, &f); err != nil {
			return err
		}
		if q.Dedup == nil {
			q.Dedup, q.DedupAt = map[string]string{}, map[string]time.Time{}
		}
		q.Dedup[p[4]], q.DedupAt[p[4]], q.Seq = f.ID, f.At, max(q.Seq, f.Seq)
	default: // m
		i := slices.IndexFunc(q.Messages, func(m *Message) bool { return m.ID == p[4] })
		if v == nil {
			if i >= 0 {
				q.Messages = slices.Delete(q.Messages, i, i+1)
			}
			return nil
		}
		m := &Message{}
		if err := json.Unmarshal(v, m); err != nil {
			return err
		}
		if i >= 0 {
			q.Messages[i] = m
		} else {
			q.Messages = append(q.Messages, m)
		}
	}
	return nil
}

// journal logs a queue without its messages, which have records of their own; lock held.
func (s *Service) journal(q *Queue) {
	def := *q
	def.Messages = nil
	s.j.Put("q/"+key(q.Account, q.Region, q.Name), &def)
}

// mkey is the journal key of a message in q.
func (q *Queue) mkey(m *Message) string { return "m/" + key(q.Account, q.Region, q.Name) + "/" + m.ID }

func errNoQueue() error {
	return core.Errorf(400, "QueueDoesNotExist", "The specified queue does not exist.")
}

func key(account, region, name string) string { return account + "/" + region + "/" + name }

func (q *Queue) arn() string { return core.ARN("sqs", q.Region, q.Account, q.Name) }
func (q *Queue) fifo() bool  { return q.Attrs["FifoQueue"] == "true" }
func (q *Queue) attr(k string) string {
	if v, ok := q.Attrs[k]; ok {
		return v
	}
	return defaults[k]
}
func (q *Queue) seconds(k string) time.Duration {
	n, _ := strconv.Atoi(q.attr(k))
	return time.Duration(n) * time.Second
}

// url builds the queue URL from the request host so containers can use it too.
func (q *Queue) url(host string) string {
	if host == "" {
		host = "localhost:7644"
	}
	return "http://" + host + "/" + q.Account + "/" + q.Name
}

// queue resolves a QueueUrl (or bare name) for the caller; lock must be held.
func (s *Service) queue(c *core.Ctx, queueURL string) (*Queue, error) {
	if queueURL == "" {
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter QueueUrl.")
	}
	p := strings.Split(strings.TrimRight(queueURL, "/"), "/")
	name, account := p[len(p)-1], c.Account
	if len(p) >= 2 && len(p[len(p)-2]) == 12 {
		account = p[len(p)-2]
	}
	if q := s.st.Queues[key(account, c.Region, name)]; q != nil {
		return q, nil
	}
	for _, q := range s.st.Queues { // URL used against another region
		if q.Account == account && q.Name == name {
			return q, nil
		}
	}
	return nil, errNoQueue()
}

// byARN finds a queue from its ARN; lock must be held.
func (s *Service) byARN(arn string) *Queue {
	_, region, account, name, ok := core.ParseARN(arn)
	if !ok {
		return nil
	}
	return s.st.Queues[key(account, region, name)]
}

var queueName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)

var settable = map[string]bool{"DelaySeconds": true, "MaximumMessageSize": true, "MessageRetentionPeriod": true, "Policy": true,
	"ReceiveMessageWaitTimeSeconds": true, "VisibilityTimeout": true, "RedrivePolicy": true, "RedriveAllowPolicy": true,
	"KmsMasterKeyId": true, "KmsDataKeyReusePeriodSeconds": true, "SqsManagedSseEnabled": true, "FifoQueue": true,
	"ContentBasedDeduplication": true, "DeduplicationScope": true, "FifoThroughputLimit": true}

func validateAttrs(attrs map[string]string, create bool) error {
	for k, v := range attrs {
		if !settable[k] || k == "FifoQueue" && !create {
			return core.Errorf(400, "InvalidAttributeName", "Unknown Attribute %s.", k)
		}
		if k == "RedrivePolicy" && v != "" {
			var rp redrivePolicy
			if err := json.Unmarshal([]byte(v), &rp); err != nil || rp.Target == "" {
				return core.Errorf(400, "InvalidAttributeValue", "Invalid value for the parameter RedrivePolicy.")
			}
		}
	}
	return nil
}

type redrivePolicy struct {
	Target   string `json:"deadLetterTargetArn"`
	MaxCount any    `json:"maxReceiveCount"` // number or string in the wild
}

func (rp redrivePolicy) max() int {
	switch v := rp.MaxCount.(type) {
	case float64:
		return int(v)
	case string:
		n, _ := strconv.Atoi(v)
		return n
	}
	return 0
}

type createQueueInput struct {
	QueueName  string
	Attributes map[string]string `query:"Attribute,flat,key=Name,val=Value"`
	Tags       map[string]string `json:"tags" query:"Tag,flat,key=Key,val=Value"`
}

func (s *Service) createQueue(c *core.Ctx, in *createQueueInput) (any, error) {
	name := in.QueueName
	fifo := strings.HasSuffix(name, ".fifo")
	if !queueName.MatchString(strings.TrimSuffix(name, ".fifo")) {
		return nil, core.Errorf(400, "InvalidParameterValueException", "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length")
	}
	if err := validateAttrs(in.Attributes, true); err != nil {
		return nil, err
	}
	if fifo != (in.Attributes["FifoQueue"] == "true") {
		if !fifo || in.Attributes["FifoQueue"] == "false" {
			return nil, core.Errorf(400, "InvalidParameterValueException", "The name of a FIFO queue can only include alphanumeric characters, hyphens, or underscores, must end with .fifo suffix and be 1 to 80 in length.")
		}
	}
	attrs := map[string]string{}
	for k, v := range in.Attributes {
		attrs[k] = v
	}
	if fifo {
		attrs["FifoQueue"] = "true"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, name)
	if q := s.st.Queues[k]; q != nil {
		for a, v := range attrs {
			if q.attr(a) != v {
				return nil, core.Errorf(400, "QueueNameExists", "A queue already exists with the same name and a different value for attribute %s", a)
			}
		}
		return map[string]string{"QueueUrl": q.url(c.Host)}, nil
	}
	now := time.Now().UTC()
	q := &Queue{Name: name, Account: c.Account, Region: c.Region, Created: now, Modified: now, Attrs: attrs, Tags: in.Tags}
	s.st.Queues[k] = q
	s.journal(q)
	return map[string]string{"QueueUrl": q.url(c.Host)}, nil
}

type queueURLInput struct {
	QueueUrl string
}

func (s *Service) getQueueURL(c *core.Ctx, in *struct {
	QueueName              string
	QueueOwnerAWSAccountId string
}) (any, error) {
	acct := c.Account
	if in.QueueOwnerAWSAccountId != "" {
		acct = in.QueueOwnerAWSAccountId
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.st.Queues[key(acct, c.Region, in.QueueName)]
	if q == nil {
		return nil, errNoQueue()
	}
	return map[string]string{"QueueUrl": q.url(c.Host)}, nil
}

type listQueuesOutput struct {
	QueueUrls []string `query:"QueueUrl,flat"`
	NextToken string   `json:",omitempty"`
}

func (s *Service) listQueues(c *core.Ctx, in *struct {
	QueueNamePrefix string
	MaxResults      int
	NextToken       string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var names []string
	for _, q := range s.st.Queues {
		if q.Account == c.Account && q.Region == c.Region && strings.HasPrefix(q.Name, in.QueueNamePrefix) {
			names = append(names, q.Name)
		}
	}
	sort.Strings(names)
	out := &listQueuesOutput{QueueUrls: []string{}}
	for _, n := range names {
		if in.NextToken != "" && n <= in.NextToken {
			continue
		}
		if in.MaxResults > 0 && len(out.QueueUrls) == in.MaxResults {
			out.NextToken = prevName(out.QueueUrls)
			break
		}
		out.QueueUrls = append(out.QueueUrls, s.st.Queues[key(c.Account, c.Region, n)].url(c.Host))
	}
	return out, nil
}

func prevName(urls []string) string {
	u := urls[len(urls)-1]
	return u[strings.LastIndex(u, "/")+1:]
}

func (s *Service) deleteQueue(c *core.Ctx, in *queueURLInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	delete(s.st.Queues, key(q.Account, q.Region, q.Name))
	s.j.Delete("q/" + key(q.Account, q.Region, q.Name))
	q.signal()
	return nil, nil
}

type attrsOutput struct {
	Attributes map[string]string `query:"Attribute,flat,key=Name,val=Value"`
}

func (s *Service) getQueueAttributes(c *core.Ctx, in *struct {
	QueueUrl       string
	AttributeNames []string `query:"AttributeName,flat"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	all := q.attributes()
	want := in.AttributeNames
	out := map[string]string{}
	for _, n := range want {
		if n == "All" {
			out = all
			break
		}
		if v, ok := all[n]; ok {
			out[n] = v
		} else if !settable[n] && !strings.HasPrefix(n, "Approximate") && n != "QueueArn" && n != "CreatedTimestamp" && n != "LastModifiedTimestamp" {
			return nil, core.Errorf(400, "InvalidAttributeName", "Unknown Attribute %s.", n)
		}
	}
	if len(out) == 0 {
		return struct{}{}, nil
	}
	return &attrsOutput{out}, nil
}

// attributes returns configured, default and computed attributes; lock held.
func (q *Queue) attributes() map[string]string {
	now := time.Now()
	visible, inflight, delayed := 0, 0, 0
	for _, m := range q.Messages {
		switch {
		case m.VisibleAt.After(now) && m.Receives > 0:
			inflight++
		case m.VisibleAt.After(now):
			delayed++
		default:
			visible++
		}
	}
	out := map[string]string{}
	for k, v := range defaults {
		out[k] = v
	}
	for k, v := range q.Attrs {
		out[k] = v
	}
	if q.fifo() {
		if out["ContentBasedDeduplication"] == "" {
			out["ContentBasedDeduplication"] = "false"
		}
		if out["DeduplicationScope"] == "" {
			out["DeduplicationScope"] = "queue"
		}
		if out["FifoThroughputLimit"] == "" {
			out["FifoThroughputLimit"] = "perQueue"
		}
	}
	out["QueueArn"] = q.arn()
	out["CreatedTimestamp"] = strconv.FormatInt(q.Created.Unix(), 10)
	out["LastModifiedTimestamp"] = strconv.FormatInt(q.Modified.Unix(), 10)
	out["ApproximateNumberOfMessages"] = strconv.Itoa(visible)
	out["ApproximateNumberOfMessagesNotVisible"] = strconv.Itoa(inflight)
	out["ApproximateNumberOfMessagesDelayed"] = strconv.Itoa(delayed)
	return out
}

func (s *Service) setQueueAttributes(c *core.Ctx, in *struct {
	QueueUrl   string
	Attributes map[string]string `query:"Attribute,flat,key=Name,val=Value"`
}) (any, error) {
	if err := validateAttrs(in.Attributes, false); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	for k, v := range in.Attributes {
		if v == "" && (k == "RedrivePolicy" || k == "Policy" || k == "RedriveAllowPolicy") {
			delete(q.Attrs, k)
			continue
		}
		q.Attrs[k] = v
	}
	q.Modified = time.Now().UTC()
	s.journal(q)
	return nil, nil
}

func (s *Service) purgeQueue(c *core.Ctx, in *queueURLInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	s.purge(q)
	return nil, nil
}

// purge drops every message; lock held.
func (s *Service) purge(q *Queue) {
	for _, m := range q.Messages {
		s.j.Delete(q.mkey(m))
	}
	q.Messages = nil
}

// ---- sending ----

type msgAttr struct {
	DataType         string
	StringValue      string   `json:",omitempty"`
	BinaryValue      []byte   `json:",omitempty"`
	StringListValues []string `json:",omitempty" query:"StringListValue,flat"`
	BinaryListValues [][]byte `json:",omitempty" query:"BinaryListValue,flat"`
}

type sendInput struct {
	QueueUrl                string
	Id                      string // batch entries
	MessageBody             string
	DelaySeconds            *int
	MessageAttributes       map[string]msgAttr `query:"MessageAttribute,flat,key=Name,val=Value"`
	MessageSystemAttributes map[string]msgAttr `query:"MessageSystemAttribute,flat,key=Name,val=Value"`
	MessageDeduplicationId  string
	MessageGroupId          string
	trace                   string // the sender's flow
	sender                  string // the sender's user id
}

type sendOutput struct {
	Id                           string `json:",omitempty"`
	MessageId                    string
	MD5OfMessageBody             string
	MD5OfMessageAttributes       string `json:",omitempty"`
	MD5OfMessageSystemAttributes string `json:",omitempty"`
	SequenceNumber               string `json:",omitempty"`
}

func toAttrs(in map[string]msgAttr) map[string]core.MessageAttribute {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]core.MessageAttribute, len(in))
	for k, v := range in {
		out[k] = core.MessageAttribute{DataType: v.DataType, StringValue: v.StringValue, BinaryValue: v.BinaryValue}
	}
	return out
}

func validAttrs(attrs map[string]msgAttr) error {
	for k, v := range attrs {
		dt := strings.SplitN(v.DataType, ".", 2)[0]
		switch {
		case dt != "String" && dt != "Number" && dt != "Binary":
			return core.Errorf(400, "InvalidParameterValueException", "The type of message (user) attribute '%s' is invalid. You must use only the following supported type prefixes: Binary, Number, String.", k)
		case dt == "Binary" && len(v.BinaryValue) == 0, dt != "Binary" && v.StringValue == "":
			return core.Errorf(400, "InvalidParameterValueException", "Message (user) attribute '%s' must contain a non-empty value of type '%s'.", k, dt)
		}
	}
	return nil
}

func (s *Service) sendMessage(c *core.Ctx, in *sendInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	in.trace, in.sender = c.Trace, s.senderID(c)
	return s.send(q, in)
}

// senderID is the caller's user id (for an assumed role: <role id>:<session name>),
// or "" when the key is not a known identity.
func (s *Service) senderID(c *core.Ctx) string {
	if id, ok := s.hub.Identity(c.AccessKey); ok {
		return id.UserID
	}
	return ""
}

// send validates and enqueues one message; lock held.
func (s *Service) send(q *Queue, in *sendInput) (*sendOutput, error) {
	if in.MessageBody == "" {
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter MessageBody.")
	}
	if max, _ := strconv.Atoi(q.attr("MaximumMessageSize")); len(in.MessageBody) > max {
		return nil, core.Errorf(400, "InvalidParameterValueException", "One or more parameters are invalid. Reason: Message must be shorter than %d bytes.", max)
	}
	if err := validAttrs(in.MessageAttributes); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	m := &Message{ID: core.UUID(), Body: in.MessageBody, Attrs: toAttrs(in.MessageAttributes), SysAttrs: toAttrs(in.MessageSystemAttributes),
		Sent: now, VisibleAt: now, GroupID: in.MessageGroupId, Trace: in.trace, Sender: in.sender}
	if t := core.TraceRoot(in.MessageSystemAttributes["AWSTraceHeader"].StringValue); t != "" {
		m.Trace = t
	}
	delay := q.seconds("DelaySeconds")
	if in.DelaySeconds != nil {
		if q.fifo() {
			return nil, core.Errorf(400, "InvalidParameterValueException", "Value %d for parameter DelaySeconds is invalid. Reason: The request include parameter that is not valid for this queue type.", *in.DelaySeconds)
		}
		delay = time.Duration(*in.DelaySeconds) * time.Second
	}
	m.VisibleAt = now.Add(delay)
	out := &sendOutput{Id: in.Id, MessageId: m.ID, MD5OfMessageBody: md5Hex([]byte(m.Body)), MD5OfMessageAttributes: attrsMD5(m.Attrs)}
	if len(m.SysAttrs) > 0 {
		out.MD5OfMessageSystemAttributes = attrsMD5(m.SysAttrs)
	}
	if q.fifo() {
		if in.MessageGroupId == "" {
			return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter MessageGroupId.")
		}
		dedup := in.MessageDeduplicationId
		if dedup == "" {
			if q.attr("ContentBasedDeduplication") != "true" {
				return nil, core.Errorf(400, "InvalidParameterValueException", "The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly")
			}
			h := sha256Hex(m.Body)
			dedup = h
		}
		if q.Dedup == nil {
			q.Dedup, q.DedupAt = map[string]string{}, map[string]time.Time{}
		}
		if q.attr("DeduplicationScope") == "messageGroup" {
			dedup = in.MessageGroupId + "/" + dedup
		}
		if id, ok := q.Dedup[dedup]; ok && now.Sub(q.DedupAt[dedup]) < 5*time.Minute {
			out.MessageId = id // duplicate: accepted, not enqueued
			out.SequenceNumber = fmt.Sprintf("%020d", q.Seq)
			return out, nil
		}
		q.Dedup[dedup], q.DedupAt[dedup] = m.ID, now
		q.Seq++
		m.DedupID, m.Seq = in.MessageDeduplicationId, fmt.Sprintf("%020d", q.Seq)
		if m.DedupID == "" {
			m.DedupID = dedup
		}
		out.SequenceNumber = m.Seq
		s.j.Put("f/"+key(q.Account, q.Region, q.Name)+"/"+dedup, fifoSend{m.ID, now, q.Seq})
	}
	q.Messages = append(q.Messages, m)
	s.j.Put(q.mkey(m), m)
	q.signal()
	return out, nil
}

type batchError struct {
	Id          string
	Code        string
	Message     string
	SenderFault bool
}

type sendBatchOutput struct {
	Successful []*sendOutput `query:"SendMessageBatchResultEntry,flat"`
	Failed     []batchError  `query:"BatchResultErrorEntry,flat"`
}

// maxBatchBytes is the most a SendMessageBatch call may carry in all (SQS raised it from 256 KiB in 2025).
const maxBatchBytes = 1 << 20

func checkBatch(n int, ids []string) error {
	if n == 0 {
		return core.Errorf(400, "EmptyBatchRequest", "There should be at least one SendMessageBatchRequestEntry in the request.")
	}
	if n > 10 {
		return core.Errorf(400, "TooManyEntriesInBatchRequest", "Maximum number of entries per request are 10. You have sent %d.", n)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			return core.Errorf(400, "BatchEntryIdsNotDistinct", "Id %s repeated.", id)
		}
		seen[id] = true
	}
	return nil
}

func (s *Service) sendMessageBatch(c *core.Ctx, in *struct {
	QueueUrl string
	Entries  []sendInput `query:"SendMessageBatchRequestEntry,flat"`
}) (any, error) {
	ids := make([]string, len(in.Entries))
	for i, e := range in.Entries {
		ids[i] = e.Id
	}
	if err := checkBatch(len(in.Entries), ids); err != nil {
		return nil, err
	}
	total := 0
	for _, e := range in.Entries {
		total += len(e.MessageBody)
	}
	if total > maxBatchBytes { // the whole call is refused, not entry by entry
		return nil, core.Errorf(400, "BatchRequestTooLong", "Batch requests cannot be longer than %d bytes. You have sent %d bytes.", maxBatchBytes, total)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	out := &sendBatchOutput{Successful: []*sendOutput{}, Failed: []batchError{}}
	for _, e := range in.Entries {
		e := e
		e.trace, e.sender = c.Trace, s.senderID(c)
		res, err := s.send(q, &e)
		if err != nil {
			ae := core.AsError(err)
			out.Failed = append(out.Failed, batchError{e.Id, ae.Code, ae.Msg, true})
			continue
		}
		out.Successful = append(out.Successful, res)
	}
	return out, nil
}

// deliver accepts messages from other services (SNS, S3, EventBridge…).
func (s *Service) deliver(arn string, m *core.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.byARN(arn)
	if q == nil {
		return fmt.Errorf("queue %s does not exist", arn)
	}
	in := &sendInput{MessageBody: m.Body, MessageGroupId: m.GroupID, MessageDeduplicationId: m.DedupID, trace: m.Trace}
	if len(m.Attributes) > 0 {
		in.MessageAttributes = map[string]msgAttr{}
		for k, v := range m.Attributes {
			in.MessageAttributes[k] = msgAttr{DataType: v.DataType, StringValue: v.StringValue, BinaryValue: v.BinaryValue}
		}
	}
	if q.fifo() && in.MessageGroupId == "" {
		in.MessageGroupId = "default"
	}
	if q.fifo() && in.MessageDeduplicationId == "" {
		in.MessageDeduplicationId = core.NewID()
	}
	_, err := s.send(q, in)
	return err
}

// ---- receiving ----

type receiveInput struct {
	QueueUrl                    string
	MaxNumberOfMessages         int
	VisibilityTimeout           *int
	WaitTimeSeconds             *int
	AttributeNames              []string `query:"AttributeName,flat"`
	MessageSystemAttributeNames []string `query:"MessageSystemAttributeName,flat"`
	MessageAttributeNames       []string `query:"MessageAttributeName,flat"`
	ReceiveRequestAttemptId     string
}

type outMessage struct {
	MessageId              string
	ReceiptHandle          string
	MD5OfBody              string
	Body                   string
	Attributes             map[string]string  `json:",omitempty" query:"Attribute,flat,key=Name,val=Value"`
	MD5OfMessageAttributes string             `json:",omitempty"`
	MessageAttributes      map[string]msgAttr `json:",omitempty" query:"MessageAttribute,flat,key=Name,val=Value"`
}

type receiveOutput struct {
	Messages []outMessage `json:",omitempty" query:"Message,flat"`
}

func (s *Service) receiveMessage(c *core.Ctx, in *receiveInput) (any, error) {
	max := in.MaxNumberOfMessages
	if max == 0 {
		max = 1
	}
	if max < 1 || max > 10 {
		return nil, core.Errorf(400, "InvalidParameterValueException", "Value %d for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, if provided.", max)
	}
	s.mu.Lock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	wait := q.seconds("ReceiveMessageWaitTimeSeconds")
	if in.WaitTimeSeconds != nil {
		wait = time.Duration(*in.WaitTimeSeconds) * time.Second
	}
	deadline := time.Now().Add(wait)
	for {
		msgs := s.receive(q, max, in)
		if len(msgs) > 0 || !time.Now().Before(deadline) {
			s.mu.Unlock()
			return &receiveOutput{Messages: msgs}, nil
		}
		wake := q.wakeChan()
		s.mu.Unlock()
		// ponytail: 200ms re-check covers delays and visibility expiry without timers.
		select {
		case <-wake:
		case <-time.After(min(time.Until(deadline), 200*time.Millisecond)):
		}
		s.mu.Lock()
		if s.st.Queues[key(q.Account, q.Region, q.Name)] != q {
			s.mu.Unlock()
			return nil, errNoQueue()
		}
	}
}

// receive takes up to max visible messages, honouring FIFO groups and redrive; lock held.
func (s *Service) receive(q *Queue, max int, in *receiveInput) []outMessage {
	now := time.Now().UTC()
	vis := q.seconds("VisibilityTimeout")
	if in.VisibilityTimeout != nil {
		vis = time.Duration(*in.VisibilityTimeout) * time.Second
	}
	retention := q.seconds("MessageRetentionPeriod")
	var rp redrivePolicy
	if v := q.Attrs["RedrivePolicy"]; v != "" {
		json.Unmarshal([]byte(v), &rp)
	}
	busy := map[string]bool{} // FIFO groups with a message in flight
	if q.fifo() {
		for _, m := range q.Messages {
			if m.Receives > 0 && m.VisibleAt.After(now) {
				busy[m.GroupID] = true
			}
		}
	}
	var out []outMessage
	kept := q.Messages[:0]
	for _, m := range q.Messages {
		if now.Sub(m.Sent) > retention {
			s.j.Delete(q.mkey(m))
			continue // expired
		}
		if len(out) == max || m.VisibleAt.After(now) || busy[m.GroupID] && q.fifo() {
			kept = append(kept, m)
			continue
		}
		if rp.Target != "" && rp.max() > 0 && m.Receives >= rp.max() {
			if dlq := s.byARN(rp.Target); dlq != nil {
				m.Source, m.Receives, m.VisibleAt = q.arn(), 0, now
				s.hub.Span(core.SpanRedrive, "sqs", "redrive", q.arn(), dlq.arn(), m.Trace, now, nil)
				dlq.Messages = append(dlq.Messages, m)
				s.j.Delete(q.mkey(m))
				s.j.Put(dlq.mkey(m), m)
				dlq.signal()
				continue
			}
		}
		m.Receives++
		if m.FirstReceive.IsZero() {
			m.FirstReceive = now
		}
		m.Receipt = core.RandString(64, core.AlphaNum) + "=="
		m.VisibleAt = now.Add(vis)
		s.j.Put(q.mkey(m), m)
		out = append(out, render(m, in.AttributeNames, in.MessageSystemAttributeNames, in.MessageAttributeNames))
		kept = append(kept, m)
	}
	clear(q.Messages[len(kept):])
	q.Messages = kept
	return out
}

func render(m *Message, attrNames, sysNames, msgAttrNames []string) outMessage {
	o := outMessage{MessageId: m.ID, ReceiptHandle: m.Receipt, MD5OfBody: md5Hex([]byte(m.Body)), Body: m.Body}
	names := append(append([]string{}, attrNames...), sysNames...)
	if len(names) > 0 {
		sender := m.Sender
		if sender == "" {
			sender = "AIDAIT2UOQQY3AUEKVGXU"
		}
		all := map[string]string{
			"SenderId": sender, "SentTimestamp": strconv.FormatInt(m.Sent.UnixMilli(), 10),
			"ApproximateReceiveCount": strconv.Itoa(m.Receives), "ApproximateFirstReceiveTimestamp": strconv.FormatInt(m.FirstReceive.UnixMilli(), 10),
		}
		if m.GroupID != "" {
			all["MessageGroupId"], all["MessageDeduplicationId"], all["SequenceNumber"] = m.GroupID, m.DedupID, m.Seq
		}
		if m.Source != "" {
			all["DeadLetterQueueSourceArn"] = m.Source
		}
		if t, ok := m.SysAttrs["AWSTraceHeader"]; ok {
			all["AWSTraceHeader"] = t.StringValue
		} else if m.Trace != "" {
			all["AWSTraceHeader"] = "Root=" + m.Trace
		}
		o.Attributes = map[string]string{}
		for _, n := range names {
			if n == "All" {
				o.Attributes = all
				break
			}
			if v, ok := all[n]; ok {
				o.Attributes[n] = v
			}
		}
	}
	sel := map[string]core.MessageAttribute{}
	for k, v := range m.Attrs {
		for _, n := range msgAttrNames {
			if n == "All" || n == ".*" || n == k || strings.HasSuffix(n, ".*") && strings.HasPrefix(k, strings.TrimSuffix(n, "*")) {
				sel[k] = v
			}
		}
	}
	if len(sel) > 0 {
		o.MessageAttributes = map[string]msgAttr{}
		for k, v := range sel {
			o.MessageAttributes[k] = msgAttr{DataType: v.DataType, StringValue: v.StringValue, BinaryValue: v.BinaryValue}
		}
		o.MD5OfMessageAttributes = attrsMD5(sel)
	}
	return o
}

func (s *Service) deleteMessage(c *core.Ctx, in *struct {
	QueueUrl      string
	ReceiptHandle string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	return nil, q.deleteReceipt(s.j, in.ReceiptHandle)
}

// receiptShape matches the handles pogi issues: 64 letters or digits, then "==".
var receiptShape = regexp.MustCompile(`^[A-Za-z0-9]{64}==$`)

func (q *Queue) deleteReceipt(j *core.Journal, receipt string) error {
	if !receiptShape.MatchString(receipt) {
		return core.Errorf(400, "ReceiptHandleIsInvalid", "The input receipt handle \"%s\" is not a valid receipt handle.", receipt)
	}
	for i, m := range q.Messages {
		if m.Receipt == receipt {
			q.Messages = append(q.Messages[:i], q.Messages[i+1:]...)
			j.Delete(q.mkey(m))
			return nil
		}
	}
	return nil // stale handles succeed silently, as in SQS
}

type batchIDOutput struct {
	Successful []struct{ Id string } `query:"DeleteMessageBatchResultEntry,flat"`
	Failed     []batchError          `query:"BatchResultErrorEntry,flat"`
}

func (s *Service) deleteMessageBatch(c *core.Ctx, in *struct {
	QueueUrl string
	Entries  []struct {
		Id            string
		ReceiptHandle string
	} `query:"DeleteMessageBatchRequestEntry,flat"`
}) (any, error) {
	ids := make([]string, len(in.Entries))
	for i, e := range in.Entries {
		ids[i] = e.Id
	}
	if err := checkBatch(len(in.Entries), ids); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	out := &batchIDOutput{Successful: []struct{ Id string }{}, Failed: []batchError{}}
	for _, e := range in.Entries {
		if err := q.deleteReceipt(s.j, e.ReceiptHandle); err != nil {
			ae := core.AsError(err)
			out.Failed = append(out.Failed, batchError{e.Id, ae.Code, ae.Msg, true})
			continue
		}
		out.Successful = append(out.Successful, struct{ Id string }{e.Id})
	}
	return out, nil
}

func (q *Queue) changeVisibility(j *core.Journal, receipt string, secs int) error {
	for _, m := range q.Messages {
		if m.Receipt == receipt && receipt != "" {
			if !m.VisibleAt.After(time.Now()) {
				return core.Errorf(400, "MessageNotInflight", "The message referred to isn't in flight.")
			}
			m.VisibleAt = time.Now().Add(time.Duration(secs) * time.Second)
			j.Put(q.mkey(m), m)
			if secs == 0 {
				q.signal()
			}
			return nil
		}
	}
	return core.Errorf(400, "ReceiptHandleIsInvalid", "The input receipt handle \"%s\" is not a valid receipt handle.", receipt)
}

func (s *Service) changeVisibility(c *core.Ctx, in *struct {
	QueueUrl          string
	ReceiptHandle     string
	VisibilityTimeout int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	return nil, q.changeVisibility(s.j, in.ReceiptHandle, in.VisibilityTimeout)
}

func (s *Service) changeVisibilityBatch(c *core.Ctx, in *struct {
	QueueUrl string
	Entries  []struct {
		Id                string
		ReceiptHandle     string
		VisibilityTimeout int
	} `query:"ChangeMessageVisibilityBatchRequestEntry,flat"`
}) (any, error) {
	ids := make([]string, len(in.Entries))
	for i, e := range in.Entries {
		ids[i] = e.Id
	}
	if err := checkBatch(len(in.Entries), ids); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	out := struct {
		Successful []struct{ Id string } `query:"ChangeMessageVisibilityBatchResultEntry,flat"`
		Failed     []batchError          `query:"BatchResultErrorEntry,flat"`
	}{Successful: []struct{ Id string }{}, Failed: []batchError{}}
	for _, e := range in.Entries {
		if err := q.changeVisibility(s.j, e.ReceiptHandle, e.VisibilityTimeout); err != nil {
			ae := core.AsError(err)
			out.Failed = append(out.Failed, batchError{e.Id, ae.Code, ae.Msg, true})
			continue
		}
		out.Successful = append(out.Successful, struct{ Id string }{e.Id})
	}
	return &out, nil
}

// ---- tags, DLQ sources, move tasks, permissions ----

func (s *Service) tagQueue(c *core.Ctx, in *struct {
	QueueUrl string
	Tags     map[string]string `query:"Tag,flat,key=Key,val=Value"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	if q.Tags == nil {
		q.Tags = map[string]string{}
	}
	for k, v := range in.Tags {
		q.Tags[k] = v
	}
	s.journal(q)
	return nil, nil
}

func (s *Service) untagQueue(c *core.Ctx, in *struct {
	QueueUrl string
	TagKeys  []string `query:"TagKey,flat"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	for _, k := range in.TagKeys {
		delete(q.Tags, k)
	}
	s.journal(q)
	return nil, nil
}

func (s *Service) listQueueTags(c *core.Ctx, in *queueURLInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	return &struct {
		Tags map[string]string `json:",omitempty" query:"Tag,flat,key=Key,val=Value"`
	}{maps.Clone(q.Tags)}, nil
}

func (s *Service) listDLQSources(c *core.Ctx, in *queueURLInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dlq, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	out := &struct {
		QueueUrls []string `json:"queueUrls" query:"QueueUrl,flat"`
	}{QueueUrls: []string{}}
	for _, q := range s.st.Queues {
		var rp redrivePolicy
		if json.Unmarshal([]byte(q.Attrs["RedrivePolicy"]), &rp) == nil && rp.Target == dlq.arn() {
			out.QueueUrls = append(out.QueueUrls, q.url(c.Host))
		}
	}
	sort.Strings(out.QueueUrls)
	return out, nil
}

// startMoveTask redrives a DLQ synchronously (to the original source queue by default).
func (s *Service) startMoveTask(c *core.Ctx, in *struct {
	SourceArn      string
	DestinationArn string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.byARN(in.SourceArn)
	if src == nil {
		return nil, core.Errorf(400, "ResourceNotFoundException", "The resource that you specified for the SourceArn parameter doesn't exist.")
	}
	moved := 0
	var kept []*Message
	for _, m := range src.Messages {
		dest := in.DestinationArn
		if dest == "" {
			dest = m.Source
		}
		if dq := s.byARN(dest); dq != nil {
			m.Receives, m.VisibleAt, m.Receipt = 0, time.Now(), ""
			dq.Messages = append(dq.Messages, m)
			s.j.Delete(src.mkey(m))
			s.j.Put(dq.mkey(m), m)
			dq.signal()
			moved++
			continue
		}
		kept = append(kept, m)
	}
	src.Messages = kept
	return map[string]string{"TaskHandle": core.NewID()}, nil
}

func (s *Service) listMoveTasks(c *core.Ctx, in *struct{ SourceArn string }) (any, error) {
	return map[string]any{"Results": []any{}}, nil
}

func (s *Service) cancelMoveTask(c *core.Ctx, in *struct{ TaskHandle string }) (any, error) {
	return nil, core.Errorf(400, "ResourceNotFoundException", "Task does not exist or has already completed.")
}

// policy returns the queue policy and its statements; an unset policy is empty.
func (q *Queue) policy() (map[string]any, []any) {
	var p map[string]any
	if json.Unmarshal([]byte(q.Attrs["Policy"]), &p) != nil || p == nil {
		p = map[string]any{"Version": "2012-10-17", "Id": q.arn() + "/SQSDefaultPolicy"}
	}
	stmts, _ := p["Statement"].([]any)
	return p, stmts
}

func hasLabel(stmts []any, label string) bool {
	return slices.ContainsFunc(stmts, func(st any) bool { m, _ := st.(map[string]any); return m["Sid"] == label })
}

func (q *Queue) setPolicy(p map[string]any, stmts []any) {
	if len(stmts) == 0 {
		delete(q.Attrs, "Policy")
		return
	}
	p["Statement"] = stmts
	b, _ := json.Marshal(p)
	q.Attrs["Policy"] = string(b)
}

// addPermission adds one statement to the queue policy, named by its label.
func (s *Service) addPermission(c *core.Ctx, in *struct {
	QueueUrl      string
	Label         string
	AWSAccountIds []string `query:"AWSAccountId,flat"`
	Actions       []string `query:"ActionName,flat"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	if in.Label == "" || len(in.AWSAccountIds) == 0 || len(in.Actions) == 0 {
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameters Label, AWSAccountId and ActionName.")
	}
	p, stmts := q.policy()
	if hasLabel(stmts, in.Label) {
		return nil, core.Errorf(400, "InvalidParameterValueException", "Value %s for parameter Label is invalid. Reason: Already exists.", in.Label)
	}
	var who, what []string
	for _, a := range in.AWSAccountIds {
		who = append(who, core.GlobalARN("iam", q.Region, a, "root"))
	}
	for _, a := range in.Actions {
		what = append(what, "SQS:"+a)
	}
	q.setPolicy(p, append(stmts, map[string]any{"Sid": in.Label, "Effect": "Allow", "Principal": map[string]any{"AWS": who}, "Action": what, "Resource": q.arn()}))
	q.Modified = time.Now().UTC()
	s.journal(q)
	return nil, nil
}

func (s *Service) removePermission(c *core.Ctx, in *struct{ QueueUrl, Label string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queue(c, in.QueueUrl)
	if err != nil {
		return nil, err
	}
	p, stmts := q.policy()
	if !hasLabel(stmts, in.Label) {
		return nil, core.Errorf(400, "InvalidParameterValueException", "Value %s for parameter Label is invalid. Reason: can't find label.", in.Label)
	}
	q.setPolicy(p, slices.DeleteFunc(stmts, func(st any) bool { m, _ := st.(map[string]any); return m["Sid"] == in.Label }))
	q.Modified = time.Now().UTC()
	s.journal(q)
	return nil, nil
}

// ---- helpers ----

func (q *Queue) wakeChan() chan struct{} {
	if q.wake == nil {
		q.wake = make(chan struct{})
	}
	return q.wake
}

// signal wakes long-polling receivers; lock held.
func (q *Queue) signal() {
	if q.wake != nil {
		close(q.wake)
		q.wake = nil
	}
}

func md5Hex(b []byte) string {
	h := md5.Sum(b)
	return hex.EncodeToString(h[:])
}

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// attrsMD5 is SQS's MD5 over message attributes (sorted, length-prefixed).
func attrsMD5(attrs map[string]core.MessageAttribute) string {
	if len(attrs) == 0 {
		return ""
	}
	var buf []byte
	put := func(b []byte) {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(b)))
		buf = append(buf, b...)
	}
	for _, k := range core.SortedKeys(attrs) {
		a := attrs[k]
		put([]byte(k))
		put([]byte(a.DataType))
		if strings.HasPrefix(a.DataType, "Binary") {
			buf = append(buf, 2)
			put(a.BinaryValue)
		} else {
			buf = append(buf, 1)
			put([]byte(a.StringValue))
		}
	}
	return md5Hex(buf)
}

// ---- console + LocalStack peek endpoint ----

// TaggedResources lists queues (ARN → tags) for the tagging API and Resource Explorer.
func (s *Service) TaggedResources(account, region string) map[string]map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]map[string]string{}
	for _, q := range s.st.Queues {
		if q.Account == account && q.Region == region {
			out[core.ARN("sqs", region, account, q.Name)] = maps.Clone(q.Tags)
		}
	}
	return out
}

// Resources lists queues for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Queues", Columns: []string{"Queue", "Region", "Account", "Visible", "In flight", "Delayed", "Type"}}
	for _, k := range core.SortedKeys(s.st.Queues) {
		q := s.st.Queues[k]
		a := q.attributes()
		t.Rows = append(t.Rows, []string{q.Name, q.Region, q.Account, a["ApproximateNumberOfMessages"],
			a["ApproximateNumberOfMessagesNotVisible"], a["ApproximateNumberOfMessagesDelayed"], map[bool]string{true: "FIFO", false: "Standard"}[q.fifo()]})
	}
	return []core.Table{t}
}

// Inspect returns a queue's attributes and tags.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range s.st.Queues {
		if q.Name == id {
			return map[string]any{"name": q.Name, "account": q.Account, "region": q.Region, "attributes": q.attributes(), "tags": q.Tags}
		}
	}
	return nil
}

// PeekedMessage is a message as seen without receiving it.
type PeekedMessage struct {
	MessageId  string                           `json:"MessageId"`
	Body       string                           `json:"Body"`
	Attributes map[string]core.MessageAttribute `json:"MessageAttributes,omitempty"`
	Sent       time.Time                        `json:"SentTimestamp"`
	Receives   int                              `json:"ApproximateReceiveCount"`
	State      string                           `json:"State"` // visible | inflight | delayed
	GroupID    string                           `json:"MessageGroupId,omitempty"`
}

// Peek returns messages without changing them.
func (s *Service) Peek(account, region, name string) ([]PeekedMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.st.Queues[key(account, region, name)]
	if q == nil {
		return nil, errNoQueue()
	}
	now := time.Now()
	out := []PeekedMessage{}
	for _, m := range q.Messages {
		st := "visible"
		if m.VisibleAt.After(now) {
			st = map[bool]string{true: "inflight", false: "delayed"}[m.Receives > 0]
		}
		out = append(out, PeekedMessage{m.ID, m.Body, m.Attrs, m.Sent, m.Receives, st, m.GroupID})
	}
	return out, nil
}

// Create makes a queue from the console (".fifo" names become FIFO queues).
func (s *Service) Create(account, region, name string) error {
	_, err := s.createQueue(&core.Ctx{Account: account, Region: region}, &createQueueInput{QueueName: name})
	return err
}

// Send enqueues a message from the console.
func (s *Service) Send(account, region, name, body, groupID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.st.Queues[key(account, region, name)]
	if q == nil {
		return errNoQueue()
	}
	in := &sendInput{MessageBody: body, MessageGroupId: groupID}
	if q.fifo() {
		if in.MessageGroupId == "" {
			in.MessageGroupId = "console"
		}
		in.MessageDeduplicationId = core.NewID()
	}
	_, err := s.send(q, in)
	return err
}

// Purge empties a queue from the console.
func (s *Service) Purge(account, region, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.st.Queues[key(account, region, name)]
	if q == nil {
		return errNoQueue()
	}
	s.purge(q)
	return nil
}

// QueueRef identifies a queue for the console.
type QueueRef struct{ Account, Region, Name string }

// Queues lists all queues.
func (s *Service) Queues() []QueueRef {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []QueueRef
	for _, k := range core.SortedKeys(s.st.Queues) {
		q := s.st.Queues[k]
		out = append(out, QueueRef{q.Account, q.Region, q.Name})
	}
	return out
}

// peekHandler serves LocalStack's /_aws/sqs/messages?QueueUrl=… (JSON).
func (s *Service) peekHandler(w http.ResponseWriter, r *http.Request) {
	account, region, name := core.DefaultAccount, core.DefaultRegion, ""
	if qu := r.URL.Query().Get("QueueUrl"); qu != "" {
		u, err := url.Parse(qu)
		if err == nil {
			p := strings.Split(strings.Trim(u.Path, "/"), "/")
			name = p[len(p)-1]
			if len(p) >= 2 {
				account = p[len(p)-2]
			}
		}
		if rg := r.URL.Query().Get("Region"); rg != "" {
			region = rg
		}
	} else if p := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/_aws/sqs/messages"), "/"), "/"); len(p) == 3 {
		region, account, name = p[0], p[1], p[2]
	}
	if region == core.DefaultRegion { // find the queue in any region when not given
		s.mu.Lock()
		for _, q := range s.st.Queues {
			if q.Name == name && q.Account == account {
				region = q.Region
			}
		}
		s.mu.Unlock()
	}
	msgs, err := s.Peek(account, region, name)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"messages": msgs})
}
