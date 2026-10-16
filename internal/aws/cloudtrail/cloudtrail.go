// Package cloudtrail emulates AWS CloudTrail (JSON 1.1): trails with event
// and insight selectors, event data stores as metadata, LookupEvents built
// from pogi's request log, and logging trails that deliver gzipped log files
// to S3 and CloudWatch Logs.
package cloudtrail

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// maxEvents is how many events CloudTrail keeps, beyond the request log's 1000.
const maxEvents = 10000

// Service is the CloudTrail emulator.
type Service struct {
	hub  *core.Hub
	rpc  *core.RPC
	stop func()
	quit context.CancelFunc

	read sync.Mutex // one reader of the request log at a time
	last int64      // the request log ID read up to; under read

	mu     sync.Mutex
	st     state
	events []*event // oldest first; ponytail: in memory like the request log, so a restart forgets them
}

type state struct {
	Trails map[string]*trail `json:"trails"`          // account/region/name
	Stores map[string]*Store `json:"eventDataStores"` // ARN
	Tags   map[string][]Tag  `json:"tags,omitempty"`  // ARN → tags
}

func newState() state {
	return state{Trails: map[string]*trail{}, Stores: map[string]*Store{}, Tags: map[string][]Tag{}}
}

// Trail is a trail in the API's shape.
type Trail struct {
	Name                       string
	S3BucketName               string
	S3KeyPrefix                string `json:",omitempty"`
	SnsTopicName               string `json:",omitempty"`
	SnsTopicARN                string `json:",omitempty"`
	IncludeGlobalServiceEvents bool
	IsMultiRegionTrail         bool
	HomeRegion                 string
	TrailARN                   string
	LogFileValidationEnabled   bool
	CloudWatchLogsLogGroupArn  string `json:",omitempty"`
	CloudWatchLogsRoleArn      string `json:",omitempty"`
	KmsKeyId                   string `json:",omitempty"`
	HasCustomEventSelectors    bool
	HasInsightSelectors        bool
	IsOrganizationTrail        bool
}

// trail is a stored trail: its API shape, selectors and delivery status.
type trail struct {
	Trail
	Account   string            `json:"account"`
	Selectors []EventSelector   `json:"selectors,omitempty"`
	Advanced  []json.RawMessage `json:"advanced,omitempty"` // advanced event selectors, kept as given
	Insights  []json.RawMessage `json:"insights,omitempty"`
	Logging   bool              `json:"logging"`
	Started   *core.Time        `json:"started,omitempty"`
	Stopped   *core.Time        `json:"stopped,omitempty"`
	Delivered *core.Time        `json:"delivered,omitempty"` // last S3 delivery
	Attempted *core.Time        `json:"attempted,omitempty"`
	Failure   string            `json:"failure,omitempty"`
	ToLogs    *core.Time        `json:"toLogs,omitempty"` // last CloudWatch Logs delivery
	LogsError string            `json:"logsError,omitempty"`
	cursor    int64             // the last event delivered
}

// EventSelector picks the events a trail logs.
type EventSelector struct {
	ReadWriteType                 string
	IncludeManagementEvents       *bool
	DataResources                 []DataResource
	ExcludeManagementEventSources []string
}

// DataResource names data events of an event selector (kept, not logged).
type DataResource struct {
	Type   string
	Values []string
}

// Store is an event data store (CloudTrail Lake), as metadata.
type Store struct {
	EventDataStoreArn            string
	Name                         string
	Status                       string
	AdvancedEventSelectors       json.RawMessage
	MultiRegionEnabled           bool
	OrganizationEnabled          bool
	RetentionPeriod              int
	TerminationProtectionEnabled bool
	BillingMode                  string
	KmsKeyId                     string `json:",omitempty"`
	CreatedTimestamp             core.Time
	UpdatedTimestamp             core.Time
}

// Tag is a resource tag.
type Tag struct {
	Key   string
	Value string `json:",omitempty"`
}

// Event is one LookupEvents result.
type Event struct {
	EventId         string
	EventName       string
	ReadOnly        string
	AccessKeyId     string `json:",omitempty"`
	EventTime       core.Time
	EventSource     string
	Username        string     `json:",omitempty"`
	Resources       []Resource `json:",omitempty"`
	CloudTrailEvent string
}

// Resource is a resource an event refers to.
type Resource struct{ ResourceType, ResourceName string }

// event is a stored event: what LookupEvents shows and the log record.
type event struct {
	id              int64 // its request log ID
	account, region string
	out             Event
	record          json.RawMessage
}

// record is a CloudTrail log record, as in CloudTrailEvent and log files.
type record struct {
	EventVersion       string          `json:"eventVersion"`
	UserIdentity       identity        `json:"userIdentity"`
	EventTime          string          `json:"eventTime"`
	EventSource        string          `json:"eventSource"`
	EventName          string          `json:"eventName"`
	AWSRegion          string          `json:"awsRegion"`
	SourceIPAddress    string          `json:"sourceIPAddress"`
	UserAgent          string          `json:"userAgent"`
	ErrorCode          string          `json:"errorCode,omitempty"`
	ErrorMessage       string          `json:"errorMessage,omitempty"`
	RequestParameters  json.RawMessage `json:"requestParameters"`
	ResponseElements   json.RawMessage `json:"responseElements"`
	RequestID          string          `json:"requestID"`
	EventID            string          `json:"eventID"`
	ReadOnly           bool            `json:"readOnly"`
	EventType          string          `json:"eventType"`
	ManagementEvent    bool            `json:"managementEvent"`
	RecipientAccountID string          `json:"recipientAccountId"`
	EventCategory      string          `json:"eventCategory"`
}

type identity struct {
	Type        string `json:"type"`
	PrincipalID string `json:"principalId"`
	ARN         string `json:"arn"`
	AccountID   string `json:"accountId"`
	AccessKeyID string `json:"accessKeyId,omitempty"`
	UserName    string `json:"userName,omitempty"`
}

// New creates the CloudTrail service: it follows the request log and
// delivers the logs of logging trails every minute.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: newState()}
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateTrail":          core.H(s.createTrail),
		"UpdateTrail":          core.H(s.updateTrail),
		"DeleteTrail":          core.H(s.deleteTrail),
		"GetTrail":             core.H(s.getTrail),
		"DescribeTrails":       core.H(s.describeTrails),
		"ListTrails":           core.H(s.listTrails),
		"StartLogging":         core.H(s.startLogging),
		"StopLogging":          core.H(s.stopLogging),
		"GetTrailStatus":       core.H(s.getTrailStatus),
		"PutEventSelectors":    core.H(s.putEventSelectors),
		"GetEventSelectors":    core.H(s.getEventSelectors),
		"PutInsightSelectors":  core.H(s.putInsightSelectors),
		"GetInsightSelectors":  core.H(s.getInsightSelectors),
		"AddTags":              core.H(s.addTags),
		"RemoveTags":           core.H(s.removeTags),
		"ListTags":             core.H(s.listTags),
		"LookupEvents":         core.H(s.lookupEvents),
		"CreateEventDataStore": core.H(s.createStore),
		"GetEventDataStore":    core.H(s.getStore),
		"ListEventDataStores":  core.H(s.listStores),
		"UpdateEventDataStore": core.H(s.updateStore),
		"DeleteEventDataStore": core.H(s.deleteStore),
	}}
	ctx, quit := context.WithCancel(context.Background())
	s.quit = quit
	core.Go("cloudtrail.New", func() {
		for ctx.Err() == nil {
			s.hub.Requests.Wait(ctx, s.follow(), time.Minute)
		}
	})
	s.stop = core.Every(time.Minute, s.Tick)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "cloudtrail", Cloud: core.AWS, Title: "CloudTrail", Targets: []string{"CloudTrail_20131101"},
		Signing: []string{"cloudtrail"}, Protocol: "json"}
}

// ServeHTTP accepts both targets SDKs send: CloudTrail_20131101.Op and
// com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.Op (routed here by its signing name).
// core.TargetOp reads the operation from either.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

// Close stops following the request log and delivering.
func (s *Service) Close() error {
	s.quit()
	s.stop()
	return nil
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st, s.events = newState(), nil
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
	defer s.mu.Unlock()
	for _, t := range st.Trails {
		t.cursor = s.newest()
	}
	s.st = st
	return nil
}

func notFound(c *core.Ctx, ref string) error {
	return core.Errorf(400, "TrailNotFoundException", "Unknown trail: %s for the user: %s", ref, c.Account)
}

func invalid(code, format string, a ...any) error { return core.Errorf(400, code, format, a...) }

// ---- following the request log ----

// follow turns the calls logged since the last read into events and returns
// the last request log ID it read.
func (s *Service) follow() int64 {
	s.read.Lock()
	defer s.read.Unlock()
	for _, sum := range s.hub.Requests.Query(s.last, 0, "", "", false) {
		s.last = sum.ID
		if sum.Cloud != core.AWS || sum.Op == "" || strings.HasPrefix(sum.Path, "/_") {
			continue // GCP, Azure, unknown operations and pogi's own endpoints
		}
		if e, ok := s.hub.Requests.Get(sum.ID); ok {
			ev := s.event(e)
			s.mu.Lock()
			if s.events = append(s.events, ev); len(s.events) > maxEvents+maxEvents/10 {
				s.events = slices.Clone(s.events[len(s.events)-maxEvents:])
			}
			s.mu.Unlock()
		}
	}
	return s.last
}

// newest is the ID of the newest event; lock held.
func (s *Service) newest() int64 {
	if len(s.events) == 0 {
		return 0
	}
	return s.events[len(s.events)-1].id
}

// recent is the newest maxEvents events; lock held.
func (s *Service) recent() []*event { return s.events[max(0, len(s.events)-maxEvents):] }

func readOnly(op string) bool {
	for _, p := range []string{"Get", "List", "Describe", "Head"} {
		if strings.HasPrefix(op, p) {
			return true
		}
	}
	return false
}

var xmlMessage = regexp.MustCompile(`<Message>([^<]*)</Message>`)

// event converts a logged call into a CloudTrail event.
func (s *Service) event(e core.Entry) *event {
	source, title := e.Service, e.Service
	if svc := s.hub.Service(e.Service); svc != nil {
		info := svc.Info()
		title = strings.ReplaceAll(info.Title, " ", "")
		if len(info.Signing) > 0 {
			source = info.Signing[0]
		}
	}
	source += ".amazonaws.com"
	ro := readOnly(e.Op)
	who, user := s.identity(e)
	r := record{EventVersion: "1.09", UserIdentity: who, EventTime: e.Time.UTC().Format(time.RFC3339), EventSource: source,
		EventName: e.Op, AWSRegion: e.Region, SourceIPAddress: "127.0.0.1", // ponytail: the request log keeps no client address
		UserAgent: e.ReqHeaders["User-Agent"], ErrorCode: e.Error, RequestID: e.RequestID, EventID: core.UUID(),
		ReadOnly: ro, EventType: "AwsApiCall", ManagementEvent: true, RecipientAccountID: e.Account, EventCategory: "Management"}
	var params map[string]any
	if bucket, key, _ := strings.Cut(strings.TrimPrefix(strings.SplitN(e.Path, "?", 2)[0], "/"), "/"); e.Service == "s3" && bucket != "" {
		params = map[string]any{"bucketName": bucket} // the body is object data, not parameters
		if key, _ = url.PathUnescape(key); key != "" {
			params["key"] = key
		}
		r.RequestParameters, _ = json.Marshal(params)
	} else if body := []byte(e.ReqBody); json.Unmarshal(body, &params) == nil && params != nil {
		r.RequestParameters = body
	}
	if body := []byte(e.RespBody); e.Error != "" {
		var m struct{ Message string } // message or Message
		json.Unmarshal(body, &m)
		r.ErrorMessage = m.Message
		if x := xmlMessage.FindStringSubmatch(e.RespBody); r.ErrorMessage == "" && x != nil {
			r.ErrorMessage = x[1]
		}
	} else if !ro && len(body) > 0 && body[0] == '{' && json.Valid(body) {
		r.ResponseElements = body
	}
	rec, _ := json.Marshal(r)
	return &event{id: e.ID, account: e.Account, region: e.Region, record: rec, out: Event{EventId: r.EventID, EventName: e.Op,
		ReadOnly: strconv.FormatBool(ro), AccessKeyId: e.AccessKey, EventTime: core.Time(e.Time), EventSource: source, Username: user,
		Resources: resources(e, title, params), CloudTrailEvent: string(rec)}}
}

// identity is who made a call, from its principal ARN, and the user name LookupEvents shows.
func (s *Service) identity(e core.Entry) (identity, string) {
	id := identity{ARN: e.Principal, AccountID: e.Account, AccessKeyID: e.AccessKey}
	_, _, account, res, _ := core.ParseARN(e.Principal)
	id.AccountID = cmp.Or(account, e.Account)
	id.PrincipalID = id.AccountID
	if who, ok := s.hub.Identity(e.AccessKey); ok && who.UserID != "" {
		id.PrincipalID = who.UserID
	}
	name := path.Base(res)
	switch kind, _, _ := strings.Cut(res, "/"); kind {
	case "root":
		id.Type, name = "Root", "root"
	case "user":
		id.Type, id.UserName = "IAMUser", name
	case "assumed-role":
		id.Type = "AssumedRole" // the user name is the session's
	case "federated-user":
		id.Type = "FederatedUser"
	default:
		id.Type = "Unknown"
	}
	return id, name
}

// resources guesses what a call was about: a Lambda function from the path,
// and the top-level *Name, *Arn and *Url request parameters (an S3 bucket's
// from its path). ponytail: types are AWS::<service title>::<field stem>,
// close to but not always CloudTrail's own.
func resources(e core.Entry, title string, params map[string]any) []Resource {
	var out []Resource
	if p := strings.Split(e.Path, "/"); e.Service == "lambda" && len(p) > 3 && p[2] == "functions" {
		out = append(out, Resource{"AWS::Lambda::Function", p[3]})
	}
	for _, k := range core.SortedKeys(params) {
		v, _ := params[k].(string)
		for _, suffix := range []string{"Name", "Arn", "ARN", "Url"} {
			if stem, ok := strings.CutSuffix(k, suffix); ok && stem != "" && v != "" {
				out = append(out, Resource{"AWS::" + title + "::" + strings.ToUpper(stem[:1]) + stem[1:], v})
			}
		}
	}
	return out
}

// ---- LookupEvents ----

type lookupInput struct {
	LookupAttributes []struct{ AttributeKey, AttributeValue string }
	StartTime        *core.Time
	EndTime          *core.Time
	EventCategory    string
	MaxResults       int
	NextToken        string
}

func (s *Service) lookupEvents(c *core.Ctx, in *lookupInput) (any, error) {
	if len(in.LookupAttributes) > 1 {
		return nil, invalid("InvalidLookupAttributesException", "You cannot specify more than one lookup attribute.")
	}
	for _, a := range in.LookupAttributes {
		if !slices.Contains([]string{"EventId", "EventName", "ReadOnly", "Username", "ResourceType", "ResourceName", "EventSource", "AccessKeyId"}, a.AttributeKey) {
			return nil, invalid("InvalidLookupAttributesException", "Invalid lookup attribute key: %s", a.AttributeKey)
		}
	}
	if in.MaxResults < 0 || in.MaxResults > 50 {
		return nil, invalid("InvalidMaxResultsException", "MaxResults must be between 1 and 50.")
	}
	from, to := time.Time{}, time.Now().Add(time.Hour)
	if in.StartTime != nil {
		from = time.Time(*in.StartTime)
	}
	if in.EndTime != nil {
		to = time.Time(*in.EndTime)
	}
	if from.After(to) {
		return nil, invalid("InvalidTimeRangeException", "Start time must be before end time.")
	}
	before := int64(1<<63 - 1)
	if in.NextToken != "" {
		n, err := strconv.ParseInt(in.NextToken, 10, 64)
		if err != nil {
			return nil, invalid("InvalidNextTokenException", "Invalid NextToken.")
		}
		before = n
	}
	limit := cmp.Or(in.MaxResults, 50)
	s.follow()
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]any{"Events": []Event{}}
	if in.EventCategory == "insight" {
		return out, nil
	}
	var list []Event
	var last int64
	evs := s.recent()
	for i := len(evs) - 1; i >= 0; i-- {
		e := evs[i]
		t := time.Time(e.out.EventTime)
		if e.id >= before || e.account != c.Account || e.region != c.Region || t.Before(from) || t.After(to) ||
			len(in.LookupAttributes) == 1 && !e.has(in.LookupAttributes[0].AttributeKey, in.LookupAttributes[0].AttributeValue) {
			continue
		}
		if len(list) == limit {
			out["NextToken"] = strconv.FormatInt(last, 10)
			break
		}
		list, last = append(list, e.out), e.id
	}
	if list != nil {
		out["Events"] = list
	}
	return out, nil
}

func (e *event) has(key, value string) bool {
	switch key {
	case "EventId":
		return e.out.EventId == value
	case "EventName":
		return e.out.EventName == value
	case "ReadOnly":
		return e.out.ReadOnly == value
	case "Username":
		return e.out.Username == value
	case "EventSource":
		return e.out.EventSource == value
	case "AccessKeyId":
		return e.out.AccessKeyId == value
	}
	return slices.ContainsFunc(e.out.Resources, func(r Resource) bool {
		return key == "ResourceName" && r.ResourceName == value || key == "ResourceType" && r.ResourceType == value
	})
}

// ---- trails ----

func key(account, region, name string) string { return account + "/" + region + "/" + name }

var trailName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{1,126}[A-Za-z0-9]$`)

type trailInput struct {
	Name                       string
	S3BucketName               *string
	S3KeyPrefix                *string
	SnsTopicName               *string
	IncludeGlobalServiceEvents *bool
	IsMultiRegionTrail         *bool
	EnableLogFileValidation    *bool
	CloudWatchLogsLogGroupArn  *string
	CloudWatchLogsRoleArn      *string
	KmsKeyId                   *string
	IsOrganizationTrail        *bool
	TagsList                   []Tag
}

// bucket checks that a trail's bucket exists.
func (s *Service) bucket(c *core.Ctx, name string) error {
	if status, _, _ := s.hub.Do(c.Account, c.Region, "s3", http.MethodHead, "/"+url.PathEscape(name), nil, nil); name == "" || status != http.StatusOK {
		return invalid("S3BucketDoesNotExistException", "S3 bucket %s does not exist.", name)
	}
	return nil
}

func (t *trail) apply(c *core.Ctx, in *trailInput) {
	for dst, v := range map[*string]*string{&t.S3BucketName: in.S3BucketName, &t.S3KeyPrefix: in.S3KeyPrefix, &t.KmsKeyId: in.KmsKeyId,
		&t.CloudWatchLogsLogGroupArn: in.CloudWatchLogsLogGroupArn, &t.CloudWatchLogsRoleArn: in.CloudWatchLogsRoleArn, &t.SnsTopicName: in.SnsTopicName} {
		if v != nil {
			*dst = *v
		}
	}
	for dst, v := range map[*bool]*bool{&t.IncludeGlobalServiceEvents: in.IncludeGlobalServiceEvents, &t.IsMultiRegionTrail: in.IsMultiRegionTrail,
		&t.LogFileValidationEnabled: in.EnableLogFileValidation, &t.IsOrganizationTrail: in.IsOrganizationTrail} {
		if v != nil {
			*dst = *v
		}
	}
	t.SnsTopicARN = ""
	if t.SnsTopicName != "" {
		t.SnsTopicARN = c.ARN("sns", t.SnsTopicName)
	}
}

func (s *Service) createTrail(c *core.Ctx, in *trailInput) (any, error) {
	if !trailName.MatchString(in.Name) {
		return nil, invalid("InvalidTrailNameException", "Trail name %s is not valid.", in.Name)
	}
	if err := s.bucket(c, *cmp.Or(in.S3BucketName, new(string))); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, in.Name)
	if s.st.Trails[k] != nil {
		return nil, invalid("TrailAlreadyExistsException", "Trail %s already exists for customer: %s", in.Name, c.Account)
	}
	t := &trail{Account: c.Account, Trail: Trail{Name: in.Name, HomeRegion: c.Region, TrailARN: c.ARN("cloudtrail", "trail/"+in.Name),
		IncludeGlobalServiceEvents: true}}
	t.apply(c, in)
	s.st.Trails[k] = t
	if len(in.TagsList) > 0 {
		s.st.Tags[t.TrailARN] = in.TagsList
	}
	return t.Trail, nil
}

// trail finds a trail by name or ARN: in this region, or a multi-region
// trail of the account elsewhere. Lock held.
func (s *Service) trail(c *core.Ctx, ref string) (*trail, error) {
	account, region, name := c.Account, c.Region, ref
	if _, r, a, res, ok := core.ParseARN(ref); ok {
		account, region, name = a, r, strings.TrimPrefix(res, "trail/")
	}
	if t := s.st.Trails[key(account, region, name)]; t != nil {
		return t, nil
	}
	for _, t := range s.st.Trails {
		if t.Account == account && t.Name == name && t.IsMultiRegionTrail {
			return t, nil
		}
	}
	return nil, notFound(c, ref)
}

// withTrail runs f on a trail under the lock.
func (s *Service) withTrail(c *core.Ctx, ref string, f func(t *trail) (any, error)) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.trail(c, ref)
	if err != nil {
		return nil, err
	}
	return f(t)
}

func (s *Service) updateTrail(c *core.Ctx, in *trailInput) (any, error) {
	if in.S3BucketName != nil {
		if err := s.bucket(c, *in.S3BucketName); err != nil {
			return nil, err
		}
	}
	return s.withTrail(c, in.Name, func(t *trail) (any, error) {
		t.apply(c, in)
		return t.Trail, nil
	})
}

func (s *Service) deleteTrail(c *core.Ctx, in *struct{ Name string }) (any, error) {
	return s.withTrail(c, in.Name, func(t *trail) (any, error) {
		delete(s.st.Trails, key(t.Account, t.HomeRegion, t.Name))
		delete(s.st.Tags, t.TrailARN)
		return nil, nil
	})
}

func (s *Service) getTrail(c *core.Ctx, in *struct{ Name string }) (any, error) {
	return s.withTrail(c, in.Name, func(t *trail) (any, error) { return map[string]any{"Trail": t.Trail}, nil })
}

func (s *Service) describeTrails(c *core.Ctx, in *struct {
	TrailNameList       []string
	IncludeShadowTrails *bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []Trail{}
	for _, k := range core.SortedKeys(s.st.Trails) {
		t := s.st.Trails[k]
		named := slices.ContainsFunc(in.TrailNameList, func(n string) bool { return n == t.Name || n == t.TrailARN })
		shadow := t.IsMultiRegionTrail && (in.IncludeShadowTrails == nil || *in.IncludeShadowTrails)
		if t.Account == c.Account && (t.HomeRegion == c.Region || shadow) && (len(in.TrailNameList) == 0 || named) {
			list = append(list, t.Trail)
		}
	}
	return map[string]any{"trailList": list}, nil
}

func (s *Service) listTrails(c *core.Ctx, _ *struct{ NextToken string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []map[string]string{}
	for _, k := range core.SortedKeys(s.st.Trails) {
		if t := s.st.Trails[k]; t.Account == c.Account {
			list = append(list, map[string]string{"TrailARN": t.TrailARN, "Name": t.Name, "HomeRegion": t.HomeRegion})
		}
	}
	return map[string]any{"Trails": list}, nil
}

func (s *Service) startLogging(c *core.Ctx, in *struct{ Name string }) (any, error) {
	s.follow() // a trail delivers the events after logging started
	return s.withTrail(c, in.Name, func(t *trail) (any, error) {
		if !t.Logging {
			now := core.Time(time.Now())
			t.Logging, t.Started, t.cursor = true, &now, s.newest()
		}
		return nil, nil
	})
}

func (s *Service) stopLogging(c *core.Ctx, in *struct{ Name string }) (any, error) {
	return s.withTrail(c, in.Name, func(t *trail) (any, error) {
		if t.Logging {
			now := core.Time(time.Now())
			t.Logging, t.Stopped = false, &now
		}
		return nil, nil
	})
}

func (s *Service) getTrailStatus(c *core.Ctx, in *struct{ Name string }) (any, error) {
	return s.withTrail(c, in.Name, func(t *trail) (any, error) {
		out := map[string]any{"IsLogging": t.Logging}
		stamp := func(k string, v *core.Time) {
			if v != nil {
				out[k] = time.Time(*v).UTC().Format(time.RFC3339)
			}
		}
		for k, v := range map[string]*core.Time{"StartLoggingTime": t.Started, "StopLoggingTime": t.Stopped, "LatestDeliveryTime": t.Delivered,
			"LatestCloudWatchLogsDeliveryTime": t.ToLogs} {
			if v != nil {
				out[k] = v
			}
		}
		stamp("TimeLoggingStarted", t.Started)
		stamp("TimeLoggingStopped", t.Stopped)
		stamp("LatestDeliveryAttemptTime", t.Attempted)
		if t.Failure == "" {
			stamp("LatestDeliveryAttemptSucceeded", t.Attempted)
		}
		if t.Failure != "" {
			out["LatestDeliveryError"] = t.Failure
		}
		if t.LogsError != "" {
			out["LatestCloudWatchLogsDeliveryError"] = t.LogsError
		}
		return out, nil
	})
}

// ---- selectors ----

func ptr[T any](v T) *T { return &v }

func (t *trail) selectors() map[string]any {
	out := map[string]any{"TrailARN": t.TrailARN}
	switch {
	case len(t.Advanced) > 0:
		out["AdvancedEventSelectors"] = t.Advanced
	case len(t.Selectors) > 0:
		out["EventSelectors"] = t.Selectors
	default:
		out["EventSelectors"] = []EventSelector{{ReadWriteType: "All", IncludeManagementEvents: ptr(true), DataResources: []DataResource{},
			ExcludeManagementEventSources: []string{}}}
	}
	return out
}

func (s *Service) putEventSelectors(c *core.Ctx, in *struct {
	TrailName              string
	EventSelectors         []EventSelector
	AdvancedEventSelectors []json.RawMessage
}) (any, error) {
	if len(in.EventSelectors) > 0 == (len(in.AdvancedEventSelectors) > 0) {
		return nil, invalid("InvalidParameterCombinationException", "Specify either EventSelectors or AdvancedEventSelectors, but not both.")
	}
	for i := range in.EventSelectors {
		sel := &in.EventSelectors[i]
		sel.ReadWriteType = cmp.Or(sel.ReadWriteType, "All")
		sel.IncludeManagementEvents = cmp.Or(sel.IncludeManagementEvents, ptr(true))
	}
	return s.withTrail(c, in.TrailName, func(t *trail) (any, error) {
		t.Selectors, t.Advanced, t.HasCustomEventSelectors = in.EventSelectors, in.AdvancedEventSelectors, true
		return t.selectors(), nil
	})
}

func (s *Service) getEventSelectors(c *core.Ctx, in *struct{ TrailName string }) (any, error) {
	return s.withTrail(c, in.TrailName, func(t *trail) (any, error) { return t.selectors(), nil })
}

func (s *Service) putInsightSelectors(c *core.Ctx, in *struct {
	TrailName        string
	InsightSelectors []json.RawMessage
}) (any, error) {
	return s.withTrail(c, in.TrailName, func(t *trail) (any, error) {
		t.Insights, t.HasInsightSelectors = append([]json.RawMessage{}, in.InsightSelectors...), len(in.InsightSelectors) > 0
		return map[string]any{"TrailARN": t.TrailARN, "InsightSelectors": t.Insights}, nil
	})
}

func (s *Service) getInsightSelectors(c *core.Ctx, in *struct{ TrailName string }) (any, error) {
	return s.withTrail(c, in.TrailName, func(t *trail) (any, error) {
		if len(t.Insights) == 0 {
			return nil, invalid("InsightNotEnabledException", "Trail %s does not have Insights enabled. Enable Insights on the trail, and then try again.", t.Name)
		}
		return map[string]any{"TrailARN": t.TrailARN, "InsightSelectors": t.Insights}, nil
	})
}

// ---- tags ----

// tagged checks that a trail or event data store has this ARN; lock held.
func (s *Service) tagged(arn string) error {
	if s.st.Stores[arn] != nil {
		return nil
	}
	for _, t := range s.st.Trails {
		if t.TrailARN == arn {
			return nil
		}
	}
	return invalid("ResourceNotFoundException", "Resource %s not found.", arn)
}

type tagsInput struct {
	ResourceId string
	TagsList   []Tag
}

func (s *Service) addTags(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.tagged(in.ResourceId); err != nil {
		return nil, err
	}
	tags := s.st.Tags[in.ResourceId]
	for _, t := range in.TagsList {
		tags = append(slices.DeleteFunc(tags, func(x Tag) bool { return x.Key == t.Key }), t)
	}
	s.st.Tags[in.ResourceId] = tags
	return nil, nil
}

func (s *Service) removeTags(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.tagged(in.ResourceId); err != nil {
		return nil, err
	}
	s.st.Tags[in.ResourceId] = slices.DeleteFunc(s.st.Tags[in.ResourceId], func(x Tag) bool {
		return slices.ContainsFunc(in.TagsList, func(t Tag) bool { return t.Key == x.Key })
	})
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, in *struct{ ResourceIdList []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []map[string]any{}
	for _, arn := range in.ResourceIdList {
		if err := s.tagged(arn); err != nil {
			return nil, err
		}
		list = append(list, map[string]any{"ResourceId": arn, "TagsList": append([]Tag{}, s.st.Tags[arn]...)})
	}
	return map[string]any{"ResourceTagList": list}, nil
}

// ---- event data stores (CloudTrail Lake), as metadata ----

type storeInput struct {
	EventDataStore               string
	Name                         *string
	AdvancedEventSelectors       json.RawMessage
	MultiRegionEnabled           *bool
	OrganizationEnabled          *bool
	RetentionPeriod              *int
	TerminationProtectionEnabled *bool
	BillingMode                  *string
	KmsKeyId                     *string
	StartIngestion               *bool
	TagsList                     []Tag
}

func (st *Store) apply(in *storeInput) {
	for dst, v := range map[*string]*string{&st.Name: in.Name, &st.BillingMode: in.BillingMode, &st.KmsKeyId: in.KmsKeyId} {
		if v != nil {
			*dst = *v
		}
	}
	for dst, v := range map[*bool]*bool{&st.MultiRegionEnabled: in.MultiRegionEnabled, &st.OrganizationEnabled: in.OrganizationEnabled,
		&st.TerminationProtectionEnabled: in.TerminationProtectionEnabled} {
		if v != nil {
			*dst = *v
		}
	}
	if in.RetentionPeriod != nil {
		st.RetentionPeriod = *in.RetentionPeriod
	}
	if len(in.AdvancedEventSelectors) > 0 && string(in.AdvancedEventSelectors) != "null" {
		st.AdvancedEventSelectors = in.AdvancedEventSelectors
	}
	st.UpdatedTimestamp = core.Time(time.Now())
}

// store finds an event data store by ARN or ID in this account and region; lock held.
func (s *Service) store(c *core.Ctx, ref string) (*Store, error) {
	for _, arn := range []string{ref, c.ARN("cloudtrail", "eventdatastore/"+ref)} {
		if st := s.st.Stores[arn]; st != nil {
			return st, nil
		}
	}
	return nil, invalid("EventDataStoreNotFoundException", "The specified event data store %s was not found.", ref)
}

func (s *Service) createStore(c *core.Ctx, in *storeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := c.ARN("cloudtrail", "eventdatastore/")
	for arn, st := range s.st.Stores {
		if strings.HasPrefix(arn, prefix) && in.Name != nil && st.Name == *in.Name {
			return nil, invalid("EventDataStoreAlreadyExistsException", "An event data store with the name %s already exists.", st.Name)
		}
	}
	st := &Store{EventDataStoreArn: prefix + core.UUID(), Status: "ENABLED", MultiRegionEnabled: true, RetentionPeriod: 2557,
		TerminationProtectionEnabled: true, BillingMode: "EXTENDABLE_RETENTION_PRICING", CreatedTimestamp: core.Time(time.Now()),
		AdvancedEventSelectors: json.RawMessage(`[{"Name":"Default management events","FieldSelectors":[{"Field":"eventCategory","Equals":["Management"]}]}]`)}
	st.apply(in)
	if in.StartIngestion != nil && !*in.StartIngestion {
		st.Status = "STOPPED_INGESTION"
	}
	s.st.Stores[st.EventDataStoreArn] = st
	if len(in.TagsList) > 0 {
		s.st.Tags[st.EventDataStoreArn] = in.TagsList
	}
	return *st, nil
}

func (s *Service) getStore(c *core.Ctx, in *storeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.store(c, in.EventDataStore)
	if err != nil {
		return nil, err
	}
	return *st, nil
}

func (s *Service) listStores(c *core.Ctx, _ *struct{ NextToken string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []Store{}
	for _, arn := range core.SortedKeys(s.st.Stores) {
		if strings.HasPrefix(arn, c.ARN("cloudtrail", "eventdatastore/")) {
			list = append(list, *s.st.Stores[arn])
		}
	}
	return map[string]any{"EventDataStores": list}, nil
}

func (s *Service) updateStore(c *core.Ctx, in *storeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.store(c, in.EventDataStore)
	if err != nil {
		return nil, err
	}
	st.apply(in)
	return *st, nil
}

func (s *Service) deleteStore(c *core.Ctx, in *storeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.store(c, in.EventDataStore)
	if err != nil {
		return nil, err
	}
	if st.TerminationProtectionEnabled {
		return nil, invalid("EventDataStoreTerminationProtectedException", "Event data store %s cannot be deleted because termination protection is enabled.", st.Name)
	}
	delete(s.st.Stores, st.EventDataStoreArn)
	delete(s.st.Tags, st.EventDataStoreArn)
	return nil, nil
}

// ---- delivery ----

// logs reports whether a trail logs an event: its account, its region (or
// any, multi-region) and its event selectors' ReadWriteType.
// ponytail: advanced event selectors log every management event.
func (t *trail) logs(e *event) bool {
	if e.account != t.Account || !t.IsMultiRegionTrail && e.region != t.HomeRegion {
		return false
	}
	if len(t.Advanced) > 0 || len(t.Selectors) == 0 {
		return true
	}
	return slices.ContainsFunc(t.Selectors, func(sel EventSelector) bool {
		return (sel.IncludeManagementEvents == nil || *sel.IncludeManagementEvents) &&
			(sel.ReadWriteType == "All" || (sel.ReadWriteType == "ReadOnly") == (e.out.ReadOnly == "true"))
	})
}

// batch is what one trail delivers in one tick.
type batch struct {
	key, account, home, bucket, prefix, group string
	records                                   map[string][]json.RawMessage // region → records, oldest first
}

var s3Code = regexp.MustCompile(`<Code>([^<]*)</Code>`)

// Tick delivers what logging trails have not delivered yet: per trail and
// region a gzipped {"Records":[…]} file to S3, and each record to the
// trail's CloudWatch Logs group.
func (s *Service) Tick(now time.Time) {
	s.follow()
	var batches []batch
	s.mu.Lock()
	for _, k := range core.SortedKeys(s.st.Trails) {
		t := s.st.Trails[k]
		if !t.Logging {
			continue
		}
		b := batch{key: k, account: t.Account, home: t.HomeRegion, bucket: t.S3BucketName, prefix: t.S3KeyPrefix,
			group: t.CloudWatchLogsLogGroupArn, records: map[string][]json.RawMessage{}}
		for _, e := range s.recent() {
			if e.id > t.cursor && t.logs(e) {
				b.records[e.region] = append(b.records[e.region], e.record)
			}
		}
		if t.cursor = s.newest(); len(b.records) > 0 {
			batches = append(batches, b)
		}
	}
	s.mu.Unlock()
	for _, b := range batches {
		s.deliver(b, now.UTC())
	}
}

func (s *Service) deliver(b batch, now time.Time) {
	failure, logsError := "", ""
	for _, region := range core.SortedKeys(b.records) {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		json.NewEncoder(zw).Encode(map[string]any{"Records": b.records[region]})
		zw.Close()
		name := b.account + "_CloudTrail_" + region + "_" + now.Format("20060102T1504Z") + "_" + core.RandString(16, core.AlphaNum) + ".json.gz"
		k := path.Join(b.prefix, "AWSLogs", b.account, "CloudTrail", region, now.Format("2006/01/02"), name)
		h := http.Header{"Content-Type": {"application/json"}, "Content-Encoding": {"gzip"}}
		if status, _, body := s.hub.Do(b.account, b.home, "s3", http.MethodPut, (&url.URL{Path: "/" + b.bucket + "/" + k}).EscapedPath(), h, buf.Bytes()); status != http.StatusOK {
			failure = "HTTP " + strconv.Itoa(status)
			if m := s3Code.FindSubmatch(body); m != nil {
				failure = string(m[1])
			}
		}
		if b.group == "" {
			continue
		}
		stream := strings.TrimSuffix(b.group, ":*") + ":log-stream:" + b.account + "_CloudTrail_" + region
		for _, r := range b.records[region] {
			if err := s.hub.Deliver(stream, &core.Message{Body: string(r)}); err != nil {
				logsError = err.Error()
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.st.Trails[b.key]
	if t == nil {
		return
	}
	at := core.Time(now)
	t.Attempted, t.Failure = &at, failure
	if failure == "" {
		t.Delivered = &at
	}
	if b.group != "" {
		t.LogsError = logsError
		if logsError == "" {
			t.ToLogs = &at
		}
	}
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	trails := core.Table{Name: "Trails", Columns: []string{"Name", "Region", "Account", "Bucket", "Logging"}}
	for _, k := range core.SortedKeys(s.st.Trails) {
		t := s.st.Trails[k]
		trails.Rows = append(trails.Rows, []string{t.Name, t.HomeRegion, t.Account, t.S3BucketName, strconv.FormatBool(t.Logging)})
	}
	stores := core.Table{Name: "Event data stores", Columns: []string{"Name", "Status", "ARN"}}
	for _, arn := range core.SortedKeys(s.st.Stores) {
		stores.Rows = append(stores.Rows, []string{s.st.Stores[arn].Name, s.st.Stores[arn].Status, arn})
	}
	events := core.Table{Name: "Events", Columns: []string{"Event ID", "Event", "Time", "Source", "Region", "User"}}
	evs := s.recent()
	for i := len(evs) - 1; i >= max(0, len(evs)-100); i-- {
		e := evs[i].out
		events.Rows = append(events.Rows, []string{e.EventId, e.EventName, time.Time(e.EventTime).Format(time.RFC3339), e.EventSource, evs[i].region, e.Username})
	}
	return []core.Table{trails, stores, events}
}

func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Trails":
		for _, t := range s.st.Trails {
			if t.Name == id {
				b, _ := json.Marshal(t) // under the lock: deliveries change it
				return json.RawMessage(b)
			}
		}
	case "Event data stores":
		for _, st := range s.st.Stores {
			if st.Name == id {
				return *st
			}
		}
	case "Events":
		for _, e := range s.events {
			if e.out.EventId == id {
				return e.record
			}
		}
	}
	return nil
}
