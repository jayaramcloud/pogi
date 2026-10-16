package s3

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// regionName is the shape of an AWS region name (eu-west-1, us-gov-east-1); no list, so new regions still work.
var regionName = regexp.MustCompile(`^[a-z]{2}(-gov|-iso[a-z]?)?-[a-z]+-[0-9]+$`)

var bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

func (s *Service) listBuckets(w http.ResponseWriter, r *http.Request, c *core.Ctx, _, _ string) error {
	q := r.URL.Query()
	prefix, region, token := q.Get("prefix"), q.Get("bucket-region"), q.Get("continuation-token")
	maxBuckets := 10000
	if n, err := strconv.Atoi(q.Get("max-buckets")); err == nil && n > 0 {
		maxBuckets = n
	}
	type bucketXML struct {
		Name         string `xml:"Name"`
		CreationDate string `xml:"CreationDate"`
		BucketRegion string `xml:"BucketRegion"`
	}
	out := struct {
		XMLName           xml.Name    `xml:"ListAllMyBucketsResult"`
		Xmlns             string      `xml:"xmlns,attr"`
		Owner             *owner      `xml:"Owner"`
		Buckets           []bucketXML `xml:"Buckets>Bucket"`
		ContinuationToken string      `xml:"ContinuationToken,omitempty"`
		Prefix            string      `xml:"Prefix,omitempty"`
	}{Xmlns: xmlns, Owner: ownerOf(c.Account), Prefix: prefix}
	s.mu.RLock()
	for _, name := range core.SortedKeys(s.st.Buckets) {
		b := s.st.Buckets[name]
		if b.Account != c.Account || !strings.HasPrefix(name, prefix) || region != "" && b.Region != region || name <= token {
			continue
		}
		if len(out.Buckets) == maxBuckets {
			out.ContinuationToken = out.Buckets[len(out.Buckets)-1].Name
			break
		}
		out.Buckets = append(out.Buckets, bucketXML{name, b.Created.Format(core.ISO), b.Region})
	}
	s.mu.RUnlock()
	return writeXML(w, 200, out)
}

func (s *Service) headBucket(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, _ string) error {
	return s.withBucket(bucket, false, func(b *Bucket) error {
		w.Header().Set("x-amz-bucket-region", b.Region)
		w.WriteHeader(http.StatusOK)
		return nil
	})
}

func (s *Service) createBucket(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, _ string) error {
	if !bucketName.MatchString(bucket) || strings.Contains(bucket, "..") {
		return core.Errorf(400, "InvalidBucketName", "The specified bucket is not valid: %s", bucket)
	}
	body, _, err := readBody(r)
	if err != nil {
		return err
	}
	var cfg struct {
		LocationConstraint string `xml:"LocationConstraint"`
	}
	if len(strings.TrimSpace(string(body))) > 0 {
		if err := xml.Unmarshal(body, &cfg); err != nil {
			return core.Errorf(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema")
		}
	}
	region := cfg.LocationConstraint
	if region == "EU" { // the old name of eu-west-1
		region = "eu-west-1"
	}
	// us-east-1 is the default, so naming it is an error on S3
	if region == "us-east-1" || region != "" && !regionName.MatchString(region) {
		return core.Errorf(400, "InvalidLocationConstraint", "The specified location-constraint is not valid")
	}
	if region == "" {
		region = c.Region
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if b := s.st.Buckets[bucket]; b != nil {
		if b.Account != c.Account {
			return core.Errorf(409, "BucketAlreadyExists", "The requested bucket name is not available. The bucket namespace is shared by all users of the system.")
		}
		if region != "us-east-1" {
			return core.Errorf(409, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.")
		}
	} else {
		b := &Bucket{Name: bucket, Account: c.Account, Region: region, Created: time.Now().UTC(), Objects: map[string][]*Object{}, Config: defaultConfig()}
		if strings.EqualFold(r.Header.Get("x-amz-bucket-object-lock-enabled"), "true") {
			b.Versioning = "Enabled"
			b.Config["object-lock"] = `<ObjectLockConfiguration xmlns="` + xmlns + `"><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>`
		}
		s.st.Buckets[bucket] = b
		s.journalBucket(b)
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Service) deleteBucket(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.st.Buckets[bucket]
	if b == nil {
		return errNoBucket(bucket)
	}
	if len(b.Objects) > 0 {
		return core.Errorf(409, "BucketNotEmpty", "The bucket you tried to delete is not empty")
	}
	delete(s.st.Buckets, bucket)
	s.j.Delete("b/" + bucket)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Service) getLocation(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, _ string) error {
	return s.withBucket(bucket, false, func(b *Bucket) error {
		region := b.Region
		if region == "us-east-1" {
			region = ""
		}
		return writeXML(w, 200, struct {
			XMLName xml.Name `xml:"LocationConstraint"`
			Xmlns   string   `xml:"xmlns,attr"`
			Region  string   `xml:",chardata"`
		}{Xmlns: xmlns, Region: region})
	})
}

// defaultOwnership is what a new bucket has: ACLs off, as on AWS since 2023.
const defaultOwnership = `<OwnershipControls xmlns="` + xmlns + `"><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>`

// defaultPublicAccessBlock is what a new bucket has: all four blocks on, as on AWS since 2023.
const defaultPublicAccessBlock = `<PublicAccessBlockConfiguration xmlns="` + xmlns + `"><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>`

// defaultConfig is the configuration of a new bucket.
func defaultConfig() map[string]string {
	return map[string]string{"ownershipControls": defaultOwnership, "publicAccessBlock": defaultPublicAccessBlock}
}

// Bucket subresources stored as raw documents. "missing" is the error when
// unset (empty means return "def"). Terraform reads all of these.
var configs = map[string]struct{ missing, def string }{
	"cors":              {"NoSuchCORSConfiguration", ""},
	"policy":            {"NoSuchBucketPolicy", ""},
	"lifecycle":         {"NoSuchLifecycleConfiguration", ""},
	"website":           {"NoSuchWebsiteConfiguration", ""},
	"replication":       {"ReplicationConfigurationNotFoundError", ""},
	"object-lock":       {"ObjectLockConfigurationNotFoundError", ""},
	"publicAccessBlock": {"NoSuchPublicAccessBlockConfiguration", ""}, // new buckets start with defaultPublicAccessBlock
	"tagging":           {"NoSuchTagSet", ""},
	"ownershipControls": {"OwnershipControlsNotFoundError", ""}, // new buckets start with defaultOwnership; deleting it makes this error
	"encryption":        {"", `<ServerSideEncryptionConfiguration xmlns="` + xmlns + `"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault><BucketKeyEnabled>false</BucketKeyEnabled></Rule></ServerSideEncryptionConfiguration>`},
	"logging":           {"", `<BucketLoggingStatus xmlns="` + xmlns + `"></BucketLoggingStatus>`},
	"accelerate":        {"", `<AccelerateConfiguration xmlns="` + xmlns + `"></AccelerateConfiguration>`},
	"requestPayment":    {"", `<RequestPaymentConfiguration xmlns="` + xmlns + `"><Payer>BucketOwner</Payer></RequestPaymentConfiguration>`},
	"notification":      {"", `<NotificationConfiguration xmlns="` + xmlns + `"></NotificationConfiguration>`},
	"versioning":        {"", ""},
	"acl":               {"", ""},
}

var subresources = []string{"versioning", "acl", "cors", "policy", "lifecycle", "website", "replication", "object-lock",
	"publicAccessBlock", "tagging", "ownershipControls", "encryption", "logging", "accelerate", "requestPayment", "notification",
	"retention", "legal-hold"}

func subresource(q url.Values) string {
	for _, s := range subresources {
		if q.Has(s) {
			return s
		}
	}
	return ""
}

func opName(sub string) string {
	if n, ok := map[string]string{"cors": "Cors", "acl": "Acl", "object-lock": "ObjectLockConfiguration", "lifecycle": "LifecycleConfiguration",
		"notification": "NotificationConfiguration", "accelerate": "AccelerateConfiguration", "legal-hold": "LegalHold"}[sub]; ok {
		return n
	}
	if sub == "" {
		return ""
	}
	return strings.ToUpper(sub[:1]) + sub[1:]
}

func (s *Service) getConfig(sub string) handler {
	return func(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, _ string) error {
		if sub == "acl" {
			return s.getACL(w, r, c, bucket, "")
		}
		spec, ok := configs[sub]
		if !ok {
			return core.Errorf(501, "NotImplemented", "pogi does not implement GetBucket%s", opName(sub))
		}
		var doc string
		err := s.withBucket(bucket, false, func(b *Bucket) error {
			if sub == "versioning" {
				doc = `<VersioningConfiguration xmlns="` + xmlns + `">`
				if b.Versioning != "" {
					doc += "<Status>" + b.Versioning + "</Status>"
				}
				doc += "</VersioningConfiguration>"
				return nil
			}
			doc = b.Config[sub]
			if doc == "" && spec.missing != "" {
				return core.Errorf(404, spec.missing, "The %s configuration does not exist", sub)
			}
			if doc == "" {
				doc = spec.def
			}
			return nil
		})
		if err != nil {
			return err
		}
		ct := "application/xml"
		if sub == "policy" {
			ct = "application/json"
		}
		w.Header().Set("Content-Type", ct)
		w.Write([]byte(doc))
		return nil
	}
}

func (s *Service) putConfig(sub string) handler {
	return func(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, _ string) error {
		body, _, err := readBody(r)
		if err != nil {
			return err
		}
		switch sub {
		case "acl", "retention", "legal-hold":
			return s.accept(w, r, c, bucket, "")
		case "versioning":
			var v struct {
				Status string `xml:"Status"`
			}
			if err := xml.Unmarshal(body, &v); err != nil || v.Status != "Enabled" && v.Status != "Suspended" {
				return core.Errorf(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema")
			}
			return s.withBucket(bucket, true, func(b *Bucket) error {
				b.Versioning = v.Status
				s.journalBucket(b)
				return nil
			})
		case "notification":
			if err := s.validateNotification(c, bucket, body); err != nil {
				return err
			}
		case "lifecycle":
			var cfg struct {
				Rules []lifecycleRule `xml:"Rule"`
			}
			if err := xml.Unmarshal(body, &cfg); err != nil || len(cfg.Rules) == 0 {
				return core.Errorf(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema")
			}
		case "policy":
			if !json.Valid(body) {
				return core.Errorf(400, "MalformedPolicy", "Policies must be valid JSON and the first byte must be '{'")
			}
		default:
			if _, ok := configs[sub]; !ok {
				return core.Errorf(501, "NotImplemented", "pogi does not implement PutBucket%s", opName(sub))
			}
		}
		err = s.withBucket(bucket, true, func(b *Bucket) error {
			b.Config[sub] = string(body)
			s.journalBucket(b)
			return nil
		})
		if err != nil {
			return err
		}
		w.WriteHeader(map[bool]int{true: http.StatusNoContent, false: http.StatusOK}[sub == "policy" || sub == "tagging"])
		return nil
	}
}

func (s *Service) deleteConfig(sub string) handler {
	return func(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, _ string) error {
		return s.withBucket(bucket, true, func(b *Bucket) error {
			delete(b.Config, sub)
			s.journalBucket(b)
			w.WriteHeader(http.StatusNoContent)
			return nil
		})
	}
}

// ---- event notifications ----

type notification struct {
	Queues      []notifyTarget `xml:"QueueConfiguration"`
	Topics      []notifyTarget `xml:"TopicConfiguration"`
	Lambdas     []notifyTarget `xml:"CloudFunctionConfiguration"`
	EventBridge *struct{}      `xml:"EventBridgeConfiguration"`
}

type notifyTarget struct {
	ID     string   `xml:"Id"`
	Queue  string   `xml:"Queue"`
	Topic  string   `xml:"Topic"`
	Lambda string   `xml:"CloudFunction"`
	Events []string `xml:"Event"`
	Rules  []struct {
		Name  string `xml:"Name"`
		Value string `xml:"Value"`
	} `xml:"Filter>S3Key>FilterRule"`
}

func (n notification) targets() []notifyTarget {
	return append(append(append([]notifyTarget{}, n.Queues...), n.Topics...), n.Lambdas...)
}

// validateNotification parses the config and sends the S3 test event to queues,
// as S3 does; unreachable destinations are rejected.
func (s *Service) validateNotification(c *core.Ctx, bucket string, body []byte) error {
	var n notification
	if err := xml.Unmarshal(body, &n); err != nil {
		return core.Errorf(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema")
	}
	for _, q := range n.Queues {
		test, _ := json.Marshal(map[string]string{"Service": "Amazon S3", "Event": "s3:TestEvent",
			"Time": time.Now().UTC().Format(core.ISO), "Bucket": bucket, "RequestId": c.RequestID, "HostId": c.RequestID})
		if err := s.hub.Deliver(q.Queue, &core.Message{Body: string(test)}); err != nil {
			return core.Errorf(400, "InvalidArgument", "Unable to validate the following destination configurations: %s (%v)", q.Queue, err)
		}
	}
	return nil
}

type pending struct {
	arn string
	msg *core.Message
}

// events builds the notifications an object change triggers (call with the lock held).
func (s *Service) events(b *Bucket, event string, o *Object) []pending {
	raw := b.Config["notification"]
	if raw == "" || o == nil {
		return nil
	}
	var n notification
	if xml.Unmarshal([]byte(raw), &n) != nil {
		return nil
	}
	var out []pending
	removal := strings.HasPrefix(event, "ObjectRemoved") || strings.HasPrefix(event, "LifecycleExpiration")
	seq := fmt.Sprintf("%016X", time.Now().UnixNano())
	for _, t := range n.targets() {
		if !t.matches(event, o.Key) {
			continue
		}
		obj := map[string]any{"key": eventKey(o.Key), "sequencer": seq}
		if !removal {
			obj["size"], obj["eTag"] = len(o.Data), trimETag(o.ETag)
		}
		if b.Versioning != "" {
			obj["versionId"] = o.VersionID
		}
		rec := map[string]any{
			"eventVersion": "2.1", "eventSource": "aws:s3", "awsRegion": b.Region, "eventTime": time.Now().UTC().Format(core.ISO),
			"eventName": event, "userIdentity": map[string]string{"principalId": "AWS:" + b.Account},
			"requestParameters": map[string]string{"sourceIPAddress": "127.0.0.1"},
			"responseElements":  map[string]string{"x-amz-request-id": core.NewID(), "x-amz-id-2": core.NewID()},
			"s3": map[string]any{"s3SchemaVersion": "1.0", "configurationId": t.ID, "object": obj,
				"bucket": map[string]any{"name": b.Name, "ownerIdentity": map[string]string{"principalId": b.Account}, "arn": core.GlobalARN("s3", b.Region, "", b.Name)}},
		}
		body, _ := json.Marshal(map[string]any{"Records": []any{rec}})
		out = append(out, pending{t.Queue + t.Topic + t.Lambda, &core.Message{Body: string(body), Subject: "Amazon S3 Notification", From: core.GlobalARN("s3", b.Region, "", b.Name)}})
	}
	if n.EventBridge != nil {
		kind := map[bool]string{true: "Object Deleted", false: "Object Created"}[removal]
		detail := map[string]any{"version": "0", "bucket": map[string]string{"name": b.Name},
			"object":     map[string]any{"key": o.Key, "size": len(o.Data), "etag": trimETag(o.ETag), "version-id": o.VersionID, "sequencer": seq},
			"request-id": core.NewID(), "requester": b.Account, "source-ip-address": "127.0.0.1", "reason": strings.TrimPrefix(event, "ObjectCreated:")}
		body, _ := json.Marshal(detail)
		out = append(out, pending{core.ARN("events", b.Region, b.Account, "event-bus/default"),
			&core.Message{Body: string(body), Source: "aws.s3", DetailType: kind, From: core.GlobalARN("s3", b.Region, "", b.Name), Resources: []string{core.GlobalARN("s3", b.Region, "", b.Name)}}})
	}
	return out
}

func (t notifyTarget) matches(event, key string) bool {
	ok := false
	for _, e := range t.Events {
		e = strings.TrimPrefix(e, "s3:")
		if e == event || strings.HasSuffix(e, ":*") && strings.HasPrefix(event, strings.TrimSuffix(e, "*")) {
			ok = true
		}
	}
	for _, r := range t.Rules {
		switch strings.ToLower(r.Name) {
		case "prefix":
			ok = ok && strings.HasPrefix(key, r.Value)
		case "suffix":
			ok = ok && strings.HasSuffix(key, r.Value)
		}
	}
	return ok
}

func eventKey(key string) string {
	return strings.ReplaceAll(url.QueryEscape(key), "%2F", "/")
}

// notify delivers pending events outside the S3 lock, in the flow of the request that caused them.
func (s *Service) notify(trace string, ev []pending) {
	for _, p := range ev {
		p.msg.Trace = trace
		if err := s.hub.Deliver(p.arn, p.msg); err != nil {
			s.hub.Log.Warn("s3 notification failed", "target", p.arn, "err", err)
		}
	}
}

// ---- CORS ----

type corsRule struct {
	Origins []string `xml:"AllowedOrigin"`
	Methods []string `xml:"AllowedMethod"`
	Headers []string `xml:"AllowedHeader"`
	Expose  []string `xml:"ExposeHeader"`
	MaxAge  int      `xml:"MaxAgeSeconds"`
}

// cors applies bucket CORS rules; without any, pogi's own policy (core's
// Hub.CORS, which also answers the preflights of APIs that land here, being
// unsigned). It returns true when it answered a preflight.
func (s *Service) cors(w http.ResponseWriter, r *http.Request, bucket string) bool {
	origin, preflight := r.Header.Get("Origin"), r.Method == http.MethodOptions
	method, reqHeaders := r.Method, r.Header.Get("Access-Control-Request-Headers")
	if preflight {
		method = r.Header.Get("Access-Control-Request-Method")
	}
	var raw string
	s.mu.RLock()
	if b := s.st.Buckets[bucket]; b != nil {
		raw = b.Config["cors"]
	}
	s.mu.RUnlock()
	if raw == "" {
		return s.hub.CORS(w, r)
	}
	h := w.Header()
	h.Add("Vary", "Origin")
	var cfg struct {
		Rules []corsRule `xml:"CORSRule"`
	}
	xml.Unmarshal([]byte(raw), &cfg)
	matched := false
	for _, rule := range cfg.Rules {
		if !anyMatch(rule.Origins, origin) || !anyMatch(rule.Methods, method) {
			continue
		}
		allHeaders := true
		for _, rh := range strings.Split(reqHeaders, ",") {
			if rh = strings.TrimSpace(rh); rh != "" && !anyMatch(rule.Headers, strings.ToLower(rh)) {
				allHeaders = false
			}
		}
		if preflight && !allHeaders {
			continue
		}
		matched = true
		if len(rule.Origins) == 1 && rule.Origins[0] == "*" {
			h.Set("Access-Control-Allow-Origin", "*")
		} else { // S3 allows credentials whenever it names the origin
			h.Set("Access-Control-Allow-Origin", origin)
			h.Set("Access-Control-Allow-Credentials", "true")
		}
		h.Set("Access-Control-Allow-Methods", strings.Join(rule.Methods, ", "))
		if reqHeaders != "" {
			h.Set("Access-Control-Allow-Headers", reqHeaders)
		}
		if len(rule.Expose) > 0 {
			h.Set("Access-Control-Expose-Headers", strings.Join(rule.Expose, ", "))
		}
		if rule.MaxAge > 0 {
			h.Set("Access-Control-Max-Age", strconv.Itoa(rule.MaxAge))
		}
		break
	}
	if !matched && preflight {
		writeErr(w, r, core.C(r), bucket, core.Errorf(403, "AccessForbidden", "CORSResponse: This CORS request is not allowed. This is usually because the evalution of Origin, request method / Access-Control-Request-Method or Access-Control-Request-Headers are not whitelisted by the resource's CORS spec."))
		return true
	}
	if preflight {
		w.WriteHeader(http.StatusOK)
	}
	return preflight
}

func anyMatch(patterns []string, s string) bool {
	for _, p := range patterns {
		if p == "*" || strings.EqualFold(p, s) {
			return true
		}
		if ok, _ := path.Match(strings.ToLower(p), strings.ToLower(s)); ok {
			return true
		}
	}
	return false
}

// ---- console API (used by the web UI) ----

// ObjectInfo is a console listing row.
type ObjectInfo = core.ObjectInfo

// Browse lists one "directory" level of a bucket.
func (s *Service) Browse(bucket, prefix string) (dirs []string, objs []ObjectInfo, err error) {
	err = s.withBucket(bucket, false, func(b *Bucket) error {
		seen := map[string]bool{}
		for _, k := range sortedKeys(b.Objects) {
			o := b.latest(k)
			if o == nil || !strings.HasPrefix(k, prefix) {
				continue
			}
			if i := strings.Index(k[len(prefix):], "/"); i >= 0 {
				if d := k[:len(prefix)+i+1]; !seen[d] {
					seen[d] = true
					dirs = append(dirs, d)
				}
				continue
			}
			objs = append(objs, ObjectInfo{Key: k, Size: len(o.Data), Modified: o.Modified, ContentType: o.ContentType})
		}
		return nil
	})
	return
}

// Get returns a copy of the latest version of an object.
func (s *Service) Get(bucket, key string) (Object, error) {
	var o Object
	err := s.withBucket(bucket, false, func(b *Bucket) error {
		p := b.latest(key)
		if p == nil {
			return errNoKey(key)
		}
		o = *p
		return nil
	})
	return o, err
}

// Put stores an object (console upload).
func (s *Service) Put(bucket, key, contentType string, data []byte) error {
	h := http.Header{}
	h.Set("Content-Type", contentType)
	o := newObject(h, key, data)
	var ev []pending
	err := s.withBucket(bucket, true, func(b *Bucket) error {
		b.put(o)
		s.journal(b, key)
		ev = s.events(b, "ObjectCreated:Put", o)
		return nil
	})
	s.notify(core.NewTrace(), ev)
	return err
}

// Delete removes an object (console).
func (s *Service) Delete(bucket, key string) error {
	var ev []pending
	err := s.withBucket(bucket, true, func(b *Bucket) error {
		if res := b.remove(key, ""); res.Event != "" {
			s.journal(b, key)
			ev = s.events(b, res.Event, res.Obj)
		}
		return nil
	})
	s.notify(core.NewTrace(), ev)
	return err
}

// CreateBucket creates a bucket (console).
func (s *Service) CreateBucket(account, region, name string) error {
	if !bucketName.MatchString(name) {
		return core.Errorf(400, "InvalidBucketName", "The specified bucket is not valid: %s", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Buckets[name] != nil {
		return core.Errorf(409, "BucketAlreadyExists", "bucket %s already exists", name)
	}
	s.st.Buckets[name] = &Bucket{Name: name, Account: account, Region: region, Created: time.Now().UTC(), Objects: map[string][]*Object{}, Config: defaultConfig()}
	s.journalBucket(s.st.Buckets[name])
	return nil
}

// DeleteBucket removes a bucket and everything in it (console).
func (s *Service) DeleteBucket(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Buckets[name] == nil {
		return errNoBucket(name)
	}
	delete(s.st.Buckets, name)
	s.j.Delete("b/" + name)
	return nil
}
