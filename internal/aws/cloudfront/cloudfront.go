// Package cloudfront emulates Amazon CloudFront (REST-XML, global):
// distributions with their invalidations, tags and monitoring
// subscriptions; cache, origin request and response headers policies (the
// managed ones built in); origin access controls and identities; key groups
// and public keys; and CloudFront Functions. Distributions serve traffic on
// <id>.cloudfront.localhost (edge.go).
package cloudfront

import (
	"cmp"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

const (
	xmlns = "http://cloudfront.amazonaws.com/doc/2020-05-31/"
	api   = "/2020-05-31/"
)

// Service is the CloudFront emulator.
type Service struct {
	hub   *core.Hub
	mux   *http.ServeMux
	mu    sync.Mutex
	st    state
	cache map[string]*entry // the edge cache, by cache key; not persisted
}

type state struct {
	Distributions map[string]*Distribution     `json:"distributions"`  // id
	Items         map[string]*Item             `json:"items"`          // id: policies, origin access, key groups, public keys
	Functions     map[string]*Function         `json:"functions"`      // ARN
	Tags          map[string]map[string]string `json:"tags,omitempty"` // ARN
}

// Distribution keeps its DistributionConfig as sent: Terraform compares it
// field by field, so it round-trips untouched.
type Distribution struct {
	Id            string          `json:"id"`
	ARN           string          `json:"arn"`
	Account       string          `json:"account"`
	ETag          string          `json:"etag"`
	Config        string          `json:"config"` // inner XML of DistributionConfig
	Modified      time.Time       `json:"modified"`
	Monitoring    string          `json:"monitoring,omitempty"` // inner XML of MonitoringSubscription
	Invalidations []*Invalidation `json:"invalidations,omitempty"`
}

// Invalidation purges cached paths when it is created; it reads back as Completed.
type Invalidation struct {
	Id        string    `json:"id"`
	Created   time.Time `json:"created"`
	CallerRef string    `json:"callerRef"`
	Batch     string    `json:"batch"` // inner XML of InvalidationBatch
}

// Item is a resource defined by one config document: a cache, origin
// request or response headers policy, an origin access control or
// identity, a key group or a public key.
type Item struct {
	Id       string    `json:"id"`
	Kind     string    `json:"kind"`    // kind.Elem
	Account  string    `json:"account"` // "" for managed policies
	ETag     string    `json:"etag"`
	Config   string    `json:"config"` // inner XML of <Kind>Config
	Modified time.Time `json:"modified"`
	Extra    string    `json:"extra,omitempty"` // S3CanonicalUserId of an origin access identity
}

// Function is a CloudFront Function: DEVELOPMENT and, once published, LIVE.
type Function struct {
	Name    string    `json:"name"`
	ARN     string    `json:"arn"`
	Account string    `json:"account"`
	Created time.Time `json:"created"`
	Dev     *Stage    `json:"dev"`
	Live    *Stage    `json:"live,omitempty"`
}

// Stage is one stage of a function.
type Stage struct {
	Config    string    `json:"config"` // inner XML of FunctionConfig
	Code      []byte    `json:"code"`
	ETag      string    `json:"etag"`
	Modified  time.Time `json:"modified"`
	Published bool      `json:"published,omitempty"` // DEVELOPMENT is what LIVE runs
}

// kind describes a family of config-document resources.
type kind struct {
	Path   string // URL segment after /2020-05-31/
	Elem   string // resource element; its config is Elem+"Config"
	List   string // list operation
	Time   string // timestamp element, if any
	Prefix string // id prefix; "" makes a UUID
	Typed  bool   // list summaries say managed or custom
	Flat   bool   // list summaries carry the config fields themselves
	Miss   string // error code for an unknown id
	InUse  string // error code when a distribution (or key group) uses it
}

var (
	cachePolicies    = &kind{"cache-policy", "CachePolicy", "ListCachePolicies", "LastModifiedTime", "", true, false, "NoSuchCachePolicy", "CachePolicyInUse"}
	requestPolicies  = &kind{"origin-request-policy", "OriginRequestPolicy", "ListOriginRequestPolicies", "LastModifiedTime", "", true, false, "NoSuchOriginRequestPolicy", "OriginRequestPolicyInUse"}
	headersPolicies  = &kind{"response-headers-policy", "ResponseHeadersPolicy", "ListResponseHeadersPolicies", "LastModifiedTime", "", true, false, "NoSuchResponseHeadersPolicy", "ResponseHeadersPolicyInUse"}
	accessControls   = &kind{"origin-access-control", "OriginAccessControl", "ListOriginAccessControls", "", "E", false, true, "NoSuchOriginAccessControl", "OriginAccessControlInUse"}
	accessIdentities = &kind{"origin-access-identity/cloudfront", "CloudFrontOriginAccessIdentity", "ListCloudFrontOriginAccessIdentities", "", "E", false, true, "NoSuchCloudFrontOriginAccessIdentity", "CloudFrontOriginAccessIdentityInUse"}
	keyGroups        = &kind{"key-group", "KeyGroup", "ListKeyGroups", "LastModifiedTime", "", false, false, "NoSuchResource", "ResourceInUse"}
	publicKeys       = &kind{"public-key", "PublicKey", "ListPublicKeys", "CreatedTime", "K", false, true, "NoSuchPublicKey", "PublicKeyInUse"}
	kinds            = []*kind{cachePolicies, requestPolicies, headersPolicies, accessControls, accessIdentities, keyGroups, publicKeys}
)

type handler func(*core.Ctx, http.ResponseWriter, *http.Request) (any, error)

// New creates the CloudFront service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux()}
	s.Reset()
	d, f, m := api+"distribution", api+"function", api+"distributions/{Id}/monitoring-subscription"
	s.route("POST "+d, "CreateDistribution", s.createDistribution)
	s.route("GET "+d, "ListDistributions", s.listDistributions)
	s.route("GET "+d+"/{Id}", "GetDistribution", s.getDistribution)
	s.route("GET "+d+"/{Id}/config", "GetDistributionConfig", s.getDistribution)
	s.route("PUT "+d+"/{Id}/config", "UpdateDistribution", s.updateDistribution)
	s.route("DELETE "+d+"/{Id}", "DeleteDistribution", s.deleteDistribution)
	s.route("POST "+d+"/{Id}/invalidation", "CreateInvalidation", s.createInvalidation)
	s.route("GET "+d+"/{Id}/invalidation", "ListInvalidations", s.listInvalidations)
	s.route("GET "+d+"/{Id}/invalidation/{Inv}", "GetInvalidation", s.getInvalidation)
	s.route("POST "+m, "CreateMonitoringSubscription", s.monitoring)
	s.route("GET "+m, "GetMonitoringSubscription", s.monitoring)
	s.route("DELETE "+m, "DeleteMonitoringSubscription", s.monitoring)
	s.route("POST "+f, "CreateFunction", s.createFunction)
	s.route("GET "+f, "ListFunctions", s.listFunctions)
	s.route("GET "+f+"/{Name}", "GetFunction", s.describeFunction)
	s.route("GET "+f+"/{Name}/describe", "DescribeFunction", s.describeFunction)
	s.route("PUT "+f+"/{Name}", "UpdateFunction", s.updateFunction)
	s.route("POST "+f+"/{Name}/publish", "PublishFunction", s.publishFunction)
	s.route("POST "+f+"/{Name}/test", "TestFunction", s.testFunction)
	s.route("DELETE "+f+"/{Name}", "DeleteFunction", s.deleteFunction)
	s.route("GET "+api+"tagging", "ListTagsForResource", s.tags)
	s.route("POST "+api+"tagging", "TagResource", s.tags)
	for _, k := range kinds {
		p := api + k.Path
		s.route("POST "+p, "Create"+k.Elem, s.createItem(k))
		s.route("GET "+p, k.List, s.listItems(k))
		s.route("GET "+p+"/{Id}", "Get"+k.Elem, s.getItem(k))
		s.route("GET "+p+"/{Id}/config", "Get"+k.Elem+"Config", s.getItem(k))
		s.route("PUT "+p+"/{Id}/config", "Update"+k.Elem, s.updateItem(k))
		s.route("DELETE "+p+"/{Id}", "Delete"+k.Elem, s.deleteItem(k))
	}
	s.route("/", "Unknown", func(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
		return nil, core.Errorf(501, "NotImplemented", "pogi does not implement CloudFront %s %s", r.Method, r.URL.Path)
	})
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "cloudfront", Cloud: core.AWS, Title: "CloudFront", Signing: []string{"cloudfront"}, Match: isEdge, Protocol: "rest-xml"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isEdge(r) {
		s.serveEdge(w, r)
		return
	}
	s.mux.ServeHTTP(w, r)
}

func newState() state {
	return state{Distributions: map[string]*Distribution{}, Items: map[string]*Item{}, Functions: map[string]*Function{}, Tags: map[string]map[string]string{}}
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st, s.cache = newState(), map[string]*entry{}
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
	s.st, s.cache = st, map[string]*entry{}
	s.mu.Unlock()
	return nil
}

// doc is an operation's XML payload: the root element and its inner XML.
type doc struct{ root, body string }

// route serves one operation. Handlers return a doc (201 for Create
// operations, else 200), raw bytes, or nil for 204.
func (s *Service) route(pattern, op string, h handler) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		c := core.C(r)
		c.Op = op
		q := r.URL.Query()
		switch {
		case q.Has("WithTags"):
			c.Op += "WithTags"
		case op == "TagResource" && q.Get("Operation") == "Untag":
			c.Op = "UntagResource"
		}
		out, err := any(nil), c.Intercept()
		if err == nil {
			out, err = h(c, w, r)
		}
		if err != nil {
			e := core.AsError(err)
			core.WriteQueryError(w, c, xmlns, e.Status, e.Code, e.Msg)
			return
		}
		w.Header().Set("x-amzn-RequestId", c.RequestID)
		switch out := out.(type) {
		case doc:
			w.Header().Set("Content-Type", "text/xml")
			w.WriteHeader(map[bool]int{true: http.StatusCreated, false: http.StatusOK}[strings.HasPrefix(c.Op, "Create")])
			fmt.Fprintf(w, `%s<%s xmlns="%s">%s</%s>`, xml.Header, out.root, xmlns, out.body, out.root)
		case []byte:
			w.Write(out)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	})
}

// inner captures the inner XML of an element.
type inner struct {
	XML string `xml:",innerxml"`
}

type tag struct{ Key, Value string }

func decode(r *http.Request, v any) error {
	b := core.Body(r)
	if len(b) == 0 {
		return core.Errorf(400, "MissingBody", "This operation requires a body.")
	}
	if err := xml.Unmarshal(b, v); err != nil {
		return core.Errorf(400, "InvalidArgument", "The XML document is malformed: %v", err)
	}
	return nil
}

// parse decodes stored inner XML into v.
func parse(inner string, v any) error { return xml.Unmarshal([]byte("<c>"+inner+"</c>"), v) }

// el writes <name>v</name>, escaped.
func el(b *strings.Builder, name string, v any) {
	b.WriteString("<" + name + ">")
	xml.EscapeText(b, []byte(fmt.Sprint(v)))
	b.WriteString("</" + name + ">")
}

func newID(prefix string) string { return prefix + core.RandString(13, core.UpperNum) }

func stamp(t time.Time) string { return t.UTC().Format(core.ISO) }

func invalid(format string, a ...any) error { return core.Errorf(400, "InvalidArgument", format, a...) }

// ifMatch checks the If-Match header against a resource's ETag.
func ifMatch(r *http.Request, etag string) error {
	switch r.Header.Get("If-Match") {
	case "":
		return core.Errorf(400, "InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.")
	case etag:
		return nil
	}
	return core.Errorf(412, "PreconditionFailed", "The precondition given in one or more of the request header fields evaluated to false.")
}

// list renders a page of a CloudFront list. The markers are offsets.
func list(r *http.Request, items []string) string {
	q := r.URL.Query()
	n, _ := strconv.Atoi(q.Get("MaxItems"))
	if n <= 0 {
		n = 100
	}
	start, _ := strconv.Atoi(q.Get("Marker"))
	start = min(max(start, 0), len(items))
	end := min(start+n, len(items))
	var b strings.Builder
	el(&b, "Marker", q.Get("Marker"))
	if end < len(items) {
		el(&b, "NextMarker", end)
	}
	el(&b, "MaxItems", n)
	el(&b, "IsTruncated", end < len(items))
	el(&b, "Quantity", end-start)
	if end > start {
		b.WriteString("<Items>" + strings.Join(items[start:end], "") + "</Items>")
	}
	return b.String()
}

// setTags adds tags to a resource; lock held.
func (s *Service) setTags(arn string, tags []tag) {
	if len(tags) == 0 {
		return
	}
	if s.st.Tags[arn] == nil {
		s.st.Tags[arn] = map[string]string{}
	}
	for _, t := range tags {
		s.st.Tags[arn][t.Key] = t.Value
	}
}

// inUse reports whether a distribution or key group of the account refers
// to id (or ARN): an element's whole value, or its last path segment
// (origin-access-identity/cloudfront/<id>); lock held.
func (s *Service) inUse(account, id string) bool {
	refers := func(doc string) bool { return strings.Contains(doc, ">"+id+"<") || strings.Contains(doc, "/"+id+"<") }
	for _, d := range s.st.Distributions {
		if d.Account == account && refers(d.Config) {
			return true
		}
	}
	for _, it := range s.st.Items {
		if it.Account == account && it.Kind == keyGroups.Elem && refers(it.Config) {
			return true
		}
	}
	return false
}

// ---- distributions ----

// distConfig is what pogi reads from a DistributionConfig.
type distConfig struct {
	CallerReference   string
	Aliases           []string `xml:"Aliases>Items>CNAME"`
	DefaultRootObject string
	Enabled           bool
	Comment           string
	ViewerCertificate struct{ IAMCertificateId string }
	Origins           []origin        `xml:"Origins>Items>Origin"`
	OriginGroups      []originGroup   `xml:"OriginGroups>Items>OriginGroup"`
	Default           behavior        `xml:"DefaultCacheBehavior"`
	Behaviors         []behavior      `xml:"CacheBehaviors>Items>CacheBehavior"`
	ErrorResponses    []errorResponse `xml:"CustomErrorResponses>Items>CustomErrorResponse"`
}

type origin struct {
	Id, DomainName, OriginPath, OriginAccessControlId string
	ConnectionAttempts                                int
	Headers                                           []struct{ HeaderName, HeaderValue string } `xml:"CustomHeaders>Items>OriginCustomHeader"`
	S3                                                *struct{ OriginAccessIdentity string }     `xml:"S3OriginConfig"`
	Custom                                            *struct {
		HTTPPort, HTTPSPort, OriginReadTimeout int
		OriginProtocolPolicy                   string
	} `xml:"CustomOriginConfig"`
}

type originGroup struct {
	Id      string
	Codes   []int    `xml:"FailoverCriteria>StatusCodes>Items>StatusCode"`
	Members []string `xml:"Members>Items>OriginGroupMember>OriginId"`
}

type behavior struct {
	PathPattern, TargetOriginId                                   string
	Methods                                                       []string `xml:"AllowedMethods>Items>Method"`
	CachedMethods                                                 []string `xml:"AllowedMethods>CachedMethods>Items>Method"`
	CachePolicyId, OriginRequestPolicyId, ResponseHeadersPolicyId string
	KeyGroups                                                     []string                                  `xml:"TrustedKeyGroups>Items>KeyGroup"`
	Functions                                                     []struct{ FunctionARN, EventType string } `xml:"FunctionAssociations>Items>FunctionAssociation"`
	MinTTL, DefaultTTL, MaxTTL                                    *int64
	Forwarded                                                     *forwarded `xml:"ForwardedValues"`
}

type errorResponse struct {
	ErrorCode          int
	ResponsePagePath   string
	ResponseCode       string
	ErrorCachingMinTTL *int64
}

// configDefaults are the elements CloudFront fills in when a
// DistributionConfig lacks them. Clients read some back without nil checks
// (Terraform: OriginGroups).
var configDefaults = []struct{ name, xml string }{
	{"Aliases", "<Aliases><Quantity>0</Quantity></Aliases>"},
	{"DefaultRootObject", "<DefaultRootObject></DefaultRootObject>"},
	{"OriginGroups", "<OriginGroups><Quantity>0</Quantity></OriginGroups>"},
	{"CacheBehaviors", "<CacheBehaviors><Quantity>0</Quantity></CacheBehaviors>"},
	{"CustomErrorResponses", "<CustomErrorResponses><Quantity>0</Quantity></CustomErrorResponses>"},
	{"Logging", "<Logging><Enabled>false</Enabled><IncludeCookies>false</IncludeCookies><Bucket></Bucket><Prefix></Prefix></Logging>"},
	{"PriceClass", "<PriceClass>PriceClass_All</PriceClass>"},
	{"ViewerCertificate", "<ViewerCertificate><CloudFrontDefaultCertificate>true</CloudFrontDefaultCertificate><SSLSupportMethod>vip</SSLSupportMethod>" +
		"<MinimumProtocolVersion>TLSv1</MinimumProtocolVersion><CertificateSource>cloudfront</CertificateSource></ViewerCertificate>"},
	{"Restrictions", "<Restrictions><GeoRestriction><RestrictionType>none</RestrictionType><Quantity>0</Quantity></GeoRestriction></Restrictions>"},
	{"WebACLId", "<WebACLId></WebACLId>"},
	{"HttpVersion", "<HttpVersion>http2</HttpVersion>"},
	{"IsIPV6Enabled", "<IsIPV6Enabled>false</IsIPV6Enabled>"},
	{"Staging", "<Staging>false</Staging>"},
}

// withDefaults adds the configDefaults a DistributionConfig lacks.
func withDefaults(cfg string) string {
	have := map[string]bool{}
	d := xml.NewDecoder(strings.NewReader("<c>" + cfg + "</c>"))
	for depth := 0; ; {
		t, err := d.Token()
		if err != nil {
			break
		}
		switch t := t.(type) {
		case xml.StartElement:
			if depth++; depth == 2 {
				have[t.Name.Local] = true
			}
		case xml.EndElement:
			depth--
		}
	}
	for _, e := range configDefaults {
		if !have[e.name] {
			cfg += e.xml
		}
	}
	for _, n := range []string{"DefaultCacheBehavior", "CacheBehavior"} {
		cfg = addMissing(cfg, n, behaviorDefaults)
	}
	cfg = addMissing(cfg, "AllowedMethods", []element{{"CachedMethods", "<CachedMethods>" + methodsXML + "</CachedMethods>"}})
	return addMissing(cfg, "CustomOriginConfig", originDefaults)
}

type element struct{ name, xml string }

const methodsXML = "<Quantity>2</Quantity><Items><Method>GET</Method><Method>HEAD</Method></Items>"

// behaviorDefaults are the elements CloudFront adds to a cache behavior that lacks them
// (floci #3067); Terraform and the SDKs read them back without nil checks.
var behaviorDefaults = []element{
	{"TrustedSigners", "<TrustedSigners><Enabled>false</Enabled><Quantity>0</Quantity></TrustedSigners>"},
	{"AllowedMethods", "<AllowedMethods>" + methodsXML + "<CachedMethods>" + methodsXML + "</CachedMethods></AllowedMethods>"},
	{"SmoothStreaming", "<SmoothStreaming>false</SmoothStreaming>"},
	{"Compress", "<Compress>false</Compress>"},
	{"LambdaFunctionAssociations", "<LambdaFunctionAssociations><Quantity>0</Quantity></LambdaFunctionAssociations>"},
	{"FunctionAssociations", "<FunctionAssociations><Quantity>0</Quantity></FunctionAssociations>"},
	{"FieldLevelEncryptionId", "<FieldLevelEncryptionId></FieldLevelEncryptionId>"},
}

// originDefaults are CloudFront's documented defaults for a custom origin (floci #3221).
// An origin sent without OriginSslProtocols reads back with TLSv1.2 only.
var originDefaults = []element{
	{"OriginKeepaliveTimeout", "<OriginKeepaliveTimeout>5</OriginKeepaliveTimeout>"},
	{"OriginReadTimeout", "<OriginReadTimeout>30</OriginReadTimeout>"},
	{"OriginSslProtocols", "<OriginSslProtocols><Quantity>1</Quantity><Items><SslProtocol>TLSv1.2</SslProtocol></Items></OriginSslProtocols>"},
}

// addMissing appends the defaults an element named parent lacks, for every such element.
func addMissing(cfg, parent string, defaults []element) string {
	const wrap = len("<c>")
	d := xml.NewDecoder(strings.NewReader("<c>" + cfg + "</c>"))
	type open struct {
		depth int
		have  map[string]bool
	}
	var stack []open
	type insert struct {
		at  int
		xml string
	}
	var inserts []insert
	depth := 0
	for {
		t, err := d.Token()
		if err != nil {
			break
		}
		switch t := t.(type) {
		case xml.StartElement:
			depth++
			for i := range stack {
				if stack[i].depth == depth-1 {
					stack[i].have[t.Name.Local] = true
				}
			}
			if t.Name.Local == parent {
				stack = append(stack, open{depth, map[string]bool{}})
			}
		case xml.EndElement:
			if n := len(stack); n > 0 && stack[n-1].depth == depth && t.Name.Local == parent {
				var add string
				for _, e := range defaults {
					if !stack[n-1].have[e.name] {
						add += e.xml
					}
				}
				if at := int(d.InputOffset()) - len("</"+parent+">") - wrap; add != "" && at >= 0 && strings.HasPrefix(cfg[at:], "</"+parent+">") {
					inserts = append(inserts, insert{at, add}) // skips a self-closed element, which has no end tag
				}
				stack = stack[:n-1]
			}
			depth--
		}
	}
	for i := len(inserts) - 1; i >= 0; i-- {
		cfg = cfg[:inserts[i].at] + inserts[i].xml + cfg[inserts[i].at:]
	}
	return cfg
}

// dist finds a distribution of the caller; lock held.
func (s *Service) dist(c *core.Ctx, id string) (*Distribution, error) {
	if d := s.st.Distributions[id]; d != nil && d.Account == c.Account {
		return d, nil
	}
	return nil, core.Errorf(404, "NoSuchDistribution", "The specified distribution does not exist.")
}

func (d *Distribution) domain() string { return strings.ToLower(d.Id) + "." + edgeBase }

func (d *Distribution) xml(status string) string {
	var b strings.Builder
	el(&b, "Id", d.Id)
	el(&b, "ARN", d.ARN)
	el(&b, "Status", status)
	el(&b, "LastModifiedTime", stamp(d.Modified))
	el(&b, "InProgressInvalidationBatches", 0)
	el(&b, "DomainName", d.domain())
	// ponytail: no signed URLs, so no key groups or signers are ever active
	b.WriteString("<ActiveTrustedSigners><Enabled>false</Enabled><Quantity>0</Quantity></ActiveTrustedSigners>")
	b.WriteString("<ActiveTrustedKeyGroups><Enabled>false</Enabled><Quantity>0</Quantity></ActiveTrustedKeyGroups>")
	b.WriteString("<DistributionConfig>" + d.Config + "</DistributionConfig>")
	return b.String()
}

// check validates a DistributionConfig of the caller's; self is the
// distribution being updated; lock held.
func (s *Service) check(c *core.Ctx, raw, self string) (*distConfig, error) {
	var cfg distConfig
	if err := parse(raw, &cfg); err != nil {
		return nil, invalid("The DistributionConfig is malformed: %v", err)
	}
	if cfg.CallerReference == "" || len(cfg.Origins) == 0 || cfg.Default.TargetOriginId == "" {
		return nil, invalid("A distribution needs a CallerReference, origins and a default cache behavior.")
	}
	for _, d := range s.st.Distributions {
		var other distConfig
		if d.Id == self || parse(d.Config, &other) != nil {
			continue
		}
		if d.Account == c.Account && other.CallerReference == cfg.CallerReference {
			return nil, core.Errorf(409, "DistributionAlreadyExists", "The caller reference that you are using to create a distribution is associated with another distribution: %s", d.Id)
		}
		for _, a := range cfg.Aliases {
			if slices.Contains(other.Aliases, a) {
				return nil, core.Errorf(409, "CNAMEAlreadyExists", "One or more of the CNAMEs you provided are already associated with a different resource: %s", a)
			}
		}
	}
	if id := cfg.ViewerCertificate.IAMCertificateId; id != "" { // ACM certificates are not checked
		if iam, ok := s.hub.Service("iam").(interface{ ServerCertIDExists(account, id string) bool }); ok && !iam.ServerCertIDExists(c.Account, id) {
			return nil, core.Errorf(400, "InvalidViewerCertificate", "The specified SSL certificate doesn't exist, isn't in us-east-1 region, isn't valid, or doesn't include a valid certificate chain.")
		}
	}
	ids := map[string]bool{}
	for _, o := range cfg.Origins {
		ids[o.Id] = true
		if o.OriginAccessControlId != "" && s.item(c.Account, accessControls, o.OriginAccessControlId) == nil {
			return nil, core.Errorf(400, "InvalidOriginAccessControl", "The origin access control %s does not exist.", o.OriginAccessControlId)
		}
		if o.S3 != nil && o.S3.OriginAccessIdentity != "" &&
			s.item(c.Account, accessIdentities, strings.TrimPrefix(o.S3.OriginAccessIdentity, "origin-access-identity/cloudfront/")) == nil {
			return nil, core.Errorf(400, "InvalidOriginAccessIdentity", "The origin access identity %s does not exist.", o.S3.OriginAccessIdentity)
		}
	}
	for _, g := range cfg.OriginGroups {
		ids[g.Id] = true
	}
	for _, b := range append(cfg.Behaviors, cfg.Default) {
		if !ids[b.TargetOriginId] {
			return nil, core.Errorf(404, "NoSuchOrigin", "No origin exists with the specified Origin Id: %s", b.TargetOriginId)
		}
		for _, ref := range []struct {
			k  *kind
			id string
		}{{cachePolicies, b.CachePolicyId}, {requestPolicies, b.OriginRequestPolicyId}, {headersPolicies, b.ResponseHeadersPolicyId}} {
			if ref.id != "" && s.item(c.Account, ref.k, ref.id) == nil {
				return nil, core.Errorf(404, ref.k.Miss, "The %s %s does not exist.", ref.k.Elem, ref.id)
			}
		}
		for _, g := range b.KeyGroups {
			if s.item(c.Account, keyGroups, g) == nil {
				return nil, core.Errorf(400, "TrustedKeyGroupDoesNotExist", "The key group %s does not exist.", g)
			}
		}
		for _, f := range b.Functions {
			if fn := s.st.Functions[f.FunctionARN]; fn == nil || fn.Account != c.Account || fn.Live == nil {
				return nil, core.Errorf(400, "InvalidFunctionAssociation", "The function %s does not exist or is not published.", f.FunctionARN)
			}
			if f.EventType != "viewer-request" && f.EventType != "viewer-response" {
				return nil, core.Errorf(400, "InvalidFunctionAssociation", "CloudFront Functions run on viewer-request and viewer-response, not %s.", f.EventType)
			}
		}
	}
	for _, e := range cfg.ErrorResponses {
		if e.ResponsePagePath != "" && !strings.HasPrefix(e.ResponsePagePath, "/") {
			return nil, core.Errorf(400, "InvalidRelativePath", "The response page path must begin with a slash: %s", e.ResponsePagePath)
		}
	}
	return &cfg, nil
}

func (s *Service) createDistribution(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	var in struct {
		Raw    string `xml:",innerxml"`
		Config *inner `xml:"DistributionConfig"` // CreateDistributionWithTags
		Tags   []tag  `xml:"Tags>Items>Tag"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	cfg := in.Raw
	if in.Config != nil {
		cfg = in.Config.XML
	}
	cfg = withDefaults(cfg)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.check(c, cfg, ""); err != nil {
		return nil, err
	}
	d := &Distribution{Id: newID("E"), Account: c.Account, ETag: newID("E"), Config: cfg, Modified: time.Now()}
	d.ARN = c.GlobalARN("cloudfront", "distribution/"+d.Id)
	s.st.Distributions[d.Id] = d
	s.setTags(d.ARN, in.Tags)
	w.Header().Set("Location", "https://cloudfront.amazonaws.com"+api+"distribution/"+d.Id)
	w.Header().Set("ETag", d.ETag)
	return doc{"Distribution", d.xml("InProgress")}, nil
}

// getDistribution serves GetDistribution and GetDistributionConfig. The
// status is Deployed as soon as anyone looks.
func (s *Service) getDistribution(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.dist(c, r.PathValue("Id"))
	if err != nil {
		return nil, err
	}
	w.Header().Set("ETag", d.ETag)
	if strings.HasSuffix(r.URL.Path, "/config") {
		return doc{"DistributionConfig", d.Config}, nil
	}
	return doc{"Distribution", d.xml("Deployed")}, nil
}

func (s *Service) updateDistribution(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	var in inner
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.dist(c, r.PathValue("Id"))
	if err != nil {
		return nil, err
	}
	if err := ifMatch(r, d.ETag); err != nil {
		return nil, err
	}
	raw := withDefaults(in.XML)
	cfg, err := s.check(c, raw, d.Id)
	if err != nil {
		return nil, err
	}
	var old distConfig
	if parse(d.Config, &old); old.CallerReference != cfg.CallerReference {
		return nil, core.Errorf(400, "IllegalUpdate", "The CallerReference of a distribution cannot change.")
	}
	d.Config, d.ETag, d.Modified = raw, newID("E"), time.Now()
	w.Header().Set("ETag", d.ETag)
	return doc{"Distribution", d.xml("InProgress")}, nil
}

func (s *Service) deleteDistribution(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.dist(c, r.PathValue("Id"))
	if err != nil {
		return nil, err
	}
	if err := ifMatch(r, d.ETag); err != nil {
		return nil, err
	}
	var cfg distConfig
	if parse(d.Config, &cfg); cfg.Enabled {
		return nil, core.Errorf(409, "DistributionNotDisabled", "The distribution you are trying to delete has not been disabled.")
	}
	delete(s.st.Distributions, d.Id)
	delete(s.st.Tags, d.ARN)
	s.purge(d.Id, []string{"*"})
	return nil, nil
}

func (s *Service) listDistributions(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var items []string
	for _, id := range core.SortedKeys(s.st.Distributions) {
		if d := s.st.Distributions[id]; d.Account == c.Account {
			var b strings.Builder
			for _, f := range [][2]string{{"Id", d.Id}, {"ARN", d.ARN}, {"ETag", d.ETag}, {"Status", "Deployed"},
				{"LastModifiedTime", stamp(d.Modified)}, {"DomainName", d.domain()}} {
				el(&b, f[0], f[1])
			}
			items = append(items, "<DistributionSummary>"+b.String()+d.Config+"</DistributionSummary>") // extra config fields are skipped by clients
		}
	}
	return doc{"DistributionList", list(r, items)}, nil
}

// ---- invalidations ----

func (inv *Invalidation) xml(status string) string {
	var b strings.Builder
	el(&b, "Id", inv.Id)
	el(&b, "Status", status)
	el(&b, "CreateTime", stamp(inv.Created))
	b.WriteString("<InvalidationBatch>" + inv.Batch + "</InvalidationBatch>")
	return b.String()
}

func (s *Service) createInvalidation(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	var in struct {
		Raw       string   `xml:",innerxml"`
		CallerRef string   `xml:"CallerReference"`
		Paths     []string `xml:"Paths>Items>Path"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	if len(in.Paths) == 0 || in.CallerRef == "" {
		return nil, invalid("An invalidation batch needs a CallerReference and at least one path.")
	}
	for _, p := range in.Paths {
		if !strings.HasPrefix(p, "/") {
			return nil, invalid("Invalidation paths must begin with a slash: %s", p)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.dist(c, r.PathValue("Id"))
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(d.Invalidations, func(inv *Invalidation) bool { return inv.CallerRef == in.CallerRef })
	if i < 0 { // the same caller reference again returns the first invalidation
		d.Invalidations = append(d.Invalidations, &Invalidation{Id: newID("I"), Created: time.Now(), CallerRef: in.CallerRef, Batch: in.Raw})
		i = len(d.Invalidations) - 1
		s.purge(d.Id, in.Paths)
	}
	inv := d.Invalidations[i]
	w.Header().Set("Location", "https://cloudfront.amazonaws.com"+api+"distribution/"+d.Id+"/invalidation/"+inv.Id)
	return doc{"Invalidation", inv.xml("InProgress")}, nil
}

func (s *Service) getInvalidation(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.dist(c, r.PathValue("Id"))
	if err != nil {
		return nil, err
	}
	for _, inv := range d.Invalidations {
		if inv.Id == r.PathValue("Inv") {
			return doc{"Invalidation", inv.xml("Completed")}, nil
		}
	}
	return nil, core.Errorf(404, "NoSuchInvalidation", "The specified invalidation does not exist.")
}

func (s *Service) listInvalidations(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.dist(c, r.PathValue("Id"))
	if err != nil {
		return nil, err
	}
	var items []string
	for _, inv := range slices.Backward(d.Invalidations) { // newest first
		var b strings.Builder
		el(&b, "Id", inv.Id)
		el(&b, "CreateTime", stamp(inv.Created))
		el(&b, "Status", "Completed")
		items = append(items, "<InvalidationSummary>"+b.String()+"</InvalidationSummary>")
	}
	return doc{"InvalidationList", list(r, items)}, nil
}

// monitoring stores a distribution's monitoring subscription as metadata.
// Creating one again replaces it, as Terraform's updates expect.
func (s *Service) monitoring(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	var in inner
	if r.Method == http.MethodPost {
		if err := decode(r, &in); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.dist(c, r.PathValue("Id"))
	if err != nil {
		return nil, err
	}
	switch {
	case r.Method == http.MethodPost:
		d.Monitoring = in.XML
	case d.Monitoring == "":
		return nil, core.Errorf(404, "NoSuchMonitoringSubscription", "A monitoring subscription does not exist for the specified distribution.")
	case r.Method == http.MethodDelete:
		d.Monitoring = ""
		return nil, nil
	}
	return doc{"MonitoringSubscription", d.Monitoring}, nil
}

// ---- tags ----

// tags serves ListTagsForResource, TagResource and UntagResource for
// distributions and functions.
func (s *Service) tags(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	var in struct {
		Tags []tag    `xml:"Items>Tag"`
		Keys []string `xml:"Items>Key"`
	}
	if r.Method == http.MethodPost {
		if err := decode(r, &in); err != nil {
			return nil, err
		}
	}
	arn := r.URL.Query().Get("Resource")
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _, account, res, _ := core.ParseARN(arn)
	typ, id, _ := strings.Cut(res, "/")
	d, f := s.st.Distributions[id], s.st.Functions[arn]
	if account != c.Account || !(typ == "distribution" && d != nil && d.ARN == arn || typ == "function" && f != nil) {
		return nil, core.Errorf(404, "NoSuchResource", "The specified resource does not exist: %s", arn)
	}
	switch c.Op {
	case "TagResource":
		s.setTags(arn, in.Tags)
		return nil, nil
	case "UntagResource":
		for _, k := range in.Keys {
			delete(s.st.Tags[arn], k)
		}
		return nil, nil
	}
	var b strings.Builder
	for _, k := range core.SortedKeys(s.st.Tags[arn]) {
		b.WriteString("<Tag>")
		el(&b, "Key", k)
		el(&b, "Value", s.st.Tags[arn][k])
		b.WriteString("</Tag>")
	}
	return doc{"Tags", "<Items>" + b.String() + "</Items>"}, nil
}

// ---- policies, origin access, key groups and public keys ----

// item finds a resource of kind k visible to account: managed or its own; lock held.
func (s *Service) item(account string, k *kind, id string) *Item {
	if it := managed[id]; it != nil && it.Kind == k.Elem {
		return it
	}
	if it := s.st.Items[id]; it != nil && it.Kind == k.Elem && it.Account == account {
		return it
	}
	return nil
}

func (it *Item) xml(k *kind) string {
	var b strings.Builder
	el(&b, "Id", it.Id)
	if it.Extra != "" {
		el(&b, "S3CanonicalUserId", it.Extra)
	}
	if k.Time != "" {
		el(&b, k.Time, stamp(it.Modified))
	}
	if k.Flat { // the caller wraps the config, or inlines it in a summary
		return b.String()
	}
	return b.String() + "<" + k.Elem + "Config>" + it.Config + "</" + k.Elem + "Config>"
}

func (it *Item) full(k *kind) string {
	if k.Flat {
		return it.xml(k) + "<" + k.Elem + "Config>" + it.Config + "</" + k.Elem + "Config>"
	}
	return it.xml(k)
}

// unique rejects a second resource with the same name (or caller
// reference) in the account; lock held.
func (s *Service) unique(account string, k *kind, cfg, self string) error {
	type ident struct{ Name, CallerReference string }
	var n ident
	if err := parse(cfg, &n); err != nil {
		return invalid("The %sConfig is malformed: %v", k.Elem, err)
	}
	for _, it := range s.st.Items {
		var o ident
		if it.Kind == k.Elem && it.Account == account && it.Id != self && parse(it.Config, &o) == nil &&
			cmp.Or(n.Name, n.CallerReference) != "" && cmp.Or(n.Name, n.CallerReference) == cmp.Or(o.Name, o.CallerReference) {
			return core.Errorf(409, k.Elem+"AlreadyExists", "A %s with the same name or caller reference already exists: %s", k.Elem, it.Id)
		}
	}
	return nil
}

func (s *Service) createItem(k *kind) handler {
	return func(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
		var in inner
		if err := decode(r, &in); err != nil {
			return nil, err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if err := s.unique(c.Account, k, in.XML, ""); err != nil {
			return nil, err
		}
		it := &Item{Id: core.UUID(), Kind: k.Elem, Account: c.Account, ETag: newID("E"), Config: in.XML, Modified: time.Now()}
		if k.Prefix != "" {
			it.Id = newID(k.Prefix)
		}
		if k == accessIdentities {
			it.Extra = core.RandHex(48)
		}
		s.st.Items[it.Id] = it
		w.Header().Set("Location", "https://cloudfront.amazonaws.com"+api+k.Path+"/"+it.Id)
		w.Header().Set("ETag", it.ETag)
		return doc{k.Elem, it.full(k)}, nil
	}
}

// getItem serves Get<Kind> and Get<Kind>Config.
func (s *Service) getItem(k *kind) handler {
	return func(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		it := s.item(c.Account, k, r.PathValue("Id"))
		if it == nil {
			return nil, core.Errorf(404, k.Miss, "The specified %s does not exist.", k.Elem)
		}
		w.Header().Set("ETag", it.ETag)
		if strings.HasSuffix(r.URL.Path, "/config") {
			return doc{k.Elem + "Config", it.Config}, nil
		}
		return doc{k.Elem, it.full(k)}, nil
	}
}

func (s *Service) updateItem(k *kind) handler {
	return func(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
		var in inner
		if err := decode(r, &in); err != nil {
			return nil, err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		it := s.item(c.Account, k, r.PathValue("Id"))
		switch {
		case it == nil:
			return nil, core.Errorf(404, k.Miss, "The specified %s does not exist.", k.Elem)
		case it.Account == "":
			return nil, core.Errorf(400, "IllegalUpdate", "Managed policies cannot be changed.")
		}
		if err := ifMatch(r, it.ETag); err != nil {
			return nil, err
		}
		if err := s.unique(c.Account, k, in.XML, it.Id); err != nil {
			return nil, err
		}
		it.Config, it.ETag, it.Modified = in.XML, newID("E"), time.Now()
		w.Header().Set("ETag", it.ETag)
		return doc{k.Elem, it.full(k)}, nil
	}
}

func (s *Service) deleteItem(k *kind) handler {
	return func(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		it := s.item(c.Account, k, r.PathValue("Id"))
		switch {
		case it == nil:
			return nil, core.Errorf(404, k.Miss, "The specified %s does not exist.", k.Elem)
		case it.Account == "":
			return nil, core.Errorf(400, "IllegalDelete", "Managed policies cannot be deleted.")
		}
		if err := ifMatch(r, it.ETag); err != nil {
			return nil, err
		}
		if s.inUse(c.Account, it.Id) {
			return nil, core.Errorf(409, k.InUse, "The %s is in use.", k.Elem)
		}
		delete(s.st.Items, it.Id)
		return nil, nil
	}
}

// listItems lists managed resources first, then the caller's; Type picks one of them.
func (s *Service) listItems(k *kind) handler {
	return func(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
		typ := r.URL.Query().Get("Type")
		s.mu.Lock()
		defer s.mu.Unlock()
		var its []*Item
		for _, src := range []map[string]*Item{managed, s.st.Items} {
			for _, id := range core.SortedKeys(src) {
				it := src[id]
				if it.Kind == k.Elem && (it.Account == c.Account && typ != "managed" || it.Account == "" && typ != "custom") {
					its = append(its, it)
				}
			}
		}
		var items []string
		for _, it := range its {
			sum := it.xml(k)
			switch {
			case k.Flat:
				sum += it.Config
			case k.Typed:
				sum = "<Type>" + map[bool]string{true: "managed", false: "custom"}[it.Account == ""] + "</Type><" + k.Elem + ">" + sum + "</" + k.Elem + ">"
			default:
				sum = "<" + k.Elem + ">" + sum + "</" + k.Elem + ">"
			}
			items = append(items, "<"+k.Elem+"Summary>"+sum+"</"+k.Elem+"Summary>")
		}
		return doc{k.Elem + "List", list(r, items)}, nil
	}
}

// ---- functions ----

var (
	functionName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)
	runtimes     = []string{"cloudfront-js-1.0", "cloudfront-js-2.0"}
)

func errNoFunction() error {
	return core.Errorf(404, "NoSuchFunctionExists", "The function does not exist.")
}

// function finds a function of the caller; lock held.
func (s *Service) function(c *core.Ctx, name string) (*Function, error) {
	if f := s.st.Functions[c.GlobalARN("cloudfront", "function/"+name)]; f != nil {
		return f, nil
	}
	return nil, errNoFunction()
}

// stage picks DEVELOPMENT (the default) or LIVE.
func (f *Function) stage(name string) (*Stage, error) {
	switch name {
	case "", "DEVELOPMENT":
		return f.Dev, nil
	case "LIVE":
		if f.Live != nil {
			return f.Live, nil
		}
		return nil, errNoFunction()
	}
	return nil, invalid("Stage must be DEVELOPMENT or LIVE.")
}

// summary renders a FunctionSummary of one stage; lock held.
func (s *Service) summary(f *Function, stage string) string {
	st, _ := f.stage(stage)
	status := "UNASSOCIATED"
	switch {
	case st == f.Dev && !st.Published:
		status = "UNPUBLISHED"
	case s.inUse(f.Account, f.ARN):
		status = "DEPLOYED"
	}
	var b strings.Builder
	el(&b, "Name", f.Name)
	el(&b, "Status", status)
	b.WriteString("<FunctionConfig>" + st.Config + "</FunctionConfig><FunctionMetadata>")
	el(&b, "FunctionARN", f.ARN)
	el(&b, "Stage", cmp.Or(stage, "DEVELOPMENT"))
	el(&b, "CreatedTime", stamp(f.Created))
	el(&b, "LastModifiedTime", stamp(st.Modified))
	b.WriteString("</FunctionMetadata>")
	return b.String()
}

// functionInput is the body of CreateFunction and UpdateFunction.
type functionInput struct {
	Name   string
	Config inner  `xml:"FunctionConfig"`
	Code   string `xml:"FunctionCode"`
	Tags   []tag  `xml:"Tags>Items>Tag"`
}

// stage validates the input and makes a new DEVELOPMENT stage of it.
func (in *functionInput) stage() (*Stage, error) {
	code, err := base64.StdEncoding.DecodeString(in.Code)
	if err != nil {
		return nil, invalid("FunctionCode is not valid base64.")
	}
	var cfg struct{ Runtime string }
	if parse(in.Config.XML, &cfg); !slices.Contains(runtimes, cfg.Runtime) {
		return nil, invalid("Runtime must be one of %s.", strings.Join(runtimes, ", "))
	}
	if len(code) > 10<<10 {
		return nil, core.Errorf(413, "FunctionSizeLimitExceeded", "The function is too large: %d bytes, the limit is 10 KB.", len(code))
	}
	return &Stage{Config: in.Config.XML, Code: code, ETag: newID("E"), Modified: time.Now()}, nil
}

func (s *Service) createFunction(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	var in functionInput
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	if !functionName.MatchString(in.Name) {
		return nil, invalid("Function names have 1-64 letters, digits, hyphens and underscores: %q", in.Name)
	}
	st, err := in.stage()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := &Function{Name: in.Name, ARN: c.GlobalARN("cloudfront", "function/"+in.Name), Account: c.Account, Created: time.Now(), Dev: st}
	if s.st.Functions[f.ARN] != nil {
		return nil, core.Errorf(409, "FunctionAlreadyExists", "A function with the same name already exists in this AWS account.")
	}
	s.st.Functions[f.ARN] = f
	s.setTags(f.ARN, in.Tags)
	w.Header().Set("Location", "https://cloudfront.amazonaws.com"+api+"function/"+f.ARN)
	w.Header().Set("ETag", st.ETag)
	return doc{"FunctionSummary", s.summary(f, "DEVELOPMENT")}, nil
}

// describeFunction serves DescribeFunction and GetFunction (the code).
func (s *Service) describeFunction(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	stage := r.URL.Query().Get("Stage")
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.function(c, r.PathValue("Name"))
	if err != nil {
		return nil, err
	}
	st, err := f.stage(stage)
	if err != nil {
		return nil, err
	}
	w.Header().Set("ETag", st.ETag)
	if c.Op == "GetFunction" {
		w.Header().Set("Content-Type", "application/octet-stream")
		return slices.Clone(st.Code), nil
	}
	return doc{"FunctionSummary", s.summary(f, stage)}, nil
}

func (s *Service) updateFunction(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	var in functionInput
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	st, err := in.stage()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.function(c, r.PathValue("Name"))
	if err != nil {
		return nil, err
	}
	if err := ifMatch(r, f.Dev.ETag); err != nil {
		return nil, err
	}
	f.Dev = st
	w.Header().Set("ETtag", st.ETag) // sic: UpdateFunction's header
	w.Header().Set("ETag", st.ETag)
	return doc{"FunctionSummary", s.summary(f, "DEVELOPMENT")}, nil
}

func (s *Service) publishFunction(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.function(c, r.PathValue("Name"))
	if err != nil {
		return nil, err
	}
	if err := ifMatch(r, f.Dev.ETag); err != nil {
		return nil, err
	}
	live := *f.Dev
	live.ETag, live.Modified = newID("E"), time.Now()
	f.Live, f.Dev.Published = &live, true
	return doc{"FunctionSummary", s.summary(f, "LIVE")}, nil
}

func (s *Service) deleteFunction(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.function(c, r.PathValue("Name"))
	if err != nil {
		return nil, err
	}
	if err := ifMatch(r, f.Dev.ETag); err != nil {
		return nil, err
	}
	if s.inUse(c.Account, f.ARN) {
		return nil, core.Errorf(409, "FunctionInUse", "Cannot delete a function that is associated with a distribution.")
	}
	delete(s.st.Functions, f.ARN)
	delete(s.st.Tags, f.ARN)
	return nil, nil
}

func (s *Service) listFunctions(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	stage := r.URL.Query().Get("Stage")
	if stage != "" && stage != "DEVELOPMENT" && stage != "LIVE" {
		return nil, invalid("Stage must be DEVELOPMENT or LIVE.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var items []string
	for _, arn := range core.SortedKeys(s.st.Functions) {
		if f := s.st.Functions[arn]; f.Account == c.Account && (stage != "LIVE" || f.Live != nil) {
			items = append(items, "<FunctionSummary>"+s.summary(f, stage)+"</FunctionSummary>")
		}
	}
	return doc{"FunctionList", list(r, items)}, nil
}

// testFunction runs a stage of a function on the event object given.
func (s *Service) testFunction(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	var in struct {
		Stage string
		Event string `xml:"EventObject"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	event, err := base64.StdEncoding.DecodeString(in.Event)
	if err != nil || !json.Valid(event) {
		return nil, invalid("EventObject must be a base64-encoded JSON event.")
	}
	s.mu.Lock()
	f, err := s.function(c, r.PathValue("Name"))
	var st *Stage
	if err == nil {
		st, err = f.stage(in.Stage)
	}
	if err == nil {
		err = ifMatch(r, st.ETag)
	}
	var sum string
	if err == nil {
		sum = s.summary(f, in.Stage)
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	start := time.Now()
	out, logs, err := runFunction(st.Code, event)
	var b strings.Builder
	b.WriteString("<FunctionSummary>" + sum + "</FunctionSummary>")
	el(&b, "ComputeUtilization", min(100, time.Since(start).Microseconds()/10)) // of the 1 ms CloudFront allows
	b.WriteString("<FunctionExecutionLogs>")
	for _, l := range logs {
		el(&b, "member", l)
	}
	b.WriteString("</FunctionExecutionLogs>")
	if err != nil {
		el(&b, "FunctionErrorMessage", err.Error())
	} else {
		var ev struct {
			Context struct{ EventType string }
		}
		json.Unmarshal(event, &ev)
		var probe struct{ StatusCode *int }
		json.Unmarshal(out, &probe)
		key := "request"
		if ev.Context.EventType == "viewer-response" || probe.StatusCode != nil {
			key = "response"
		}
		el(&b, "FunctionOutput", `{"`+key+`":`+string(out)+`}`)
	}
	return doc{"TestResult", b.String()}, nil
}

// ---- console ----

// Resources lists distributions, functions and the config-document resources.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	dt := core.Table{Name: "Distributions", Columns: []string{"Id", "Domain", "Enabled", "Origins", "Comment", "Account"}}
	for _, id := range core.SortedKeys(s.st.Distributions) {
		d := s.st.Distributions[id]
		var cfg distConfig
		parse(d.Config, &cfg)
		var origins []string
		for _, o := range cfg.Origins {
			origins = append(origins, o.DomainName)
		}
		dt.Rows = append(dt.Rows, []string{d.Id, d.domain(), strconv.FormatBool(cfg.Enabled), strings.Join(origins, ", "), cfg.Comment, d.Account})
	}
	ft := core.Table{Name: "Functions", Columns: []string{"ARN", "Name", "Published", "Account"}}
	for _, arn := range core.SortedKeys(s.st.Functions) {
		f := s.st.Functions[arn]
		ft.Rows = append(ft.Rows, []string{f.ARN, f.Name, strconv.FormatBool(f.Live != nil), f.Account})
	}
	it := core.Table{Name: "Policies and access", Columns: []string{"Id", "Kind", "Name", "Account"}}
	for _, id := range core.SortedKeys(s.st.Items) {
		i := s.st.Items[id]
		var n struct{ Name, Comment string }
		parse(i.Config, &n)
		it.Rows = append(it.Rows, []string{i.Id, i.Kind, cmp.Or(n.Name, n.Comment), i.Account})
	}
	return []core.Table{dt, ft, it}
}

// Inspect returns a distribution, function or policy as stored.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var v any
	switch table {
	case "Distributions":
		if d := s.st.Distributions[id]; d != nil {
			v = d
		}
	case "Functions":
		if f := s.st.Functions[id]; f != nil {
			v = f
		}
	case "Policies and access":
		if it := s.st.Items[id]; it != nil {
			v = it
		}
	}
	if v == nil {
		return nil
	}
	b, _ := json.Marshal(v)
	return json.RawMessage(b)
}

// ---- managed policies ----

// managed are the AWS managed policies, at their real ids.
var managed = func() map[string]*Item {
	// sel renders a headers, cookies or query strings config: "behavior" or "behavior:name,name".
	sel := func(config, behavior, list, spec string) string {
		b, names, _ := strings.Cut(spec, ":")
		s := "<" + config + "><" + behavior + ">" + b + "</" + behavior + ">"
		if names != "" {
			ns := strings.Split(names, ",")
			s += fmt.Sprintf("<%s><Quantity>%d</Quantity><Items><Name>%s</Name></Items></%s>", list, len(ns), strings.Join(ns, "</Name><Name>"), list)
		}
		return s + "</" + config + ">"
	}
	params := func(headers, cookies, query string) string {
		return sel("HeadersConfig", "HeaderBehavior", "Headers", headers) + sel("CookiesConfig", "CookieBehavior", "Cookies", cookies) +
			sel("QueryStringsConfig", "QueryStringBehavior", "QueryStrings", query)
	}
	cache := func(name, comment string, minTTL, def, maxTTL int, gzip, br bool, headers, cookies, query string) string {
		return fmt.Sprintf("<Comment>%s</Comment><Name>%s</Name><DefaultTTL>%d</DefaultTTL><MaxTTL>%d</MaxTTL><MinTTL>%d</MinTTL>"+
			"<ParametersInCacheKeyAndForwardedToOrigin><EnableAcceptEncodingGzip>%t</EnableAcceptEncodingGzip>"+
			"<EnableAcceptEncodingBrotli>%t</EnableAcceptEncodingBrotli>%s</ParametersInCacheKeyAndForwardedToOrigin>",
			comment, name, def, maxTTL, minTTL, gzip, br, params(headers, cookies, query))
	}
	request := func(name, comment, headers, cookies, query string) string {
		return "<Comment>" + comment + "</Comment><Name>" + name + "</Name>" + params(headers, cookies, query)
	}
	cors := func(expose string) string {
		return "<CorsConfig><AccessControlAllowOrigins><Quantity>1</Quantity><Items><Origin>*</Origin></Items></AccessControlAllowOrigins>" +
			"<AccessControlAllowHeaders><Quantity>1</Quantity><Items><Header>*</Header></Items></AccessControlAllowHeaders>" +
			"<AccessControlAllowMethods><Quantity>1</Quantity><Items><Method>ALL</Method></Items></AccessControlAllowMethods>" +
			"<AccessControlAllowCredentials>false</AccessControlAllowCredentials>" + expose + "<OriginOverride>false</OriginOverride></CorsConfig>"
	}
	headers := func(name, comment, body string) string {
		return "<Name>" + name + "</Name><Comment>" + comment + "</Comment>" + body
	}
	const (
		expose   = "<AccessControlExposeHeaders><Quantity>1</Quantity><Items><Header>*</Header></Items></AccessControlExposeHeaders>"
		security = "<SecurityHeadersConfig><XSSProtection><Override>false</Override><Protection>true</Protection><ModeBlock>true</ModeBlock></XSSProtection>" +
			"<FrameOptions><Override>false</Override><FrameOption>SAMEORIGIN</FrameOption></FrameOptions>" +
			"<ReferrerPolicy><Override>false</Override><ReferrerPolicy>strict-origin-when-cross-origin</ReferrerPolicy></ReferrerPolicy>" +
			"<ContentTypeOptions><Override>true</Override></ContentTypeOptions>" +
			"<StrictTransportSecurity><Override>false</Override><IncludeSubdomains>false</IncludeSubdomains><Preload>false</Preload>" +
			"<AccessControlMaxAgeSec>31536000</AccessControlMaxAgeSec></StrictTransportSecurity></SecurityHeadersConfig>"
		methodHeaders = "whitelist:host,origin,x-http-method-override,x-http-method,x-method-override"
	)
	m := map[string]*Item{}
	for _, p := range []struct {
		k          *kind
		id, config string
	}{
		{cachePolicies, "658327ea-f89d-4fab-a63d-7e88639e58f6", cache("Managed-CachingOptimized", "Policy with caching enabled. Supports Gzip and Brotli compression.", 1, 86400, 31536000, true, true, "none", "none", "none")},
		{cachePolicies, "4135ea2d-6df8-44a3-9df3-4b5a84be39ad", cache("Managed-CachingDisabled", "Policy with caching disabled", 0, 0, 0, false, false, "none", "none", "none")},
		{cachePolicies, "b2884449-e4de-46a7-ac36-70bc7f1ddd6d", cache("Managed-CachingOptimizedForUncompressedObjects", "Default policy when compression is disabled", 1, 86400, 31536000, false, false, "none", "none", "none")},
		{cachePolicies, "08627262-05a9-4f76-9ded-b50ca2e3a84f", cache("Managed-Elemental-MediaPackage", "Policy for Elemental MediaPackage Origin", 0, 86400, 31536000, true, false, "whitelist:origin", "none", "whitelist:aws.manifestfilter,start,end,m")},
		{cachePolicies, "2e54312d-136d-493c-8eb9-b001f22f67d2", cache("Managed-Amplify", "Policy for Amplify Origin", 2, 2, 31536000, true, true, "whitelist:Authorization,CloudFront-Viewer-Country,Host", "all", "all")},
		{cachePolicies, "83da9c7e-98b4-4e11-a168-04f0df8e2c65", cache("UseOriginCacheControlHeaders", "Policy for origins that return Cache-Control headers. Query strings are not included in the cache key.", 0, 0, 31536000, true, true, methodHeaders, "all", "none")},
		{cachePolicies, "4cc15a8a-d715-48a4-82b8-cc0b614638fe", cache("UseOriginCacheControlHeaders-QueryStrings", "Policy for origins that return Cache-Control headers and serve different content based on values present in the query string.", 0, 0, 31536000, true, true, methodHeaders, "all", "all")},
		{requestPolicies, "216adef6-5c7f-47e4-b989-5492eafa07d3", request("Managed-AllViewer", "Policy to forward all parameters in viewer requests", "allViewer", "all", "all")},
		{requestPolicies, "33f36d7e-f396-46d9-90e0-52428a34d9dc", request("Managed-AllViewerAndCloudFrontHeaders-2022-06", "Policy to forward all parameters in viewer requests and all CloudFront headers that are available at time of release",
			"allViewerAndWhitelistCloudFront:CloudFront-Viewer-Address,CloudFront-Viewer-Country,CloudFront-Viewer-City,CloudFront-Is-Mobile-Viewer,CloudFront-Is-Desktop-Viewer,CloudFront-Forwarded-Proto", "all", "all")},
		{requestPolicies, "b689b0a8-53d0-40ab-baf2-68738e2966ac", request("Managed-AllViewerExceptHostHeader", "Policy to forward all parameters in viewer requests except for the Host header", "allExcept:host", "all", "all")},
		{requestPolicies, "59781a5b-3903-41f3-afcb-af62929ccde1", request("Managed-CORS-CustomOrigin", "Policy for Custom Origin with CORS", "whitelist:origin", "none", "none")},
		{requestPolicies, "88a5eaf4-2fd4-4709-b370-b4c650ea3fcf", request("Managed-CORS-S3Origin", "Policy for S3 origin with CORS", "whitelist:origin,access-control-request-headers,access-control-request-method", "none", "none")},
		{requestPolicies, "775133bc-15f2-49f9-abea-afb2e0bf67d2", request("Managed-Elemental-MediaTailor-PersonalizedManifests", "Policy for Elemental MediaTailor Personalized Manifests", "whitelist:origin,access-control-request-headers,x-forwarded-for,access-control-request-method,user-agent", "none", "all")},
		{requestPolicies, "acba4595-bd28-49b8-b9fe-13317c0390fa", request("Managed-UserAgentRefererHeaders", "Policy to forward user-agent and referer headers to origin", "whitelist:user-agent,referer", "none", "all")},
		{headersPolicies, "60669652-455b-4ae9-85a4-c4c02393f86c", headers("Managed-SimpleCORS", "Allows all origins for simple CORS requests", cors(""))},
		{headersPolicies, "5cc3b908-e619-4b99-88e5-2cf7f45965bd", headers("Managed-CORS-With-Preflight", "Allows all origins for CORS requests, including preflight requests", cors(expose))},
		{headersPolicies, "67f7725c-6f97-4210-82d7-5512b31e9d03", headers("Managed-SecurityHeadersPolicy", "Adds a set of security headers to every response", security)},
		{headersPolicies, "e61eb60c-9c35-4d20-a928-2b84e02af89c", headers("Managed-CORS-and-SecurityHeadersPolicy", "Allows all origins for simple CORS requests and adds security headers", cors("")+security)},
		{headersPolicies, "eaab4381-ed33-4a86-88ca-d9558dc6cd63", headers("Managed-CORS-with-preflight-and-SecurityHeadersPolicy", "Allows all origins for CORS requests, including preflight requests, and adds security headers", cors(expose)+security)},
	} {
		m[p.id] = &Item{Id: p.id, Kind: p.k.Elem, ETag: "E23ZP02F085DFQ", Config: p.config, Modified: time.Date(2021, 11, 2, 0, 0, 0, 0, time.UTC)}
	}
	return m
}()
