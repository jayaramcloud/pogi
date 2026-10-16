// Package iot emulates AWS IoT Core. The control plane (REST-JSON, signing
// "iot") holds things, thing types and groups, certificates signed by a pogi
// CA, policies and their versions, principals, topic rules and tags. The data
// plane is its own service, iotdata (data.go): publish, device shadows and
// retained messages.
//
// Both share an embedded MQTT broker (mochi-mqtt): plain TCP on
// localhost:<Config.MQTTPort> (1883 under `pogi start`, opened on the first
// IoT call like ECR's registry port), and MQTT over WebSocket at /mqtt on
// pogi's own port. DescribeEndpoint answers "localhost". Every message, from
// MQTT clients or the HTTP API, goes through the shadow topics and the topic
// rules (rules.go).
package iot

import (
	"cmp"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"log/slog"
	"maps"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the IoT Core control plane; it owns the broker and the state of
// the data plane too.
type Service struct {
	hub     *core.Hub
	mux     *http.ServeMux
	mu      sync.Mutex
	st      state
	queries map[string]*query // parsed rule SQL; lock held
	ca      *x509.Certificate // pogi's IoT CA, made on first use; lock held
	caKey   *rsa.PrivateKey
	mq      *mqtt.Server
	exposed atomic.Bool // localhost:<MQTTPort> is served
}

type state struct {
	Things   map[string]*Thing            `json:"things"`   // account/region/name
	Types    map[string]*ThingType        `json:"types"`    // account/region/name
	Groups   map[string]*Group            `json:"groups"`   // account/region/name
	Members  map[string][]string          `json:"members"`  // group key → thing names
	Certs    map[string]*Cert             `json:"certs"`    // certificate id
	Policies map[string]*Policy           `json:"policies"` // account/region/name
	Rules    map[string]*Rule             `json:"rules"`    // account/region/name
	Shadows  map[string]*Shadow           `json:"shadows"`  // thing/shadow name ("" is the classic shadow)
	Tags     map[string]map[string]string `json:"tags"`     // ARN → tags
	CA       []byte                       `json:"ca,omitempty"`
	CAKey    []byte                       `json:"caKey,omitempty"`
}

// Thing is a registry thing (API shape, plus its principals).
type Thing struct {
	ThingName     string            `json:"thingName"`
	ThingId       string            `json:"thingId"`
	ThingArn      string            `json:"thingArn"`
	ThingTypeName string            `json:"thingTypeName,omitempty"`
	Attributes    map[string]string `json:"attributes"`
	Version       int64             `json:"version"`
	Principals    []Principal       `json:"principals,omitempty"`
}

// Principal is a certificate ARN or Cognito identity attached to a thing
// (API shape of ThingPrincipalObject).
type Principal struct {
	Principal          string `json:"principal"`
	ThingPrincipalType string `json:"thingPrincipalType"`
}

func (t *Thing) has(principal string) bool {
	return slices.ContainsFunc(t.Principals, func(p Principal) bool { return p.Principal == principal })
}

// ThingType is a thing type (API shape).
type ThingType struct {
	ThingTypeName       string          `json:"thingTypeName"`
	ThingTypeId         string          `json:"thingTypeId"`
	ThingTypeArn        string          `json:"thingTypeArn"`
	ThingTypeProperties json.RawMessage `json:"thingTypeProperties,omitempty"`
	ThingTypeMetadata   struct {
		Deprecated      bool       `json:"deprecated"`
		DeprecationDate *core.Time `json:"deprecationDate,omitempty"`
		CreationDate    core.Time  `json:"creationDate"`
	} `json:"thingTypeMetadata"`
}

// Group is a static thing group (API shape).
type Group struct {
	ThingGroupName       string     `json:"thingGroupName"`
	ThingGroupId         string     `json:"thingGroupId"`
	ThingGroupArn        string     `json:"thingGroupArn"`
	Version              int64      `json:"version"`
	ThingGroupProperties groupProps `json:"thingGroupProperties"`
	ThingGroupMetadata   struct {
		ParentGroupName         string       `json:"parentGroupName,omitempty"`
		RootToParentThingGroups []nameAndArn `json:"rootToParentThingGroups,omitempty"`
		CreationDate            core.Time    `json:"creationDate"`
	} `json:"thingGroupMetadata"`
}

type groupProps struct {
	ThingGroupDescription string `json:"thingGroupDescription,omitempty"`
	AttributePayload      *struct {
		Attributes map[string]string `json:"attributes"`
	} `json:"attributePayload,omitempty"`
}

type nameAndArn struct {
	GroupName string `json:"groupName"`
	GroupArn  string `json:"groupArn"`
}

// Cert is a device certificate (API shape of its description).
type Cert struct {
	CertificateArn   string    `json:"certificateArn"`
	CertificateId    string    `json:"certificateId"`
	CaCertificateId  string    `json:"caCertificateId,omitempty"`
	Status           string    `json:"status"`
	CertificatePem   string    `json:"certificatePem"`
	OwnedBy          string    `json:"ownedBy"`
	CreationDate     core.Time `json:"creationDate"`
	LastModifiedDate core.Time `json:"lastModifiedDate"`
	CustomerVersion  int       `json:"customerVersion"`
	GenerationId     string    `json:"generationId"`
	CertificateMode  string    `json:"certificateMode"`
	Validity         struct {
		NotBefore core.Time `json:"notBefore"`
		NotAfter  core.Time `json:"notAfter"`
	} `json:"validity"`
}

// Policy is an IoT policy with its versions and the targets it is attached to.
type Policy struct {
	Name     string          `json:"name"`
	Arn      string          `json:"arn"`
	Default  string          `json:"default"`
	Versions []PolicyVersion `json:"versions"`
	Next     int             `json:"next"` // the id of the next version
	Targets  []string        `json:"targets,omitempty"`
	Created  core.Time       `json:"created"`
	Modified core.Time       `json:"modified"`
}

// PolicyVersion is one version of a policy document.
type PolicyVersion struct {
	Id       string    `json:"id"`
	Document string    `json:"document"`
	Created  core.Time `json:"created"`
}

// Rule is a topic rule (API shape); actions are kept as sent.
type Rule struct {
	RuleName         string          `json:"ruleName"`
	Sql              string          `json:"sql"`
	Description      string          `json:"description"`
	CreatedAt        core.Time       `json:"createdAt"`
	Actions          json.RawMessage `json:"actions"`
	RuleDisabled     bool            `json:"ruleDisabled"`
	AwsIotSqlVersion string          `json:"awsIotSqlVersion"`
	ErrorAction      json.RawMessage `json:"errorAction,omitempty"`
}

// New creates the IoT control plane and starts its broker.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux()}
	s.Reset()
	type op struct {
		name string
		fn   func(*core.Ctx, *http.Request) (any, error)
	}
	for pattern, o := range map[string]op{
		"POST /things/{name}":                         {"CreateThing", s.createThing},
		"GET /things/{name}":                          {"DescribeThing", s.describeThing},
		"PATCH /things/{name}":                        {"UpdateThing", s.updateThing},
		"DELETE /things/{name}":                       {"DeleteThing", s.deleteThing},
		"GET /things":                                 {"ListThings", s.listThings},
		"POST /thing-types/{name}":                    {"CreateThingType", s.createType},
		"GET /thing-types/{name}":                     {"DescribeThingType", s.describeType},
		"POST /thing-types/{name}/deprecate":          {"DeprecateThingType", s.deprecateType},
		"DELETE /thing-types/{name}":                  {"DeleteThingType", s.deleteType},
		"GET /thing-types":                            {"ListThingTypes", s.listTypes},
		"POST /thing-groups/{name}":                   {"CreateThingGroup", s.createGroup},
		"GET /thing-groups/{name}":                    {"DescribeThingGroup", s.describeGroup},
		"PATCH /thing-groups/{name}":                  {"UpdateThingGroup", s.updateGroup},
		"DELETE /thing-groups/{name}":                 {"DeleteThingGroup", s.deleteGroup},
		"GET /thing-groups":                           {"ListThingGroups", s.listGroups},
		"PUT /thing-groups/addThingToThingGroup":      {"AddThingToThingGroup", s.addToGroup},
		"PUT /thing-groups/removeThingFromThingGroup": {"RemoveThingFromThingGroup", s.removeFromGroup},
		"GET /thing-groups/{name}/things":             {"ListThingsInThingGroup", s.groupThings},
		"GET /things/{name}/thing-groups":             {"ListThingGroupsForThing", s.thingGroups},
		"POST /keys-and-certificate":                  {"CreateKeysAndCertificate", s.createKeysAndCert},
		"POST /certificates":                          {"CreateCertificateFromCsr", s.certFromCSR},
		"POST /certificate/register":                  {"RegisterCertificate", s.registerCert},
		"POST /certificate/register-no-ca":            {"RegisterCertificateWithoutCA", s.registerCert},
		"GET /certificates/{id}":                      {"DescribeCertificate", s.describeCert},
		"PUT /certificates/{id}":                      {"UpdateCertificate", s.updateCert},
		"DELETE /certificates/{id}":                   {"DeleteCertificate", s.deleteCert},
		"GET /certificates":                           {"ListCertificates", s.listCerts},
		"POST /policies/{name}":                       {"CreatePolicy", s.createPolicy},
		"GET /policies/{name}":                        {"GetPolicy", s.getPolicy},
		"DELETE /policies/{name}":                     {"DeletePolicy", s.deletePolicy},
		"GET /policies":                               {"ListPolicies", s.listPolicies},
		"POST /policies/{name}/version":               {"CreatePolicyVersion", s.createVersion},
		"GET /policies/{name}/version":                {"ListPolicyVersions", s.listVersions},
		"GET /policies/{name}/version/{v}":            {"GetPolicyVersion", s.getVersion},
		"PATCH /policies/{name}/version/{v}":          {"SetDefaultPolicyVersion", s.setDefaultVersion},
		"DELETE /policies/{name}/version/{v}":         {"DeletePolicyVersion", s.deleteVersion},
		"PUT /target-policies/{name}":                 {"AttachPolicy", s.attachPolicy},
		"POST /target-policies/{name}":                {"DetachPolicy", s.attachPolicy},
		"POST /attached-policies/{target...}":         {"ListAttachedPolicies", s.attachedPolicies},
		"POST /policy-targets/{name}":                 {"ListTargetsForPolicy", s.policyTargets},
		"PUT /things/{name}/principals":               {"AttachThingPrincipal", s.attachPrincipal},
		"DELETE /things/{name}/principals":            {"DetachThingPrincipal", s.attachPrincipal},
		"GET /things/{name}/principals":               {"ListThingPrincipals", s.thingPrincipals},
		"GET /things/{name}/principals-v2":            {"ListThingPrincipalsV2", s.thingPrincipals},
		"GET /principals/things":                      {"ListPrincipalThings", s.principalThings},
		"GET /principals/things-v2":                   {"ListPrincipalThingsV2", s.principalThings},
		"GET /endpoint":                               {"DescribeEndpoint", s.describeEndpoint},
		"POST /rules/{name}":                          {"CreateTopicRule", s.createRule},
		"GET /rules/{name}":                           {"GetTopicRule", s.getRule},
		"PATCH /rules/{name}":                         {"ReplaceTopicRule", s.replaceRule},
		"DELETE /rules/{name}":                        {"DeleteTopicRule", s.deleteRule},
		"POST /rules/{name}/enable":                   {"EnableTopicRule", s.enableRule},
		"POST /rules/{name}/disable":                  {"DisableTopicRule", s.enableRule},
		"GET /rules":                                  {"ListTopicRules", s.listRules},
		"POST /tags":                                  {"TagResource", s.tagResource},
		"POST /untag":                                 {"UntagResource", s.tagResource},
		"GET /tags":                                   {"ListTagsForResource", s.listTags},
		"/":                                           {"Unknown", s.unknown},
	} {
		s.mux.HandleFunc(pattern, s.handle(o.name, o.fn))
	}
	s.mq = mqtt.New(&mqtt.Options{InlineClient: true, Logger: slog.New(slog.DiscardHandler), SysTopicResendInterval: 3600})
	s.mq.AddHook(&hook{s: s}, nil)
	s.mq.AddListener(wsListener{})
	s.mq.Serve()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "iot", Cloud: core.AWS, Title: "IoT Core", Signing: []string{"iot"}, Protocol: "rest-json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// handle runs one operation and writes its REST-JSON result or error.
func (s *Service) handle(op string, fn func(*core.Ctx, *http.Request) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := core.C(r)
		c.Op = op
		s.expose()
		var out any
		err := c.Intercept()
		if err == nil {
			out, err = fn(c, r)
		}
		if err != nil {
			core.RESTJSONError(w, c, err)
			return
		}
		if out == nil {
			out = struct{}{}
		}
		core.RESTJSON(w, c, http.StatusOK, out)
	}
}

func (s *Service) unknown(c *core.Ctx, r *http.Request) (any, error) {
	return nil, core.Errorf(404, "UnknownOperationException", "pogi does not implement this IoT request (%s %s)", r.Method, r.URL.Path)
}

// expose serves MQTT on localhost:<MQTTPort> once IoT is used, like ECR's
// registry port: a pogi that never sees IoT holds no extra port.
func (s *Service) expose() {
	p := s.hub.Cfg.MQTTPort
	if p <= 0 || !s.exposed.CompareAndSwap(false, true) {
		return
	}
	l := listeners.NewTCP(listeners.Config{ID: "tcp", Address: net.JoinHostPort(cmp.Or(s.hub.Cfg.Host, "127.0.0.1"), strconv.Itoa(p))})
	if err := s.mq.AddListener(l); err != nil {
		s.hub.Log.Warn("IoT MQTT port unavailable: MQTT over WebSocket at /mqtt still works", "port", p, "err", err)
		return
	}
	s.mq.Listeners.Serve(l.ID(), s.mq.EstablishConnection)
	s.hub.Log.Info("IoT MQTT broker", "host", "localhost:"+strconv.Itoa(p))
}

// Close stops the broker and disconnects its clients.
func (s *Service) Close() error {
	if s.exposed.Load() {
		s.hub.ClosedPort("iot", "MQTT", s.hub.Cfg.MQTTPort)
	}
	return s.mq.Close()
}

func (st *state) fill() {
	made(&st.Things)
	made(&st.Types)
	made(&st.Groups)
	made(&st.Members)
	made(&st.Certs)
	made(&st.Policies)
	made(&st.Rules)
	made(&st.Shadows)
	made(&st.Tags)
}

// made gives a nil map its empty value.
func made[M ~map[string]V, V any](m *M) {
	if *m == nil {
		*m = M{}
	}
}

// Reset clears the state and the broker's retained messages.
// ponytail: retained messages live in the broker's memory only, they are not saved.
func (s *Service) Reset() {
	s.mu.Lock()
	s.st, s.queries, s.ca, s.caKey = state{}, map[string]*query{}, nil, nil
	s.st.fill()
	s.mu.Unlock()
	if s.mq != nil {
		for topic := range s.mq.Topics.Retained.GetAll() {
			s.mq.Topics.RetainMessage(packets.Packet{TopicName: topic})
		}
	}
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores the state; with IoT resources in it, the MQTT port opens.
func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	st.fill()
	s.mu.Lock()
	s.st, s.queries, s.ca, s.caKey = st, map[string]*query{}, nil, nil
	s.mu.Unlock()
	if len(st.Things)+len(st.Rules)+len(st.Certs) > 0 {
		s.expose()
	}
	return nil
}

func key(c *core.Ctx, name string) string { return c.Account + "/" + c.Region + "/" + name }

func notFound(format string, a ...any) error {
	return core.Errorf(404, "ResourceNotFoundException", format, a...)
}

func exists(format string, a ...any) error {
	return core.Errorf(409, "ResourceAlreadyExistsException", format, a...)
}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidRequestException", format, a...)
}

func conflict(format string, a ...any) error {
	return core.Errorf(409, "DeleteConflictException", format, a...)
}

func decode(r *http.Request, v any) error {
	if b := core.Body(r); len(b) > 0 && json.Unmarshal(b, v) != nil {
		return invalid("The request body is not valid JSON.")
	}
	return nil
}

var nameRe = regexp.MustCompile(`^[a-zA-Z0-9:_-]{1,128}$`)

// ---- things ----

type attributePayload struct {
	Attributes map[string]string `json:"attributes"`
	Merge      bool              `json:"merge"`
}

func (s *Service) thing(c *core.Ctx, name string) (*Thing, error) {
	if t := s.st.Things[key(c, name)]; t != nil {
		return t, nil
	}
	return nil, notFound("Thing %s cannot be found.", name)
}

// usableType checks a thing type a thing is given; lock held.
func (s *Service) usableType(c *core.Ctx, name string) error {
	tt := s.st.Types[key(c, name)]
	switch {
	case name == "":
		return nil
	case tt == nil:
		return notFound("Thing type %s cannot be found.", name)
	case tt.ThingTypeMetadata.Deprecated:
		return invalid("Can not create new thing with deprecated thing type: %s", name)
	}
	return nil
}

func (s *Service) createThing(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		ThingTypeName    string            `json:"thingTypeName"`
		AttributePayload *attributePayload `json:"attributePayload"`
	}
	name := r.PathValue("name")
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	if !nameRe.MatchString(name) {
		return nil, invalid("Thing name %s is not valid.", name)
	}
	attrs := map[string]string{}
	if in.AttributePayload != nil && in.AttributePayload.Attributes != nil {
		attrs = in.AttributePayload.Attributes
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usableType(c, in.ThingTypeName); err != nil {
		return nil, err
	}
	t := s.st.Things[key(c, name)]
	switch {
	case t != nil && (t.ThingTypeName != in.ThingTypeName || !maps.Equal(t.Attributes, attrs)):
		return nil, exists("Thing %s already exists with different tags or attributes.", name)
	case t == nil: // an identical thing is returned as it is
		t = &Thing{ThingName: name, ThingId: core.UUID(), ThingArn: c.ARN("iot", "thing/"+name), ThingTypeName: in.ThingTypeName, Attributes: attrs, Version: 1}
		s.st.Things[key(c, name)] = t
	}
	return map[string]string{"thingName": t.ThingName, "thingArn": t.ThingArn, "thingId": t.ThingId}, nil
}

func (s *Service) describeThing(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.thing(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	v := *t
	v.Principals = nil
	return struct {
		Thing
		DefaultClientId string `json:"defaultClientId"`
	}{v, t.ThingName}, nil
}

func (s *Service) updateThing(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		ThingTypeName    string            `json:"thingTypeName"`
		RemoveThingType  bool              `json:"removeThingType"`
		ExpectedVersion  *int64            `json:"expectedVersion"`
		AttributePayload *attributePayload `json:"attributePayload"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.thing(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	if in.ExpectedVersion != nil && *in.ExpectedVersion != t.Version {
		return nil, core.Errorf(409, "VersionConflictException", "The version for thing %s does not match the expected version.", t.ThingName)
	}
	if err := s.usableType(c, in.ThingTypeName); err != nil {
		return nil, err
	}
	v := *t
	switch {
	case in.RemoveThingType:
		v.ThingTypeName = ""
	case in.ThingTypeName != "":
		v.ThingTypeName = in.ThingTypeName
	}
	if p := in.AttributePayload; p != nil {
		attrs := map[string]string{}
		if p.Merge {
			attrs = maps.Clone(t.Attributes)
		}
		for k, val := range p.Attributes {
			if val == "" && p.Merge {
				delete(attrs, k)
			} else {
				attrs[k] = val
			}
		}
		v.Attributes = attrs
	}
	v.Version++
	s.st.Things[key(c, t.ThingName)] = &v
	return nil, nil
}

// deleteThing succeeds for a thing that does not exist, as in AWS.
func (s *Service) deleteThing(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.st.Things[key(c, r.PathValue("name"))]
	if t == nil {
		return nil, nil
	}
	if v := r.URL.Query().Get("expectedVersion"); v != "" && v != strconv.FormatInt(t.Version, 10) {
		return nil, core.Errorf(409, "VersionConflictException", "The version for thing %s does not match the expected version.", t.ThingName)
	}
	if len(t.Principals) > 0 {
		return nil, invalid("Cannot delete thing %s: it still has principals attached.", t.ThingName)
	}
	delete(s.st.Things, key(c, t.ThingName))
	for g, names := range s.st.Members {
		if strings.HasPrefix(g, key(c, "")) {
			s.st.Members[g] = slices.DeleteFunc(slices.Clone(names), func(n string) bool { return n == t.ThingName })
		}
	}
	return nil, nil
}

func (s *Service) listThings(c *core.Ctx, r *http.Request) (any, error) {
	q := r.URL.Query()
	var all []Thing
	s.mu.Lock()
	for k, t := range s.st.Things {
		attr, ok := t.Attributes[q.Get("attributeName")]
		switch {
		case !strings.HasPrefix(k, key(c, "")):
		case q.Get("thingTypeName") != "" && t.ThingTypeName != q.Get("thingTypeName"):
		case q.Get("attributeName") != "" && (!ok || q.Get("attributeValue") != "" && attr != q.Get("attributeValue")):
		default:
			v := *t
			v.Principals = nil
			all = append(all, v)
		}
	}
	s.mu.Unlock()
	items, next := page(all, func(t Thing) string { return t.ThingName }, q.Get("nextToken"), q.Get("maxResults"))
	return map[string]any{"things": items, "nextToken": next}, nil
}

// ---- thing types ----

type tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// tag sets the tags of a new resource; lock held.
func (s *Service) tag(arn string, tags []tag) {
	if len(tags) == 0 {
		return
	}
	m := map[string]string{}
	for _, t := range tags {
		m[t.Key] = t.Value
	}
	s.st.Tags[arn] = m
}

func (s *Service) createType(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		ThingTypeProperties json.RawMessage `json:"thingTypeProperties"`
		Tags                []tag           `json:"tags"`
	}
	name := r.PathValue("name")
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	if !nameRe.MatchString(name) {
		return nil, invalid("Thing type name %s is not valid.", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Types[key(c, name)] != nil {
		return nil, exists("Thing type %s already exists.", name)
	}
	tt := &ThingType{ThingTypeName: name, ThingTypeId: core.UUID(), ThingTypeArn: c.ARN("iot", "thingtype/"+name), ThingTypeProperties: in.ThingTypeProperties}
	tt.ThingTypeMetadata.CreationDate = core.Time(time.Now())
	s.st.Types[key(c, name)] = tt
	s.tag(tt.ThingTypeArn, in.Tags)
	return map[string]string{"thingTypeName": name, "thingTypeArn": tt.ThingTypeArn, "thingTypeId": tt.ThingTypeId}, nil
}

func (s *Service) thingType(c *core.Ctx, name string) (*ThingType, error) {
	if tt := s.st.Types[key(c, name)]; tt != nil {
		return tt, nil
	}
	return nil, notFound("Thing type %s cannot be found.", name)
}

func (s *Service) describeType(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.thingType(c, r.PathValue("name"))
}

func (s *Service) deprecateType(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		UndoDeprecate bool `json:"undoDeprecate"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tt, err := s.thingType(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	v := *tt
	v.ThingTypeMetadata.Deprecated, v.ThingTypeMetadata.DeprecationDate = !in.UndoDeprecate, nil
	if !in.UndoDeprecate {
		now := core.Time(time.Now())
		v.ThingTypeMetadata.DeprecationDate = &now
	}
	s.st.Types[key(c, tt.ThingTypeName)] = &v
	return nil, nil
}

// ponytail: no 5-minute wait between deprecating and deleting a thing type.
func (s *Service) deleteType(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tt, err := s.thingType(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	if !tt.ThingTypeMetadata.Deprecated {
		return nil, invalid("Thing type %s must be deprecated before it is deleted.", tt.ThingTypeName)
	}
	for k, t := range s.st.Things {
		if strings.HasPrefix(k, key(c, "")) && t.ThingTypeName == tt.ThingTypeName {
			return nil, invalid("Can not delete thing type %s: things are still associated with it.", tt.ThingTypeName)
		}
	}
	delete(s.st.Types, key(c, tt.ThingTypeName))
	delete(s.st.Tags, tt.ThingTypeArn)
	return nil, nil
}

func (s *Service) listTypes(c *core.Ctx, r *http.Request) (any, error) {
	q := r.URL.Query()
	var all []ThingType
	s.mu.Lock()
	for k, tt := range s.st.Types {
		if strings.HasPrefix(k, key(c, "")) && (q.Get("thingTypeName") == "" || tt.ThingTypeName == q.Get("thingTypeName")) {
			all = append(all, *tt)
		}
	}
	s.mu.Unlock()
	items, next := page(all, func(t ThingType) string { return t.ThingTypeName }, q.Get("nextToken"), q.Get("maxResults"))
	return map[string]any{"thingTypes": items, "nextToken": next}, nil
}

// ---- thing groups ----

func (s *Service) group(c *core.Ctx, name string) (*Group, error) {
	if g := s.st.Groups[key(c, name)]; g != nil {
		return g, nil
	}
	return nil, notFound("Thing group %s cannot be found.", name)
}

func (s *Service) createGroup(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		ParentGroupName      string     `json:"parentGroupName"`
		ThingGroupProperties groupProps `json:"thingGroupProperties"`
		Tags                 []tag      `json:"tags"`
	}
	name := r.PathValue("name")
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	if !nameRe.MatchString(name) {
		return nil, invalid("Thing group name %s is not valid.", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Groups[key(c, name)] != nil {
		return nil, exists("Thing group %s already exists.", name)
	}
	g := &Group{ThingGroupName: name, ThingGroupId: core.UUID(), ThingGroupArn: c.ARN("iot", "thinggroup/"+name), Version: 1, ThingGroupProperties: in.ThingGroupProperties}
	g.ThingGroupMetadata.CreationDate = core.Time(time.Now())
	if in.ParentGroupName != "" {
		parent, err := s.group(c, in.ParentGroupName)
		if err != nil {
			return nil, err
		}
		g.ThingGroupMetadata.ParentGroupName = parent.ThingGroupName
		g.ThingGroupMetadata.RootToParentThingGroups = append(slices.Clone(parent.ThingGroupMetadata.RootToParentThingGroups),
			nameAndArn{parent.ThingGroupName, parent.ThingGroupArn})
	}
	s.st.Groups[key(c, name)] = g
	s.tag(g.ThingGroupArn, in.Tags)
	return map[string]string{"thingGroupName": name, "thingGroupArn": g.ThingGroupArn, "thingGroupId": g.ThingGroupId}, nil
}

func (s *Service) describeGroup(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	return struct {
		Group
		Status string `json:"status"`
	}{*g, "ACTIVE"}, nil
}

func (s *Service) updateGroup(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		ExpectedVersion      *int64 `json:"expectedVersion"`
		ThingGroupProperties struct {
			groupProps
			AttributePayload *attributePayload `json:"attributePayload"`
		} `json:"thingGroupProperties"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	if in.ExpectedVersion != nil && *in.ExpectedVersion != g.Version {
		return nil, core.Errorf(409, "VersionConflictException", "The version for thing group %s does not match the expected version.", g.ThingGroupName)
	}
	v := *g
	v.ThingGroupProperties.ThingGroupDescription = in.ThingGroupProperties.ThingGroupDescription
	if p := in.ThingGroupProperties.AttributePayload; p != nil {
		attrs := map[string]string{}
		if old := g.ThingGroupProperties.AttributePayload; p.Merge && old != nil {
			attrs = maps.Clone(old.Attributes)
		}
		maps.Copy(attrs, p.Attributes)
		v.ThingGroupProperties.AttributePayload = &struct {
			Attributes map[string]string `json:"attributes"`
		}{attrs}
	}
	v.Version++
	s.st.Groups[key(c, g.ThingGroupName)] = &v
	return map[string]int64{"version": v.Version}, nil
}

// deleteGroup succeeds for a group that does not exist, as in AWS.
func (s *Service) deleteGroup(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.st.Groups[key(c, r.PathValue("name"))]
	if g == nil {
		return nil, nil
	}
	for k, child := range s.st.Groups {
		if strings.HasPrefix(k, key(c, "")) && child.ThingGroupMetadata.ParentGroupName == g.ThingGroupName {
			return nil, invalid("Cannot delete thing group %s: it still has child groups.", g.ThingGroupName)
		}
	}
	delete(s.st.Groups, key(c, g.ThingGroupName))
	delete(s.st.Members, key(c, g.ThingGroupName))
	delete(s.st.Tags, g.ThingGroupArn)
	return nil, nil
}

// children are the groups under group, recursively; lock held.
func (s *Service) children(c *core.Ctx, group string) []*Group {
	var out []*Group
	for k, g := range s.st.Groups {
		if strings.HasPrefix(k, key(c, "")) && slices.ContainsFunc(g.ThingGroupMetadata.RootToParentThingGroups, func(p nameAndArn) bool { return p.GroupName == group }) {
			out = append(out, g)
		}
	}
	return out
}

func (s *Service) listGroups(c *core.Ctx, r *http.Request) (any, error) {
	q := r.URL.Query()
	parent, prefix, recursive := q.Get("parentGroup"), q.Get("namePrefixFilter"), q.Get("recursive") != "false"
	var all []nameAndArn
	s.mu.Lock()
	for k, g := range s.st.Groups {
		md := g.ThingGroupMetadata
		under := parent == "" || md.ParentGroupName == parent ||
			recursive && slices.ContainsFunc(md.RootToParentThingGroups, func(p nameAndArn) bool { return p.GroupName == parent })
		if strings.HasPrefix(k, key(c, "")) && under && strings.HasPrefix(g.ThingGroupName, prefix) && (recursive || parent != "" || md.ParentGroupName == "") {
			all = append(all, nameAndArn{g.ThingGroupName, g.ThingGroupArn})
		}
	}
	s.mu.Unlock()
	items, next := page(all, func(g nameAndArn) string { return g.GroupName }, q.Get("nextToken"), q.Get("maxResults"))
	return map[string]any{"thingGroups": items, "nextToken": next}, nil
}

type membership struct {
	ThingGroupName string `json:"thingGroupName"`
	ThingGroupArn  string `json:"thingGroupArn"`
	ThingName      string `json:"thingName"`
	ThingArn       string `json:"thingArn"`
}

// members resolves the thing and group a membership request names; lock held.
func (s *Service) members(c *core.Ctx, r *http.Request) (*Thing, *Group, error) {
	var in membership
	if err := decode(r, &in); err != nil {
		return nil, nil, err
	}
	g, err := s.group(c, cmp.Or(in.ThingGroupName, last(in.ThingGroupArn)))
	if err != nil {
		return nil, nil, err
	}
	t, err := s.thing(c, cmp.Or(in.ThingName, last(in.ThingArn)))
	return t, g, err
}

func (s *Service) addToGroup(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, g, err := s.members(c, r)
	if err != nil {
		return nil, err
	}
	k := key(c, g.ThingGroupName)
	if !slices.Contains(s.st.Members[k], t.ThingName) {
		s.st.Members[k] = append(slices.Clone(s.st.Members[k]), t.ThingName)
	}
	return nil, nil
}

func (s *Service) removeFromGroup(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, g, err := s.members(c, r)
	if err != nil {
		return nil, err
	}
	k := key(c, g.ThingGroupName)
	s.st.Members[k] = slices.DeleteFunc(slices.Clone(s.st.Members[k]), func(n string) bool { return n == t.ThingName })
	return nil, nil
}

func (s *Service) groupThings(c *core.Ctx, r *http.Request) (any, error) {
	q := r.URL.Query()
	s.mu.Lock()
	g, err := s.group(c, r.PathValue("name"))
	var names []string
	if err == nil {
		names = slices.Clone(s.st.Members[key(c, g.ThingGroupName)])
		if q.Get("recursive") == "true" {
			for _, child := range s.children(c, g.ThingGroupName) {
				names = append(names, s.st.Members[key(c, child.ThingGroupName)]...)
			}
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	slices.Sort(names)
	items, next := page(slices.Compact(names), func(n string) string { return n }, q.Get("nextToken"), q.Get("maxResults"))
	return map[string]any{"things": items, "nextToken": next}, nil
}

func (s *Service) thingGroups(c *core.Ctx, r *http.Request) (any, error) {
	q := r.URL.Query()
	s.mu.Lock()
	t, err := s.thing(c, r.PathValue("name"))
	var all []nameAndArn
	for k, names := range s.st.Members {
		if g := s.st.Groups[k]; err == nil && g != nil && strings.HasPrefix(k, key(c, "")) && slices.Contains(names, t.ThingName) {
			all = append(all, nameAndArn{g.ThingGroupName, g.ThingGroupArn})
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	items, next := page(all, func(g nameAndArn) string { return g.GroupName }, q.Get("nextToken"), q.Get("maxResults"))
	return map[string]any{"thingGroups": items, "nextToken": next}, nil
}

// ---- certificates ----

// authority returns pogi's IoT CA, made on first use; lock held.
func (s *Service) authority() (*x509.Certificate, *rsa.PrivateKey, error) {
	if s.ca != nil {
		return s.ca, s.caKey, nil
	}
	if s.st.CA == nil {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return nil, nil, err
		}
		now := time.Now()
		tmpl := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: "pogi IoT CA", Organization: []string{"pogi"}},
			NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(30, 0, 0), IsCA: true, BasicConstraintsValid: true,
			KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		if err != nil {
			return nil, nil, err
		}
		s.st.CA, s.st.CAKey = der, x509.MarshalPKCS1PrivateKey(key)
	}
	ca, err := x509.ParseCertificate(s.st.CA)
	if err != nil {
		return nil, nil, err
	}
	key, err := x509.ParsePKCS1PrivateKey(s.st.CAKey)
	if err != nil {
		return nil, nil, err
	}
	s.ca, s.caKey = ca, key
	return ca, key, nil
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	return n
}

// issue signs a device certificate for pub with pogi's CA, valid until 2049 as AWS's are; lock held.
func (s *Service) issue(pub any, subject pkix.Name) ([]byte, error) {
	ca, key, err := s.authority()
	if err != nil {
		return nil, err
	}
	if subject.CommonName == "" {
		subject = pkix.Name{CommonName: "AWS IoT Certificate"}
	}
	tmpl := &x509.Certificate{SerialNumber: serial(), Subject: subject, NotBefore: time.Now().Add(-time.Minute),
		NotAfter: time.Date(2049, 12, 31, 23, 59, 59, 0, time.UTC), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	return x509.CreateCertificate(rand.Reader, tmpl, ca, pub, key)
}

// addCert records a certificate; lock held.
func (s *Service) addCert(c *core.Ctx, der []byte, status, mode string) (*Cert, error) {
	x, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, invalid("The certificate is not a valid X.509 certificate: %v", err)
	}
	sum := sha256.Sum256(der)
	id := hex.EncodeToString(sum[:])
	if s.st.Certs[id] != nil {
		return nil, exists("The certificate is already provisioned or registered")
	}
	now := core.Time(time.Now())
	cert := &Cert{CertificateArn: c.ARN("iot", "cert/"+id), CertificateId: id, Status: status, OwnedBy: c.Account,
		CertificatePem: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), CreationDate: now, LastModifiedDate: now,
		CustomerVersion: 1, GenerationId: core.UUID(), CertificateMode: mode}
	cert.Validity.NotBefore, cert.Validity.NotAfter = core.Time(x.NotBefore), core.Time(x.NotAfter)
	s.st.Certs[id] = cert
	return cert, nil
}

func active(r *http.Request) string {
	if r.URL.Query().Get("setAsActive") == "true" {
		return "ACTIVE"
	}
	return "INACTIVE"
}

func (s *Service) createKeysAndCert(c *core.Ctx, r *http.Request) (any, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048) // before the lock: it takes a while
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	der, err := s.issue(&key.PublicKey, pkix.Name{})
	if err != nil {
		return nil, err
	}
	cert, err := s.addCert(c, der, active(r), "DEFAULT")
	if err != nil {
		return nil, err
	}
	pub, _ := x509.MarshalPKIXPublicKey(&key.PublicKey)
	return map[string]any{"certificateArn": cert.CertificateArn, "certificateId": cert.CertificateId, "certificatePem": cert.CertificatePem,
		"keyPair": map[string]string{"PublicKey": string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pub})),
			"PrivateKey": string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))}}, nil
}

func (s *Service) certFromCSR(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		CertificateSigningRequest string `json:"certificateSigningRequest"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	block, _ := pem.Decode([]byte(in.CertificateSigningRequest))
	if block == nil {
		return nil, invalid("The certificate signing request is not PEM encoded.")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err == nil {
		err = csr.CheckSignature()
	}
	if err != nil {
		return nil, invalid("The certificate signing request is not valid: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	der, err := s.issue(csr.PublicKey, csr.Subject)
	if err != nil {
		return nil, err
	}
	cert, err := s.addCert(c, der, active(r), "DEFAULT")
	if err != nil {
		return nil, err
	}
	return map[string]string{"certificateArn": cert.CertificateArn, "certificateId": cert.CertificateId, "certificatePem": cert.CertificatePem}, nil
}

// registerCert takes a certificate made elsewhere (RegisterCertificate and RegisterCertificateWithoutCA).
// ponytail: the issuing CA is not checked against registered CA certificates.
func (s *Service) registerCert(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		CertificatePem string `json:"certificatePem"`
		Status         string `json:"status"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	block, _ := pem.Decode([]byte(in.CertificatePem))
	if block == nil {
		return nil, invalid("The certificate is not PEM encoded.")
	}
	status := cmp.Or(in.Status, active(r))
	if !slices.Contains(statuses, status) {
		return nil, invalid("Invalid certificate status %s.", status)
	}
	mode := "DEFAULT"
	if c.Op == "RegisterCertificateWithoutCA" {
		mode = "SNI_ONLY"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cert, err := s.addCert(c, block.Bytes, status, mode)
	if err != nil {
		return nil, err
	}
	return map[string]string{"certificateArn": cert.CertificateArn, "certificateId": cert.CertificateId}, nil
}

var statuses = []string{"ACTIVE", "INACTIVE", "REVOKED", "PENDING_TRANSFER", "REGISTER_INACTIVE", "PENDING_ACTIVATION"}

func (s *Service) cert(c *core.Ctx, id string) (*Cert, error) {
	if cert := s.st.Certs[id]; cert != nil && cert.OwnedBy == c.Account && strings.Contains(cert.CertificateArn, ":"+c.Region+":") {
		return cert, nil
	}
	return nil, notFound("The certificate %s does not exist.", id)
}

func (s *Service) describeCert(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cert, err := s.cert(c, r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	return map[string]any{"certificateDescription": *cert}, nil
}

func (s *Service) updateCert(c *core.Ctx, r *http.Request) (any, error) {
	status := r.URL.Query().Get("newStatus")
	if !slices.Contains(statuses, status) {
		return nil, invalid("Invalid certificate status %s.", status)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cert, err := s.cert(c, r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	if cert.Status == "REVOKED" && status != "REVOKED" {
		return nil, core.Errorf(406, "CertificateStateException", "Certificate %s is revoked; its status cannot change.", cert.CertificateId)
	}
	v := *cert
	v.Status, v.LastModifiedDate = status, core.Time(time.Now())
	s.st.Certs[cert.CertificateId] = &v
	return nil, nil
}

func (s *Service) deleteCert(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cert, err := s.cert(c, r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	if cert.Status == "ACTIVE" {
		return nil, core.Errorf(406, "CertificateStateException", "Certificate must be deactivated (not ACTIVE) before deletion.")
	}
	for _, t := range s.st.Things {
		if t.has(cert.CertificateArn) {
			return nil, conflict("Things must be detached before deletion (arn: %s)", cert.CertificateArn)
		}
	}
	force := r.URL.Query().Get("forceDelete") == "true"
	for k, p := range s.st.Policies {
		if !slices.Contains(p.Targets, cert.CertificateArn) {
			continue
		}
		if !force {
			return nil, conflict("Certificate policies must be detached before deletion (arn: %s)", cert.CertificateArn)
		}
		v := *p
		v.Targets = slices.DeleteFunc(slices.Clone(p.Targets), func(t string) bool { return t == cert.CertificateArn })
		s.st.Policies[k] = &v
	}
	delete(s.st.Certs, cert.CertificateId)
	return nil, nil
}

func (s *Service) listCerts(c *core.Ctx, r *http.Request) (any, error) {
	type summary struct {
		CertificateArn  string    `json:"certificateArn"`
		CertificateId   string    `json:"certificateId"`
		Status          string    `json:"status"`
		CertificateMode string    `json:"certificateMode"`
		CreationDate    core.Time `json:"creationDate"`
	}
	q := r.URL.Query()
	var all []summary
	s.mu.Lock()
	for id := range s.st.Certs {
		if cert, err := s.cert(c, id); err == nil {
			all = append(all, summary{cert.CertificateArn, cert.CertificateId, cert.Status, cert.CertificateMode, cert.CreationDate})
		}
	}
	s.mu.Unlock()
	items, next := page(all, func(x summary) string { return x.CertificateId }, q.Get("marker"), q.Get("pageSize"))
	return map[string]any{"certificates": items, "nextMarker": next}, nil
}

// ---- policies ----

func (s *Service) policy(c *core.Ctx, name string) (*Policy, error) {
	if p := s.st.Policies[key(c, name)]; p != nil {
		return p, nil
	}
	return nil, notFound("Policy %s does not exist.", name)
}

func document(r *http.Request) (string, error) {
	var in struct {
		PolicyDocument string `json:"policyDocument"`
		Tags           []tag  `json:"tags"`
	}
	if err := decode(r, &in); err != nil {
		return "", err
	}
	if !json.Valid([]byte(in.PolicyDocument)) {
		return "", core.Errorf(400, "MalformedPolicyException", "The policy document is not valid JSON.")
	}
	return in.PolicyDocument, nil
}

func (p *Policy) version(id string) *PolicyVersion {
	if i := slices.IndexFunc(p.Versions, func(v PolicyVersion) bool { return v.Id == id }); i >= 0 {
		return &p.Versions[i]
	}
	return nil
}

func (s *Service) createPolicy(c *core.Ctx, r *http.Request) (any, error) {
	doc, err := document(r)
	if err != nil {
		return nil, err
	}
	var in struct {
		Tags []tag `json:"tags"`
	}
	decode(r, &in)
	name := r.PathValue("name")
	if !nameRe.MatchString(name) {
		return nil, invalid("Policy name %s is not valid.", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Policies[key(c, name)] != nil {
		return nil, exists("Policy %s already exists.", name)
	}
	now := core.Time(time.Now())
	p := &Policy{Name: name, Arn: c.ARN("iot", "policy/"+name), Default: "1", Next: 2, Versions: []PolicyVersion{{"1", doc, now}}, Created: now, Modified: now}
	s.st.Policies[key(c, name)] = p
	s.tag(p.Arn, in.Tags)
	return map[string]string{"policyName": name, "policyArn": p.Arn, "policyDocument": doc, "policyVersionId": "1"}, nil
}

func (s *Service) getPolicy(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.policy(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	return map[string]any{"policyName": p.Name, "policyArn": p.Arn, "policyDocument": p.version(p.Default).Document, "defaultVersionId": p.Default,
		"creationDate": p.Created, "lastModifiedDate": p.Modified, "generationId": p.Default}, nil
}

func (s *Service) deletePolicy(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.policy(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	switch {
	case len(p.Targets) > 0:
		return nil, conflict("The policy cannot be deleted as the policy is attached to one or more principals (name=%s)", p.Name)
	case len(p.Versions) > 1:
		return nil, conflict("Cannot delete the policy because it has one or more policy versions attached to it (name=%s)", p.Name)
	}
	delete(s.st.Policies, key(c, p.Name))
	delete(s.st.Tags, p.Arn)
	return nil, nil
}

type policyRef struct {
	PolicyName string `json:"policyName"`
	PolicyArn  string `json:"policyArn"`
}

func (s *Service) listPolicies(c *core.Ctx, r *http.Request) (any, error) {
	q := r.URL.Query()
	var all []policyRef
	s.mu.Lock()
	for k, p := range s.st.Policies {
		if strings.HasPrefix(k, key(c, "")) {
			all = append(all, policyRef{p.Name, p.Arn})
		}
	}
	s.mu.Unlock()
	items, next := page(all, func(p policyRef) string { return p.PolicyName }, q.Get("marker"), q.Get("pageSize"))
	return map[string]any{"policies": items, "nextMarker": next}, nil
}

func (s *Service) createVersion(c *core.Ctx, r *http.Request) (any, error) {
	doc, err := document(r)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.policy(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	if len(p.Versions) >= 5 {
		return nil, core.Errorf(409, "VersionsLimitExceededException", "The policy %s already has the maximum number of versions (5)", p.Name)
	}
	now := core.Time(time.Now())
	v := *p
	id := strconv.Itoa(p.Next)
	v.Versions, v.Next, v.Modified = append(slices.Clone(p.Versions), PolicyVersion{id, doc, now}), p.Next+1, now
	isDefault := r.URL.Query().Get("setAsDefault") == "true"
	if isDefault {
		v.Default = id
	}
	s.st.Policies[key(c, p.Name)] = &v
	return map[string]any{"policyArn": p.Arn, "policyDocument": doc, "policyVersionId": id, "isDefaultVersion": isDefault}, nil
}

func (s *Service) listVersions(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.policy(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, v := range slices.Backward(p.Versions) {
		out = append(out, map[string]any{"versionId": v.Id, "isDefaultVersion": v.Id == p.Default, "createDate": v.Created})
	}
	return map[string]any{"policyVersions": out}, nil
}

// policyVersion finds a version of a policy; lock held.
func (s *Service) policyVersion(c *core.Ctx, r *http.Request) (*Policy, *PolicyVersion, error) {
	p, err := s.policy(c, r.PathValue("name"))
	if err != nil {
		return nil, nil, err
	}
	if v := p.version(r.PathValue("v")); v != nil {
		return p, v, nil
	}
	return nil, nil, notFound("Policy version %s of %s does not exist.", r.PathValue("v"), p.Name)
}

func (s *Service) getVersion(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, v, err := s.policyVersion(c, r)
	if err != nil {
		return nil, err
	}
	return map[string]any{"policyArn": p.Arn, "policyName": p.Name, "policyDocument": v.Document, "policyVersionId": v.Id,
		"isDefaultVersion": v.Id == p.Default, "creationDate": v.Created, "lastModifiedDate": v.Created, "generationId": v.Id}, nil
}

func (s *Service) setDefaultVersion(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, v, err := s.policyVersion(c, r)
	if err != nil {
		return nil, err
	}
	np := *p
	np.Default, np.Modified = v.Id, core.Time(time.Now())
	s.st.Policies[key(c, p.Name)] = &np
	return nil, nil
}

func (s *Service) deleteVersion(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, v, err := s.policyVersion(c, r)
	if err != nil {
		return nil, err
	}
	if v.Id == p.Default {
		return nil, conflict("Cannot delete the default version %s of policy %s", v.Id, p.Name)
	}
	np := *p
	np.Versions = slices.DeleteFunc(slices.Clone(p.Versions), func(x PolicyVersion) bool { return x.Id == v.Id })
	s.st.Policies[key(c, p.Name)] = &np
	return nil, nil
}

// attachPolicy attaches (AttachPolicy) or detaches (DetachPolicy) a policy to
// a target: a certificate ARN, a thing group ARN or a Cognito identity id.
func (s *Service) attachPolicy(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		Target string `json:"target"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	if in.Target == "" {
		return nil, invalid("A target is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.policy(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	v := *p
	v.Targets = slices.DeleteFunc(slices.Clone(p.Targets), func(t string) bool { return t == in.Target })
	if c.Op == "AttachPolicy" {
		v.Targets = append(v.Targets, in.Target)
	}
	s.st.Policies[key(c, p.Name)] = &v
	return nil, nil
}

func (s *Service) attachedPolicies(c *core.Ctx, r *http.Request) (any, error) {
	target, q := r.PathValue("target"), r.URL.Query()
	var all []policyRef
	s.mu.Lock()
	for k, p := range s.st.Policies {
		if strings.HasPrefix(k, key(c, "")) && slices.Contains(p.Targets, target) {
			all = append(all, policyRef{p.Name, p.Arn})
		}
	}
	s.mu.Unlock()
	items, next := page(all, func(p policyRef) string { return p.PolicyName }, q.Get("marker"), q.Get("pageSize"))
	return map[string]any{"policies": items, "nextMarker": next}, nil
}

func (s *Service) policyTargets(c *core.Ctx, r *http.Request) (any, error) {
	q := r.URL.Query()
	s.mu.Lock()
	p, err := s.policy(c, r.PathValue("name"))
	var targets []string
	if err == nil {
		targets = slices.Clone(p.Targets)
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	items, next := page(targets, func(t string) string { return t }, q.Get("marker"), q.Get("pageSize"))
	return map[string]any{"targets": items, "nextMarker": next}, nil
}

// ---- thing principals ----

// attachPrincipal attaches (AttachThingPrincipal) or detaches (DetachThingPrincipal)
// a principal: a certificate ARN or a Cognito identity id.
func (s *Service) attachPrincipal(c *core.Ctx, r *http.Request) (any, error) {
	principal := r.Header.Get("X-Amzn-Principal")
	if principal == "" {
		return nil, invalid("The x-amzn-principal header is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.thing(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	if _, _, _, res, ok := core.ParseARN(principal); ok && strings.HasPrefix(res, "cert/") && s.st.Certs[last(res)] == nil {
		return nil, notFound("The certificate %s does not exist.", last(res))
	}
	v := *t
	v.Principals = slices.DeleteFunc(slices.Clone(t.Principals), func(p Principal) bool { return p.Principal == principal })
	if c.Op == "AttachThingPrincipal" {
		v.Principals = append(v.Principals, Principal{principal, cmp.Or(r.URL.Query().Get("thingPrincipalType"), "NON_EXCLUSIVE_THING")})
	}
	s.st.Things[key(c, t.ThingName)] = &v
	return nil, nil
}

// thingPrincipals lists a thing's principals (ListThingPrincipals), with
// their types for ListThingPrincipalsV2.
func (s *Service) thingPrincipals(c *core.Ctx, r *http.Request) (any, error) {
	q := r.URL.Query()
	s.mu.Lock()
	t, err := s.thing(c, r.PathValue("name"))
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	typ := q.Get("thingPrincipalType")
	all := slices.DeleteFunc(slices.Clone(t.Principals), func(p Principal) bool { return typ != "" && p.ThingPrincipalType != typ })
	items, next := page(all, func(p Principal) string { return p.Principal }, q.Get("nextToken"), q.Get("maxResults"))
	if c.Op == "ListThingPrincipalsV2" {
		return map[string]any{"thingPrincipalObjects": items, "nextToken": next}, nil
	}
	arns := []string{}
	for _, p := range items {
		arns = append(arns, p.Principal)
	}
	return map[string]any{"principals": arns, "nextToken": next}, nil
}

// principalThings lists the things of a principal (ListPrincipalThings),
// with the attachment types for ListPrincipalThingsV2.
func (s *Service) principalThings(c *core.Ctx, r *http.Request) (any, error) {
	type object struct {
		ThingName          string `json:"thingName"`
		ThingPrincipalType string `json:"thingPrincipalType"`
	}
	principal, q := r.Header.Get("X-Amzn-Principal"), r.URL.Query()
	var all []object
	s.mu.Lock()
	for k, t := range s.st.Things {
		for _, p := range t.Principals {
			if strings.HasPrefix(k, key(c, "")) && p.Principal == principal {
				all = append(all, object{t.ThingName, p.ThingPrincipalType})
			}
		}
	}
	s.mu.Unlock()
	items, next := page(all, func(o object) string { return o.ThingName }, q.Get("nextToken"), q.Get("maxResults"))
	if c.Op == "ListPrincipalThingsV2" {
		return map[string]any{"principalThingObjects": items, "nextToken": next}, nil
	}
	names := []string{}
	for _, o := range items {
		names = append(names, o.ThingName)
	}
	return map[string]any{"things": names, "nextToken": next}, nil
}

// describeEndpoint answers the broker's host for every endpoint type
// (iot:Data-ATS, iot:Data, iot:CredentialProvider, iot:Jobs): MQTT is plain
// TCP on Config.MQTTPort, or WebSocket at /mqtt on pogi's port.
func (s *Service) describeEndpoint(c *core.Ctx, r *http.Request) (any, error) {
	return map[string]string{"endpointAddress": cmp.Or(s.hub.Cfg.Hostname, "localhost")}, nil
}

// ---- tags ----

// taggable checks that an ARN names a resource of the caller; lock held.
func (s *Service) taggable(c *core.Ctx, arn string) error {
	_, _, _, res, _ := core.ParseARN(arn)
	kind, name, _ := strings.Cut(res, "/")
	var ok bool
	switch kind {
	case "thing":
		ok = s.st.Things[key(c, name)] != nil
	case "thingtype":
		ok = s.st.Types[key(c, name)] != nil
	case "thinggroup":
		ok = s.st.Groups[key(c, name)] != nil
	case "policy":
		ok = s.st.Policies[key(c, name)] != nil
	case "rule":
		ok = s.st.Rules[key(c, name)] != nil
	}
	if !ok || !owns(c, arn) {
		return notFound("Resource %s does not exist.", arn)
	}
	return nil
}

func owns(c *core.Ctx, arn string) bool {
	_, region, account, _, _ := core.ParseARN(arn)
	return region == c.Region && account == c.Account
}

// tagResource tags (TagResource) or untags (UntagResource) a resource.
func (s *Service) tagResource(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		ResourceArn string   `json:"resourceArn"`
		Tags        []tag    `json:"tags"`
		TagKeys     []string `json:"tagKeys"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taggable(c, in.ResourceArn); err != nil {
		return nil, err
	}
	tags := maps.Clone(s.st.Tags[in.ResourceArn])
	if tags == nil {
		tags = map[string]string{}
	}
	for _, t := range in.Tags {
		tags[t.Key] = t.Value
	}
	for _, k := range in.TagKeys {
		delete(tags, k)
	}
	s.st.Tags[in.ResourceArn] = tags
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, r *http.Request) (any, error) {
	arn := r.URL.Query().Get("resourceArn")
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.taggable(c, arn); err != nil {
		return nil, err
	}
	out := []tag{}
	for _, k := range core.SortedKeys(s.st.Tags[arn]) {
		out = append(out, tag{k, s.st.Tags[arn][k]})
	}
	return map[string]any{"tags": out}, nil
}

// ---- helpers ----

// last is the part of a name or ARN after its last '/'.
func last(ref string) string { return ref[strings.LastIndexByte(ref, '/')+1:] }

// page sorts items by key and returns those after token, at most max (a query
// value; default 100), and the token of the next page.
func page[T any](items []T, key func(T) string, token, max string) ([]T, string) {
	slices.SortFunc(items, func(a, b T) int { return strings.Compare(key(a), key(b)) })
	if token != "" {
		t, _ := url.QueryUnescape(token)
		items = slices.DeleteFunc(items, func(x T) bool { return key(x) <= t })
	}
	n, _ := strconv.Atoi(max)
	if n <= 0 {
		n = 100
	}
	if len(items) > n {
		return items[:n], key(items[n-1])
	}
	if items == nil {
		items = []T{}
	}
	return items, ""
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	things := core.Table{Name: "Things", Columns: []string{"Name", "Type", "Version", "Attributes", "Principals"}}
	certs := core.Table{Name: "Certificates", Columns: []string{"Id", "Status", "Mode", "Created"}}
	policies := core.Table{Name: "Policies", Columns: []string{"Name", "Default version", "Targets"}}
	rules := core.Table{Name: "Rules", Columns: []string{"Name", "SQL", "Enabled"}}
	shadows := core.Table{Name: "Shadows", Columns: []string{"Thing/name", "Version"}}
	for _, k := range core.SortedKeys(s.st.Things) {
		t := s.st.Things[k]
		b, _ := json.Marshal(t.Attributes)
		things.Rows = append(things.Rows, []string{t.ThingName, t.ThingTypeName, strconv.FormatInt(t.Version, 10), string(b), strconv.Itoa(len(t.Principals))})
	}
	for _, k := range core.SortedKeys(s.st.Certs) {
		cert := s.st.Certs[k]
		certs.Rows = append(certs.Rows, []string{cert.CertificateId, cert.Status, cert.CertificateMode, time.Time(cert.CreationDate).UTC().Format(time.RFC3339)})
	}
	for _, k := range core.SortedKeys(s.st.Policies) {
		p := s.st.Policies[k]
		policies.Rows = append(policies.Rows, []string{p.Name, p.Default, strings.Join(p.Targets, ", ")})
	}
	for _, k := range core.SortedKeys(s.st.Rules) {
		r := s.st.Rules[k]
		rules.Rows = append(rules.Rows, []string{r.RuleName, r.Sql, strconv.FormatBool(!r.RuleDisabled)})
	}
	for _, k := range core.SortedKeys(s.st.Shadows) {
		shadows.Rows = append(shadows.Rows, []string{k, strconv.FormatInt(s.st.Shadows[k].Version, 10)})
	}
	return []core.Table{things, certs, policies, rules, shadows}
}

// Inspect returns a thing, certificate, policy, rule or shadow.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range map[string]any{"Things": s.st.Things, "Policies": s.st.Policies, "Rules": s.st.Rules} {
		if k != table {
			continue
		}
		b, _ := json.Marshal(v)
		var m map[string]json.RawMessage
		json.Unmarshal(b, &m)
		for mk, raw := range m {
			if last(mk) == id {
				return raw
			}
		}
	}
	switch table {
	case "Certificates":
		if cert := s.st.Certs[id]; cert != nil {
			return *cert
		}
	case "Shadows":
		if sh := s.st.Shadows[id]; sh != nil {
			b, _ := json.Marshal(sh.doc()) // under the lock: the document holds the shadow's maps
			return json.RawMessage(b)
		}
	}
	return nil
}
