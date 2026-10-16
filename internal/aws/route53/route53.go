// Package route53 emulates Amazon Route 53 (REST-XML, global): hosted zones
// with their SOA and NS records, record set changes, tags and health checks.
package route53

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

const xmlns = "https://route53.amazonaws.com/doc/2013-04-01/"

// Service is the Route 53 emulator.
type Service struct {
	hub *core.Hub
	mux *http.ServeMux
	mu  sync.Mutex
	st  state
}

type state struct {
	Zones  map[string]*Zone        `json:"zones"`  // zone id
	Checks map[string]*HealthCheck `json:"checks"` // health check id
	// ponytail: change ids are kept forever; cap them if anyone makes millions of changes.
	Changes map[string]time.Time `json:"changes"` // change id → submitted
}

// Zone is a hosted zone with its record sets, sorted as Route 53 lists them.
type Zone struct {
	Id          string            `json:"id"`
	Account     string            `json:"account"`
	Name        string            `json:"name"`
	CallerRef   string            `json:"callerRef"`
	Comment     string            `json:"comment,omitempty"`
	VPCs        []vpc             `json:"vpcs,omitempty"` // set for private zones
	NameServers []string          `json:"nameServers"`
	Records     []RRSet           `json:"records"`
	Tags        map[string]string `json:"tags,omitempty"`
}

// RRSet is a resource record set. Elements pogi does not interpret (Weight,
// Region, GeoLocation, Failover…) round-trip through Extra.
type RRSet struct {
	Name            string
	Type            string
	SetIdentifier   string       `xml:",omitempty"`
	TTL             *int64       `xml:",omitempty"`
	ResourceRecords []rr         `xml:"ResourceRecords>ResourceRecord,omitempty"`
	AliasTarget     *aliasTarget `xml:",omitempty"`
	Extra           []element    `xml:",any"`
}

type rr struct{ Value string }

type aliasTarget struct {
	HostedZoneId         string
	DNSName              string
	EvaluateTargetHealth bool
}

type element struct {
	XMLName xml.Name
	Inner   string `xml:",innerxml"`
}

type vpc struct {
	VPCRegion string `xml:",omitempty"`
	VPCId     string `xml:",omitempty"`
}

type tag struct{ Key, Value string }

// HealthCheck is a health check; its configuration round-trips as XML.
type HealthCheck struct {
	Id        string            `json:"id"`
	Account   string            `json:"account"`
	CallerRef string            `json:"callerRef"`
	Config    string            `json:"config"` // inner XML of HealthCheckConfig
	Tags      map[string]string `json:"tags,omitempty"`
}

// New creates the Route 53 service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux()}
	s.Reset()
	const p = "/2013-04-01/"
	s.route("POST "+p+"hostedzone", "CreateHostedZone", s.createZone)
	s.route("GET "+p+"hostedzone", "ListHostedZones", s.listZones)
	s.route("GET "+p+"hostedzone/{id}", "GetHostedZone", s.getZone)
	s.route("DELETE "+p+"hostedzone/{id}", "DeleteHostedZone", s.deleteZone)
	s.route("POST "+p+"hostedzone/{id}/rrset", "ChangeResourceRecordSets", s.changeRecords)
	s.route("POST "+p+"hostedzone/{id}/rrset/", "ChangeResourceRecordSets", s.changeRecords)
	s.route("GET "+p+"hostedzone/{id}/rrset", "ListResourceRecordSets", s.listRecords)
	s.route("GET "+p+"hostedzonesbyname", "ListHostedZonesByName", s.listZonesByName)
	s.route("GET "+p+"hostedzonecount", "GetHostedZoneCount", s.zoneCount)
	s.route("GET "+p+"change/{id}", "GetChange", s.getChange)
	s.route("POST "+p+"tags/{type}/{id}", "ChangeTagsForResource", s.changeTags)
	s.route("GET "+p+"tags/{type}/{id}", "ListTagsForResource", s.listTags)
	s.route("POST "+p+"healthcheck", "CreateHealthCheck", s.createCheck)
	s.route("GET "+p+"healthcheck", "ListHealthChecks", s.listChecks)
	s.route("GET "+p+"healthcheck/{id}", "GetHealthCheck", s.getCheck)
	s.route("DELETE "+p+"healthcheck/{id}", "DeleteHealthCheck", s.deleteCheck)
	s.route("/", "Unknown", func(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
		return nil, core.Errorf(501, "NotImplemented", "pogi does not implement Route 53 %s %s", r.Method, r.URL.Path)
	})
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "route53", Cloud: core.AWS, Title: "Route 53", Signing: []string{"route53"}, Protocol: "rest-xml"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Zones: map[string]*Zone{}, Checks: map[string]*HealthCheck{}, Changes: map[string]time.Time{}}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := state{Zones: map[string]*Zone{}, Checks: map[string]*HealthCheck{}, Changes: map[string]time.Time{}}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// route serves one operation: the handler's output becomes <Op>Response,
// with status 201 for Create operations.
func (s *Service) route(pattern, op string, h func(*core.Ctx, http.ResponseWriter, *http.Request) (any, error)) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		c := core.C(r)
		c.Op = op
		out, err := any(nil), c.Intercept()
		if err == nil {
			out, err = h(c, w, r)
		}
		if err != nil {
			e := core.AsError(err)
			core.WriteQueryError(w, c, xmlns, e.Status, e.Code, e.Msg)
			return
		}
		var b bytes.Buffer
		b.WriteString(xml.Header)
		xml.NewEncoder(&b).EncodeElement(out, xml.StartElement{Name: xml.Name{Space: xmlns, Local: op + "Response"}})
		w.Header().Set("Content-Type", "text/xml")
		w.Header().Set("x-amzn-RequestId", c.RequestID)
		w.WriteHeader(map[bool]int{true: http.StatusCreated, false: http.StatusOK}[strings.HasPrefix(op, "Create")])
		w.Write(b.Bytes())
	})
}

func decode(r *http.Request, v any) error {
	if err := xml.Unmarshal(core.Body(r), v); err != nil {
		return core.Errorf(400, "InvalidInput", "Invalid XML: %v", err)
	}
	return nil
}

// fqdn normalizes a DNS name the way Route 53 returns it.
func fqdn(name string) string { return strings.ToLower(strings.TrimSuffix(name, ".")) + "." }

// sortKey orders records like Route 53: by name with labels reversed, then type.
func sortKey(name, typ, setID string) string {
	labels := strings.Split(strings.TrimSuffix(name, "."), ".")
	slices.Reverse(labels)
	return strings.Join(labels, "\x01") + "\x00" + typ + "\x00" + setID
}

func (rs *RRSet) key() string { return sortKey(rs.Name, rs.Type, rs.SetIdentifier) }

func maxItems(v string, def int) int {
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return n
	}
	return def
}

// change records a new, already applied, change; lock held.
func (s *Service) change() changeInfo {
	id, now := "C"+core.RandString(13, core.UpperNum), time.Now().UTC()
	s.st.Changes[id] = now
	return changeInfo{"/change/" + id, "INSYNC", now.Format(core.ISO)}
}

type changeInfo struct {
	Id          string
	Status      string
	SubmittedAt string
}

type zoneXML struct {
	Id              string
	Name            string
	CallerReference string
	Config          struct {
		Comment     string `xml:",omitempty"`
		PrivateZone bool
	}
	ResourceRecordSetCount int
}

type delegationSet struct {
	NameServers []string `xml:"NameServers>NameServer"`
}

func (z *Zone) wire() zoneXML {
	x := zoneXML{Id: "/hostedzone/" + z.Id, Name: z.Name, CallerReference: z.CallerRef, ResourceRecordSetCount: len(z.Records)}
	x.Config.Comment, x.Config.PrivateZone = z.Comment, len(z.VPCs) > 0
	return x
}

func (z *Zone) delegation() *delegationSet {
	if len(z.VPCs) > 0 {
		return nil // private zones have no delegation set
	}
	return &delegationSet{z.NameServers}
}

// zone finds a hosted zone of the caller; lock held.
func (s *Service) zone(c *core.Ctx, id string) (*Zone, error) {
	id = strings.TrimPrefix(id, "/hostedzone/")
	if z := s.st.Zones[id]; z != nil && z.Account == c.Account {
		return z, nil
	}
	return nil, core.Errorf(404, "NoSuchHostedZone", "No hosted zone found with ID: %s", id)
}

// zones returns the caller's zones ordered by less; lock held.
func (s *Service) zones(c *core.Ctx, less func(a, b *Zone) int) []*Zone {
	var out []*Zone
	for _, z := range s.st.Zones {
		if z.Account == c.Account {
			out = append(out, z)
		}
	}
	slices.SortFunc(out, less)
	return out
}

func (s *Service) createZone(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	var in struct {
		Name             string
		CallerReference  string
		HostedZoneConfig struct{ Comment string }
		VPC              *vpc
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	name := fqdn(in.Name)
	if in.Name == "" || strings.Contains(name, "..") || len(name) > 255 {
		return nil, core.Errorf(400, "InvalidDomainName", "%s is not a valid domain name.", in.Name)
	}
	if in.CallerReference == "" {
		return nil, core.Errorf(400, "InvalidInput", "CallerReference is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, z := range s.st.Zones {
		if z.Account == c.Account && z.CallerRef == in.CallerReference {
			return nil, core.Errorf(409, "HostedZoneAlreadyExists", "A hosted zone has already been created with the specified caller reference.")
		}
	}
	z := &Zone{Id: "Z" + core.RandString(20, core.UpperNum), Account: c.Account, Name: name, CallerRef: in.CallerReference, Comment: in.HostedZoneConfig.Comment}
	if in.VPC != nil {
		z.VPCs = []vpc{*in.VPC}
	}
	for _, tld := range []string{"com", "net", "org", "co.uk"} {
		z.NameServers = append(z.NameServers, fmt.Sprintf("ns-%d.awsdns-%02d.%s", rand.IntN(2048), rand.IntN(64), tld))
	}
	ns := RRSet{Name: name, Type: "NS", TTL: new(int64(172800))}
	for _, n := range z.NameServers {
		ns.ResourceRecords = append(ns.ResourceRecords, rr{n + "."})
	}
	soa := RRSet{Name: name, Type: "SOA", TTL: new(int64(900)), ResourceRecords: []rr{{z.NameServers[0] + ". awsdns-hostmaster.amazon.com. 1 7200 900 1209600 86400"}}}
	z.Records = []RRSet{ns, soa}
	s.st.Zones[z.Id] = z
	w.Header().Set("Location", "https://route53.amazonaws.com/2013-04-01/hostedzone/"+z.Id)
	return &struct {
		HostedZone    zoneXML
		ChangeInfo    changeInfo
		DelegationSet *delegationSet
		VPC           *vpc
	}{z.wire(), s.change(), z.delegation(), in.VPC}, nil
}

func (s *Service) getZone(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	z, err := s.zone(c, r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	return &struct {
		HostedZone    zoneXML
		DelegationSet *delegationSet
		VPCs          []vpc `xml:"VPCs>VPC,omitempty"`
	}{z.wire(), z.delegation(), z.VPCs}, nil
}

type zoneList struct {
	HostedZones []zoneXML `xml:"HostedZones>HostedZone"`
	IsTruncated bool
	MaxItems    int
}

func (s *Service) listZones(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	q := r.URL.Query()
	out := &struct {
		zoneList
		Marker     string
		NextMarker string `xml:",omitempty"`
	}{zoneList: zoneList{MaxItems: maxItems(q.Get("maxitems"), 100)}, Marker: q.Get("marker")}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, z := range s.zones(c, func(a, b *Zone) int { return strings.Compare(a.Id, b.Id) }) {
		switch {
		case z.Id < out.Marker:
		case len(out.HostedZones) == out.MaxItems:
			if !out.IsTruncated {
				out.IsTruncated, out.NextMarker = true, z.Id
			}
		default:
			out.HostedZones = append(out.HostedZones, z.wire())
		}
	}
	return out, nil
}

func (s *Service) listZonesByName(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	q := r.URL.Query()
	out := &struct {
		zoneList
		DNSName          string `xml:",omitempty"`
		HostedZoneId     string `xml:",omitempty"`
		NextDNSName      string `xml:",omitempty"`
		NextHostedZoneId string `xml:",omitempty"`
	}{zoneList: zoneList{MaxItems: maxItems(q.Get("maxitems"), 100)}, DNSName: q.Get("dnsname"), HostedZoneId: q.Get("hostedzoneid")}
	start := ""
	if out.DNSName != "" {
		start = sortKey(fqdn(out.DNSName), "", "") + strings.TrimPrefix(out.HostedZoneId, "/hostedzone/")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	byName := func(z *Zone) string { return sortKey(z.Name, "", "") + z.Id }
	for _, z := range s.zones(c, func(a, b *Zone) int { return strings.Compare(byName(a), byName(b)) }) {
		switch {
		case byName(z) < start:
		case len(out.HostedZones) == out.MaxItems:
			if !out.IsTruncated {
				out.IsTruncated, out.NextDNSName, out.NextHostedZoneId = true, z.Name, z.Id
			}
		default:
			out.HostedZones = append(out.HostedZones, z.wire())
		}
	}
	return out, nil
}

func (s *Service) zoneCount(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &struct{ HostedZoneCount int }{len(s.zones(c, func(a, b *Zone) int { return 0 }))}, nil
}

func apex(z *Zone, rs RRSet) bool { return rs.Name == z.Name && (rs.Type == "SOA" || rs.Type == "NS") }

func (s *Service) deleteZone(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	z, err := s.zone(c, r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	for _, rs := range z.Records {
		if !apex(z, rs) {
			return nil, core.Errorf(400, "HostedZoneNotEmpty", "The specified hosted zone contains non-required resource record sets and so cannot be deleted.")
		}
	}
	delete(s.st.Zones, z.Id)
	return &struct{ ChangeInfo changeInfo }{s.change()}, nil
}

// changeRecords applies a change batch atomically.
func (s *Service) changeRecords(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	var in struct {
		Changes []struct {
			Action            string
			ResourceRecordSet RRSet
		} `xml:"ChangeBatch>Changes>Change"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	z, err := s.zone(c, r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	records, problems := slices.Clone(z.Records), []string{}
	for _, ch := range in.Changes {
		rs := ch.ResourceRecordSet
		rs.Name = fqdn(rs.Name)
		for i := range rs.Extra {
			rs.Extra[i].XMLName.Space = ""
		}
		if rs.AliasTarget != nil {
			rs.AliasTarget.DNSName = fqdn(rs.AliasTarget.DNSName)
		}
		i := slices.IndexFunc(records, func(x RRSet) bool { return x.key() == rs.key() })
		what := fmt.Sprintf("resource record set [name='%s', type='%s']", rs.Name, rs.Type)
		switch {
		case rs.Name != z.Name && !strings.HasSuffix(rs.Name, "."+z.Name):
			problems = append(problems, fmt.Sprintf("RRSet with DNS name %s is not permitted in zone %s", rs.Name, z.Name))
		case ch.Action == "CREATE" && i >= 0:
			problems = append(problems, "Tried to create "+what+" but it already exists")
		case ch.Action == "DELETE" && i < 0:
			problems = append(problems, "Tried to delete "+what+" but it was not found")
		case ch.Action == "DELETE" && apex(z, rs):
			problems = append(problems, "A HostedZone must contain at least one NS record and exactly one SOA record for the zone itself.")
		case ch.Action == "DELETE":
			records = slices.Delete(records, i, i+1)
		case ch.Action == "UPSERT" && i >= 0:
			records[i] = rs
		case ch.Action == "CREATE" || ch.Action == "UPSERT":
			records = append(records, rs)
		default:
			return nil, core.Errorf(400, "InvalidInput", "Invalid change action %q", ch.Action)
		}
	}
	if len(problems) > 0 {
		return nil, core.Errorf(400, "InvalidChangeBatch", "[%s]", strings.Join(problems, ", "))
	}
	slices.SortFunc(records, func(a, b RRSet) int { return strings.Compare(a.key(), b.key()) })
	z.Records = records
	return &struct{ ChangeInfo changeInfo }{s.change()}, nil
}

func (s *Service) listRecords(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	q := r.URL.Query()
	out := &struct {
		ResourceRecordSets   []RRSet `xml:"ResourceRecordSets>ResourceRecordSet"`
		IsTruncated          bool
		NextRecordName       string `xml:",omitempty"`
		NextRecordType       string `xml:",omitempty"`
		NextRecordIdentifier string `xml:",omitempty"`
		MaxItems             int
	}{MaxItems: maxItems(q.Get("maxitems"), 300)}
	start := ""
	if q.Get("name") != "" {
		start = sortKey(fqdn(q.Get("name")), q.Get("type"), q.Get("identifier"))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	z, err := s.zone(c, r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	for _, rs := range z.Records {
		switch {
		case rs.key() < start:
		case len(out.ResourceRecordSets) == out.MaxItems:
			if !out.IsTruncated {
				out.IsTruncated, out.NextRecordName, out.NextRecordType, out.NextRecordIdentifier = true, rs.Name, rs.Type, rs.SetIdentifier
			}
		default:
			out.ResourceRecordSets = append(out.ResourceRecordSets, rs)
		}
	}
	return out, nil
}

func (s *Service) getChange(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	id := strings.TrimPrefix(r.PathValue("id"), "/change/")
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.st.Changes[id]
	if !ok {
		return nil, core.Errorf(404, "NoSuchChange", "A change with the specified change ID does not exist.")
	}
	return &struct{ ChangeInfo changeInfo }{changeInfo{"/change/" + id, "INSYNC", t.Format(core.ISO)}}, nil
}

// tagsOf returns the tag map of a zone or health check; lock held.
func (s *Service) tagsOf(c *core.Ctx, typ, id string) (*map[string]string, error) {
	switch typ {
	case "hostedzone":
		z, err := s.zone(c, id)
		if err != nil {
			return nil, err
		}
		return &z.Tags, nil
	case "healthcheck":
		h, err := s.check(c, id)
		if err != nil {
			return nil, err
		}
		return &h.Tags, nil
	}
	return nil, core.Errorf(400, "InvalidInput", "Invalid resource type %q", typ)
}

func (s *Service) changeTags(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	var in struct {
		AddTags       []tag    `xml:"AddTags>Tag"`
		RemoveTagKeys []string `xml:"RemoveTagKeys>Key"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tagsOf(c, r.PathValue("type"), r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	if *tags == nil {
		*tags = map[string]string{}
	}
	for _, t := range in.AddTags {
		(*tags)[t.Key] = t.Value
	}
	for _, k := range in.RemoveTagKeys {
		delete(*tags, k)
	}
	return &struct{}{}, nil
}

func (s *Service) listTags(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tagsOf(c, r.PathValue("type"), r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	set := struct {
		ResourceType string
		ResourceId   string
		Tags         []tag `xml:"Tags>Tag"`
	}{ResourceType: r.PathValue("type"), ResourceId: r.PathValue("id")}
	for _, k := range core.SortedKeys(*tags) {
		set.Tags = append(set.Tags, tag{k, (*tags)[k]})
	}
	return &struct{ ResourceTagSet any }{set}, nil
}

// ---- health checks ----

type checkXML struct {
	Id                string
	CallerReference   string
	HealthCheckConfig struct {
		Inner string `xml:",innerxml"`
	}
	HealthCheckVersion int
}

func (h *HealthCheck) wire() checkXML {
	x := checkXML{Id: h.Id, CallerReference: h.CallerRef, HealthCheckVersion: 1}
	x.HealthCheckConfig.Inner = h.Config
	return x
}

// check finds a health check of the caller; lock held.
func (s *Service) check(c *core.Ctx, id string) (*HealthCheck, error) {
	if h := s.st.Checks[id]; h != nil && h.Account == c.Account {
		return h, nil
	}
	return nil, core.Errorf(404, "NoSuchHealthCheck", "No health check exists with the specified ID %s.", id)
}

func (s *Service) createCheck(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	var in struct {
		CallerReference   string
		HealthCheckConfig struct {
			Inner string `xml:",innerxml"`
		}
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h := &HealthCheck{Id: core.UUID(), Account: c.Account, CallerRef: in.CallerReference, Config: in.HealthCheckConfig.Inner}
	for _, old := range s.st.Checks {
		if old.Account == c.Account && old.CallerRef == in.CallerReference {
			if old.Config != h.Config {
				return nil, core.Errorf(409, "HealthCheckAlreadyExists", "A health check with the specified CallerReference already exists and has different settings.")
			}
			h = old // same request again: Route 53 returns the existing check
		}
	}
	s.st.Checks[h.Id] = h
	w.Header().Set("Location", "https://route53.amazonaws.com/2013-04-01/healthcheck/"+h.Id)
	return &struct{ HealthCheck checkXML }{h.wire()}, nil
}

func (s *Service) getCheck(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, err := s.check(c, r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	return &struct{ HealthCheck checkXML }{h.wire()}, nil
}

func (s *Service) listChecks(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	out := &struct {
		HealthChecks []checkXML `xml:"HealthChecks>HealthCheck"`
		Marker       string
		IsTruncated  bool
		MaxItems     int
	}{Marker: r.URL.Query().Get("marker"), MaxItems: maxItems(r.URL.Query().Get("maxitems"), 100)}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range core.SortedKeys(s.st.Checks) {
		if h := s.st.Checks[id]; h.Account == c.Account {
			out.HealthChecks = append(out.HealthChecks, h.wire())
		}
	}
	return out, nil
}

func (s *Service) deleteCheck(c *core.Ctx, w http.ResponseWriter, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, err := s.check(c, r.PathValue("id"))
	if err != nil {
		return nil, err
	}
	delete(s.st.Checks, h.Id)
	return &struct{}{}, nil
}

// TaggedResources lists zones and health checks (ARN → tags) for the tagging
// API and Resource Explorer; Route 53 is global, so they show up in us-east-1.
func (s *Service) TaggedResources(account, region string) map[string]map[string]string {
	out := map[string]map[string]string{}
	if region != core.DefaultRegion {
		return out
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, z := range s.st.Zones {
		if z.Account == account {
			out[core.GlobalARN("route53", region, "", "hostedzone/"+z.Id)] = maps.Clone(z.Tags)
		}
	}
	for _, h := range s.st.Checks {
		if h.Account == account {
			out[core.GlobalARN("route53", region, "", "healthcheck/"+h.Id)] = maps.Clone(h.Tags)
		}
	}
	return out
}

// Resources lists hosted zones and health checks.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	zt := core.Table{Name: "Hosted zones", Columns: []string{"Zone", "Name", "Records", "Private", "Account"}}
	for _, id := range core.SortedKeys(s.st.Zones) {
		z := s.st.Zones[id]
		zt.Rows = append(zt.Rows, []string{z.Id, z.Name, strconv.Itoa(len(z.Records)), strconv.FormatBool(len(z.VPCs) > 0), z.Account})
	}
	ht := core.Table{Name: "Health checks", Columns: []string{"Health check", "Caller reference", "Account"}}
	for _, id := range core.SortedKeys(s.st.Checks) {
		h := s.st.Checks[id]
		ht.Rows = append(ht.Rows, []string{h.Id, h.CallerRef, h.Account})
	}
	return []core.Table{zt, ht}
}

// Inspect returns a hosted zone with its records, or a health check.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if z := s.st.Zones[id]; z != nil && table == "Hosted zones" {
		cp := *z
		cp.Tags = maps.Clone(z.Tags)
		return cp
	}
	if h := s.st.Checks[id]; h != nil && table == "Health checks" {
		cp := *h
		cp.Tags = maps.Clone(h.Tags)
		return cp
	}
	return nil
}
