// Package servicediscovery emulates AWS Cloud Map (JSON 1.1): HTTP, private
// DNS and public DNS namespaces (the DNS ones are Route 53 hosted zones),
// services, instances (with their A, AAAA, CNAME and SRV records in Route 53),
// custom health, DiscoverInstances, tags, and operations that succeed at once.
package servicediscovery

import (
	"cmp"
	"encoding/json"
	"encoding/xml"
	"hash/fnv"
	"maps"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the Cloud Map emulator.
type Service struct {
	hub *core.Hub
	rpc *core.RPC
	mu  sync.Mutex
	st  state
}

type state struct {
	Namespaces map[string]*Namespace           `json:"namespaces"` // by id
	Services   map[string]*Svc                 `json:"services"`   // by id
	Instances  map[string]map[string]*Instance `json:"instances"`  // service id → instance id
	Operations map[string]*Operation           `json:"operations"` // by id
	Tags       map[string][]Tag                `json:"tags"`       // by ARN
}

// Namespace is a namespace (API shape; ServiceCount is filled in on reads).
type Namespace struct {
	Id, Arn, Name, Type string
	Description         string `json:",omitempty"`
	CreatorRequestId    string `json:",omitempty"`
	Properties          struct {
		DnsProperties *struct {
			HostedZoneId string
			SOA          *struct{ TTL int64 } `json:",omitempty"`
		} `json:",omitempty"`
		HttpProperties struct{ HttpName string }
	}
	ServiceCount  int
	CreateDate    core.Time
	ResourceOwner string
}

// Svc is a service (API shape; InstanceCount is filled in on reads).
type Svc struct {
	Id, Arn, Name, NamespaceId string
	Description                string     `json:",omitempty"`
	Type                       string     `json:",omitempty"`
	DnsConfig                  *DnsConfig `json:",omitempty"`
	HealthCheckConfig          *struct {
		Type             string
		ResourcePath     string `json:",omitempty"`
		FailureThreshold int    `json:",omitempty"`
	} `json:",omitempty"`
	HealthCheckCustomConfig *struct {
		FailureThreshold int `json:",omitempty"`
	} `json:",omitempty"`
	InstanceCount                   int
	CreateDate                      core.Time
	CreatorRequestId                string `json:",omitempty"`
	ResourceOwner, CreatedByAccount string
}

// DnsConfig says which records each instance of a service gets (API shape).
type DnsConfig struct {
	NamespaceId   string `json:",omitempty"`
	RoutingPolicy string `json:",omitempty"`
	DnsRecords    []struct {
		Type string
		TTL  int64
	}
}

// Instance is a registered instance and its health.
type Instance struct {
	Id               string            `json:"id"`
	CreatorRequestId string            `json:"creatorRequestId,omitempty"`
	Attributes       map[string]string `json:"attributes"`
	Account          string            `json:"account"`
	Health           string            `json:"health"` // HEALTHY or UNHEALTHY
}

// Operation is an asynchronous change, done by the time its id is returned (API shape).
type Operation struct {
	Id, Type, Status       string
	CreateDate, UpdateDate core.Time
	Targets                map[string]string
	OwnerAccount           string
}

// Tag is a resource tag (API shape).
type Tag struct{ Key, Value string }

// New creates the Cloud Map service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateHttpNamespace":              core.H(s.createHTTP),
		"CreatePrivateDnsNamespace":        core.H(s.createPrivate),
		"CreatePublicDnsNamespace":         core.H(s.createPublic),
		"GetNamespace":                     core.H(s.getNamespace),
		"ListNamespaces":                   core.H(s.listNamespaces),
		"UpdateHttpNamespace":              core.H(s.updateNamespace),
		"UpdatePrivateDnsNamespace":        core.H(s.updateNamespace),
		"UpdatePublicDnsNamespace":         core.H(s.updateNamespace),
		"DeleteNamespace":                  core.H(s.deleteNamespace),
		"CreateService":                    core.H(s.createService),
		"GetService":                       core.H(s.getService),
		"ListServices":                     core.H(s.listServices),
		"UpdateService":                    core.H(s.updateService),
		"DeleteService":                    core.H(s.deleteService),
		"RegisterInstance":                 core.H(s.registerInstance),
		"DeregisterInstance":               core.H(s.deregisterInstance),
		"GetInstance":                      core.H(s.getInstance),
		"ListInstances":                    core.H(s.listInstances),
		"GetInstancesHealthStatus":         core.H(s.healthStatus),
		"UpdateInstanceCustomHealthStatus": core.H(s.updateHealth),
		"DiscoverInstances":                core.H(s.discover),
		"DiscoverInstancesRevision":        core.H(s.discoverRevision),
		"GetOperation":                     core.H(s.getOperation),
		"ListOperations":                   core.H(s.listOperations),
		"TagResource":                      core.H(s.tagResource),
		"UntagResource":                    core.H(s.untagResource),
		"ListTagsForResource":              core.H(s.listTags),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "servicediscovery", Cloud: core.AWS, Title: "Cloud Map", Targets: []string{"Route53AutoNaming_v20170314"},
		Signing: []string{"servicediscovery"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func (st *state) fill() {
	if st.Namespaces == nil {
		st.Namespaces = map[string]*Namespace{}
	}
	if st.Services == nil {
		st.Services = map[string]*Svc{}
	}
	if st.Instances == nil {
		st.Instances = map[string]map[string]*Instance{}
	}
	if st.Operations == nil {
		st.Operations = map[string]*Operation{}
	}
	if st.Tags == nil {
		st.Tags = map[string][]Tag{}
	}
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{}
	s.st.fill()
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
	st.fill()
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func invalid(format string, a ...any) error { return core.Errorf(400, "InvalidInput", format, a...) }

func notFound(code, format string, a ...any) error { return core.Errorf(404, code, format, a...) }

const lower = "abcdefghijklmnopqrstuvwxyz0123456789"

// owned reports whether a resource ARN belongs to the caller's account and region.
func owned(c *core.Ctx, arn string) bool {
	_, region, account, _, _ := core.ParseARN(arn)
	return region == c.Region && account == c.Account
}

// lastPart takes an id that may be given as an ARN.
func lastPart(id string) string { return id[strings.LastIndexByte(id, '/')+1:] }

// namespace finds a namespace of the caller; lock held.
func (s *Service) namespace(c *core.Ctx, id string) (*Namespace, error) {
	if ns := s.st.Namespaces[lastPart(id)]; ns != nil && owned(c, ns.Arn) {
		return ns, nil
	}
	return nil, notFound("NamespaceNotFound", "Namespace not found: %s", id)
}

// service finds a service of the caller; lock held.
func (s *Service) service(c *core.Ctx, id string) (*Svc, error) {
	if sv := s.st.Services[lastPart(id)]; sv != nil && owned(c, sv.Arn) {
		return sv, nil
	}
	return nil, notFound("ServiceNotFound", "Service not found: %s", id)
}

// op records an operation that has already succeeded; lock held.
func (s *Service) op(c *core.Ctx, typ string, targets map[string]string) map[string]string {
	now := core.Time(time.Now())
	o := &Operation{Id: core.RandString(32, lower) + "-" + core.RandString(8, lower), Type: typ, Status: "SUCCESS",
		CreateDate: now, UpdateDate: now, Targets: targets, OwnerAccount: c.Account}
	s.st.Operations[o.Id] = o
	return map[string]string{"OperationId": o.Id}
}

// ---- Route 53 ----

const r53ns = "https://route53.amazonaws.com/doc/2013-04-01/"

// route53 sends a Route 53 request as the caller and returns its answer.
func (s *Service) route53(c *core.Ctx, method, path string, body any) ([]byte, error) {
	var b []byte
	if body != nil {
		b, _ = xml.Marshal(body)
	}
	status, _, out := s.hub.Do(c.Account, c.Region, "route53", method, "/2013-04-01/"+path, http.Header{"Content-Type": {"text/xml"}}, b)
	if status >= 300 {
		var e struct {
			Error struct{ Code, Message string }
		}
		xml.Unmarshal(out, &e)
		return nil, invalid("Route 53: %s %s", e.Error.Code, cmp.Or(e.Error.Message, strings.TrimSpace(string(out))))
	}
	return out, nil
}

type rrset struct {
	Name             string
	Type             string
	SetIdentifier    string `xml:",omitempty"`
	Weight           *int   `xml:",omitempty"`
	MultiValueAnswer bool   `xml:",omitempty"`
	TTL              int64
	Records          []struct{ Value string } `xml:"ResourceRecords>ResourceRecord"`
}

// records are the record sets of an instance of a DNS service.
func records(ns *Namespace, sv *Svc, in *Instance) []rrset {
	if sv.DnsConfig == nil {
		return nil
	}
	a, name := in.Attributes, sv.Name+"."+ns.Name+"."
	var out []rrset
	var srv bool
	for _, r := range sv.DnsConfig.DnsRecords {
		set := rrset{Name: name, Type: r.Type, SetIdentifier: in.Id, TTL: r.TTL, MultiValueAnswer: sv.DnsConfig.RoutingPolicy != "WEIGHTED"}
		if !set.MultiValueAnswer {
			set.Weight = new(1)
		}
		v := map[string]string{"A": a["AWS_INSTANCE_IPV4"], "AAAA": a["AWS_INSTANCE_IPV6"], "CNAME": a["AWS_INSTANCE_CNAME"],
			"SRV": "1 1 " + a["AWS_INSTANCE_PORT"] + " " + in.Id + "." + name}[r.Type]
		set.Records = []struct{ Value string }{{v}}
		out = append(out, set)
		srv = srv || r.Type == "SRV"
	}
	if srv { // the SRV target resolves through records of the instance's own name
		for typ, attr := range map[string]string{"A": "AWS_INSTANCE_IPV4", "AAAA": "AWS_INSTANCE_IPV6"} {
			if ip := a[attr]; ip != "" {
				out = append(out, rrset{Name: in.Id + "." + name, Type: typ, TTL: sv.DnsConfig.DnsRecords[0].TTL, Records: []struct{ Value string }{{ip}}})
			}
		}
	}
	return out
}

// changeRecords applies record changes in the namespace's hosted zone.
func (s *Service) changeRecords(c *core.Ctx, zone, action string, sets []rrset) error {
	if zone == "" || len(sets) == 0 {
		return nil
	}
	type change struct {
		Action string
		Set    rrset `xml:"ResourceRecordSet"`
	}
	body := struct {
		XMLName xml.Name `xml:"ChangeResourceRecordSetsRequest"`
		Xmlns   string   `xml:"xmlns,attr"`
		Changes []change `xml:"ChangeBatch>Changes>Change"`
	}{Xmlns: r53ns}
	for _, set := range sets {
		body.Changes = append(body.Changes, change{action, set})
	}
	_, err := s.route53(c, http.MethodPost, "hostedzone/"+zone+"/rrset", body)
	return err
}

// ---- namespaces ----

type namespaceInput struct {
	Name, CreatorRequestId, Description, Vpc string
	Tags                                     []Tag
	Properties                               struct {
		DnsProperties struct{ SOA *struct{ TTL int64 } }
	}
}

func (s *Service) createHTTP(c *core.Ctx, in *namespaceInput) (any, error) {
	return s.createNamespace(c, in, "HTTP")
}

func (s *Service) createPrivate(c *core.Ctx, in *namespaceInput) (any, error) {
	if in.Vpc == "" {
		return nil, invalid("Vpc is required for a private DNS namespace.")
	}
	return s.createNamespace(c, in, "DNS_PRIVATE")
}

func (s *Service) createPublic(c *core.Ctx, in *namespaceInput) (any, error) {
	return s.createNamespace(c, in, "DNS_PUBLIC")
}

func (s *Service) createNamespace(c *core.Ctx, in *namespaceInput, typ string) (any, error) {
	name := strings.TrimSuffix(in.Name, ".")
	if name == "" || len(name) > 1024 {
		return nil, invalid("Invalid namespace name %q.", in.Name)
	}
	exists := func() error {
		for _, ns := range s.st.Namespaces {
			if owned(c, ns.Arn) && strings.EqualFold(ns.Name, name) {
				return core.Errorf(409, "NamespaceAlreadyExists", "Namespace with name %s already exists.", name)
			}
		}
		return nil
	}
	s.mu.Lock()
	err := exists()
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	id := "ns-" + core.RandString(16, lower)
	ns := &Namespace{Id: id, Arn: c.ARN("servicediscovery", "namespace/"+id), Name: name, Type: typ, Description: in.Description,
		CreatorRequestId: in.CreatorRequestId, CreateDate: core.Time(time.Now()), ResourceOwner: c.Account}
	ns.Properties.HttpProperties.HttpName = name
	if typ != "HTTP" { // a hosted zone, made before taking the lock
		zone := struct {
			XMLName          xml.Name `xml:"CreateHostedZoneRequest"`
			Xmlns            string   `xml:"xmlns,attr"`
			Name             string
			CallerReference  string
			HostedZoneConfig struct {
				Comment     string
				PrivateZone bool
			}
			VPC *struct{ VPCRegion, VPCId string } `xml:",omitempty"`
		}{Xmlns: r53ns, Name: name, CallerReference: id}
		zone.HostedZoneConfig.Comment, zone.HostedZoneConfig.PrivateZone = "Created by AWS Cloud Map namespace with ARN "+ns.Arn, typ == "DNS_PRIVATE"
		if typ == "DNS_PRIVATE" {
			zone.VPC = &struct{ VPCRegion, VPCId string }{c.Region, in.Vpc}
		}
		b, err := s.route53(c, http.MethodPost, "hostedzone", zone)
		if err != nil {
			return nil, err
		}
		var out struct{ HostedZone struct{ Id string } }
		xml.Unmarshal(b, &out)
		ns.Properties.DnsProperties = &struct {
			HostedZoneId string
			SOA          *struct{ TTL int64 } `json:",omitempty"`
		}{HostedZoneId: strings.TrimPrefix(out.HostedZone.Id, "/hostedzone/"), SOA: in.Properties.DnsProperties.SOA}
	}
	s.mu.Lock()
	if err := exists(); err != nil { // lost a race to a namespace of the same name
		s.mu.Unlock()
		if dns := ns.Properties.DnsProperties; dns != nil {
			s.route53(c, http.MethodDelete, "hostedzone/"+dns.HostedZoneId, nil)
		}
		return nil, err
	}
	s.st.Namespaces[id] = ns
	if len(in.Tags) > 0 {
		s.st.Tags[ns.Arn] = in.Tags
	}
	out := s.op(c, "CREATE_NAMESPACE", map[string]string{"NAMESPACE": id})
	s.mu.Unlock()
	return out, nil
}

// count fills in a namespace's service count; lock held.
func (s *Service) count(ns *Namespace) Namespace {
	v := *ns
	for _, sv := range s.st.Services {
		if sv.NamespaceId == ns.Id {
			v.ServiceCount++
		}
	}
	return v
}

func (s *Service) getNamespace(c *core.Ctx, in *struct{ Id string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, err := s.namespace(c, in.Id)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Namespace": s.count(ns)}, nil
}

type filter struct {
	Name, Condition string
	Values          []string
}

// match applies a list filter (EQ, IN or BEGINS_WITH) to a value.
// ponytail: BETWEEN (UPDATE_DATE of operations) matches everything.
func (f filter) match(v string) bool {
	switch f.Condition {
	case "BEGINS_WITH":
		return len(f.Values) > 0 && strings.HasPrefix(v, f.Values[0])
	case "BETWEEN":
		return true
	}
	return slices.Contains(f.Values, v)
}

type listInput struct {
	NextToken  string
	MaxResults int
	Filters    []filter
}

func (s *Service) listNamespaces(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	var all []Namespace
	for _, ns := range s.st.Namespaces {
		ok := owned(c, ns.Arn)
		for _, f := range in.Filters {
			v := map[string]string{"TYPE": ns.Type, "NAME": ns.Name, "HTTP_NAME": ns.Properties.HttpProperties.HttpName, "RESOURCE_OWNER": ns.ResourceOwner}[f.Name]
			ok = ok && f.match(v)
		}
		if ok {
			all = append(all, s.count(ns))
		}
	}
	s.mu.Unlock()
	items, next := page(all, func(ns Namespace) string { return ns.Id }, in.NextToken, in.MaxResults)
	return map[string]any{"Namespaces": items, "NextToken": next}, nil
}

func (s *Service) updateNamespace(c *core.Ctx, in *struct {
	Id        string
	Namespace struct {
		Description *string
		Properties  struct {
			DnsProperties struct{ SOA *struct{ TTL int64 } }
		}
	}
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, err := s.namespace(c, in.Id)
	if err != nil {
		return nil, err
	}
	want := map[string]string{"UpdateHttpNamespace": "HTTP", "UpdatePrivateDnsNamespace": "DNS_PRIVATE", "UpdatePublicDnsNamespace": "DNS_PUBLIC"}[c.Op]
	if ns.Type != want {
		return nil, invalid("Namespace %s is of type %s.", ns.Id, ns.Type)
	}
	v := *ns // replaced, not changed: readers may hold the old value
	if in.Namespace.Description != nil {
		v.Description = *in.Namespace.Description
	}
	if soa := in.Namespace.Properties.DnsProperties.SOA; soa != nil && v.Properties.DnsProperties != nil {
		dns := *v.Properties.DnsProperties
		dns.SOA = soa
		v.Properties.DnsProperties = &dns
	}
	s.st.Namespaces[ns.Id] = &v
	return s.op(c, "UPDATE_NAMESPACE", map[string]string{"NAMESPACE": ns.Id}), nil
}

func (s *Service) deleteNamespace(c *core.Ctx, in *struct{ Id string }) (any, error) {
	s.mu.Lock()
	ns, err := s.namespace(c, in.Id)
	if err == nil && s.count(ns).ServiceCount > 0 {
		err = core.Errorf(400, "ResourceInUse", "Namespace %s still has services.", ns.Id)
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	delete(s.st.Namespaces, ns.Id)
	delete(s.st.Tags, ns.Arn)
	out := s.op(c, "DELETE_NAMESPACE", map[string]string{"NAMESPACE": ns.Id})
	s.mu.Unlock()
	if dns := ns.Properties.DnsProperties; dns != nil {
		if _, err := s.route53(c, http.MethodDelete, "hostedzone/"+dns.HostedZoneId, nil); err != nil {
			s.hub.Log.Debug("cloud map: hosted zone not deleted", "zone", dns.HostedZoneId, "err", err)
		}
	}
	return out, nil
}

// ---- services ----

func (s *Service) createService(c *core.Ctx, in *struct {
	Svc
	Tags []Tag
}) (any, error) {
	sv := in.Svc
	if sv.DnsConfig != nil {
		sv.NamespaceId = cmp.Or(sv.NamespaceId, sv.DnsConfig.NamespaceId)
		sv.DnsConfig.NamespaceId = sv.NamespaceId
		sv.DnsConfig.RoutingPolicy = cmp.Or(sv.DnsConfig.RoutingPolicy, "MULTIVALUE")
	}
	if sv.HealthCheckConfig != nil && sv.HealthCheckCustomConfig != nil {
		return nil, invalid("A service can have HealthCheckConfig or HealthCheckCustomConfig, not both.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, err := s.namespace(c, sv.NamespaceId)
	if err != nil {
		return nil, err
	}
	switch {
	case sv.Name == "" || len(sv.Name) > 127:
		return nil, invalid("Invalid service name %q.", sv.Name)
	case ns.Type == "HTTP" && sv.DnsConfig != nil:
		return nil, invalid("DnsConfig is not supported for services in HTTP namespaces.")
	case ns.Type != "HTTP" && sv.DnsConfig == nil && sv.Type != "HTTP":
		return nil, invalid("DnsConfig is required for services in DNS namespaces.")
	case sv.DnsConfig != nil && len(sv.DnsConfig.DnsRecords) == 0:
		return nil, invalid("DnsConfig needs at least one DnsRecord.")
	}
	for _, o := range s.st.Services {
		if o.NamespaceId == ns.Id && strings.EqualFold(o.Name, sv.Name) {
			return nil, core.Errorf(409, "ServiceAlreadyExists", "Service with name %s already exists in namespace %s.", sv.Name, ns.Id)
		}
	}
	sv.Id = "srv-" + core.RandString(16, lower)
	sv.Arn, sv.NamespaceId, sv.CreateDate = c.ARN("servicediscovery", "service/"+sv.Id), ns.Id, core.Time(time.Now())
	sv.ResourceOwner, sv.CreatedByAccount, sv.InstanceCount = c.Account, c.Account, 0
	sv.Type = "HTTP"
	if sv.DnsConfig != nil {
		sv.Type = "DNS_HTTP"
	}
	s.st.Services[sv.Id] = &sv
	if len(in.Tags) > 0 {
		s.st.Tags[sv.Arn] = in.Tags
	}
	return map[string]any{"Service": sv}, nil
}

// counted fills in a service's instance count; lock held.
func (s *Service) counted(sv *Svc) Svc {
	v := *sv
	v.InstanceCount = len(s.st.Instances[sv.Id])
	return v
}

func (s *Service) getService(c *core.Ctx, in *struct{ Id string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sv, err := s.service(c, in.Id)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Service": s.counted(sv)}, nil
}

func (s *Service) listServices(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	var all []Svc
	for _, sv := range s.st.Services {
		ok := owned(c, sv.Arn)
		for _, f := range in.Filters {
			ok = ok && f.match(map[string]string{"NAMESPACE_ID": sv.NamespaceId, "RESOURCE_OWNER": sv.ResourceOwner}[f.Name])
		}
		if ok {
			all = append(all, s.counted(sv))
		}
	}
	s.mu.Unlock()
	items, next := page(all, func(sv Svc) string { return sv.Id }, in.NextToken, in.MaxResults)
	return map[string]any{"Services": items, "NextToken": next}, nil
}

// ponytail: new TTLs apply to instances registered afterwards; Route 53 keeps the old ones.
func (s *Service) updateService(c *core.Ctx, in *struct {
	Id      string
	Service struct {
		Description       *string
		DnsConfig         *DnsConfig
		HealthCheckConfig json.RawMessage
	}
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sv, err := s.service(c, in.Id)
	if err != nil {
		return nil, err
	}
	v := *sv
	if d := in.Service.Description; d != nil {
		v.Description = *d
	}
	if d := in.Service.DnsConfig; d != nil && v.DnsConfig != nil {
		dc := *v.DnsConfig
		dc.DnsRecords = d.DnsRecords
		v.DnsConfig = &dc
	}
	if len(in.Service.HealthCheckConfig) > 0 {
		json.Unmarshal(in.Service.HealthCheckConfig, &v.HealthCheckConfig)
	}
	s.st.Services[sv.Id] = &v
	return s.op(c, "UPDATE_SERVICE", map[string]string{"SERVICE": sv.Id}), nil
}

func (s *Service) deleteService(c *core.Ctx, in *struct{ Id string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sv, err := s.service(c, in.Id)
	if err != nil {
		return nil, err
	}
	if len(s.st.Instances[sv.Id]) > 0 {
		return nil, core.Errorf(400, "ResourceInUse", "Service %s still has registered instances.", sv.Id)
	}
	delete(s.st.Services, sv.Id)
	delete(s.st.Instances, sv.Id)
	delete(s.st.Tags, sv.Arn)
	return nil, nil
}

// ---- instances ----

func (s *Service) registerInstance(c *core.Ctx, in *struct {
	ServiceId, InstanceId, CreatorRequestId string
	Attributes                              map[string]string
}) (any, error) {
	a := in.Attributes
	if in.InstanceId == "" || len(in.InstanceId) > 64 {
		return nil, invalid("Invalid instance id %q.", in.InstanceId)
	}
	s.mu.Lock()
	sv, err := s.service(c, in.ServiceId)
	var ns *Namespace
	if err == nil {
		ns, err = s.namespace(c, sv.NamespaceId)
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if sv.DnsConfig != nil {
		for _, r := range sv.DnsConfig.DnsRecords {
			need := map[string]string{"A": "AWS_INSTANCE_IPV4", "AAAA": "AWS_INSTANCE_IPV6", "CNAME": "AWS_INSTANCE_CNAME", "SRV": "AWS_INSTANCE_PORT"}[r.Type]
			if a[need] == "" {
				return nil, invalid("%s is required for a service with %s records.", need, r.Type)
			}
		}
	}
	x := &Instance{Id: in.InstanceId, CreatorRequestId: in.CreatorRequestId, Attributes: a, Account: c.Account, Health: "HEALTHY"}
	if sv.HealthCheckCustomConfig != nil && a["AWS_INIT_HEALTH_STATUS"] == "UNHEALTHY" {
		x.Health = "UNHEALTHY"
	}
	if ns.Properties.DnsProperties != nil {
		if err := s.changeRecords(c, ns.Properties.DnsProperties.HostedZoneId, "UPSERT", records(ns, sv, x)); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Services[sv.Id] == nil {
		return nil, notFound("ServiceNotFound", "Service not found: %s", sv.Id)
	}
	insts := maps.Clone(s.st.Instances[sv.Id])
	if insts == nil {
		insts = map[string]*Instance{}
	}
	insts[x.Id] = x
	s.st.Instances[sv.Id] = insts
	return s.op(c, "REGISTER_INSTANCE", map[string]string{"SERVICE": sv.Id, "INSTANCE": x.Id}), nil
}

// instance finds an instance of a caller's service; lock held.
func (s *Service) instance(c *core.Ctx, service, id string) (*Svc, *Instance, error) {
	sv, err := s.service(c, service)
	if err != nil {
		return nil, nil, err
	}
	if x := s.st.Instances[sv.Id][id]; x != nil {
		return sv, x, nil
	}
	return nil, nil, notFound("InstanceNotFound", "Instance not found: %s", id)
}

func (s *Service) deregisterInstance(c *core.Ctx, in *struct{ ServiceId, InstanceId string }) (any, error) {
	s.mu.Lock()
	sv, x, err := s.instance(c, in.ServiceId, in.InstanceId)
	var ns *Namespace
	if err == nil {
		ns = s.st.Namespaces[sv.NamespaceId]
		insts := maps.Clone(s.st.Instances[sv.Id])
		delete(insts, x.Id)
		s.st.Instances[sv.Id] = insts
	}
	var out map[string]string
	if err == nil {
		out = s.op(c, "DEREGISTER_INSTANCE", map[string]string{"SERVICE": sv.Id, "INSTANCE": x.Id})
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if ns != nil && ns.Properties.DnsProperties != nil {
		if err := s.changeRecords(c, ns.Properties.DnsProperties.HostedZoneId, "DELETE", records(ns, sv, x)); err != nil {
			s.hub.Log.Debug("cloud map: records not deleted", "instance", x.Id, "err", err)
		}
	}
	return out, nil
}

type instanceShape struct {
	Id               string
	CreatorRequestId string `json:",omitempty"`
	Attributes       map[string]string
	CreatedByAccount string
}

func (x *Instance) shape() instanceShape {
	return instanceShape{x.Id, x.CreatorRequestId, x.Attributes, x.Account}
}

func (s *Service) getInstance(c *core.Ctx, in *struct{ ServiceId, InstanceId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sv, x, err := s.instance(c, in.ServiceId, in.InstanceId)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Instance": x.shape(), "ResourceOwner": sv.ResourceOwner}, nil
}

func (s *Service) listInstances(c *core.Ctx, in *struct {
	ServiceId, NextToken string
	MaxResults           int
}) (any, error) {
	s.mu.Lock()
	sv, err := s.service(c, in.ServiceId)
	var all []instanceShape
	if err == nil {
		for _, x := range s.st.Instances[sv.Id] {
			all = append(all, x.shape())
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	items, next := page(all, func(x instanceShape) string { return x.Id }, in.NextToken, in.MaxResults)
	return map[string]any{"Instances": items, "NextToken": next, "ResourceOwner": sv.ResourceOwner}, nil
}

// health is what Cloud Map reports for an instance: UNKNOWN without health checks.
func health(sv *Svc, x *Instance) string {
	if sv.HealthCheckConfig == nil && sv.HealthCheckCustomConfig == nil {
		return "UNKNOWN"
	}
	return x.Health // ponytail: Route 53 health checks are not run, their instances stay HEALTHY
}

func (s *Service) healthStatus(c *core.Ctx, in *struct {
	ServiceId, NextToken string
	Instances            []string
	MaxResults           int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sv, err := s.service(c, in.ServiceId)
	if err != nil {
		return nil, err
	}
	ids := in.Instances
	if len(ids) == 0 {
		ids = slices.Collect(maps.Keys(s.st.Instances[sv.Id]))
	}
	ids, next := page(ids, func(id string) string { return id }, in.NextToken, in.MaxResults)
	status := map[string]string{}
	for _, id := range ids {
		x := s.st.Instances[sv.Id][id]
		if x == nil {
			return nil, notFound("InstanceNotFound", "Instance not found: %s", id)
		}
		status[id] = health(sv, x)
	}
	return map[string]any{"Status": status, "NextToken": next}, nil
}

func (s *Service) updateHealth(c *core.Ctx, in *struct{ ServiceId, InstanceId, Status string }) (any, error) {
	if in.Status != "HEALTHY" && in.Status != "UNHEALTHY" {
		return nil, invalid("Status must be HEALTHY or UNHEALTHY.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sv, x, err := s.instance(c, in.ServiceId, in.InstanceId)
	if err != nil {
		return nil, err
	}
	if sv.HealthCheckCustomConfig == nil {
		return nil, notFound("CustomHealthNotFound", "Service %s has no custom health check.", sv.Id)
	}
	y := *x
	y.Health = in.Status
	insts := maps.Clone(s.st.Instances[sv.Id])
	insts[x.Id] = &y
	s.st.Instances[sv.Id] = insts
	return nil, nil
}

// ---- discovery ----

type discoverInput struct {
	NamespaceName, ServiceName, HealthStatus string
	MaxResults                               int
	QueryParameters, OptionalParameters      map[string]string
}

// discovered finds the service DiscoverInstances names and its instances; lock held.
func (s *Service) discovered(c *core.Ctx, in *discoverInput) (*Namespace, *Svc, map[string]*Instance, error) {
	for _, ns := range s.st.Namespaces {
		if !owned(c, ns.Arn) || !strings.EqualFold(ns.Properties.HttpProperties.HttpName, in.NamespaceName) {
			continue
		}
		for _, sv := range s.st.Services {
			if sv.NamespaceId == ns.Id && strings.EqualFold(sv.Name, in.ServiceName) {
				return ns, sv, s.st.Instances[sv.Id], nil
			}
		}
		return nil, nil, nil, notFound("ServiceNotFound", "Service %s not found in namespace %s.", in.ServiceName, in.NamespaceName)
	}
	return nil, nil, nil, notFound("NamespaceNotFound", "Namespace %s not found.", in.NamespaceName)
}

// revision changes whenever the instances of a service or their health do.
func revision(insts map[string]*Instance) int64 {
	h := fnv.New64a()
	for _, id := range core.SortedKeys(insts) {
		b, _ := json.Marshal(insts[id])
		h.Write(b)
	}
	return int64(h.Sum64() >> 1)
}

func (s *Service) discover(c *core.Ctx, in *discoverInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, sv, insts, err := s.discovered(c, in)
	if err != nil {
		return nil, err
	}
	has := func(x *Instance, params map[string]string) bool {
		for k, v := range params {
			if x.Attributes[k] != v {
				return false
			}
		}
		return true
	}
	var found, optional []*Instance
	for _, id := range core.SortedKeys(insts) {
		if x := insts[id]; has(x, in.QueryParameters) {
			found = append(found, x)
			if has(x, in.OptionalParameters) {
				optional = append(optional, x)
			}
		}
	}
	if len(in.OptionalParameters) > 0 && len(optional) > 0 {
		found = optional
	}
	if want := cmp.Or(in.HealthStatus, "HEALTHY"); want != "ALL" && health(sv, &Instance{}) != "UNKNOWN" {
		healthy := slices.DeleteFunc(slices.Clone(found), func(x *Instance) bool { return x.Health != "HEALTHY" })
		switch want {
		case "HEALTHY":
			found = healthy
		case "UNHEALTHY":
			found = slices.DeleteFunc(found, func(x *Instance) bool { return x.Health == "HEALTHY" })
		case "HEALTHY_OR_ELSE_ALL":
			if len(healthy) > 0 {
				found = healthy
			}
		}
	}
	out := []map[string]any{}
	for _, x := range found[:min(len(found), cmp.Or(in.MaxResults, 100))] {
		out = append(out, map[string]any{"InstanceId": x.Id, "NamespaceName": ns.Properties.HttpProperties.HttpName,
			"ServiceName": sv.Name, "HealthStatus": health(sv, x), "Attributes": x.Attributes})
	}
	return map[string]any{"Instances": out, "InstancesRevision": revision(insts)}, nil
}

func (s *Service) discoverRevision(c *core.Ctx, in *discoverInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _, insts, err := s.discovered(c, in)
	if err != nil {
		return nil, err
	}
	return map[string]any{"InstancesRevision": revision(insts)}, nil
}

// ---- operations ----

func (s *Service) getOperation(c *core.Ctx, in *struct{ OperationId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o := s.st.Operations[in.OperationId]; o != nil && o.OwnerAccount == c.Account {
		return map[string]any{"Operation": o}, nil
	}
	return nil, notFound("OperationNotFound", "Operation not found: %s", in.OperationId)
}

func (s *Service) listOperations(c *core.Ctx, in *listInput) (any, error) {
	type summary struct{ Id, Status string }
	s.mu.Lock()
	var all []summary
	for _, o := range s.st.Operations {
		ok := o.OwnerAccount == c.Account
		for _, f := range in.Filters {
			v := map[string]string{"NAMESPACE_ID": o.Targets["NAMESPACE"], "SERVICE_ID": o.Targets["SERVICE"], "STATUS": o.Status, "TYPE": o.Type}[f.Name]
			ok = ok && f.match(v)
		}
		if ok {
			all = append(all, summary{o.Id, o.Status})
		}
	}
	s.mu.Unlock()
	items, next := page(all, func(o summary) string { return o.Id }, in.NextToken, in.MaxResults)
	return map[string]any{"Operations": items, "NextToken": next}, nil
}

// ---- tags ----

// tagged checks that an ARN names a namespace or service of the caller; lock held.
func (s *Service) tagged(c *core.Ctx, arn string) error {
	for _, ns := range s.st.Namespaces {
		if ns.Arn == arn && owned(c, arn) {
			return nil
		}
	}
	for _, sv := range s.st.Services {
		if sv.Arn == arn && owned(c, arn) {
			return nil
		}
	}
	return notFound("ResourceNotFoundException", "Resource not found: %s", arn)
}

func (s *Service) tagResource(c *core.Ctx, in *struct {
	ResourceARN string
	Tags        []Tag
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.tagged(c, in.ResourceARN); err != nil {
		return nil, err
	}
	tags := slices.Clone(s.st.Tags[in.ResourceARN])
	for _, t := range in.Tags {
		tags = slices.DeleteFunc(tags, func(x Tag) bool { return x.Key == t.Key })
		tags = append(tags, t)
	}
	if len(tags) > 50 {
		return nil, core.Errorf(400, "TooManyTagsException", "A resource can have at most 50 tags.")
	}
	s.st.Tags[in.ResourceARN] = tags
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, in *struct {
	ResourceARN string
	TagKeys     []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.tagged(c, in.ResourceARN); err != nil {
		return nil, err
	}
	s.st.Tags[in.ResourceARN] = slices.DeleteFunc(slices.Clone(s.st.Tags[in.ResourceARN]), func(t Tag) bool { return slices.Contains(in.TagKeys, t.Key) })
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, in *struct{ ResourceARN string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.tagged(c, in.ResourceARN); err != nil {
		return nil, err
	}
	return map[string]any{"Tags": append([]Tag{}, s.st.Tags[in.ResourceARN]...)}, nil
}

// page sorts items by key and returns those after token, at most max (default 100).
func page[T any](items []T, key func(T) string, token string, max int) ([]T, string) {
	slices.SortFunc(items, func(a, b T) int { return strings.Compare(key(a), key(b)) })
	items = items[sort.Search(len(items), func(i int) bool { return key(items[i]) > token }):]
	if max <= 0 {
		max = 100
	}
	if len(items) > max {
		return items[:max], key(items[max-1])
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
	nss := core.Table{Name: "Namespaces", Columns: []string{"Id", "Name", "Type", "Hosted zone", "Services"}}
	svcs := core.Table{Name: "Services", Columns: []string{"Id", "Name", "Namespace", "Type", "Instances"}}
	insts := core.Table{Name: "Instances", Columns: []string{"Id", "Service", "Health", "Attributes"}}
	for _, id := range core.SortedKeys(s.st.Namespaces) {
		ns, zone := s.count(s.st.Namespaces[id]), ""
		if ns.Properties.DnsProperties != nil {
			zone = ns.Properties.DnsProperties.HostedZoneId
		}
		nss.Rows = append(nss.Rows, []string{ns.Id, ns.Name, ns.Type, zone, strconv.Itoa(ns.ServiceCount)})
	}
	for _, id := range core.SortedKeys(s.st.Services) {
		sv := s.counted(s.st.Services[id])
		svcs.Rows = append(svcs.Rows, []string{sv.Id, sv.Name, sv.NamespaceId, sv.Type, strconv.Itoa(sv.InstanceCount)})
		for _, iid := range core.SortedKeys(s.st.Instances[id]) {
			x := s.st.Instances[id][iid]
			b, _ := json.Marshal(x.Attributes)
			insts.Rows = append(insts.Rows, []string{x.Id, sv.Name, health(&sv, x), string(b)})
		}
	}
	return []core.Table{nss, svcs, insts}
}

// Inspect returns a namespace, or a service with its instances.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Namespaces":
		if ns := s.st.Namespaces[id]; ns != nil {
			return s.count(ns)
		}
	case "Services":
		if sv := s.st.Services[id]; sv != nil {
			return map[string]any{"Service": s.counted(sv), "Instances": s.st.Instances[id]}
		}
	}
	return nil
}
