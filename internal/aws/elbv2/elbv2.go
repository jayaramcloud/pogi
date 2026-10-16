// Package elbv2 emulates Elastic Load Balancing v2 over the Query protocol:
// application, network and gateway load balancers, listeners and rules,
// target groups with health checks, and a data plane. ALBs answer on pogi's
// port for Host <name>-<id>.<region>.elb.localhost and on each listener's own
// port on 127.0.0.1; NLBs proxy TCP from a 127.0.0.1 port; Lambda targets get
// the ALB event (proxy.go).
package elbv2

import (
	"cmp"
	"encoding/json"
	"encoding/xml"
	"io"
	"maps"
	"math"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

const xmlns = "http://elasticloadbalancing.amazonaws.com/doc/2015-12-01/"

// Service is the ELB v2 emulator.
type Service struct {
	hub  *core.Hub
	rpc  *core.RPC
	mu   sync.Mutex
	st   state
	open map[string]io.Closer // listener ARN → what serves its port on 127.0.0.1
	rr   map[string]int       // round-robin position per target group
	stop func()
}

type state struct {
	LBs       map[string]*loadBalancer     `json:"lbs"`
	Listeners map[string]*listener         `json:"listeners"`
	Rules     map[string]*rule             `json:"rules"` // default rules too
	Groups    map[string]*targetGroup      `json:"groups"`
	Tags      map[string]map[string]string `json:"tags"` // resource ARN → tags
}

func newState() state {
	return state{LBs: map[string]*loadBalancer{}, Listeners: map[string]*listener{}, Rules: map[string]*rule{},
		Groups: map[string]*targetGroup{}, Tags: map[string]map[string]string{}}
}

// ---- API shapes (stored as is; lowercase json and query:"-" fields are pogi's) ----

type loadBalancer struct {
	LoadBalancerArn, LoadBalancerName, DNSName, CanonicalHostedZoneId string
	CreatedTime                                                       core.Time
	Scheme, Type, IpAddressType, VpcId                                string
	State                                                             struct{ Code string }
	AvailabilityZones                                                 []zone
	SecurityGroups                                                    []string
	CustomerOwnedIpv4Pool                                             string
	EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic              string
	Attrs                                                             map[string]string            `json:"attrs,omitempty" query:"-"`
	Capacity                                                          *struct{ CapacityUnits int } `json:"capacity,omitempty" query:"-"`
	Pogi                                                              struct {
		Ports map[int]int `json:"ports,omitempty"` // listener port → the port serving it on 127.0.0.1
	} `json:"pogi" query:"-"`
}

type zone struct {
	ZoneName, SubnetId    string
	LoadBalancerAddresses []mapping
}

// mapping is a SubnetMapping in requests and a LoadBalancerAddress in responses.
type mapping struct{ SubnetId, AllocationId, PrivateIPv4Address, IPv6Address string }

type listener struct {
	ListenerArn, LoadBalancerArn string
	Port                         *int
	Protocol, SslPolicy          string
	Certificates                 []cert
	DefaultActions               []action // the default rule's, filled in by view
	AlpnPolicy                   []string
	MutualAuthentication         *mtls
	More                         []string          `json:"more,omitempty" query:"-"` // AddListenerCertificates
	Rule                         string            `json:"rule" query:"-"`           // the default rule's ARN
	Attrs                        map[string]string `json:"attrs,omitempty" query:"-"`
}

type mtls struct {
	Mode, TrustStoreArn, AdvertiseTrustStoreCaNames string
	IgnoreClientCertificateExpiry                   *bool
}

type cert struct {
	CertificateArn string
	IsDefault      *bool
}

type rule struct {
	RuleArn, Priority string
	Conditions        []condition
	Actions           []action
	IsDefault         bool
	Listener          string `json:"listener" query:"-"`
}

type action struct {
	Type                string
	Order               int
	TargetGroupArn      string
	ForwardConfig       *forward
	RedirectConfig      *struct{ Protocol, Port, Host, Path, Query, StatusCode string }
	FixedResponseConfig *struct{ StatusCode, ContentType, MessageBody string }
	// ponytail: authenticate-* and jwt-validation are stored and echoed; the
	// data plane lets requests through unauthenticated.
	AuthenticateOidcConfig *struct {
		AuthorizationEndpoint, ClientId, ClientSecret, Issuer, TokenEndpoint, UserInfoEndpoint string
		OnUnauthenticatedRequest, Scope, SessionCookieName                                     string
		SessionTimeout                                                                         *int64
		UseExistingClientSecret                                                                *bool
		AuthenticationRequestExtraParams                                                       map[string]string
	}
	AuthenticateCognitoConfig *struct {
		UserPoolArn, UserPoolClientId, UserPoolDomain      string
		OnUnauthenticatedRequest, Scope, SessionCookieName string
		SessionTimeout                                     *int64
		AuthenticationRequestExtraParams                   map[string]string
	}
	JwtValidationConfig *struct {
		JwksEndpoint, Issuer string
		AdditionalClaims     []struct {
			Format, Name string
			Values       []string
		}
	}
}

type forward struct {
	TargetGroups                []weighted
	TargetGroupStickinessConfig *stickiness
}

type weighted struct {
	TargetGroupArn string
	Weight         *int
}

type stickiness struct {
	Enabled         bool
	DurationSeconds *int
}

type condition struct {
	Field             string
	Values            []string
	HostHeaderConfig  *values
	PathPatternConfig *values
	HttpHeaderConfig  *struct {
		HttpHeaderName      string
		Values, RegexValues []string
	}
	HttpRequestMethodConfig *values
	QueryStringConfig       *struct{ Values []kv }
	SourceIpConfig          *values
}

type values struct{ Values, RegexValues []string }

// kv is a tag, an attribute or a query-string condition pair.
type kv struct{ Key, Value string }

type targetGroup struct {
	TargetGroupArn, TargetGroupName, Protocol, ProtocolVersion string
	Port                                                       *int
	VpcId, TargetType, IpAddressType                           string
	HealthCheckEnabled                                         bool
	HealthCheckProtocol, HealthCheckPort, HealthCheckPath      string
	HealthCheckIntervalSeconds, HealthCheckTimeoutSeconds      int
	HealthyThresholdCount, UnhealthyThresholdCount             int
	Matcher                                                    *struct{ HttpCode, GrpcCode string }
	LoadBalancerArns                                           []string          // filled in by view
	Targets                                                    []*target         `json:"targets,omitempty" query:"-"`
	Attrs                                                      map[string]string `json:"attrs,omitempty" query:"-"`
}

// target is a registered target and its health.
type target struct {
	Id          string    `json:"id"`
	Port        int       `json:"port,omitempty"`
	Zone        string    `json:"zone,omitempty"`
	State       string    `json:"state"`
	Reason      string    `json:"reason,omitempty"`
	Description string    `json:"description,omitempty"`
	Until       time.Time `json:"until,omitzero"` // the end of draining
	ok, bad     int       // consecutive passed and failed checks
	next        time.Time // the next health check
}

// New creates the ELB v2 service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: newState(), open: map[string]io.Closer{}, rr: map[string]int{}}
	s.rpc = &core.RPC{XMLNS: xmlns, Ops: map[string]core.Handler{
		"CreateLoadBalancer":             core.H(s.createLoadBalancer),
		"DescribeLoadBalancers":          core.H(s.describeLoadBalancers),
		"DeleteLoadBalancer":             core.H(s.deleteLoadBalancer),
		"SetSubnets":                     core.H(s.setSubnets),
		"SetSecurityGroups":              core.H(s.setSecurityGroups),
		"SetIpAddressType":               core.H(s.setIpAddressType),
		"DescribeLoadBalancerAttributes": core.H(s.lbAttributes),
		"ModifyLoadBalancerAttributes":   core.H(s.lbAttributes),
		"DescribeCapacityReservation":    core.H(s.capacity),
		"ModifyCapacityReservation":      core.H(s.capacity),
		"CreateListener":                 core.H(s.createListener),
		"DescribeListeners":              core.H(s.describeListeners),
		"ModifyListener":                 core.H(s.modifyListener),
		"DeleteListener":                 core.H(s.deleteListener),
		"DescribeListenerAttributes":     core.H(s.listenerAttributes),
		"ModifyListenerAttributes":       core.H(s.listenerAttributes),
		"AddListenerCertificates":        core.H(s.addListenerCertificates),
		"RemoveListenerCertificates":     core.H(s.removeListenerCertificates),
		"DescribeListenerCertificates":   core.H(s.describeListenerCertificates),
		"DescribeSSLPolicies":            core.H(s.describeSSLPolicies),
		"CreateRule":                     core.H(s.createRule),
		"DescribeRules":                  core.H(s.describeRules),
		"ModifyRule":                     core.H(s.modifyRule),
		"DeleteRule":                     core.H(s.deleteRule),
		"SetRulePriorities":              core.H(s.setRulePriorities),
		"CreateTargetGroup":              core.H(s.createTargetGroup),
		"DescribeTargetGroups":           core.H(s.describeTargetGroups),
		"ModifyTargetGroup":              core.H(s.modifyTargetGroup),
		"DeleteTargetGroup":              core.H(s.deleteTargetGroup),
		"DescribeTargetGroupAttributes":  core.H(s.tgAttributes),
		"ModifyTargetGroupAttributes":    core.H(s.tgAttributes),
		"RegisterTargets":                core.H(s.registerTargets),
		"DeregisterTargets":              core.H(s.deregisterTargets),
		"DescribeTargetHealth":           core.H(s.describeTargetHealth),
		"AddTags":                        core.H(s.addTags),
		"RemoveTags":                     core.H(s.removeTags),
		"DescribeTags":                   core.H(s.describeTags),
		"DescribeAccountLimits":          core.H(s.describeAccountLimits),
	}}
	s.stop = core.Every(time.Second, s.Tick)
	return s
}

func (s *Service) Info() core.Info {
	// The smithy service name is a JSON target for pogi's own in-process calls (ECS registering tasks).
	return core.Info{Name: "elbv2", Cloud: core.AWS, Title: "ELB v2", Signing: []string{"elasticloadbalancing"},
		Targets: []string{"ElasticLoadBalancing_v10"}, Match: isLB, Protocol: "query"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isLB(r) {
		s.serveLB(w, r)
		return
	}
	s.rpc.ServeHTTP(w, r)
}

func (s *Service) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeAll()
	s.st = newState()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores the state and opens the listeners' ports again.
func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st = st
	s.closeAll() // the old ports, and the saved ones, which are stale
	for _, arn := range core.SortedKeys(s.st.Listeners) {
		s.bind(s.st.Listeners[arn])
	}
	return nil
}

// Close stops the health checks and closes the listeners' ports.
func (s *Service) Close() error {
	s.stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeAll()
	return nil
}

func errf(code, format string, a ...any) error { return core.Errorf(400, code, format, a...) }

func invalid(format string, a ...any) error { return errf("ValidationError", format, a...) }

// mine reports whether arn belongs to the caller's account and region.
func mine(c *core.Ctx, arn string) bool {
	return strings.HasPrefix(arn, c.ARN("elasticloadbalancing", ""))
}

// lookups; lock held.

func (s *Service) lb(c *core.Ctx, arn string) (*loadBalancer, error) {
	if lb := s.st.LBs[arn]; lb != nil && mine(c, arn) {
		return lb, nil
	}
	return nil, errf("LoadBalancerNotFound", "Load balancer '%s' not found", arn)
}

func (s *Service) listener(c *core.Ctx, arn string) (*listener, error) {
	if l := s.st.Listeners[arn]; l != nil && mine(c, arn) {
		return l, nil
	}
	return nil, errf("ListenerNotFound", "Listener '%s' not found", arn)
}

func (s *Service) rule(c *core.Ctx, arn string) (*rule, error) {
	if ru := s.st.Rules[arn]; ru != nil && mine(c, arn) {
		return ru, nil
	}
	return nil, errf("RuleNotFound", "Rule '%s' not found", arn)
}

func (s *Service) group(c *core.Ctx, arn string) (*targetGroup, error) {
	if g := s.st.Groups[arn]; g != nil && mine(c, arn) {
		return g, nil
	}
	return nil, errf("TargetGroupNotFound", "Target group '%s' not found", arn)
}

// views are copies for responses: they leave pogi's fields out, and stored
// slices and maps are only ever replaced, never changed in place.

func (lb *loadBalancer) view() loadBalancer {
	v := *lb
	v.Attrs, v.Pogi.Ports = nil, nil
	return v
}

func (s *Service) listenerView(l *listener) listener {
	v := *l
	v.More, v.Attrs = nil, nil
	if ru := s.st.Rules[l.Rule]; ru != nil {
		v.DefaultActions = ru.Actions
	}
	return v
}

func (s *Service) groupView(g *targetGroup, used map[string][]string) targetGroup {
	v := *g
	v.Targets, v.Attrs, v.LoadBalancerArns = nil, nil, used[g.TargetGroupArn]
	return v
}

// page cuts a listing at marker (an offset) into pages of PageSize, at most 400.
func page[T any](items []T, marker string, size int) ([]T, string) {
	from, _ := strconv.Atoi(marker)
	from = min(max(from, 0), len(items))
	if size <= 0 || size > 400 {
		size = 400
	}
	to, next := min(from+size, len(items)), ""
	if to < len(items) {
		next = strconv.Itoa(to)
	}
	return items[from:to], next
}

func or[T any](p *T, def T) T {
	if p != nil {
		return *p
	}
	return def
}

func portOf(l *listener) int { return or(l.Port, 0) }

// ---- load balancers ----

var (
	nameRE    = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]{0,30}[a-zA-Z0-9])?$`)
	fixedCode = regexp.MustCompile(`^[245][0-9][0-9]$`)
	lbKinds   = map[string]string{"application": "app", "network": "net", "gateway": "gwy"}
	ipTypes   = []string{"ipv4", "dualstack", "dualstack-without-public-ipv4"}
)

// hostedZones are ELB's Route 53 zones per region: application, then network.
// ponytail: common regions only; the others get us-east-1's.
var hostedZones = map[string][2]string{
	"us-east-1": {"Z35SXDOTRQ7X7K", "Z26RNL4JYFTOTI"}, "us-east-2": {"Z3AADJGX6KTTL2", "ZLMOA37VPKANP"},
	"us-west-1": {"Z368ELLRRE2KJ0", "Z24FKFUX50B4VW"}, "us-west-2": {"Z1H1FL5HABSF5", "Z18D5FSROUN65G"},
	"ca-central-1": {"ZQSVJUPU6J1EY", "Z2EPGBW3API2WT"}, "eu-west-1": {"Z32O12XQLNTSW2", "Z2IFOLAFXWLO4F"},
	"eu-west-2": {"ZHURV8PSTC4K8", "ZD4D7Y8KGAS4G"}, "eu-central-1": {"Z215JYRZR1TBD5", "Z3F0SRJ5LGBH90"},
	"ap-south-1": {"ZP97RAFLXTNZK", "ZVDDRBQ08TROA"}, "ap-northeast-1": {"Z14GRHDCWA56QT", "Z31USIVHYNEOWT"},
	"ap-southeast-1": {"Z1LMS91P8CMLE5", "ZKVM4W9LS7TM"}, "ap-southeast-2": {"Z1GM3OXH4ZPM65", "ZCT6FZBF4DROD"},
	"sa-east-1": {"Z2P70J7HTTTPLU", "ZTK26PT1VY4CU"},
}

func zoneID(region, typ string) string {
	z, ok := hostedZones[region]
	if !ok {
		z = hostedZones["us-east-1"]
	}
	if typ == "application" {
		return z[0]
	}
	return z[1]
}

// zones describes the subnets of a load balancer and finds its VPC in pogi's
// EC2; subnets EC2 does not know go to zones a, b, c. It refuses subnets of
// two VPCs, two subnets in one zone, and an ALB in fewer than two zones.
// ponytail: a load balancer with no subnets at all is still accepted.
func (s *Service) zones(c *core.Ctx, typ string, ms []mapping) (vpc string, out []zone, err error) {
	var known struct {
		Subnets []struct {
			ID   string `xml:"subnetId"`
			VPC  string `xml:"vpcId"`
			Zone string `xml:"availabilityZone"`
		} `xml:"subnetSet>item"`
	}
	if len(ms) > 0 {
		form := url.Values{"Action": {"DescribeSubnets"}, "Version": {"2016-11-15"}}
		for i, m := range ms {
			form.Set("SubnetId."+strconv.Itoa(i+1), m.SubnetId)
		}
		status, _, body := s.hub.Do(c.Account, c.Region, "ec2", http.MethodPost, "/",
			http.Header{"Content-Type": {"application/x-www-form-urlencoded"}}, []byte(form.Encode()))
		if status == http.StatusOK {
			xml.Unmarshal(body, &known)
		}
	}
	for i, m := range ms {
		z := zone{ZoneName: c.Region + string(rune('a'+i%3)), SubnetId: m.SubnetId}
		found := false
		for _, sn := range known.Subnets {
			if sn.ID == m.SubnetId {
				found = true
				if vpc != "" && vpc != sn.VPC {
					return "", nil, invalid("Subnets %s and %s belong to different VPCs (%s and %s)", out[0].SubnetId, m.SubnetId, vpc, sn.VPC)
				}
				z.ZoneName, vpc = sn.Zone, sn.VPC
			}
		}
		if found && slices.ContainsFunc(out, func(o zone) bool { return o.ZoneName == z.ZoneName }) { // zones of unknown subnets are made up
			return "", nil, errf("InvalidConfigurationRequest", "A load balancer cannot be attached to multiple subnets in the same Availability Zone")
		}
		if m.AllocationId+m.PrivateIPv4Address+m.IPv6Address != "" {
			z.LoadBalancerAddresses = []mapping{{AllocationId: m.AllocationId, PrivateIPv4Address: m.PrivateIPv4Address, IPv6Address: m.IPv6Address}}
		}
		out = append(out, z)
	}
	if typ == "application" && len(out) == 1 {
		return "", nil, invalid("At least two subnets in two different Availability Zones must be specified")
	}
	return vpc, out, nil
}

func (s *Service) createLoadBalancer(c *core.Ctx, in *struct {
	Name, Type, Scheme, IpAddressType, CustomerOwnedIpv4Pool string
	Subnets, SecurityGroups                                  []string
	SubnetMappings                                           []mapping
	Tags                                                     []kv
}) (any, error) {
	typ, scheme, ip := cmp.Or(in.Type, "application"), cmp.Or(in.Scheme, "internet-facing"), cmp.Or(in.IpAddressType, "ipv4")
	switch {
	case lbKinds[typ] == "":
		return nil, invalid("Load balancer type '%s' must be one of 'application, network, gateway'", typ)
	case !nameRE.MatchString(in.Name) || strings.HasPrefix(in.Name, "internal-"):
		return nil, invalid("Load balancer name '%s' must have at most 32 alphanumeric characters or hyphens, must not begin or end with a hyphen, and must not begin with 'internal-'", in.Name)
	case scheme != "internet-facing" && scheme != "internal":
		return nil, invalid("Scheme '%s' must be one of 'internet-facing, internal'", scheme)
	case !slices.Contains(ipTypes, ip):
		return nil, invalid("IP address type '%s' must be one of 'ipv4, dualstack, dualstack-without-public-ipv4'", ip)
	}
	tags, err := tagMap(in.Tags)
	if err != nil {
		return nil, err
	}
	for _, id := range in.Subnets {
		in.SubnetMappings = append(in.SubnetMappings, mapping{SubnetId: id})
	}
	vpc, zones, err := s.zones(c, typ, in.SubnetMappings)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, lb := range s.st.LBs {
		if mine(c, lb.LoadBalancerArn) && lb.LoadBalancerName == in.Name { // idempotent for the same settings
			if lb.Type != typ || lb.Scheme != scheme {
				return nil, errf("DuplicateLoadBalancerName", "A load balancer with the same name '%s' exists, but with different settings", in.Name)
			}
			return map[string]any{"LoadBalancers": []loadBalancer{lb.view()}}, nil
		}
	}
	id := core.RandHex(8)
	dns := in.Name + "-" + id + "." + c.Region + ".elb.localhost"
	if scheme == "internal" {
		dns = "internal-" + dns
	}
	lb := &loadBalancer{LoadBalancerArn: c.ARN("elasticloadbalancing", "loadbalancer/"+lbKinds[typ]+"/"+in.Name+"/"+id),
		LoadBalancerName: in.Name, DNSName: dns, CanonicalHostedZoneId: zoneID(c.Region, typ), CreatedTime: core.Time(time.Now().UTC()),
		Scheme: scheme, Type: typ, IpAddressType: ip, VpcId: vpc, AvailabilityZones: zones, SecurityGroups: in.SecurityGroups,
		CustomerOwnedIpv4Pool: in.CustomerOwnedIpv4Pool, Attrs: map[string]string{}}
	lb.State.Code = "active"
	s.st.LBs[lb.LoadBalancerArn] = lb
	s.st.Tags[lb.LoadBalancerArn] = tags
	v := lb.view()
	v.State.Code = "provisioning" // CreateLoadBalancer's answer; it is active by the next describe
	return map[string]any{"LoadBalancers": []loadBalancer{v}}, nil
}

func (s *Service) describeLoadBalancers(c *core.Ctx, in *struct {
	LoadBalancerArns, Names []string
	Marker                  string
	PageSize                int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []loadBalancer
	for _, lb := range s.st.LBs {
		if mine(c, lb.LoadBalancerArn) && (len(in.LoadBalancerArns)+len(in.Names) == 0 ||
			slices.Contains(in.LoadBalancerArns, lb.LoadBalancerArn) || slices.Contains(in.Names, lb.LoadBalancerName)) {
			out = append(out, lb.view())
		}
	}
	if len(out) < len(in.LoadBalancerArns)+len(in.Names) {
		return nil, errf("LoadBalancerNotFound", "One or more load balancers not found")
	}
	slices.SortFunc(out, func(a, b loadBalancer) int { return cmp.Compare(a.LoadBalancerName, b.LoadBalancerName) })
	out, next := page(out, in.Marker, in.PageSize)
	return map[string]any{"LoadBalancers": out, "NextMarker": next}, nil
}

// deleteLoadBalancer takes its listeners and rules along; deleting a load
// balancer that is gone succeeds, as in ELB.
func (s *Service) deleteLoadBalancer(c *core.Ctx, in *struct{ LoadBalancerArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lb, err := s.lb(c, in.LoadBalancerArn)
	if err != nil {
		return nil, nil
	}
	if lb.Attrs["deletion_protection.enabled"] == "true" {
		return nil, errf("OperationNotPermitted", "Load balancer '%s' cannot be deleted because deletion protection is enabled", lb.LoadBalancerArn)
	}
	for _, l := range s.st.Listeners {
		if l.LoadBalancerArn == lb.LoadBalancerArn {
			s.dropListener(l)
		}
	}
	delete(s.st.LBs, lb.LoadBalancerArn)
	delete(s.st.Tags, lb.LoadBalancerArn)
	return nil, nil
}

func (s *Service) setSubnets(c *core.Ctx, in *struct {
	LoadBalancerArn, IpAddressType string
	Subnets                        []string
	SubnetMappings                 []mapping
}) (any, error) {
	for _, id := range in.Subnets {
		in.SubnetMappings = append(in.SubnetMappings, mapping{SubnetId: id})
	}
	s.mu.Lock()
	lb, err := s.lb(c, in.LoadBalancerArn)
	var typ string
	if lb != nil {
		typ = lb.Type
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	vpc, zones, err := s.zones(c, typ, in.SubnetMappings)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if lb, err = s.lb(c, in.LoadBalancerArn); err != nil {
		return nil, err
	}
	lb.AvailabilityZones, lb.VpcId, lb.IpAddressType = zones, cmp.Or(vpc, lb.VpcId), cmp.Or(in.IpAddressType, lb.IpAddressType)
	return map[string]any{"AvailabilityZones": zones, "IpAddressType": lb.IpAddressType}, nil
}

func (s *Service) setSecurityGroups(c *core.Ctx, in *struct {
	LoadBalancerArn, EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic string
	SecurityGroups                                                        []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lb, err := s.lb(c, in.LoadBalancerArn)
	if err != nil {
		return nil, err
	}
	if lb.Type == "gateway" {
		return nil, errf("InvalidConfigurationRequest", "Security groups are not supported for load balancers with type 'gateway'")
	}
	lb.SecurityGroups = in.SecurityGroups
	lb.EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic = cmp.Or(in.EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic, lb.EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic)
	return map[string]any{"SecurityGroupIds": lb.SecurityGroups, "EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic": lb.EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic}, nil
}

func (s *Service) setIpAddressType(c *core.Ctx, in *struct{ LoadBalancerArn, IpAddressType string }) (any, error) {
	if !slices.Contains(ipTypes, in.IpAddressType) {
		return nil, invalid("IP address type '%s' must be one of 'ipv4, dualstack, dualstack-without-public-ipv4'", in.IpAddressType)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	lb, err := s.lb(c, in.LoadBalancerArn)
	if err != nil {
		return nil, err
	}
	lb.IpAddressType = in.IpAddressType
	return map[string]any{"IpAddressType": lb.IpAddressType}, nil
}

// capacity serves DescribeCapacityReservation and ModifyCapacityReservation.
// ponytail: the minimum capacity is kept, not provisioned per zone.
func (s *Service) capacity(c *core.Ctx, in *struct {
	LoadBalancerArn             string
	MinimumLoadBalancerCapacity *struct{ CapacityUnits int }
	ResetCapacityReservation    bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lb, err := s.lb(c, in.LoadBalancerArn)
	if err != nil {
		return nil, err
	}
	if in.MinimumLoadBalancerCapacity != nil || in.ResetCapacityReservation {
		lb.Capacity = in.MinimumLoadBalancerCapacity
	}
	return map[string]any{"MinimumLoadBalancerCapacity": lb.Capacity}, nil
}

// ---- attributes: ELB's defaults per kind, overlaid with what was set ----

// table parses "key=value key=value…".
func table(s string) map[string]string {
	m := map[string]string{}
	for _, f := range strings.Fields(s) {
		k, v, _ := strings.Cut(f, "=")
		m[k] = v
	}
	return m
}

const tgHealth = ` target_group_health.dns_failover.minimum_healthy_targets.count=1
	target_group_health.dns_failover.minimum_healthy_targets.percentage=off
	target_group_health.unhealthy_state_routing.minimum_healthy_targets.count=1
	target_group_health.unhealthy_state_routing.minimum_healthy_targets.percentage=off`

const httpResponse = `routing.http.response.server.enabled=true
	routing.http.response.strict_transport_security.header_value= routing.http.response.access_control_allow_origin.header_value=
	routing.http.response.access_control_allow_methods.header_value= routing.http.response.access_control_allow_headers.header_value=
	routing.http.response.access_control_allow_credentials.header_value= routing.http.response.access_control_expose_headers.header_value=
	routing.http.response.access_control_max_age.header_value= routing.http.response.content_security_policy.header_value=
	routing.http.response.x_content_type_options.header_value= routing.http.response.x_frame_options.header_value=`

var (
	lbAttrs = map[string]map[string]string{
		"application": table(`access_logs.s3.enabled=false access_logs.s3.bucket= access_logs.s3.prefix= client_keep_alive.seconds=3600
			connection_logs.s3.enabled=false connection_logs.s3.bucket= connection_logs.s3.prefix= deletion_protection.enabled=false
			health_check_logs.s3.enabled=false health_check_logs.s3.bucket= health_check_logs.s3.prefix= idle_timeout.timeout_seconds=60
			load_balancing.cross_zone.enabled=true routing.http.desync_mitigation_mode=defensive
			routing.http.drop_invalid_header_fields.enabled=false routing.http.preserve_host_header.enabled=false
			routing.http.x_amzn_tls_version_and_cipher_suite.enabled=false routing.http.xff_client_port.enabled=false
			routing.http.xff_header_processing.mode=append routing.http2.enabled=true waf.fail_open.enabled=false
			zonal_shift.config.enabled=false`),
		"network": table(`access_logs.s3.enabled=false access_logs.s3.bucket= access_logs.s3.prefix= deletion_protection.enabled=false
			dns_record.client_routing_policy=any_availability_zone load_balancing.cross_zone.enabled=false
			secondary_ips.auto_assigned.per_subnet=0 zonal_shift.config.enabled=false`),
		"gateway": table(`deletion_protection.enabled=false load_balancing.cross_zone.enabled=false`),
	}
	tgAttrs = map[string]map[string]string{
		"application": table(`deregistration_delay.timeout_seconds=300 load_balancing.algorithm.anomaly_mitigation=off
			load_balancing.algorithm.type=round_robin load_balancing.cross_zone.enabled=use_load_balancer_configuration
			slow_start.duration_seconds=0 stickiness.app_cookie.cookie_name= stickiness.app_cookie.duration_seconds=86400
			stickiness.enabled=false stickiness.lb_cookie.duration_seconds=86400 stickiness.type=lb_cookie` + tgHealth),
		"network": table(`deregistration_delay.connection_termination.enabled=false deregistration_delay.timeout_seconds=300
			load_balancing.cross_zone.enabled=use_load_balancer_configuration preserve_client_ip.enabled=true
			proxy_protocol_v2.enabled=false stickiness.enabled=false stickiness.type=source_ip
			target_health_state.unhealthy.connection_termination.enabled=true
			target_health_state.unhealthy.draining_interval_seconds=0` + tgHealth),
		"gateway": table(`deregistration_delay.timeout_seconds=300 stickiness.enabled=false stickiness.type=source_ip_dest_ip_proto
			target_failover.on_deregistration=no_rebalance target_failover.on_unhealthy=no_rebalance` + tgHealth),
		"lambda": table(`lambda.multi_value_headers.enabled=false`),
	}
	listenerAttrs = map[string]map[string]string{
		"HTTP": table(httpResponse),
		"HTTPS": table(httpResponse + ` routing.http.request.x_amzn_mtls_clientcert_serial_number.header_name=X-Amzn-Mtls-Clientcert-Serial-Number
			routing.http.request.x_amzn_mtls_clientcert_issuer.header_name=X-Amzn-Mtls-Clientcert-Issuer
			routing.http.request.x_amzn_mtls_clientcert_subject.header_name=X-Amzn-Mtls-Clientcert-Subject
			routing.http.request.x_amzn_mtls_clientcert_validity.header_name=X-Amzn-Mtls-Clientcert-Validity
			routing.http.request.x_amzn_mtls_clientcert_leaf.header_name=X-Amzn-Mtls-Clientcert-Leaf
			routing.http.request.x_amzn_mtls_clientcert.header_name=X-Amzn-Mtls-Clientcert
			routing.http.request.x_amzn_tls_version.header_name=X-Amzn-Tls-Version
			routing.http.request.x_amzn_tls_cipher_suite.header_name=X-Amzn-Tls-Cipher-Suite`),
		"TCP": table(`tcp.idle_timeout.seconds=350`),
	}
)

// attributes lists defaults overlaid with what was set, by key.
func attributes(defaults, set map[string]string) []kv {
	all := maps.Clone(set)
	if all == nil {
		all = map[string]string{}
	}
	for k, v := range defaults {
		if _, ok := all[k]; !ok {
			all[k] = v
		}
	}
	out := make([]kv, 0, len(all))
	for _, k := range slices.Sorted(maps.Keys(all)) {
		out = append(out, kv{k, all[k]})
	}
	return out
}

// setAttributes stores attributes whose keys any kind of the resource knows.
// ponytail: a key of another kind (an NLB key on an ALB) is accepted.
func setAttributes(set *map[string]string, kinds map[string]map[string]string, what string, in []kv) error {
	for _, a := range in {
		known := false
		for _, d := range kinds {
			_, ok := d[a.Key]
			known = known || ok
		}
		if !known {
			return invalid("%s attribute key '%s' is not recognized", what, a.Key)
		}
	}
	if *set == nil {
		*set = map[string]string{}
	}
	for _, a := range in {
		(*set)[a.Key] = a.Value
	}
	return nil
}

type attrsIn struct {
	LoadBalancerArn, TargetGroupArn, ListenerArn string
	Attributes                                   []kv
}

// lbAttributes serves DescribeLoadBalancerAttributes and ModifyLoadBalancerAttributes.
func (s *Service) lbAttributes(c *core.Ctx, in *attrsIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lb, err := s.lb(c, in.LoadBalancerArn)
	if err != nil {
		return nil, err
	}
	if err := setAttributes(&lb.Attrs, lbAttrs, "Load balancer", in.Attributes); err != nil {
		return nil, err
	}
	return map[string]any{"Attributes": attributes(lbAttrs[lb.Type], lb.Attrs)}, nil
}

// tgAttributes serves DescribeTargetGroupAttributes and ModifyTargetGroupAttributes.
func (s *Service) tgAttributes(c *core.Ctx, in *attrsIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.TargetGroupArn)
	if err != nil {
		return nil, err
	}
	if err := setAttributes(&g.Attrs, tgAttrs, "Target group", in.Attributes); err != nil {
		return nil, err
	}
	return map[string]any{"Attributes": attributes(g.defaults(), g.Attrs)}, nil
}

// listenerAttributes serves DescribeListenerAttributes and ModifyListenerAttributes.
func (s *Service) listenerAttributes(c *core.Ctx, in *attrsIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.listener(c, in.ListenerArn)
	if err != nil {
		return nil, err
	}
	if err := setAttributes(&l.Attrs, listenerAttrs, "Listener", in.Attributes); err != nil {
		return nil, err
	}
	return map[string]any{"Attributes": attributes(listenerAttrs[l.Protocol], l.Attrs)}, nil
}

// defaults are the attributes a target group has unless set.
func (g *targetGroup) defaults() map[string]string {
	switch {
	case g.TargetType == "lambda":
		return tgAttrs["lambda"]
	case g.Protocol == "GENEVE":
		return tgAttrs["gateway"]
	case g.Protocol == "HTTP" || g.Protocol == "HTTPS":
		return tgAttrs["application"]
	}
	d := maps.Clone(tgAttrs["network"]) // client IPs are kept, except for IP targets over TCP and TLS
	d["preserve_client_ip.enabled"] = strconv.FormatBool(g.TargetType != "ip" || g.Protocol != "TCP" && g.Protocol != "TLS")
	return d
}

func (g *targetGroup) attr(key string) string { return cmp.Or(g.Attrs[key], g.defaults()[key]) }

// ---- listeners ----

// checkListener validates a listener's protocol, port and certificate and
// fills in ELB's defaults.
func checkListener(lb *loadBalancer, l *listener) error {
	switch lb.Type {
	case "gateway":
		l.Port, l.Protocol, l.SslPolicy, l.Certificates = nil, "", "", nil
		return nil
	case "application":
		if l.Protocol != "HTTP" && l.Protocol != "HTTPS" {
			return invalid("Listener protocol '%s' must be one of 'HTTPS, HTTP'", l.Protocol)
		}
	default:
		if !slices.Contains([]string{"TCP", "TLS", "UDP", "TCP_UDP", "QUIC", "TCP_QUIC"}, l.Protocol) {
			return invalid("Listener protocol '%s' must be one of 'TCP_UDP, TLS, UDP, TCP, QUIC, TCP_QUIC'", l.Protocol)
		}
	}
	if p := portOf(l); p < 1 || p > 65535 {
		return invalid("A listener port between 1 and 65535 must be specified")
	}
	if l.Protocol != "HTTPS" && l.Protocol != "TLS" { // changing to HTTP drops the policy and certificate
		l.SslPolicy, l.Certificates = "", nil
		return nil
	}
	if len(l.Certificates) == 0 || l.Certificates[0].CertificateArn == "" {
		return invalid("A certificate must be specified for %s listeners", l.Protocol)
	}
	l.SslPolicy = cmp.Or(l.SslPolicy, "ELBSecurityPolicy-2016-08")
	if !strings.HasPrefix(l.SslPolicy, "ELBSecurityPolicy-") {
		return errf("SSLPolicyNotFound", "SSL policy '%s' not found", l.SslPolicy)
	}
	l.Certificates = []cert{{CertificateArn: l.Certificates[0].CertificateArn}}
	return nil
}

type listenerIn struct {
	LoadBalancerArn, ListenerArn, Protocol, SslPolicy string
	Port                                              *int
	Certificates                                      []cert
	DefaultActions                                    []action
	AlpnPolicy                                        []string
	MutualAuthentication                              *mtls
	Tags                                              []kv
}

// clash finds another listener of l's load balancer on l's port; lock held.
func (s *Service) clash(l *listener) *listener {
	for _, x := range s.st.Listeners {
		if x.ListenerArn != l.ListenerArn && x.LoadBalancerArn == l.LoadBalancerArn && l.Port != nil && portOf(x) == *l.Port {
			return x
		}
	}
	return nil
}

// CertInUse tells IAM whether a listener uses a server certificate.
func (s *Service) CertInUse(arn string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range s.st.Listeners {
		if slices.ContainsFunc(l.Certificates, func(ct cert) bool { return ct.CertificateArn == arn }) || slices.Contains(l.More, arn) {
			return true
		}
	}
	return false
}

// checkCerts refuses an IAM server certificate that IAM does not have. IAM
// checks for listeners (CertInUse) after it removes a certificate, so a
// listener made while a certificate is deleted ends up consistent either way.
// ACM certificates are not checked. Lock held (order: elbv2, then iam).
func (s *Service) checkCerts(arns ...string) error {
	iam, ok := s.hub.Service("iam").(interface{ ServerCertExists(arn string) bool })
	for _, arn := range arns {
		if ok && strings.Contains(arn, ":iam::") && strings.Contains(arn, ":server-certificate/") && !iam.ServerCertExists(arn) {
			return errf("CertificateNotFound", "Certificate '%s' not found", arn)
		}
	}
	return nil
}

func (s *Service) createListener(c *core.Ctx, in *listenerIn) (any, error) {
	tags, err := tagMap(in.Tags)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	lb, err := s.lb(c, in.LoadBalancerArn)
	if err != nil {
		return nil, err
	}
	l := &listener{LoadBalancerArn: lb.LoadBalancerArn, Port: in.Port, Protocol: in.Protocol, SslPolicy: in.SslPolicy,
		Certificates: in.Certificates, AlpnPolicy: in.AlpnPolicy, MutualAuthentication: in.MutualAuthentication}
	if err := checkListener(lb, l); err != nil {
		return nil, err
	}
	for _, ct := range l.Certificates {
		if err := s.checkCerts(ct.CertificateArn); err != nil {
			return nil, err
		}
	}
	acts, err := s.actions(in.DefaultActions)
	if err != nil {
		return nil, err
	}
	if x := s.clash(l); x != nil { // idempotent for the same settings
		if x.Protocol != l.Protocol || !reflect.DeepEqual(s.st.Rules[x.Rule].Actions, acts) {
			return nil, errf("DuplicateListener", "A listener already exists on this port for this load balancer '%s'", lb.LoadBalancerArn)
		}
		return map[string]any{"Listeners": []listener{s.listenerView(x)}}, nil
	}
	l.ListenerArn = strings.Replace(lb.LoadBalancerArn, ":loadbalancer/", ":listener/", 1) + "/" + core.RandHex(8)
	l.Rule = strings.Replace(l.ListenerArn, ":listener/", ":listener-rule/", 1) + "/" + core.RandHex(8)
	s.st.Rules[l.Rule] = &rule{RuleArn: l.Rule, Priority: "default", IsDefault: true, Actions: acts, Listener: l.ListenerArn}
	s.st.Listeners[l.ListenerArn] = l
	s.st.Tags[l.ListenerArn] = tags
	s.bind(l)
	return map[string]any{"Listeners": []listener{s.listenerView(l)}}, nil
}

func (s *Service) modifyListener(c *core.Ctx, in *listenerIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.listener(c, in.ListenerArn)
	if err != nil {
		return nil, err
	}
	n := *l
	n.Port = cmp.Or(in.Port, n.Port)
	n.Protocol, n.SslPolicy = cmp.Or(in.Protocol, n.Protocol), cmp.Or(in.SslPolicy, n.SslPolicy)
	if in.Certificates != nil {
		n.Certificates = in.Certificates
	}
	if in.AlpnPolicy != nil {
		n.AlpnPolicy = in.AlpnPolicy
	}
	n.MutualAuthentication = cmp.Or(in.MutualAuthentication, n.MutualAuthentication)
	if err := checkListener(s.st.LBs[l.LoadBalancerArn], &n); err != nil {
		return nil, err
	}
	for _, ct := range n.Certificates {
		if err := s.checkCerts(ct.CertificateArn); err != nil {
			return nil, err
		}
	}
	if s.clash(&n) != nil {
		return nil, errf("DuplicateListener", "A listener already exists on this port for this load balancer '%s'", l.LoadBalancerArn)
	}
	if in.DefaultActions != nil {
		acts, err := s.actions(in.DefaultActions)
		if err != nil {
			return nil, err
		}
		s.st.Rules[l.Rule].Actions = acts
	}
	moved := portOf(&n) != portOf(l) || n.Protocol != l.Protocol
	if moved {
		s.unbind(l)
	}
	*l = n
	if moved {
		s.bind(l)
	}
	return map[string]any{"Listeners": []listener{s.listenerView(l)}}, nil
}

// dropListener deletes a listener with its rules and closes its port; lock held.
func (s *Service) dropListener(l *listener) {
	s.unbind(l)
	for arn, ru := range s.st.Rules {
		if ru.Listener == l.ListenerArn {
			delete(s.st.Rules, arn)
			delete(s.st.Tags, arn)
		}
	}
	delete(s.st.Listeners, l.ListenerArn)
	delete(s.st.Tags, l.ListenerArn)
}

func (s *Service) deleteListener(c *core.Ctx, in *struct{ ListenerArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.listener(c, in.ListenerArn)
	if err != nil {
		return nil, err
	}
	s.dropListener(l)
	return nil, nil
}

// listenersOf lists a load balancer's listeners by port; lock held.
func (s *Service) listenersOf(lbArn string) []*listener {
	var out []*listener
	for _, l := range s.st.Listeners {
		if l.LoadBalancerArn == lbArn {
			out = append(out, l)
		}
	}
	slices.SortFunc(out, func(a, b *listener) int {
		return cmp.Or(cmp.Compare(portOf(a), portOf(b)), cmp.Compare(a.ListenerArn, b.ListenerArn))
	})
	return out
}

func (s *Service) describeListeners(c *core.Ctx, in *struct {
	LoadBalancerArn string
	ListenerArns    []string
	Marker          string
	PageSize        int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ls []*listener
	switch {
	case in.LoadBalancerArn != "":
		if _, err := s.lb(c, in.LoadBalancerArn); err != nil {
			return nil, err
		}
		ls = s.listenersOf(in.LoadBalancerArn)
	case len(in.ListenerArns) > 0:
		for _, arn := range in.ListenerArns {
			l, err := s.listener(c, arn)
			if err != nil {
				return nil, err
			}
			ls = append(ls, l)
		}
	default:
		return nil, invalid("You must specify either listener ARNs or a load balancer ARN")
	}
	var out []listener
	for _, l := range ls {
		out = append(out, s.listenerView(l))
	}
	out, next := page(out, in.Marker, in.PageSize)
	return map[string]any{"Listeners": out, "NextMarker": next}, nil
}

type certsIn struct {
	ListenerArn, Marker string
	Certificates        []cert
	PageSize            int
}

func (s *Service) addListenerCertificates(c *core.Ctx, in *certsIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.listener(c, in.ListenerArn)
	if err != nil {
		return nil, err
	}
	for _, ct := range in.Certificates {
		if err := s.checkCerts(ct.CertificateArn); err != nil {
			return nil, err
		}
	}
	for _, ct := range in.Certificates {
		if !slices.Contains(l.More, ct.CertificateArn) {
			l.More = append(l.More, ct.CertificateArn)
		}
	}
	return map[string]any{"Certificates": certList(l.More, false)}, nil
}

func (s *Service) removeListenerCertificates(c *core.Ctx, in *certsIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.listener(c, in.ListenerArn)
	if err != nil {
		return nil, err
	}
	var keep []string
	for _, arn := range l.More {
		if !slices.ContainsFunc(in.Certificates, func(ct cert) bool { return ct.CertificateArn == arn }) {
			keep = append(keep, arn)
		}
	}
	l.More = keep
	return nil, nil
}

func (s *Service) describeListenerCertificates(c *core.Ctx, in *certsIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.listener(c, in.ListenerArn)
	if err != nil {
		return nil, err
	}
	var all []string
	for _, ct := range l.Certificates {
		all = append(all, ct.CertificateArn)
	}
	out, next := page(append(certList(all, true), certList(l.More, false)...), in.Marker, in.PageSize)
	return map[string]any{"Certificates": out, "NextMarker": next}, nil
}

func certList(arns []string, isDefault bool) []cert {
	var out []cert
	for _, arn := range arns {
		out = append(out, cert{CertificateArn: arn, IsDefault: &isDefault})
	}
	return out
}

type sslPolicy struct {
	Name                       string
	SslProtocols               []string
	Ciphers                    []cipher
	SupportedLoadBalancerTypes []string
}

type cipher struct {
	Name     string
	Priority int
}

// sslPolicies are some of ELB's security policies: name, protocols, ciphers.
// ponytail: listeners accept any ELBSecurityPolicy-* name.
var sslPolicies = func() []sslPolicy {
	var out []sslPolicy
	for _, p := range [][3]string{
		{"ELBSecurityPolicy-2016-08", "TLSv1 TLSv1.1 TLSv1.2", "ECDHE-ECDSA-AES128-GCM-SHA256 ECDHE-RSA-AES128-GCM-SHA256 " +
			"ECDHE-ECDSA-AES128-SHA256 ECDHE-RSA-AES128-SHA256 ECDHE-ECDSA-AES128-SHA ECDHE-RSA-AES128-SHA ECDHE-ECDSA-AES256-GCM-SHA384 " +
			"ECDHE-RSA-AES256-GCM-SHA384 ECDHE-ECDSA-AES256-SHA384 ECDHE-RSA-AES256-SHA384 ECDHE-RSA-AES256-SHA ECDHE-ECDSA-AES256-SHA " +
			"AES128-GCM-SHA256 AES128-SHA256 AES128-SHA AES256-GCM-SHA384 AES256-SHA256 AES256-SHA"},
		{"ELBSecurityPolicy-TLS13-1-2-2021-06", "TLSv1.2 TLSv1.3", "TLS_AES_128_GCM_SHA256 TLS_AES_256_GCM_SHA384 " +
			"TLS_CHACHA20_POLY1305_SHA256 ECDHE-ECDSA-AES128-GCM-SHA256 ECDHE-RSA-AES128-GCM-SHA256 ECDHE-ECDSA-AES128-SHA256 " +
			"ECDHE-RSA-AES128-SHA256 ECDHE-ECDSA-AES256-GCM-SHA384 ECDHE-RSA-AES256-GCM-SHA384 ECDHE-ECDSA-AES256-SHA384 ECDHE-RSA-AES256-SHA384"},
		{"ELBSecurityPolicy-TLS13-1-3-2021-06", "TLSv1.3", "TLS_AES_128_GCM_SHA256 TLS_AES_256_GCM_SHA384 TLS_CHACHA20_POLY1305_SHA256"},
		{"ELBSecurityPolicy-TLS-1-2-2017-01", "TLSv1.2", "ECDHE-ECDSA-AES128-GCM-SHA256 ECDHE-RSA-AES128-GCM-SHA256 " +
			"ECDHE-ECDSA-AES128-SHA256 ECDHE-RSA-AES128-SHA256 ECDHE-ECDSA-AES256-GCM-SHA384 ECDHE-RSA-AES256-GCM-SHA384 " +
			"ECDHE-ECDSA-AES256-SHA384 ECDHE-RSA-AES256-SHA384 AES128-GCM-SHA256 AES128-SHA256 AES256-GCM-SHA384 AES256-SHA256"},
	} {
		sp := sslPolicy{Name: p[0], SslProtocols: strings.Fields(p[1]), SupportedLoadBalancerTypes: []string{"application", "network"}}
		for i, c := range strings.Fields(p[2]) {
			sp.Ciphers = append(sp.Ciphers, cipher{c, i + 1})
		}
		out = append(out, sp)
	}
	return out
}()

func (s *Service) describeSSLPolicies(c *core.Ctx, in *struct {
	Names                    []string
	LoadBalancerType, Marker string
	PageSize                 int
}) (any, error) {
	var out []sslPolicy
	for _, p := range sslPolicies {
		if len(in.Names) == 0 || slices.Contains(in.Names, p.Name) {
			out = append(out, p)
		}
	}
	if len(out) < len(in.Names) {
		return nil, errf("SSLPolicyNotFound", "One or more SSL policies not found")
	}
	out, next := page(out, in.Marker, in.PageSize)
	return map[string]any{"SslPolicies": out, "NextMarker": next}, nil
}

// ---- actions and rules ----

// actions validates actions and fills them in the way ELB echoes them: an
// order each, a forward's target group both ways, a redirect's defaults; lock held.
func (s *Service) actions(in []action) ([]action, error) {
	if len(in) == 0 {
		return nil, invalid("At least one action must be specified")
	}
	out := slices.Clone(in)
	for i := range out {
		a := &out[i]
		a.Order = cmp.Or(a.Order, i+1)
		switch a.Type {
		case "forward":
			if a.ForwardConfig == nil {
				if a.TargetGroupArn == "" {
					return nil, invalid("A target group ARN must be specified for forward actions")
				}
				a.ForwardConfig = &forward{TargetGroups: []weighted{{TargetGroupArn: a.TargetGroupArn}}}
			}
			f := *a.ForwardConfig
			f.TargetGroups = slices.Clone(f.TargetGroups)
			for j := range f.TargetGroups {
				t := &f.TargetGroups[j]
				if s.st.Groups[t.TargetGroupArn] == nil {
					return nil, errf("TargetGroupNotFound", "Target group '%s' not found", t.TargetGroupArn)
				}
				t.Weight = cmp.Or(t.Weight, new(1))
			}
			if len(f.TargetGroups) == 1 {
				a.TargetGroupArn = f.TargetGroups[0].TargetGroupArn
			}
			f.TargetGroupStickinessConfig = cmp.Or(f.TargetGroupStickinessConfig, &stickiness{})
			a.ForwardConfig = &f
		case "redirect":
			if a.RedirectConfig == nil || a.RedirectConfig.StatusCode != "HTTP_301" && a.RedirectConfig.StatusCode != "HTTP_302" {
				return nil, invalid("A redirect action needs a RedirectConfig with status code 'HTTP_301' or 'HTTP_302'")
			}
			r := *a.RedirectConfig
			r.Protocol, r.Port, r.Host = cmp.Or(r.Protocol, "#{protocol}"), cmp.Or(r.Port, "#{port}"), cmp.Or(r.Host, "#{host}")
			r.Path, r.Query = cmp.Or(r.Path, "/#{path}"), cmp.Or(r.Query, "#{query}")
			a.RedirectConfig = &r
		case "fixed-response":
			if a.FixedResponseConfig == nil || !fixedCode.MatchString(a.FixedResponseConfig.StatusCode) {
				return nil, invalid("A fixed-response action needs a FixedResponseConfig with a 2XX, 4XX or 5XX status code")
			}
		case "authenticate-oidc", "authenticate-cognito", "jwt-validation":
		default:
			return nil, invalid("Action type '%s' must be one of 'redirect, fixed-response, forward, authenticate-cognito, authenticate-oidc, jwt-validation'", a.Type)
		}
	}
	slices.SortStableFunc(out, func(a, b action) int { return cmp.Compare(a.Order, b.Order) })
	return out, nil
}

// conditions validates rule conditions; path and host patterns are echoed
// both in Values and in their config, as ELB does.
func conditions(in []condition) ([]condition, error) {
	if len(in) == 0 {
		return nil, invalid("A rule must have at least one condition")
	}
	out := slices.Clone(in)
	for i := range out {
		c := &out[i]
		var set bool
		switch c.Field {
		case "path-pattern", "host-header":
			cfg := &c.PathPatternConfig
			if c.Field == "host-header" {
				cfg = &c.HostHeaderConfig
			}
			if *cfg == nil {
				*cfg = &values{Values: c.Values}
			}
			if c.Values == nil {
				c.Values = (*cfg).Values
			}
			set = len((*cfg).Values)+len((*cfg).RegexValues) > 0
		case "http-header":
			set = c.HttpHeaderConfig != nil && c.HttpHeaderConfig.HttpHeaderName != "" && len(c.HttpHeaderConfig.Values)+len(c.HttpHeaderConfig.RegexValues) > 0
		case "http-request-method":
			set = c.HttpRequestMethodConfig != nil && len(c.HttpRequestMethodConfig.Values) > 0
		case "query-string":
			set = c.QueryStringConfig != nil && len(c.QueryStringConfig.Values) > 0
		case "source-ip":
			set = c.SourceIpConfig != nil && len(c.SourceIpConfig.Values) > 0
		default:
			return nil, invalid("Condition field '%s' must be one of '[http-header, http-request-method, host-header, path-pattern, query-string, source-ip]'", c.Field)
		}
		if !set {
			return nil, invalid("A '%s' condition must have values", c.Field)
		}
	}
	return out, nil
}

func priority(ru *rule) int {
	if ru.IsDefault {
		return math.MaxInt
	}
	n, _ := strconv.Atoi(ru.Priority)
	return n
}

// rulesOf lists a listener's rules by priority, the default last; lock held.
func (s *Service) rulesOf(listenerArn string) []*rule {
	var out []*rule
	for _, ru := range s.st.Rules {
		if ru.Listener == listenerArn {
			out = append(out, ru)
		}
	}
	slices.SortFunc(out, func(a, b *rule) int { return cmp.Compare(priority(a), priority(b)) })
	return out
}

type ruleIn struct {
	ListenerArn, RuleArn string
	Priority             int
	Conditions           []condition
	Actions              []action
	Tags                 []kv
}

func (s *Service) createRule(c *core.Ctx, in *ruleIn) (any, error) {
	if in.Priority < 1 || in.Priority > 50000 {
		return nil, invalid("Priority '%d' must be between 1 and 50000", in.Priority)
	}
	tags, err := tagMap(in.Tags)
	if err != nil {
		return nil, err
	}
	conds, err := conditions(in.Conditions)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	l, err := s.listener(c, in.ListenerArn)
	if err != nil {
		return nil, err
	}
	acts, err := s.actions(in.Actions)
	if err != nil {
		return nil, err
	}
	p := strconv.Itoa(in.Priority)
	for _, ru := range s.st.Rules {
		if ru.Listener == l.ListenerArn && ru.Priority == p {
			return nil, errf("PriorityInUse", "Priority '%s' is currently in use", p)
		}
	}
	ru := &rule{RuleArn: strings.Replace(l.ListenerArn, ":listener/", ":listener-rule/", 1) + "/" + core.RandHex(8),
		Priority: p, Conditions: conds, Actions: acts, Listener: l.ListenerArn}
	s.st.Rules[ru.RuleArn] = ru
	s.st.Tags[ru.RuleArn] = tags
	return map[string]any{"Rules": []rule{*ru}}, nil
}

func (s *Service) describeRules(c *core.Ctx, in *struct {
	ListenerArn string
	RuleArns    []string
	Marker      string
	PageSize    int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rules []*rule
	switch {
	case in.ListenerArn != "":
		if _, err := s.listener(c, in.ListenerArn); err != nil {
			return nil, err
		}
		rules = s.rulesOf(in.ListenerArn)
	case len(in.RuleArns) > 0:
		for _, arn := range in.RuleArns {
			ru, err := s.rule(c, arn)
			if err != nil {
				return nil, err
			}
			rules = append(rules, ru)
		}
	default:
		return nil, invalid("You must specify either listener ARN or rule ARNs")
	}
	var out []rule
	for _, ru := range rules {
		out = append(out, *ru)
	}
	out, next := page(out, in.Marker, in.PageSize)
	return map[string]any{"Rules": out, "NextMarker": next}, nil
}

// modifyRule changes conditions and actions; the default rule's actions are
// its listener's default actions.
func (s *Service) modifyRule(c *core.Ctx, in *ruleIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ru, err := s.rule(c, in.RuleArn)
	if err != nil {
		return nil, err
	}
	n := *ru
	if in.Conditions != nil {
		if ru.IsDefault {
			return nil, errf("OperationNotPermitted", "Conditions cannot be set on the default rule")
		}
		if n.Conditions, err = conditions(in.Conditions); err != nil {
			return nil, err
		}
	}
	if in.Actions != nil {
		if n.Actions, err = s.actions(in.Actions); err != nil {
			return nil, err
		}
	}
	*ru = n
	return map[string]any{"Rules": []rule{n}}, nil
}

func (s *Service) deleteRule(c *core.Ctx, in *ruleIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ru, err := s.rule(c, in.RuleArn)
	if err != nil {
		return nil, err
	}
	if ru.IsDefault {
		return nil, errf("OperationNotPermitted", "Default rule '%s' cannot be deleted", ru.RuleArn)
	}
	delete(s.st.Rules, ru.RuleArn)
	delete(s.st.Tags, ru.RuleArn)
	return nil, nil
}

func (s *Service) setRulePriorities(c *core.Ctx, in *struct {
	RulePriorities []struct {
		RuleArn  string
		Priority int
	}
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := map[*rule]string{}
	for _, p := range in.RulePriorities {
		ru, err := s.rule(c, p.RuleArn)
		switch {
		case err != nil:
			return nil, err
		case ru.IsDefault:
			return nil, errf("OperationNotPermitted", "Default rule '%s' cannot have its priority changed", ru.RuleArn)
		case p.Priority < 1 || p.Priority > 50000:
			return nil, invalid("Priority '%d' must be between 1 and 50000", p.Priority)
		}
		next[ru] = strconv.Itoa(p.Priority)
	}
	for ru, p := range next { // a priority is in use when a rule keeps it, or moves to it too
		for _, o := range s.st.Rules {
			if o != ru && o.Listener == ru.Listener && !o.IsDefault && cmp.Or(next[o], o.Priority) == p {
				return nil, errf("PriorityInUse", "Priority '%s' is currently in use", p)
			}
		}
	}
	var out []rule
	for ru, p := range next {
		ru.Priority = p
		out = append(out, *ru)
	}
	slices.SortFunc(out, func(a, b rule) int { return cmp.Compare(priority(&a), priority(&b)) })
	return map[string]any{"Rules": out}, nil
}

// ---- target groups ----

var protocols = []string{"HTTP", "HTTPS", "TCP", "TLS", "UDP", "TCP_UDP", "GENEVE", "QUIC", "TCP_QUIC"}

type groupIn struct {
	Name, TargetGroupArn, Protocol, ProtocolVersion, VpcId, TargetType, IpAddressType string
	HealthCheckProtocol, HealthCheckPort, HealthCheckPath                             string
	Port, HealthCheckIntervalSeconds, HealthCheckTimeoutSeconds                       *int
	HealthyThresholdCount, UnhealthyThresholdCount                                    *int
	HealthCheckEnabled                                                                *bool
	Matcher                                                                           *struct{ HttpCode, GrpcCode string }
	Tags                                                                              []kv
}

// health applies the health check settings in; on create, unset ones get
// ELB's defaults for the protocol and target type.
func (g *targetGroup) health(in *groupIn, create bool) {
	if create {
		alb := g.Protocol == "HTTP" || g.Protocol == "HTTPS"
		hc, port, interval, timeout, path, code := "TCP", "traffic-port", 30, 10, "", ""
		switch hp := cmp.Or(in.HealthCheckProtocol, map[bool]string{true: g.Protocol}[alb]); {
		case g.TargetType == "lambda":
			hc, port, interval, timeout, path, code = "", "", 35, 30, "/", "200"
		case alb:
			hc, timeout, path, code = hp, 5, "/", "200"
			if g.ProtocolVersion = cmp.Or(g.ProtocolVersion, "HTTP1"); g.ProtocolVersion == "GRPC" {
				path, code = "/AWS.ALB/healthcheck", ""
				g.Matcher = &struct{ HttpCode, GrpcCode string }{GrpcCode: "12"}
			}
		case hp == "HTTP" || hp == "HTTPS":
			hc, timeout, path, code = hp, map[string]int{"HTTP": 6, "HTTPS": 10}[hp], "/", "200-399"
		}
		if g.Protocol == "GENEVE" {
			port, interval, timeout = "80", 10, 5
		}
		g.HealthCheckEnabled = g.TargetType != "lambda"
		g.HealthCheckProtocol, g.HealthCheckPort, g.HealthCheckPath = hc, port, path
		g.HealthCheckIntervalSeconds, g.HealthCheckTimeoutSeconds = interval, timeout
		g.HealthyThresholdCount, g.UnhealthyThresholdCount = 5, 2
		if code != "" {
			g.Matcher = &struct{ HttpCode, GrpcCode string }{HttpCode: code}
		}
	}
	g.HealthCheckEnabled = or(in.HealthCheckEnabled, g.HealthCheckEnabled)
	g.HealthCheckProtocol = cmp.Or(in.HealthCheckProtocol, g.HealthCheckProtocol)
	g.HealthCheckPort = cmp.Or(in.HealthCheckPort, g.HealthCheckPort)
	g.HealthCheckPath = cmp.Or(in.HealthCheckPath, g.HealthCheckPath)
	g.HealthCheckIntervalSeconds = or(in.HealthCheckIntervalSeconds, g.HealthCheckIntervalSeconds)
	g.HealthCheckTimeoutSeconds = or(in.HealthCheckTimeoutSeconds, g.HealthCheckTimeoutSeconds)
	g.HealthyThresholdCount = or(in.HealthyThresholdCount, g.HealthyThresholdCount)
	g.UnhealthyThresholdCount = or(in.UnhealthyThresholdCount, g.UnhealthyThresholdCount)
	g.Matcher = cmp.Or(in.Matcher, g.Matcher)
}

// checkHealthPort refuses a HealthCheckPort that is not "traffic-port" or a port from 1 to 65535.
func checkHealthPort(p string) error {
	if n, err := strconv.Atoi(p); p != "" && p != "traffic-port" && (err != nil || n < 1 || n > 65535) {
		return invalid("Health check port '%s' must be 'traffic-port' or an integer between 1 and 65535", p)
	}
	return nil
}

func (s *Service) createTargetGroup(c *core.Ctx, in *groupIn) (any, error) {
	typ := cmp.Or(in.TargetType, "instance")
	switch {
	case !nameRE.MatchString(in.Name):
		return nil, invalid("Target group name '%s' must have at most 32 alphanumeric characters or hyphens, and must not begin or end with a hyphen", in.Name)
	case !slices.Contains([]string{"instance", "ip", "lambda", "alb"}, typ):
		return nil, invalid("Target type '%s' must be one of 'instance, ip, lambda, alb'", typ)
	case typ == "lambda" && (in.Protocol != "" || in.Port != nil || in.VpcId != ""):
		return nil, invalid("Protocol, port and VPC ID cannot be specified for target groups with target type 'lambda'")
	case typ != "lambda" && (in.Protocol == "" || in.Port == nil || in.VpcId == ""):
		return nil, invalid("A protocol, a port and a VPC ID must be specified for target groups with target type '%s'", typ)
	case in.Protocol != "" && !slices.Contains(protocols, in.Protocol):
		return nil, invalid("Protocol '%s' must be one of '%s'", in.Protocol, strings.Join(protocols, ", "))
	}
	if err := checkHealthPort(in.HealthCheckPort); err != nil {
		return nil, err
	}
	tags, err := tagMap(in.Tags)
	if err != nil {
		return nil, err
	}
	g := &targetGroup{TargetGroupName: in.Name, Protocol: in.Protocol, ProtocolVersion: in.ProtocolVersion, Port: in.Port,
		VpcId: in.VpcId, TargetType: typ, IpAddressType: in.IpAddressType}
	if typ != "lambda" {
		g.IpAddressType = cmp.Or(g.IpAddressType, "ipv4")
	}
	g.health(in, true)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, x := range s.st.Groups {
		if mine(c, x.TargetGroupArn) && x.TargetGroupName == in.Name { // idempotent for the same settings
			if x.Protocol != g.Protocol || or(x.Port, 0) != or(g.Port, 0) || x.VpcId != g.VpcId || x.TargetType != g.TargetType {
				return nil, errf("DuplicateTargetGroupName", "A target group with the same name '%s' exists, but with different settings", in.Name)
			}
			return map[string]any{"TargetGroups": []targetGroup{s.groupView(x, s.usage())}}, nil
		}
	}
	g.TargetGroupArn = c.ARN("elasticloadbalancing", "targetgroup/"+in.Name+"/"+core.RandHex(8))
	s.st.Groups[g.TargetGroupArn] = g
	s.st.Tags[g.TargetGroupArn] = tags
	return map[string]any{"TargetGroups": []targetGroup{s.groupView(g, nil)}}, nil
}

// usage maps each target group to the load balancers whose listeners or
// rules forward to it; lock held.
func (s *Service) usage() map[string][]string {
	out := map[string][]string{}
	for _, ru := range s.st.Rules {
		l := s.st.Listeners[ru.Listener]
		for _, a := range ru.Actions {
			if l == nil || a.ForwardConfig == nil {
				continue
			}
			for _, t := range a.ForwardConfig.TargetGroups {
				if !slices.Contains(out[t.TargetGroupArn], l.LoadBalancerArn) {
					out[t.TargetGroupArn] = append(out[t.TargetGroupArn], l.LoadBalancerArn)
				}
			}
		}
	}
	return out
}

func (s *Service) describeTargetGroups(c *core.Ctx, in *struct {
	LoadBalancerArn, Marker string
	TargetGroupArns, Names  []string
	PageSize                int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.LoadBalancerArn != "" {
		if _, err := s.lb(c, in.LoadBalancerArn); err != nil {
			return nil, err
		}
	}
	used := s.usage()
	var out []targetGroup
	for _, g := range s.st.Groups {
		if mine(c, g.TargetGroupArn) && (len(in.TargetGroupArns)+len(in.Names) == 0 ||
			slices.Contains(in.TargetGroupArns, g.TargetGroupArn) || slices.Contains(in.Names, g.TargetGroupName)) &&
			(in.LoadBalancerArn == "" || slices.Contains(used[g.TargetGroupArn], in.LoadBalancerArn)) {
			out = append(out, s.groupView(g, used))
		}
	}
	if len(out) < len(in.TargetGroupArns)+len(in.Names) {
		return nil, errf("TargetGroupNotFound", "One or more target groups not found")
	}
	slices.SortFunc(out, func(a, b targetGroup) int { return cmp.Compare(a.TargetGroupName, b.TargetGroupName) })
	out, next := page(out, in.Marker, in.PageSize)
	return map[string]any{"TargetGroups": out, "NextMarker": next}, nil
}

func (s *Service) modifyTargetGroup(c *core.Ctx, in *groupIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.TargetGroupArn)
	if err != nil {
		return nil, err
	}
	if err := checkHealthPort(in.HealthCheckPort); err != nil {
		return nil, err
	}
	g.health(in, false)
	return map[string]any{"TargetGroups": []targetGroup{s.groupView(g, s.usage())}}, nil
}

// deleteTargetGroup refuses a group in use; deleting one that is gone succeeds.
func (s *Service) deleteTargetGroup(c *core.Ctx, in *groupIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.TargetGroupArn)
	if err != nil {
		return nil, nil
	}
	if len(s.usage()[g.TargetGroupArn]) > 0 {
		return nil, errf("ResourceInUse", "Target group '%s' is currently in use by a listener or a rule", g.TargetGroupArn)
	}
	delete(s.st.Groups, g.TargetGroupArn)
	delete(s.st.Tags, g.TargetGroupArn)
	delete(s.rr, g.TargetGroupArn)
	return nil, nil
}

type targetsIn struct {
	TargetGroupArn string
	Targets        []struct {
		Id, AvailabilityZone string
		Port                 int
	}
}

// find returns the registered target id:port; lock held.
func (g *targetGroup) find(id string, port int) *target {
	if port == 0 {
		port = or(g.Port, 0)
	}
	for _, t := range g.Targets {
		if t.Id == id && t.Port == port {
			return t
		}
	}
	return nil
}

func (t *target) set(state, reason, desc string) {
	t.State, t.Reason, t.Description = state, reason, desc
}

func (s *Service) registerTargets(c *core.Ctx, in *targetsIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.TargetGroupArn)
	if err != nil {
		return nil, err
	}
	if len(in.Targets) == 0 {
		return nil, invalid("You must specify at least one target")
	}
	for _, t := range in.Targets {
		_, _, _, res, isARN := core.ParseARN(t.Id)
		switch {
		case g.TargetType == "ip" && net.ParseIP(t.Id) == nil:
			return nil, invalid("The IP address '%s' is not a valid IPv4 address", t.Id)
		case g.TargetType == "instance" && !strings.HasPrefix(t.Id, "i-"):
			return nil, invalid("Instance ID '%s' is not valid", t.Id)
		case g.TargetType == "lambda" && !strings.HasPrefix(res, "function:"):
			return nil, invalid("Target '%s' is not a valid Lambda function ARN", t.Id)
		case g.TargetType == "alb" && !isARN:
			return nil, invalid("Target '%s' is not a valid load balancer ARN", t.Id)
		}
	}
	for _, x := range in.Targets {
		port := cmp.Or(x.Port, or(g.Port, 0))
		if g.TargetType == "lambda" {
			port = 0
		}
		t := g.find(x.Id, port)
		if t == nil {
			t = &target{Id: x.Id, Port: port, Zone: x.AvailabilityZone}
			g.Targets = append(g.Targets, t)
		} else if t.State != "draining" {
			continue
		}
		t.set("initial", "Elb.RegistrationInProgress", "Target registration is in progress")
		t.ok, t.bad, t.next = 0, 0, time.Time{}
	}
	return nil, nil
}

// deregisterTargets drains targets for the group's deregistration delay;
// targets of a group no load balancer uses go at once.
func (s *Service) deregisterTargets(c *core.Ctx, in *targetsIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.TargetGroupArn)
	if err != nil {
		return nil, err
	}
	delay, _ := strconv.Atoi(g.attr("deregistration_delay.timeout_seconds"))
	used := len(s.usage()[g.TargetGroupArn]) > 0
	for _, x := range in.Targets {
		t := g.find(x.Id, x.Port)
		if t == nil {
			continue
		}
		if delay == 0 || !used {
			g.Targets = slices.DeleteFunc(slices.Clone(g.Targets), func(x *target) bool { return x == t })
			continue
		}
		t.set("draining", "Target.DeregistrationInProgress", "Target deregistration is in progress")
		t.Until = time.Now().Add(time.Duration(delay) * time.Second)
	}
	return nil, nil
}

type healthDesc struct {
	Target struct {
		Id               string
		Port             *int
		AvailabilityZone string
	}
	HealthCheckPort string
	TargetHealth    struct{ State, Reason, Description string }
}

func (s *Service) describeTargetHealth(c *core.Ctx, in *targetsIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.TargetGroupArn)
	if err != nil {
		return nil, err
	}
	used := len(s.usage()[g.TargetGroupArn]) > 0
	describe := func(id string, port int, t *target) healthDesc {
		var d healthDesc
		d.Target.Id, d.Target.Port = id, &port
		if port == 0 {
			d.Target.Port = nil
		}
		if g.HealthCheckPort != "" {
			d.HealthCheckPort = g.HealthCheckPort
			if d.HealthCheckPort == "traffic-port" {
				d.HealthCheckPort = strconv.Itoa(port)
			}
		}
		h := &d.TargetHealth
		switch {
		case t == nil:
			h.State, h.Reason, h.Description = "unused", "Target.NotRegistered", "Target is not registered to the target group"
		case !used && t.State != "draining":
			d.Target.AvailabilityZone = t.Zone
			h.State, h.Reason, h.Description = "unused", "Target.NotInUse", "Target group is not configured to receive traffic from the load balancer"
		default:
			d.Target.AvailabilityZone = t.Zone
			h.State, h.Reason, h.Description = t.State, t.Reason, t.Description
		}
		return d
	}
	var out []healthDesc
	if len(in.Targets) == 0 {
		for _, t := range g.Targets {
			out = append(out, describe(t.Id, t.Port, t))
		}
	}
	for _, q := range in.Targets {
		port := cmp.Or(q.Port, or(g.Port, 0))
		if g.TargetType == "lambda" {
			port = 0
		}
		out = append(out, describe(q.Id, port, g.find(q.Id, port)))
	}
	return map[string]any{"TargetHealthDescriptions": out}, nil
}

// ---- tags ----

func tagMap(tags []kv) (map[string]string, error) {
	m := map[string]string{}
	for _, t := range tags {
		if _, dup := m[t.Key]; dup {
			return nil, errf("DuplicateTagKeys", "Tag key '%s' specified more than once", t.Key)
		}
		m[t.Key] = t.Value
	}
	if len(m) > 50 {
		return nil, errf("TooManyTags", "A resource can have at most 50 tags")
	}
	return m, nil
}

// taggable checks that a resource exists, with the error ELB gives; lock held.
func (s *Service) taggable(c *core.Ctx, arn string) error {
	_, _, _, res, _ := core.ParseARN(arn)
	kind, _, _ := strings.Cut(res, "/")
	var err error
	switch kind {
	case "loadbalancer":
		_, err = s.lb(c, arn)
	case "listener":
		_, err = s.listener(c, arn)
	case "listener-rule":
		_, err = s.rule(c, arn)
	case "targetgroup":
		_, err = s.group(c, arn)
	default:
		err = invalid("'%s' is not a valid load balancer, listener, rule or target group ARN", arn)
	}
	return err
}

type tagsIn struct {
	ResourceArns, TagKeys []string
	Tags                  []kv
}

func (s *Service) addTags(c *core.Ctx, in *tagsIn) (any, error) {
	tags, err := tagMap(in.Tags)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := map[string]map[string]string{}
	for _, arn := range in.ResourceArns {
		if err := s.taggable(c, arn); err != nil {
			return nil, err
		}
		m := maps.Clone(s.st.Tags[arn])
		if m == nil {
			m = map[string]string{}
		}
		if maps.Copy(m, tags); len(m) > 50 {
			return nil, errf("TooManyTags", "A resource can have at most 50 tags")
		}
		next[arn] = m
	}
	maps.Copy(s.st.Tags, next)
	return nil, nil
}

func (s *Service) removeTags(c *core.Ctx, in *tagsIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, arn := range in.ResourceArns {
		if err := s.taggable(c, arn); err != nil {
			return nil, err
		}
	}
	for _, arn := range in.ResourceArns {
		for _, k := range in.TagKeys {
			delete(s.st.Tags[arn], k)
		}
	}
	return nil, nil
}

func kvs(m map[string]string) []kv {
	var out []kv
	for _, k := range core.SortedKeys(m) {
		out = append(out, kv{k, m[k]})
	}
	return out
}

func (s *Service) describeTags(c *core.Ctx, in *tagsIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	type tagDesc struct {
		ResourceArn string
		Tags        []kv
	}
	var out []tagDesc
	for _, arn := range in.ResourceArns {
		if err := s.taggable(c, arn); err != nil {
			return nil, err
		}
		out = append(out, tagDesc{arn, kvs(s.st.Tags[arn])})
	}
	return map[string]any{"TagDescriptions": out}, nil
}

var limits = table(`application-load-balancers=50 condition-values-per-alb-rule=5 condition-wildcards-per-alb-rule=5
	gateway-load-balancers=100 gateway-load-balancers-per-vpc=100 geneve-target-groups=100
	listeners-per-application-load-balancer=50 listeners-per-network-load-balancer=50 network-load-balancer-enis-per-vpc=1200
	network-load-balancers=50 rules-per-application-load-balancer=100 target-groups=3000
	target-groups-per-action-on-application-load-balancer=5 target-groups-per-action-on-network-load-balancer=1
	target-groups-per-application-load-balancer=100 targets-per-application-load-balancer=1000
	targets-per-availability-zone-per-gateway-load-balancer=300 targets-per-availability-zone-per-network-load-balancer=500
	targets-per-network-load-balancer=3000`)

// describeAccountLimits reports ELB's default quotas. ponytail: not enforced.
func (s *Service) describeAccountLimits(c *core.Ctx, in *struct {
	Marker   string
	PageSize int
}) (any, error) {
	var out []struct{ Name, Max string }
	for _, k := range core.SortedKeys(limits) {
		out = append(out, struct{ Name, Max string }{k, limits[k]})
	}
	out, next := page(out, in.Marker, in.PageSize)
	return map[string]any{"Limits": out, "NextMarker": next}, nil
}

// ---- console ----

// Resources lists load balancers (with the ports serving their listeners)
// and target groups for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	lbs := core.Table{Name: "Load balancers", Columns: []string{"Name", "Type", "State", "DNS name", "Listeners"}}
	for _, arn := range core.SortedKeys(s.st.LBs) {
		lb := s.st.LBs[arn]
		var ls []string
		for _, l := range s.listenersOf(arn) {
			x := l.Protocol + ":" + strconv.Itoa(portOf(l))
			if p := lb.Pogi.Ports[portOf(l)]; p != 0 {
				x += " → 127.0.0.1:" + strconv.Itoa(p)
			}
			ls = append(ls, x)
		}
		lbs.Rows = append(lbs.Rows, []string{lb.LoadBalancerName, lb.Type, lb.State.Code, lb.DNSName, strings.Join(ls, ", ")})
	}
	tgs := core.Table{Name: "Target groups", Columns: []string{"Name", "Target type", "Protocol", "Port", "Healthy"}}
	for _, arn := range core.SortedKeys(s.st.Groups) {
		g := s.st.Groups[arn]
		healthy := 0
		for _, t := range g.Targets {
			if t.State == "healthy" {
				healthy++
			}
		}
		tgs.Rows = append(tgs.Rows, []string{g.TargetGroupName, g.TargetType, g.Protocol, strconv.Itoa(or(g.Port, 0)),
			strconv.Itoa(healthy) + "/" + strconv.Itoa(len(g.Targets))})
	}
	return []core.Table{lbs, tgs}
}

// Inspect returns a load balancer (by name or ARN) with its attributes,
// listeners, rules and ports, or a target group with its targets.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Load balancers":
		for _, lb := range s.st.LBs {
			if lb.LoadBalancerName != id && lb.LoadBalancerArn != id {
				continue
			}
			var ls []any
			for _, l := range s.listenersOf(lb.LoadBalancerArn) {
				var rules []rule
				for _, ru := range s.rulesOf(l.ListenerArn) {
					rules = append(rules, *ru)
				}
				ls = append(ls, map[string]any{"listener": s.listenerView(l), "rules": rules, "attributes": attributes(listenerAttrs[l.Protocol], l.Attrs)})
			}
			return map[string]any{"loadBalancer": lb.view(), "attributes": attributes(lbAttrs[lb.Type], lb.Attrs), "listeners": ls,
				"ports": maps.Clone(lb.Pogi.Ports), "tags": maps.Clone(s.st.Tags[lb.LoadBalancerArn])}
		}
	case "Target groups":
		for _, g := range s.st.Groups {
			if g.TargetGroupName != id && g.TargetGroupArn != id {
				continue
			}
			var ts []target
			for _, t := range g.Targets {
				ts = append(ts, *t)
			}
			return map[string]any{"targetGroup": s.groupView(g, s.usage()), "attributes": attributes(g.defaults(), g.Attrs),
				"targets": ts, "tags": maps.Clone(s.st.Tags[g.TargetGroupArn])}
		}
	}
	return nil
}
