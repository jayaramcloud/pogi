package ec2

// VPC endpoints (gateway endpoints add routes, interface endpoints get a
// network interface in each subnet), the AWS service prefix lists they
// use, and managed prefix lists.

import (
	"cmp"
	"fmt"
	"hash/fnv"
	"maps"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// ---- prefix lists ----

// PrefixList is a managed prefix list; the AWS ones (S3 and DynamoDB) are
// made when asked for and never stored.
type PrefixList struct {
	PrefixListId   string        `xml:"prefixListId"`
	AddressFamily  string        `xml:"addressFamily"`
	State          string        `xml:"state"`
	PrefixListName string        `xml:"prefixListName"`
	MaxEntries     int           `xml:"maxEntries"`
	Version        int           `xml:"version"`
	PrefixListArn  string        `xml:"prefixListArn"`
	OwnerId        string        `xml:"ownerId"`
	TagSet         []Tag         `xml:"tagSet>item" json:"-"`
	Entries        []prefixEntry `xml:"-"`
}

type prefixEntry struct {
	Cidr        string `xml:"cidr"`
	Description string `xml:"description,omitempty"`
}

func (p *PrefixList) view() (string, *[]Tag, map[string][]string) {
	return p.PrefixListId, &p.TagSet, map[string][]string{"prefix-list-id": {p.PrefixListId}, "prefix-list-name": {p.PrefixListName}, "owner-id": {p.OwnerId}}
}

// serviceCidrs are the ranges the AWS prefix lists hold.
// ponytail: example ranges, not AWS's published ones.
var serviceCidrs = map[string][]string{
	"s3":       {"52.216.0.0/15", "54.231.0.0/16", "3.5.0.0/19"},
	"dynamodb": {"52.94.0.0/22", "52.119.224.0/20", "3.218.182.0/24"},
}

// awsPrefixList is the prefix list of an AWS service in a region.
func awsPrefixList(region, service string) *PrefixList {
	h := fnv.New32a()
	h.Write([]byte(region + service))
	p := &PrefixList{PrefixListId: fmt.Sprintf("pl-%08x", h.Sum32()), AddressFamily: "IPv4", State: "create-complete",
		PrefixListName: "com.amazonaws." + region + "." + service, Version: 1, OwnerId: "AWS"}
	p.PrefixListArn = core.ARN("ec2", region, "aws", "prefix-list/"+p.PrefixListId)
	for _, c := range serviceCidrs[service] {
		p.Entries = append(p.Entries, prefixEntry{Cidr: c})
	}
	p.MaxEntries = len(p.Entries)
	return p
}

// prefixLists returns every prefix list of the region by id: the AWS ones and the account's own.
func (r *region) prefixLists() map[string]*PrefixList {
	out := map[string]*PrefixList{}
	for _, svc := range core.SortedKeys(serviceCidrs) {
		p := awsPrefixList(r.Name, svc)
		out[p.PrefixListId] = p
	}
	maps.Copy(out, r.Net.Prefixes)
	return out
}

func (s *Service) describePrefixLists(c *core.Ctx, in *struct {
	Ids     []string `query:"PrefixListId"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	type info struct {
		PrefixListId   string   `xml:"prefixListId"`
		PrefixListName string   `xml:"prefixListName"`
		Cidrs          []string `xml:"cidrSet>item"`
	}
	out := &struct {
		Header
		Lists []info `xml:"prefixListSet>item"`
	}{}
	for _, id := range in.Ids {
		if p := r.prefixLists()[id]; p == nil || p.OwnerId != "AWS" {
			return nil, core.Errorf(400, "InvalidPrefixListID.NotFound", "The prefix list ID '%s' does not exist", id)
		}
	}
	for _, svc := range core.SortedKeys(serviceCidrs) {
		p := awsPrefixList(r.Name, svc)
		if len(in.Ids) > 0 && !slices.Contains(in.Ids, p.PrefixListId) {
			continue
		}
		if _, _, attrs := p.view(); match(in.Filters, attrs, nil) {
			it := info{PrefixListId: p.PrefixListId, PrefixListName: p.PrefixListName}
			for _, e := range p.Entries {
				it.Cidrs = append(it.Cidrs, e.Cidr)
			}
			out.Lists = append(out.Lists, it)
		}
	}
	return out, nil
}

type prefixOut struct {
	Header
	List PrefixList `xml:"prefixList"`
}

func (r *region) prefixList(id string) (*PrefixList, error) {
	if p := r.Net.Prefixes[id]; p != nil {
		return p, nil
	}
	return nil, core.Errorf(400, "InvalidPrefixListID.NotFound", "The prefix list ID '%s' does not exist", id)
}

// entries checks and normalizes entries for an address family.
func entries(in []prefixEntry, family string) ([]prefixEntry, error) {
	var out []prefixEntry
	for _, e := range in {
		_, n, err := net.ParseCIDR(e.Cidr)
		if err != nil || (family == "IPv4") != (n.IP.To4() != nil) {
			return nil, invalid("Value (%s) for parameter cidr is invalid. This is not a valid %s CIDR block.", e.Cidr, family)
		}
		if slices.ContainsFunc(out, func(o prefixEntry) bool { return o.Cidr == n.String() }) {
			return nil, invalid("The cidr %s is listed more than once.", e.Cidr)
		}
		out = append(out, prefixEntry{n.String(), e.Description})
	}
	return out, nil
}

func checkListName(name string) error {
	if name == "" || len(name) > 255 {
		return invalid("Value (%s) for parameter prefixListName is invalid. The name must be 1 to 255 characters.", name)
	}
	if strings.HasPrefix(strings.ToLower(name), "com.amazonaws.") {
		return invalid("Value (%s) for parameter prefixListName is invalid. The name cannot start with 'com.amazonaws.'.", name)
	}
	return nil
}

func tooMany(max int) error {
	return core.Errorf(400, "PrefixListMaxEntriesExceeded", "The prefix list would have more than its maximum of %d entries.", max)
}

func (s *Service) createPrefixList(c *core.Ctx, in *struct {
	PrefixListName, AddressFamily string
	MaxEntries                    *int
	Entries                       []prefixEntry `query:"Entry"`
	TagSpecs                      []tagSpec     `query:"TagSpecification"`
}) (any, error) {
	if err := checkListName(in.PrefixListName); err != nil {
		return nil, err
	}
	if in.AddressFamily != "IPv4" && in.AddressFamily != "IPv6" {
		return nil, invalid("Value (%s) for parameter addressFamily is invalid. Must be IPv4 or IPv6.", in.AddressFamily)
	}
	if in.MaxEntries == nil || *in.MaxEntries < 1 {
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter MaxEntries (1 or more)")
	}
	list, err := entries(in.Entries, in.AddressFamily)
	if err != nil {
		return nil, err
	}
	if len(list) > *in.MaxEntries {
		return nil, tooMany(*in.MaxEntries)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	p := &PrefixList{PrefixListId: newID("pl"), AddressFamily: in.AddressFamily, State: "create-complete", PrefixListName: in.PrefixListName,
		MaxEntries: *in.MaxEntries, Version: 1, OwnerId: r.Account, Entries: list}
	p.PrefixListArn = core.ARN("ec2", r.Name, r.Account, "prefix-list/"+p.PrefixListId)
	r.Net.Prefixes[p.PrefixListId] = p
	r.tag(p.PrefixListId, in.TagSpecs)
	return &prefixOut{List: withTags(r, p, (*PrefixList).view)}, nil
}

func (s *Service) describeManagedLists(c *core.Ctx, in *struct {
	Ids     []string `query:"PrefixListId"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	lists, err := describe(r, r.prefixLists(), in.Ids, in.Filters, "InvalidPrefixListID.NotFound", "prefix list ID", (*PrefixList).view)
	return &struct {
		Header
		Lists []PrefixList `xml:"prefixListSet>item"`
	}{Lists: lists}, err
}

// prefixListEntries lists the current entries.
// ponytail: only the current version is kept, so TargetVersion is ignored.
func (s *Service) prefixListEntries(c *core.Ctx, in *struct {
	PrefixListId  string
	TargetVersion *int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.reg(c).prefixLists()[in.PrefixListId]
	if p == nil {
		return nil, core.Errorf(400, "InvalidPrefixListID.NotFound", "The prefix list ID '%s' does not exist", in.PrefixListId)
	}
	return &struct {
		Header
		Entries []prefixEntry `xml:"entrySet>item"`
	}{Entries: slices.Clone(p.Entries)}, nil
}

func (s *Service) modifyPrefixList(c *core.Ctx, in *struct {
	PrefixListId, PrefixListName string
	CurrentVersion, MaxEntries   *int
	AddEntries                   []prefixEntry `query:"AddEntry"`
	RemoveEntries                []prefixEntry `query:"RemoveEntry"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	p, err := r.prefixList(in.PrefixListId)
	if err != nil {
		return nil, err
	}
	if in.CurrentVersion != nil && *in.CurrentVersion != p.Version {
		return nil, core.Errorf(400, "IncorrectState", "The current version %d is not the latest version %d of the prefix list.", *in.CurrentVersion, p.Version)
	}
	if in.PrefixListName != "" {
		if err := checkListName(in.PrefixListName); err != nil {
			return nil, err
		}
	}
	add, err := entries(in.AddEntries, p.AddressFamily)
	if err != nil {
		return nil, err
	}
	remove, err := entries(in.RemoveEntries, p.AddressFamily)
	if err != nil {
		return nil, err
	}
	next := slices.Clone(p.Entries)
	for _, e := range remove {
		i := slices.IndexFunc(next, func(o prefixEntry) bool { return o.Cidr == e.Cidr })
		if i < 0 {
			return nil, invalid("The prefix list does not contain the entry %s.", e.Cidr)
		}
		next = slices.Delete(next, i, i+1)
	}
	for _, e := range add {
		if i := slices.IndexFunc(next, func(o prefixEntry) bool { return o.Cidr == e.Cidr }); i >= 0 {
			next[i] = e
		} else {
			next = append(next, e)
		}
	}
	max := p.MaxEntries
	if in.MaxEntries != nil {
		max = *in.MaxEntries
	}
	if len(next) > max {
		return nil, tooMany(max)
	}
	if len(add)+len(remove) > 0 {
		p.Version++
	}
	p.Entries, p.MaxEntries, p.PrefixListName, p.State = next, max, cmp.Or(in.PrefixListName, p.PrefixListName), "modify-complete"
	return &prefixOut{List: withTags(r, p, (*PrefixList).view)}, nil
}

func (s *Service) deletePrefixList(c *core.Ctx, in *struct{ PrefixListId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	p, err := r.prefixList(in.PrefixListId)
	if err != nil {
		return nil, err
	}
	out := &prefixOut{List: withTags(r, p, (*PrefixList).view)}
	out.List.State = "delete-complete"
	delete(r.Net.Prefixes, p.PrefixListId)
	delete(r.Tags, p.PrefixListId)
	return out, nil
}

// ---- VPC endpoints ----

// VpcEndpoint is a VPC endpoint.
type VpcEndpoint struct {
	VpcEndpointId       string            `xml:"vpcEndpointId"`
	VpcEndpointType     string            `xml:"vpcEndpointType"`
	VpcId               string            `xml:"vpcId"`
	ServiceName         string            `xml:"serviceName"`
	State               string            `xml:"state"`
	PolicyDocument      string            `xml:"policyDocument"`
	RouteTableIds       []string          `xml:"routeTableIdSet>item"`
	SubnetIds           []string          `xml:"subnetIdSet>item"`
	Groups              []GroupIdentifier `xml:"groupSet>item"`
	IpAddressType       string            `xml:"ipAddressType"`
	DnsOptions          dnsOptions        `xml:"dnsOptions"`
	PrivateDnsEnabled   bool              `xml:"privateDnsEnabled"`
	RequesterManaged    bool              `xml:"requesterManaged"`
	NetworkInterfaceIds []string          `xml:"networkInterfaceIdSet>item"`
	DnsEntries          []dnsEntry        `xml:"dnsEntrySet>item"`
	CreationTimestamp   string            `xml:"creationTimestamp"`
	TagSet              []Tag             `xml:"tagSet>item" json:"-"`
	OwnerId             string            `xml:"ownerId"`
}

type dnsOptions struct {
	DnsRecordIpType string `xml:"dnsRecordIpType"`
}

type dnsEntry struct {
	DnsName      string `xml:"dnsName"`
	HostedZoneId string `xml:"hostedZoneId"`
}

const defaultEndpointPolicy = `{"Statement":[{"Action":"*","Effect":"Allow","Principal":"*","Resource":"*"}]}`

func (e *VpcEndpoint) view() (string, *[]Tag, map[string][]string) {
	return e.VpcEndpointId, &e.TagSet, map[string][]string{"vpc-endpoint-id": {e.VpcEndpointId}, "vpc-id": {e.VpcId}, "service-name": {e.ServiceName},
		"vpc-endpoint-state": {e.State}, "vpc-endpoint-type": {e.VpcEndpointType}, "ip-address-type": {e.IpAddressType}}
}

// serviceOf is the short name of an AWS service: s3 for com.amazonaws.us-east-1.s3.
func serviceOf(name string) string {
	rest := strings.TrimPrefix(name, "com.amazonaws.")
	if _, after, ok := strings.Cut(rest, "."); ok {
		return after
	}
	return rest
}

// addRoute makes the route a gateway endpoint puts in a route table; lock held.
func (r *region) addEndpointRoute(e *VpcEndpoint, table string) error {
	t := r.Tables[table]
	if t == nil || t.VpcId != e.VpcId {
		return core.Errorf(400, "InvalidRouteTableId.NotFound", "The routeTable ID '%s' does not exist", table)
	}
	pl := awsPrefixList(r.Name, serviceOf(e.ServiceName)).PrefixListId
	if t.routeIndex(pl) >= 0 {
		return core.Errorf(400, "RouteAlreadyExists", "route table %s already has a route for the prefix list %s", table, pl)
	}
	t.Routes = append(t.Routes, route{DestinationPrefixListId: pl, GatewayId: e.VpcEndpointId, State: "active", Origin: "CreateRoute"})
	e.RouteTableIds = append(e.RouteTableIds, table)
	return nil
}

func (r *region) dropEndpointRoute(e *VpcEndpoint, table string) {
	if t := r.Tables[table]; t != nil {
		t.Routes = slices.DeleteFunc(t.Routes, func(rt route) bool { return rt.GatewayId == e.VpcEndpointId })
	}
	e.RouteTableIds = slices.DeleteFunc(e.RouteTableIds, func(id string) bool { return id == table })
}

// addEndpointSubnet gives an interface endpoint a network interface in a subnet; lock held.
func (r *region) addEndpointSubnet(e *VpcEndpoint, id string) error {
	sn := r.Subnets[id]
	if sn == nil || sn.VpcId != e.VpcId {
		return core.Errorf(400, "InvalidSubnetID.NotFound", "The subnet ID '%s' does not exist", id)
	}
	for _, o := range e.SubnetIds {
		if r.Subnets[o].AvailabilityZone == sn.AvailabilityZone {
			return core.Errorf(400, "DuplicateSubnetsInSameZone", "Found another VPC endpoint subnet in the availability zone of %s", id)
		}
	}
	n, err := r.newEni(sn, "", "VPC Endpoint Interface "+e.VpcEndpointId, "vpc_endpoint", e.VpcEndpointId, slices.Clone(e.Groups))
	if err != nil {
		return err
	}
	e.SubnetIds = append(e.SubnetIds, id)
	e.NetworkInterfaceIds = append(e.NetworkInterfaceIds, n.NetworkInterfaceId)
	return nil
}

func (r *region) dropEndpointSubnet(e *VpcEndpoint, id string) {
	for _, eni := range slices.Clone(e.NetworkInterfaceIds) {
		if n := r.Net.Enis[eni]; n != nil && n.SubnetId == id {
			r.dropEni(eni)
			e.NetworkInterfaceIds = slices.DeleteFunc(e.NetworkInterfaceIds, func(x string) bool { return x == eni })
		}
	}
	e.SubnetIds = slices.DeleteFunc(e.SubnetIds, func(x string) bool { return x == id })
}

// dnsNames sets the names an interface endpoint answers to.
func (r *region) dnsNames(e *VpcEndpoint) {
	e.DnsEntries = nil
	if e.VpcEndpointType != "Interface" {
		return
	}
	const zone = "Z7HUB22UULQXV"
	svc, id := serviceOf(e.ServiceName), strings.TrimPrefix(e.VpcEndpointId, "vpce-")
	suffix := "." + svc + "." + r.Name + ".vpce.amazonaws.com"
	e.DnsEntries = []dnsEntry{{"vpce-" + id + suffix, zone}}
	for _, sid := range e.SubnetIds {
		e.DnsEntries = append(e.DnsEntries, dnsEntry{"vpce-" + id + "-" + r.Subnets[sid].AvailabilityZone + suffix, zone})
	}
	if e.PrivateDnsEnabled {
		e.DnsEntries = append(e.DnsEntries, dnsEntry{svc + "." + r.Name + ".amazonaws.com", zone})
	}
}

func (s *Service) createEndpoint(c *core.Ctx, in *struct {
	VpcId, ServiceName, VpcEndpointType, PolicyDocument, IpAddressType, ClientToken string
	RouteTableIds                                                                   []string `query:"RouteTableId"`
	SubnetIds                                                                       []string `query:"SubnetId"`
	SecurityGroupIds                                                                []string `query:"SecurityGroupId"`
	PrivateDnsEnabled                                                               *bool
	DnsOptions                                                                      *dnsOptions
	TagSpecs                                                                        []tagSpec `query:"TagSpecification"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	v, err := r.vpc(in.VpcId)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(in.ServiceName, "com.amazonaws.") {
		return nil, core.Errorf(400, "InvalidServiceName", "The Vpc Endpoint Service '%s' does not exist", in.ServiceName)
	}
	kind := cmp.Or(in.VpcEndpointType, "Gateway")
	if kind != "Gateway" && kind != "Interface" {
		return nil, invalid("Value (%s) for parameter vpcEndpointType is invalid. Must be Gateway or Interface.", kind)
	}
	if svc := serviceOf(in.ServiceName); kind == "Gateway" && serviceCidrs[svc] == nil {
		return nil, core.Errorf(400, "InvalidParameter", "The Vpc Endpoint Service '%s' does not support the endpoint type 'Gateway'", in.ServiceName)
	}
	e := &VpcEndpoint{VpcEndpointId: newID("vpce"), VpcEndpointType: kind, VpcId: v.VpcId, ServiceName: in.ServiceName, State: "available",
		PolicyDocument: cmp.Or(in.PolicyDocument, defaultEndpointPolicy), IpAddressType: cmp.Or(in.IpAddressType, "ipv4"),
		DnsOptions: dnsOptions{"ipv4"}, CreationTimestamp: time.Now().UTC().Format(core.ISO), OwnerId: r.Account}
	if in.DnsOptions != nil && in.DnsOptions.DnsRecordIpType != "" {
		e.DnsOptions = *in.DnsOptions
	}
	if kind == "Interface" {
		e.PrivateDnsEnabled = in.PrivateDnsEnabled == nil || *in.PrivateDnsEnabled
		if e.Groups, err = r.groupsFor(v.VpcId, in.SecurityGroupIds, nil); err != nil {
			return nil, err
		}
	}
	undo := func() {
		for _, id := range e.NetworkInterfaceIds {
			r.dropEni(id)
		}
		for _, id := range slices.Clone(e.RouteTableIds) {
			r.dropEndpointRoute(e, id)
		}
	}
	for _, id := range in.RouteTableIds {
		if err := r.addEndpointRoute(e, id); err != nil {
			undo()
			return nil, err
		}
	}
	if kind == "Interface" {
		for _, id := range in.SubnetIds {
			if err := r.addEndpointSubnet(e, id); err != nil {
				undo()
				return nil, err
			}
		}
	}
	r.dnsNames(e)
	r.Net.Endpoints[e.VpcEndpointId] = e
	r.tag(e.VpcEndpointId, in.TagSpecs)
	out := withTags(r, e, (*VpcEndpoint).view)
	out.State = "pending" // the first answer, as in EC2; it is available when described
	return &struct {
		Header
		ClientToken string      `xml:"clientToken,omitempty"`
		Endpoint    VpcEndpoint `xml:"vpcEndpoint"`
	}{ClientToken: in.ClientToken, Endpoint: out}, nil
}

func (s *Service) describeEndpoints(c *core.Ctx, in *struct {
	Ids     []string `query:"VpcEndpointId"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	eps, err := describe(r, r.Net.Endpoints, in.Ids, in.Filters, "InvalidVpcEndpointId.NotFound", "VpcEndpoint Id", (*VpcEndpoint).view)
	return &struct {
		Header
		Endpoints []VpcEndpoint `xml:"vpcEndpointSet>item"`
	}{Endpoints: eps}, err
}

func (s *Service) modifyEndpoint(c *core.Ctx, in *struct {
	VpcEndpointId, PolicyDocument, IpAddressType string
	ResetPolicy, PrivateDnsEnabled               *bool
	DnsOptions                                   *dnsOptions
	AddRouteTableIds                             []string `query:"AddRouteTableId"`
	RemoveRouteTableIds                          []string `query:"RemoveRouteTableId"`
	AddSubnetIds                                 []string `query:"AddSubnetId"`
	RemoveSubnetIds                              []string `query:"RemoveSubnetId"`
	AddSecurityGroupIds                          []string `query:"AddSecurityGroupId"`
	RemoveSecurityGroupIds                       []string `query:"RemoveSecurityGroupId"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	e := r.Net.Endpoints[in.VpcEndpointId]
	if e == nil {
		return nil, core.Errorf(400, "InvalidVpcEndpointId.NotFound", "The VpcEndpoint Id '%s' does not exist", in.VpcEndpointId)
	}
	for _, id := range in.RemoveRouteTableIds {
		r.dropEndpointRoute(e, id)
	}
	for _, id := range in.AddRouteTableIds {
		if err := r.addEndpointRoute(e, id); err != nil {
			return nil, err
		}
	}
	for _, id := range in.RemoveSubnetIds {
		r.dropEndpointSubnet(e, id)
	}
	if e.VpcEndpointType == "Interface" {
		for _, id := range in.AddSubnetIds {
			if err := r.addEndpointSubnet(e, id); err != nil {
				return nil, err
			}
		}
		groups := slices.DeleteFunc(slices.Clone(e.Groups), func(g GroupIdentifier) bool { return slices.Contains(in.RemoveSecurityGroupIds, g.GroupId) })
		var added []GroupIdentifier
		if len(in.AddSecurityGroupIds) > 0 {
			var err error
			if added, err = r.groupsFor(e.VpcId, in.AddSecurityGroupIds, nil); err != nil {
				return nil, err
			}
		}
		for _, g := range added {
			if !slices.Contains(groups, g) {
				groups = append(groups, g)
			}
		}
		e.Groups = groups
		for _, id := range e.NetworkInterfaceIds {
			r.Net.Enis[id].Groups = slices.Clone(groups)
		}
		if in.PrivateDnsEnabled != nil {
			e.PrivateDnsEnabled = *in.PrivateDnsEnabled
		}
	}
	switch {
	case in.ResetPolicy != nil && *in.ResetPolicy:
		e.PolicyDocument = defaultEndpointPolicy
	case in.PolicyDocument != "":
		e.PolicyDocument = in.PolicyDocument
	}
	e.IpAddressType = cmp.Or(in.IpAddressType, e.IpAddressType)
	if in.DnsOptions != nil && in.DnsOptions.DnsRecordIpType != "" {
		e.DnsOptions = *in.DnsOptions
	}
	r.dnsNames(e)
	return ok(), nil
}

func (s *Service) deleteEndpoints(c *core.Ctx, in *struct {
	Ids []string `query:"VpcEndpointId"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	type failure struct {
		ResourceId string `xml:"resourceId"`
		Code       string `xml:"error>code"`
		Message    string `xml:"error>message"`
	}
	out := &struct {
		Header
		Unsuccessful []failure `xml:"unsuccessful>item"`
	}{}
	for _, id := range in.Ids {
		e := r.Net.Endpoints[id]
		if e == nil {
			out.Unsuccessful = append(out.Unsuccessful, failure{id, "InvalidVpcEndpointId.NotFound", "The VpcEndpoint Id '" + id + "' does not exist"})
			continue
		}
		for _, t := range slices.Clone(e.RouteTableIds) {
			r.dropEndpointRoute(e, t)
		}
		for _, n := range e.NetworkInterfaceIds {
			r.dropEni(n)
		}
		delete(r.Net.Endpoints, id)
		delete(r.Tags, id)
	}
	return out, nil
}
