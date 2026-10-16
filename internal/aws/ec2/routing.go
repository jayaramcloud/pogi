package ec2

// Routing: route tables, routes and their associations, internet gateways and
// egress-only internet gateways. Elastic IPs, NAT gateways and network
// interfaces are in nat.go, VPC endpoints and prefix lists in endpoints.go,
// network ACLs in acls.go.

import (
	"cmp"
	"maps"
	"net"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

func init() {
	maps.Copy(resourceTypes, map[string]string{"eipalloc": "elastic-ip", "nat": "natgateway", "eigw": "egress-only-internet-gateway",
		"vpce": "vpc-endpoint", "pl": "prefix-list", "eni": "network-interface"})
}

// netState holds what the networking files add to a region.
type netState struct {
	Addresses map[string]*Address          `json:"addresses"`
	Nats      map[string]*NatGateway       `json:"nats"`
	Enis      map[string]*NetworkInterface `json:"enis"` // the ones NAT gateways and endpoints made
	Endpoints map[string]*VpcEndpoint      `json:"endpoints"`
	Eigws     map[string]*EgressGateway    `json:"eigws"`
	Prefixes  map[string]*PrefixList       `json:"prefixes"` // managed prefix lists made by the account
}

// fillNet makes the maps a state saved before they existed lacks, and puts
// subnets that have no network ACL association into their VPC's default ACL.
func (r *region) fillNet() {
	n := &r.Net
	if n.Addresses == nil {
		n.Addresses, n.Nats, n.Enis = map[string]*Address{}, map[string]*NatGateway{}, map[string]*NetworkInterface{}
		n.Endpoints, n.Eigws, n.Prefixes = map[string]*VpcEndpoint{}, map[string]*EgressGateway{}, map[string]*PrefixList{}
	}
	for _, id := range core.SortedKeys(r.Subnets) {
		if !r.hasAclAssoc(id) {
			r.joinDefaultAcl(r.Subnets[id])
		}
	}
}

func (s *Service) netOps() map[string]core.Handler {
	return map[string]core.Handler{
		"CreateInternetGateway":              core.H(s.createGateway),
		"AttachInternetGateway":              core.H(s.attachGateway),
		"DetachInternetGateway":              core.H(s.detachGateway),
		"DeleteInternetGateway":              core.H(s.deleteGateway),
		"CreateEgressOnlyInternetGateway":    core.H(s.createEigw),
		"DescribeEgressOnlyInternetGateways": core.H(s.describeEigws),
		"DeleteEgressOnlyInternetGateway":    core.H(s.deleteEigw),
		"CreateRouteTable":                   core.H(s.createRouteTable),
		"DeleteRouteTable":                   core.H(s.deleteRouteTable),
		"AssociateRouteTable":                core.H(s.associateRouteTable),
		"DisassociateRouteTable":             core.H(s.disassociateRouteTable),
		"ReplaceRouteTableAssociation":       core.H(s.replaceAssociation),
		"CreateRoute":                        core.H(s.createRoute),
		"ReplaceRoute":                       core.H(s.replaceRoute),
		"DeleteRoute":                        core.H(s.deleteRoute),
		"AllocateAddress":                    core.H(s.allocateAddress),
		"AssociateAddress":                   core.H(s.associateAddress),
		"DisassociateAddress":                core.H(s.disassociateAddress),
		"ReleaseAddress":                     core.H(s.releaseAddress),
		"DescribeAddresses":                  core.H(s.describeAddresses),
		"DescribeAddressesAttribute":         core.H(s.describeAddressesAttribute),
		"CreateNatGateway":                   core.H(s.createNat),
		"DescribeNatGateways":                core.H(s.describeNats),
		"DeleteNatGateway":                   core.H(s.deleteNat),
		"DescribeNetworkInterfaces":          core.H(s.describeEnis),
		"CreateVpcEndpoint":                  core.H(s.createEndpoint),
		"DescribeVpcEndpoints":               core.H(s.describeEndpoints),
		"ModifyVpcEndpoint":                  core.H(s.modifyEndpoint),
		"DeleteVpcEndpoints":                 core.H(s.deleteEndpoints),
		"DescribePrefixLists":                core.H(s.describePrefixLists),
		"CreateManagedPrefixList":            core.H(s.createPrefixList),
		"DescribeManagedPrefixLists":         core.H(s.describeManagedLists),
		"GetManagedPrefixListEntries":        core.H(s.prefixListEntries),
		"ModifyManagedPrefixList":            core.H(s.modifyPrefixList),
		"DeleteManagedPrefixList":            core.H(s.deletePrefixList),
		"CreateNetworkAcl":                   core.H(s.createAcl),
		"DeleteNetworkAcl":                   core.H(s.deleteAcl),
		"CreateNetworkAclEntry":              core.H(s.aclEntryOp("create")),
		"ReplaceNetworkAclEntry":             core.H(s.aclEntryOp("replace")),
		"DeleteNetworkAclEntry":              core.H(s.deleteAclEntry),
		"ReplaceNetworkAclAssociation":       core.H(s.replaceAclAssociation),
	}
}

// netExists reports whether a taggable networking resource exists; lock held.
func (r *region) netExists(id string) bool {
	n := &r.Net
	switch resourceType(id) {
	case "elastic-ip":
		return n.Addresses[id] != nil
	case "natgateway":
		return n.Nats[id] != nil
	case "egress-only-internet-gateway":
		return n.Eigws[id] != nil
	case "vpc-endpoint":
		return n.Endpoints[id] != nil
	case "prefix-list":
		return n.Prefixes[id] != nil
	case "network-interface":
		return r.allEnis()[id] != nil
	}
	return false
}

// netBusy reports what keeps a VPC from being deleted; lock held.
func (r *region) netBusy(vpc string) bool {
	n := &r.Net
	return slices.ContainsFunc(slices.Collect(maps.Values(n.Nats)), func(g *NatGateway) bool { return g.VpcId == vpc && g.State != "deleted" }) ||
		slices.ContainsFunc(slices.Collect(maps.Values(n.Endpoints)), func(e *VpcEndpoint) bool { return e.VpcId == vpc }) ||
		slices.ContainsFunc(slices.Collect(maps.Values(n.Eigws)), func(g *EgressGateway) bool {
			return slices.ContainsFunc(g.Attachments, func(a attachment) bool { return a.VpcId == vpc })
		}) ||
		slices.ContainsFunc(slices.Collect(maps.Values(n.Enis)), func(e *NetworkInterface) bool { return e.VpcId == vpc })
}

// dropSubnet forgets what hangs on a deleted subnet; lock held.
func (r *region) dropSubnet(id string) {
	for _, t := range r.Tables {
		t.Associations = slices.DeleteFunc(t.Associations, func(a rtAssoc) bool { return a.SubnetId == id })
	}
	for _, a := range r.Acls {
		a.Associations = slices.DeleteFunc(a.Associations, func(x aclAssoc) bool { return x.SubnetId == id })
	}
}

// setLocalRoute adds or removes the local route of a VPC CIDR block in every
// route table of the VPC; lock held.
func (r *region) setLocalRoute(vpc, cidr string, add bool) {
	for _, t := range r.Tables {
		if t.VpcId != vpc {
			continue
		}
		if add {
			t.Routes = append(t.Routes, localRoute(cidr))
		} else {
			t.Routes = slices.DeleteFunc(t.Routes, func(rt route) bool { return rt.GatewayId == "local" && rt.DestinationCidrBlock == cidr })
		}
	}
}

// ---- internet gateways ----

type tagged struct {
	TagSpecs []tagSpec `query:"TagSpecification"`
}

func (s *Service) createGateway(c *core.Ctx, in *tagged) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	g := &Gateway{InternetGatewayId: newID("igw"), OwnerId: r.Account}
	r.Gateways[g.InternetGatewayId] = g
	r.tag(g.InternetGatewayId, in.TagSpecs)
	return &struct {
		Header
		Gateway Gateway `xml:"internetGateway"`
	}{Gateway: withTags(r, g, (*Gateway).view)}, nil
}

func (r *region) gateway(id string) (*Gateway, error) {
	if g := r.Gateways[id]; g != nil {
		return g, nil
	}
	return nil, core.Errorf(400, "InvalidInternetGatewayID.NotFound", "The internetGateway ID '%s' does not exist", id)
}

func (s *Service) attachGateway(c *core.Ctx, in *struct{ InternetGatewayId, VpcId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	g, err := r.gateway(in.InternetGatewayId)
	if err != nil {
		return nil, err
	}
	v, err := r.vpc(in.VpcId)
	if err != nil {
		return nil, err
	}
	if len(g.Attachments) > 0 {
		return nil, core.Errorf(400, "Resource.AlreadyAssociated", "resource %s is already attached to network %s", g.InternetGatewayId, g.Attachments[0].VpcId)
	}
	for _, o := range r.Gateways {
		if slices.ContainsFunc(o.Attachments, func(a attachment) bool { return a.VpcId == v.VpcId }) {
			return nil, core.Errorf(400, "Resource.AlreadyAssociated", "resource %s already has an internet gateway attached", v.VpcId)
		}
	}
	g.Attachments = []attachment{{v.VpcId, "available"}}
	return ok(), nil
}

func (s *Service) detachGateway(c *core.Ctx, in *struct{ InternetGatewayId, VpcId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	g, err := r.gateway(in.InternetGatewayId)
	if err != nil {
		return nil, err
	}
	if _, err := r.vpc(in.VpcId); err != nil {
		return nil, err
	}
	if !slices.ContainsFunc(g.Attachments, func(a attachment) bool { return a.VpcId == in.VpcId }) {
		return nil, core.Errorf(400, "Gateway.NotAttached", "resource %s is not attached to network %s", g.InternetGatewayId, in.VpcId)
	}
	g.Attachments = nil
	return ok(), nil
}

func (s *Service) deleteGateway(c *core.Ctx, in *struct{ InternetGatewayId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	g, err := r.gateway(in.InternetGatewayId)
	if err != nil {
		return nil, err
	}
	if len(g.Attachments) > 0 {
		return nil, core.Errorf(400, "DependencyViolation", "The internetGateway '%s' has dependencies and cannot be deleted.", g.InternetGatewayId)
	}
	delete(r.Gateways, g.InternetGatewayId)
	delete(r.Tags, g.InternetGatewayId)
	return ok(), nil
}

// ---- egress-only internet gateways ----

// EgressGateway is an egress-only internet gateway.
type EgressGateway struct {
	EgressOnlyInternetGatewayId string       `xml:"egressOnlyInternetGatewayId"`
	Attachments                 []attachment `xml:"attachmentSet>item"`
	TagSet                      []Tag        `xml:"tagSet>item" json:"-"`
}

func (g *EgressGateway) view() (string, *[]Tag, map[string][]string) {
	return g.EgressOnlyInternetGatewayId, &g.TagSet, map[string][]string{"egress-only-internet-gateway-id": {g.EgressOnlyInternetGatewayId}}
}

func (s *Service) createEigw(c *core.Ctx, in *struct {
	VpcId       string
	ClientToken string
	TagSpecs    []tagSpec `query:"TagSpecification"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	v, err := r.vpc(in.VpcId)
	if err != nil {
		return nil, err
	}
	g := &EgressGateway{EgressOnlyInternetGatewayId: newID("eigw"), Attachments: []attachment{{v.VpcId, "attached"}}}
	r.Net.Eigws[g.EgressOnlyInternetGatewayId] = g
	r.tag(g.EgressOnlyInternetGatewayId, in.TagSpecs)
	return &struct {
		Header
		ClientToken string        `xml:"clientToken,omitempty"`
		Gateway     EgressGateway `xml:"egressOnlyInternetGateway"`
	}{ClientToken: in.ClientToken, Gateway: withTags(r, g, (*EgressGateway).view)}, nil
}

func (s *Service) describeEigws(c *core.Ctx, in *struct {
	Ids     []string `query:"EgressOnlyInternetGatewayId"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	gws, err := describe(r, r.Net.Eigws, in.Ids, in.Filters, "InvalidGatewayID.NotFound", "egress only internet gateway ID", (*EgressGateway).view)
	return &struct {
		Header
		Gateways []EgressGateway `xml:"egressOnlyInternetGatewaySet>item"`
	}{Gateways: gws}, err
}

func (s *Service) deleteEigw(c *core.Ctx, in *struct{ EgressOnlyInternetGatewayId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	if r.Net.Eigws[in.EgressOnlyInternetGatewayId] == nil {
		return nil, core.Errorf(400, "InvalidGatewayID.NotFound", "The egress only internet gateway ID '%s' does not exist", in.EgressOnlyInternetGatewayId)
	}
	id := in.EgressOnlyInternetGatewayId
	r.blackhole(func(rt route) bool { return rt.EgressOnlyInternetGatewayId == id })
	delete(r.Net.Eigws, id)
	delete(r.Tags, id)
	return &struct {
		Header
		ReturnCode bool `xml:"returnCode"`
	}{ReturnCode: true}, nil
}

// ---- route tables ----

func (r *region) table(id string) (*RouteTable, error) {
	if t := r.Tables[id]; t != nil {
		return t, nil
	}
	return nil, core.Errorf(400, "InvalidRouteTableID.NotFound", "The routeTable ID '%s' does not exist", id)
}

func (rt route) dest() string {
	return cmp.Or(rt.DestinationCidrBlock, rt.DestinationIpv6CidrBlock, rt.DestinationPrefixListId)
}

func (t *RouteTable) routeIndex(dest string) int {
	return slices.IndexFunc(t.Routes, func(rt route) bool { return rt.dest() == dest })
}

// blackhole marks the routes that match as unusable, as EC2 does when the
// gateway they point at is deleted; lock held.
func (r *region) blackhole(match func(route) bool) {
	for _, t := range r.Tables {
		for i := range t.Routes {
			if match(t.Routes[i]) {
				t.Routes[i].State = "blackhole"
			}
		}
	}
}

// declare makes names filters the resource serves even when it has no value
// for them: match skips names a resource does not list.
func declare(attrs map[string][]string, names ...string) {
	for _, n := range names {
		if attrs[n] == nil {
			attrs[n] = []string{}
		}
	}
}

func (t *RouteTable) view() (string, *[]Tag, map[string][]string) {
	attrs := map[string][]string{"route-table-id": {t.RouteTableId}, "vpc-id": {t.VpcId}, "owner-id": {t.OwnerId}, "association.main": {"false"}}
	declare(attrs, "association.route-table-association-id", "association.route-table-id", "association.subnet-id", "association.gateway-id",
		"route.gateway-id", "route.destination-cidr-block", "route.destination-ipv6-cidr-block", "route.destination-prefix-list-id", "route.nat-gateway-id",
		"route.egress-only-internet-gateway-id", "route.instance-id", "route.network-interface-id", "route.vpc-peering-connection-id",
		"route.transit-gateway-id", "route.state", "route.origin")
	add := func(k, v string) {
		if v != "" {
			attrs[k] = append(attrs[k], v)
		}
	}
	for _, a := range t.Associations {
		if a.Main {
			attrs["association.main"] = []string{"true"}
		}
		add("association.route-table-association-id", a.RouteTableAssociationId)
		add("association.route-table-id", a.RouteTableId)
		add("association.subnet-id", a.SubnetId)
		add("association.gateway-id", a.GatewayId)
	}
	for _, rt := range t.Routes {
		add("route.gateway-id", rt.GatewayId)
		add("route.destination-cidr-block", rt.DestinationCidrBlock)
		add("route.destination-ipv6-cidr-block", rt.DestinationIpv6CidrBlock)
		add("route.destination-prefix-list-id", rt.DestinationPrefixListId)
		add("route.nat-gateway-id", rt.NatGatewayId)
		add("route.egress-only-internet-gateway-id", rt.EgressOnlyInternetGatewayId)
		add("route.instance-id", rt.InstanceId)
		add("route.network-interface-id", rt.NetworkInterfaceId)
		add("route.vpc-peering-connection-id", rt.VpcPeeringConnectionId)
		add("route.transit-gateway-id", rt.TransitGatewayId)
		add("route.state", rt.State)
		add("route.origin", rt.Origin)
	}
	return t.RouteTableId, &t.TagSet, attrs
}

func localRoute(cidr string) route {
	return route{DestinationCidrBlock: cidr, GatewayId: "local", State: "active", Origin: "CreateRouteTable"}
}

func (s *Service) createRouteTable(c *core.Ctx, in *struct {
	VpcId    string
	TagSpecs []tagSpec `query:"TagSpecification"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	v, err := r.vpc(in.VpcId)
	if err != nil {
		return nil, err
	}
	t := &RouteTable{RouteTableId: newID("rtb"), VpcId: v.VpcId, OwnerId: r.Account, Routes: []route{localRoute(v.CidrBlock)}}
	r.Tables[t.RouteTableId] = t
	r.tag(t.RouteTableId, in.TagSpecs)
	return &struct {
		Header
		Table RouteTable `xml:"routeTable"`
	}{Table: withTags(r, t, (*RouteTable).view)}, nil
}

func (s *Service) deleteRouteTable(c *core.Ctx, in *struct{ RouteTableId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	t, err := r.table(in.RouteTableId)
	if err != nil {
		return nil, err
	}
	if len(t.Associations) > 0 {
		return nil, core.Errorf(400, "DependencyViolation", "The routeTable '%s' has dependencies and cannot be deleted.", t.RouteTableId)
	}
	for _, e := range r.Net.Endpoints {
		e.RouteTableIds = slices.DeleteFunc(e.RouteTableIds, func(id string) bool { return id == t.RouteTableId })
	}
	delete(r.Tables, t.RouteTableId)
	delete(r.Tags, t.RouteTableId)
	return ok(), nil
}

// assoc finds an association by id; lock held.
func (r *region) assoc(id string) (*RouteTable, int) {
	for _, k := range core.SortedKeys(r.Tables) {
		t := r.Tables[k]
		if i := slices.IndexFunc(t.Associations, func(a rtAssoc) bool { return a.RouteTableAssociationId == id }); i >= 0 {
			return t, i
		}
	}
	return nil, -1
}

func notAssociated(id string) error {
	return core.Errorf(400, "InvalidAssociationID.NotFound", "The association ID '%s' does not exist", id)
}

type associated struct {
	Header
	AssociationId string `xml:"associationId"`
	State         status `xml:"associationState"`
}

func (s *Service) associateRouteTable(c *core.Ctx, in *struct{ RouteTableId, SubnetId, GatewayId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	t, err := r.table(in.RouteTableId)
	if err != nil {
		return nil, err
	}
	if (in.SubnetId == "") == (in.GatewayId == "") {
		return nil, core.Errorf(400, "MissingParameter", "The request must contain exactly one of the parameters subnetId and gatewayId")
	}
	a := rtAssoc{RouteTableAssociationId: newID("rtbassoc"), RouteTableId: t.RouteTableId, SubnetId: in.SubnetId, GatewayId: in.GatewayId, AssociationState: status{"associated"}}
	if in.SubnetId != "" {
		sn := r.Subnets[in.SubnetId]
		if sn == nil {
			return nil, core.Errorf(400, "InvalidSubnetID.NotFound", "The subnet ID '%s' does not exist", in.SubnetId)
		}
		if sn.VpcId != t.VpcId {
			return nil, invalid("Route table %s and subnet %s belong to different networks", t.RouteTableId, sn.SubnetId)
		}
	} else if r.Gateways[in.GatewayId] == nil && !strings.HasPrefix(in.GatewayId, "vgw-") {
		return nil, core.Errorf(400, "InvalidGatewayID.NotFound", "The gateway ID '%s' does not exist", in.GatewayId)
	}
	for _, o := range r.Tables {
		if slices.ContainsFunc(o.Associations, func(x rtAssoc) bool {
			return x.SubnetId != "" && x.SubnetId == a.SubnetId || x.GatewayId != "" && x.GatewayId == a.GatewayId
		}) {
			return nil, core.Errorf(400, "Resource.AlreadyAssociated", "the specified association for route table %s conflicts with an existing association", t.RouteTableId)
		}
	}
	t.Associations = append(t.Associations, a)
	return &associated{AssociationId: a.RouteTableAssociationId, State: a.AssociationState}, nil
}

func (s *Service) disassociateRouteTable(c *core.Ctx, in *struct{ AssociationId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	t, i := r.assoc(in.AssociationId)
	if t == nil {
		return nil, notAssociated(in.AssociationId)
	}
	if t.Associations[i].Main {
		return nil, invalid("cannot disassociate the main route table association %s", in.AssociationId)
	}
	t.Associations = slices.Delete(t.Associations, i, i+1)
	return ok(), nil
}

// replaceAssociation moves a subnet, or the main route table role, to
// another route table of the same VPC under a new association id.
func (s *Service) replaceAssociation(c *core.Ctx, in *struct{ AssociationId, RouteTableId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	old, i := r.assoc(in.AssociationId)
	if old == nil {
		return nil, notAssociated(in.AssociationId)
	}
	t, err := r.table(in.RouteTableId)
	if err != nil {
		return nil, err
	}
	if t.VpcId != old.VpcId {
		return nil, invalid("Route table %s and association %s belong to different networks", t.RouteTableId, in.AssociationId)
	}
	a := old.Associations[i]
	old.Associations = slices.Delete(old.Associations, i, i+1)
	a.RouteTableAssociationId, a.RouteTableId = newID("rtbassoc"), t.RouteTableId
	t.Associations = append(t.Associations, a)
	return &struct {
		Header
		AssociationId string `xml:"newAssociationId"`
		State         status `xml:"associationState"`
	}{AssociationId: a.RouteTableAssociationId, State: a.AssociationState}, nil
}

// ---- routes ----

type routeInput struct {
	RouteTableId                                                                                      string
	DestinationCidrBlock, DestinationIpv6CidrBlock, DestinationPrefixListId                           string
	GatewayId, NatGatewayId, InstanceId, NetworkInterfaceId, VpcPeeringConnectionId, TransitGatewayId string
	EgressOnlyInternetGatewayId                                                                       string
}

// dest is the destination of the request as a route with nothing else set.
func (in *routeInput) dest() (route, error) {
	var rt route
	n := 0
	if in.DestinationCidrBlock != "" {
		n++
		_, p, err := net.ParseCIDR(in.DestinationCidrBlock)
		if err != nil || p.IP.To4() == nil {
			return rt, invalid("Value (%s) for parameter destinationCidrBlock is invalid. This is not a valid CIDR block.", in.DestinationCidrBlock)
		}
		rt.DestinationCidrBlock = p.String()
	}
	if in.DestinationIpv6CidrBlock != "" {
		n++
		_, p, err := net.ParseCIDR(in.DestinationIpv6CidrBlock)
		if err != nil || p.IP.To4() != nil {
			return rt, invalid("Value (%s) for parameter destinationIpv6CidrBlock is invalid. This is not a valid CIDR block.", in.DestinationIpv6CidrBlock)
		}
		rt.DestinationIpv6CidrBlock = p.String()
	}
	if in.DestinationPrefixListId != "" {
		n++
		rt.DestinationPrefixListId = in.DestinationPrefixListId
	}
	if n != 1 {
		return rt, core.Errorf(400, "MissingParameter", "The request must contain exactly one of destinationCidrBlock, destinationIpv6CidrBlock and destinationPrefixListId")
	}
	return rt, nil
}

// target sets the one target of the request on rt after checking it exists
// and belongs to the table's VPC. Peering connections and transit gateways
// are taken as given.
// ponytail: pcx- and tgw- targets are not looked up.
func (r *region) target(t *RouteTable, in *routeInput, rt *route) error {
	count := 0
	for _, v := range []string{in.GatewayId, in.NatGatewayId, in.InstanceId, in.NetworkInterfaceId, in.VpcPeeringConnectionId, in.TransitGatewayId, in.EgressOnlyInternetGatewayId} {
		if v != "" {
			count++
		}
	}
	if count == 0 {
		return core.Errorf(400, "MissingParameter", "The request must contain a target for the route")
	}
	if count > 1 {
		return core.Errorf(400, "InvalidParameterCombination", "The request must contain only one target for the route")
	}
	apart := func(id string) error {
		return invalid("route table %s and network gateway %s belong to different networks", t.RouteTableId, id)
	}
	switch {
	case in.GatewayId != "":
		id := in.GatewayId
		g := r.Gateways[id]
		switch {
		case strings.HasPrefix(id, "vgw-"):
		case g == nil:
			return core.Errorf(400, "InvalidGatewayID.NotFound", "The gateway ID '%s' does not exist", id)
		case !slices.ContainsFunc(g.Attachments, func(a attachment) bool { return a.VpcId == t.VpcId }):
			return apart(id)
		}
		rt.GatewayId = id
	case in.NatGatewayId != "":
		n := r.Net.Nats[in.NatGatewayId]
		if n == nil || n.State == "deleted" {
			return core.Errorf(400, "InvalidNatGatewayID.NotFound", "The natGateway ID '%s' does not exist", in.NatGatewayId)
		}
		if n.VpcId != t.VpcId {
			return apart(n.NatGatewayId)
		}
		rt.NatGatewayId = n.NatGatewayId
	case in.EgressOnlyInternetGatewayId != "":
		g := r.Net.Eigws[in.EgressOnlyInternetGatewayId]
		if g == nil {
			return core.Errorf(400, "InvalidGatewayID.NotFound", "The gateway ID '%s' does not exist", in.EgressOnlyInternetGatewayId)
		}
		if !slices.ContainsFunc(g.Attachments, func(a attachment) bool { return a.VpcId == t.VpcId }) {
			return apart(in.EgressOnlyInternetGatewayId)
		}
		rt.EgressOnlyInternetGatewayId = in.EgressOnlyInternetGatewayId
	case in.InstanceId != "":
		if i := r.Instances[in.InstanceId]; i == nil || i.State.Name == "terminated" {
			return core.Errorf(400, "InvalidInstanceID.NotFound", "The instance ID '%s' does not exist", in.InstanceId)
		}
		rt.InstanceId = in.InstanceId
	case in.NetworkInterfaceId != "":
		if r.allEnis()[in.NetworkInterfaceId] == nil {
			return core.Errorf(400, "InvalidNetworkInterfaceID.NotFound", "The networkInterface ID '%s' does not exist", in.NetworkInterfaceId)
		}
		rt.NetworkInterfaceId = in.NetworkInterfaceId
	default:
		rt.VpcPeeringConnectionId, rt.TransitGatewayId = in.VpcPeeringConnectionId, in.TransitGatewayId
	}
	if rt.DestinationPrefixListId != "" && r.prefixLists()[rt.DestinationPrefixListId] == nil {
		return core.Errorf(400, "InvalidPrefixListID.NotFound", "The prefix list ID '%s' does not exist", rt.DestinationPrefixListId)
	}
	rt.State, rt.Origin = "active", "CreateRoute"
	return nil
}

func (s *Service) createRoute(c *core.Ctx, in *routeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	t, err := r.table(in.RouteTableId)
	if err != nil {
		return nil, err
	}
	rt, err := in.dest()
	if err != nil {
		return nil, err
	}
	if t.routeIndex(rt.dest()) >= 0 {
		return nil, core.Errorf(400, "RouteAlreadyExists", "The route identified by %s already exists.", rt.dest())
	}
	if err := r.target(t, in, &rt); err != nil {
		return nil, err
	}
	t.Routes = append(t.Routes, rt)
	return ok(), nil
}

func noRoute(dest, table string) error {
	return invalid("no route with destination-cidr-block %s in route table %s", dest, table)
}

func (s *Service) replaceRoute(c *core.Ctx, in *routeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	t, err := r.table(in.RouteTableId)
	if err != nil {
		return nil, err
	}
	rt, err := in.dest()
	if err != nil {
		return nil, err
	}
	i := t.routeIndex(rt.dest())
	if i < 0 {
		return nil, noRoute(rt.dest(), t.RouteTableId)
	}
	if t.Routes[i].GatewayId == "local" {
		return nil, invalid("cannot replace local route %s", rt.dest())
	}
	if err := r.target(t, in, &rt); err != nil {
		return nil, err
	}
	t.Routes[i] = rt
	return ok(), nil
}

func (s *Service) deleteRoute(c *core.Ctx, in *routeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	t, err := r.table(in.RouteTableId)
	if err != nil {
		return nil, err
	}
	rt, err := in.dest()
	if err != nil {
		return nil, err
	}
	i := t.routeIndex(rt.dest())
	if i < 0 {
		return nil, core.Errorf(400, "InvalidRoute.NotFound", "no route with destination-cidr-block %s in route table %s", rt.dest(), t.RouteTableId)
	}
	if t.Routes[i].GatewayId == "local" {
		return nil, invalid("cannot remove local route %s in route table %s", rt.dest(), t.RouteTableId)
	}
	t.Routes = slices.Delete(t.Routes, i, i+1)
	return ok(), nil
}
