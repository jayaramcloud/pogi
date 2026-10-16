package ec2

// Transit gateways, their VPC attachments and peering attachments. States
// change at once (a gateway is available when created); a peering waits in
// pendingAcceptance until it is accepted. Every gateway has one default
// route table; custom route tables and routes are not built.

import (
	"net"
	"slices"
	"strconv"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// tgwOptions are the options of a transit gateway; "enable" or "disable" except the ASN.
type tgwOptions struct {
	AmazonSideAsn                   int64    `xml:"amazonSideAsn"`
	TransitGatewayCidrBlocks        []string `xml:"transitGatewayCidrBlocks>item"`
	AutoAcceptSharedAttachments     string   `xml:"autoAcceptSharedAttachments"`
	DefaultRouteTableAssociation    string   `xml:"defaultRouteTableAssociation"`
	AssociationDefaultRouteTableId  string   `xml:"associationDefaultRouteTableId,omitempty"`
	DefaultRouteTablePropagation    string   `xml:"defaultRouteTablePropagation"`
	PropagationDefaultRouteTableId  string   `xml:"propagationDefaultRouteTableId,omitempty"`
	VpnEcmpSupport                  string   `xml:"vpnEcmpSupport"`
	DnsSupport                      string   `xml:"dnsSupport"`
	SecurityGroupReferencingSupport string   `xml:"securityGroupReferencingSupport"`
	MulticastSupport                string   `xml:"multicastSupport"`
}

// flags are the enable/disable options in a fixed order.
func (o *tgwOptions) flags() []*string {
	return []*string{&o.AutoAcceptSharedAttachments, &o.DefaultRouteTableAssociation, &o.DefaultRouteTablePropagation,
		&o.VpnEcmpSupport, &o.DnsSupport, &o.SecurityGroupReferencingSupport, &o.MulticastSupport}
}

// apply sets the options a request gives.
func (o *tgwOptions) apply(in tgwOptions) error {
	if err := enabled("options", in.flags()...); err != nil {
		return err
	}
	for k, p := range in.flags() {
		if *p != "" {
			*o.flags()[k] = *p
		}
	}
	if in.AmazonSideAsn != 0 {
		if a := in.AmazonSideAsn; a < 64512 || a > 65534 && a < 4200000000 || a > 4294967294 {
			return invalid("Value (%d) for parameter amazonSideAsn is invalid.", a)
		}
		o.AmazonSideAsn = in.AmazonSideAsn
	}
	if in.AssociationDefaultRouteTableId != "" {
		o.AssociationDefaultRouteTableId = in.AssociationDefaultRouteTableId
	}
	if in.PropagationDefaultRouteTableId != "" {
		o.PropagationDefaultRouteTableId = in.PropagationDefaultRouteTableId
	}
	return nil
}

// TransitGateway is a transit gateway.
type TransitGateway struct {
	TransitGatewayId  string     `xml:"transitGatewayId"`
	TransitGatewayArn string     `xml:"transitGatewayArn"`
	State             string     `xml:"state"`
	OwnerId           string     `xml:"ownerId"`
	Description       string     `xml:"description"`
	CreationTime      string     `xml:"creationTime"`
	Options           tgwOptions `xml:"options"`
	TagSet            []Tag      `xml:"tagSet>item" json:"-"`
	RouteTableId      string     `xml:"-"` // the default route table
}

func (t *TransitGateway) view() (string, *[]Tag, map[string][]string) {
	t.Options.TransitGatewayCidrBlocks = slices.Clone(t.Options.TransitGatewayCidrBlocks)
	return t.TransitGatewayId, &t.TagSet, map[string][]string{"transit-gateway-id": {t.TransitGatewayId}, "state": {t.State},
		"owner-id": {t.OwnerId}, "description": {t.Description}, "options.amazon-side-asn": {strconv.FormatInt(t.Options.AmazonSideAsn, 10)},
		"options.association-default-route-table-id": {t.Options.AssociationDefaultRouteTableId},
		"options.propagation-default-route-table-id": {t.Options.PropagationDefaultRouteTableId}}
}

// live reports whether a state is one an attachment or gateway still holds resources in.
func live(state string) bool {
	return !slices.Contains([]string{"deleted", "deleting", "rejected", "rejecting", "failed"}, state)
}

// tgw finds an available transit gateway; lock held.
func (e *extras) tgw(id string) (*TransitGateway, error) {
	if t := e.Gateways[id]; t != nil && t.State == "available" {
		return t, nil
	}
	return nil, core.Errorf(400, "InvalidTransitGatewayID.NotFound", "The transit gateway '%s' does not exist", id)
}

func (s *Service) createTransitGateway(c *core.Ctx, in *struct {
	Description       string
	Options           tgwOptions
	TagSpecifications []tagSpec `query:"TagSpecification"`
}) (any, error) {
	for _, b := range in.Options.TransitGatewayCidrBlocks {
		if _, _, err := net.ParseCIDR(b); err != nil {
			return nil, invalid("Value (%s) for parameter transitGatewayCidrBlocks is invalid. This is not a valid CIDR block.", b)
		}
	}
	opt := tgwOptions{AmazonSideAsn: 64512, AutoAcceptSharedAttachments: "disable", DefaultRouteTableAssociation: "enable",
		DefaultRouteTablePropagation: "enable", VpnEcmpSupport: "enable", DnsSupport: "enable",
		SecurityGroupReferencingSupport: "disable", MulticastSupport: "disable", TransitGatewayCidrBlocks: in.Options.TransitGatewayCidrBlocks}
	if err := opt.apply(in.Options); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	t := &TransitGateway{TransitGatewayId: newID("tgw"), State: "available", OwnerId: r.Account, Description: in.Description,
		CreationTime: time.Now().UTC().Format(core.ISO), Options: opt, RouteTableId: newID("tgw-rtb")}
	t.TransitGatewayArn = core.ARN("ec2", r.Name, r.Account, "transit-gateway/"+t.TransitGatewayId)
	if opt.DefaultRouteTableAssociation == "enable" {
		t.Options.AssociationDefaultRouteTableId = t.RouteTableId
	}
	if opt.DefaultRouteTablePropagation == "enable" {
		t.Options.PropagationDefaultRouteTableId = t.RouteTableId
	}
	r.x().Gateways[t.TransitGatewayId] = t
	r.tag(t.TransitGatewayId, typed(in.TagSpecifications, "transit-gateway"))
	return &struct {
		Header
		Gateway TransitGateway `xml:"transitGateway"`
	}{Gateway: withTags(r, t, (*TransitGateway).view)}, nil
}

func (s *Service) describeTransitGateways(c *core.Ctx, in *struct {
	Ids     []string `query:"TransitGatewayIds"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	_, _, served := (&TransitGateway{}).view()
	if err := checkFilters(in.Filters, served); err != nil {
		return nil, err
	}
	list, err := describe(r, r.x().Gateways, in.Ids, in.Filters, "InvalidTransitGatewayID.NotFound", "transit gateway", (*TransitGateway).view)
	return &struct {
		Header
		Gateways []TransitGateway `xml:"transitGatewaySet>item"`
	}{Gateways: list}, err
}

func (s *Service) modifyTransitGateway(c *core.Ctx, in *struct {
	TransitGatewayId, Description string
	Options                       struct {
		tgwOptions
		AddTransitGatewayCidrBlocks, RemoveTransitGatewayCidrBlocks []string
	}
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	t, err := r.x().tgw(in.TransitGatewayId)
	if err != nil {
		return nil, err
	}
	opt := t.Options
	if err := opt.apply(in.Options.tgwOptions); err != nil {
		return nil, err
	}
	blocks := slices.Clone(opt.TransitGatewayCidrBlocks)
	for _, b := range in.Options.AddTransitGatewayCidrBlocks {
		if _, _, err := net.ParseCIDR(b); err != nil {
			return nil, invalid("Value (%s) for parameter transitGatewayCidrBlocks is invalid. This is not a valid CIDR block.", b)
		}
		if !slices.Contains(blocks, b) {
			blocks = append(blocks, b)
		}
	}
	blocks = slices.DeleteFunc(blocks, func(b string) bool { return slices.Contains(in.Options.RemoveTransitGatewayCidrBlocks, b) })
	opt.TransitGatewayCidrBlocks = blocks
	t.Options = opt
	if in.Description != "" {
		t.Description = in.Description
	}
	return &struct {
		Header
		Gateway TransitGateway `xml:"transitGateway"`
	}{Gateway: withTags(r, t, (*TransitGateway).view)}, nil
}

func (s *Service) deleteTransitGateway(c *core.Ctx, in *struct{ TransitGatewayId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	e := r.x()
	t, err := e.tgw(in.TransitGatewayId)
	if err != nil {
		return nil, err
	}
	for _, a := range e.VpcAttach {
		if a.TransitGatewayId == t.TransitGatewayId && live(a.State) {
			return nil, core.Errorf(400, "IncorrectState", "Transit gateway %s has attachments (%s) and cannot be deleted.", t.TransitGatewayId, a.TransitGatewayAttachmentId)
		}
	}
	for _, p := range s.allPeerings() {
		if live(p.State) && (p.RequesterTgwInfo.TransitGatewayId == t.TransitGatewayId || p.AccepterTgwInfo.TransitGatewayId == t.TransitGatewayId) {
			return nil, core.Errorf(400, "IncorrectState", "Transit gateway %s has attachments (%s) and cannot be deleted.", t.TransitGatewayId, p.TransitGatewayAttachmentId)
		}
	}
	t.State = "deleted"
	out := withTags(r, t, (*TransitGateway).view)
	out.State = "deleting"
	return &struct {
		Header
		Gateway TransitGateway `xml:"transitGateway"`
	}{Gateway: out}, nil
}

// ---- VPC attachments ----

type vpcAttachOptions struct {
	DnsSupport                      string `xml:"dnsSupport"`
	SecurityGroupReferencingSupport string `xml:"securityGroupReferencingSupport"`
	Ipv6Support                     string `xml:"ipv6Support"`
	ApplianceModeSupport            string `xml:"applianceModeSupport"`
}

// TgwVpcAttachment attaches a VPC to a transit gateway.
type TgwVpcAttachment struct {
	TransitGatewayAttachmentId string           `xml:"transitGatewayAttachmentId"`
	TransitGatewayId           string           `xml:"transitGatewayId"`
	VpcId                      string           `xml:"vpcId"`
	VpcOwnerId                 string           `xml:"vpcOwnerId"`
	State                      string           `xml:"state"`
	SubnetIds                  []string         `xml:"subnetIds>item"`
	CreationTime               string           `xml:"creationTime"`
	Options                    vpcAttachOptions `xml:"options"`
	TagSet                     []Tag            `xml:"tagSet>item" json:"-"`
}

func (a *TgwVpcAttachment) view() (string, *[]Tag, map[string][]string) {
	a.SubnetIds = slices.Clone(a.SubnetIds)
	return a.TransitGatewayAttachmentId, &a.TagSet, map[string][]string{"transit-gateway-attachment-id": {a.TransitGatewayAttachmentId},
		"transit-gateway-id": {a.TransitGatewayId}, "vpc-id": {a.VpcId}, "vpc-owner-id": {a.VpcOwnerId}, "state": {a.State}}
}

// apply sets the options a request gives.
func (o *vpcAttachOptions) apply(in vpcAttachOptions) error {
	if err := enabled("options", &in.DnsSupport, &in.SecurityGroupReferencingSupport, &in.Ipv6Support, &in.ApplianceModeSupport); err != nil {
		return err
	}
	for _, f := range [][2]*string{{&o.DnsSupport, &in.DnsSupport}, {&o.SecurityGroupReferencingSupport, &in.SecurityGroupReferencingSupport},
		{&o.Ipv6Support, &in.Ipv6Support}, {&o.ApplianceModeSupport, &in.ApplianceModeSupport}} {
		if *f[1] != "" {
			*f[0] = *f[1]
		}
	}
	return nil
}

// checkSubnets accepts subnets of the VPC, at most one a zone; lock held.
func (r *region) checkSubnets(vpc string, ids []string) error {
	zs := map[string]bool{}
	for _, id := range ids {
		sn := r.Subnets[id]
		if sn == nil || sn.VpcId != vpc {
			return core.Errorf(400, "InvalidSubnetID.NotFound", "The subnet ID '%s' does not exist", id)
		}
		if zs[sn.AvailabilityZone] {
			return core.Errorf(400, "DuplicateSubnetsInSameZone", "Found more than one subnet in availability zone %s.", sn.AvailabilityZone)
		}
		zs[sn.AvailabilityZone] = true
	}
	return nil
}

func (s *Service) createTgwVpcAttachment(c *core.Ctx, in *struct {
	TransitGatewayId, VpcId string
	SubnetIds               []string
	Options                 vpcAttachOptions
	TagSpecifications       []tagSpec `query:"TagSpecification"`
}) (any, error) {
	opt := vpcAttachOptions{"enable", "enable", "disable", "disable"}
	if err := opt.apply(in.Options); err != nil {
		return nil, err
	}
	if len(in.SubnetIds) == 0 {
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter SubnetIds")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	e := r.x()
	t, err := e.tgw(in.TransitGatewayId)
	if err != nil {
		return nil, err
	}
	if _, err := r.vpc(in.VpcId); err != nil {
		return nil, err
	}
	if err := r.checkSubnets(in.VpcId, in.SubnetIds); err != nil {
		return nil, err
	}
	for _, a := range e.VpcAttach {
		if a.TransitGatewayId == t.TransitGatewayId && a.VpcId == in.VpcId && live(a.State) {
			return nil, core.Errorf(400, "DuplicateTransitGatewayAttachment", "Transit gateway %s already has an attachment for %s.", t.TransitGatewayId, in.VpcId)
		}
	}
	a := &TgwVpcAttachment{TransitGatewayAttachmentId: newID("tgw-attach"), TransitGatewayId: t.TransitGatewayId, VpcId: in.VpcId,
		VpcOwnerId: r.Account, State: "available", SubnetIds: in.SubnetIds, CreationTime: time.Now().UTC().Format(core.ISO), Options: opt}
	e.VpcAttach[a.TransitGatewayAttachmentId] = a
	r.tag(a.TransitGatewayAttachmentId, typed(in.TagSpecifications, "transit-gateway-attachment"))
	return &struct {
		Header
		Attachment TgwVpcAttachment `xml:"transitGatewayVpcAttachment"`
	}{Attachment: withTags(r, a, (*TgwVpcAttachment).view)}, nil
}

func (s *Service) describeTgwVpcAttachments(c *core.Ctx, in *struct {
	Ids     []string `query:"TransitGatewayAttachmentIds"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	_, _, served := (&TgwVpcAttachment{}).view()
	if err := checkFilters(in.Filters, served); err != nil {
		return nil, err
	}
	list, err := describe(r, r.x().VpcAttach, in.Ids, in.Filters, "InvalidTransitGatewayAttachmentID.NotFound", "transit gateway attachment", (*TgwVpcAttachment).view)
	return &struct {
		Header
		Attachments []TgwVpcAttachment `xml:"transitGatewayVpcAttachments>item"`
	}{Attachments: list}, err
}

func (s *Service) modifyTgwVpcAttachment(c *core.Ctx, in *struct {
	TransitGatewayAttachmentId    string
	AddSubnetIds, RemoveSubnetIds []string
	Options                       vpcAttachOptions
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	a := r.x().VpcAttach[in.TransitGatewayAttachmentId]
	if a == nil || !live(a.State) {
		return nil, core.Errorf(400, "InvalidTransitGatewayAttachmentID.NotFound", "The transit gateway attachment '%s' does not exist", in.TransitGatewayAttachmentId)
	}
	opt := a.Options
	if err := opt.apply(in.Options); err != nil {
		return nil, err
	}
	subnets := slices.DeleteFunc(slices.Clone(a.SubnetIds), func(id string) bool { return slices.Contains(in.RemoveSubnetIds, id) })
	for _, id := range in.AddSubnetIds {
		if !slices.Contains(subnets, id) {
			subnets = append(subnets, id)
		}
	}
	if len(subnets) == 0 {
		return nil, invalid("An attachment needs at least one subnet.")
	}
	if err := r.checkSubnets(a.VpcId, subnets); err != nil {
		return nil, err
	}
	a.SubnetIds, a.Options = subnets, opt
	return &struct {
		Header
		Attachment TgwVpcAttachment `xml:"transitGatewayVpcAttachment"`
	}{Attachment: withTags(r, a, (*TgwVpcAttachment).view)}, nil
}

func (s *Service) deleteTgwVpcAttachment(c *core.Ctx, in *struct{ TransitGatewayAttachmentId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	a := r.x().VpcAttach[in.TransitGatewayAttachmentId]
	if a == nil || !live(a.State) {
		return nil, core.Errorf(400, "InvalidTransitGatewayAttachmentID.NotFound", "The transit gateway attachment '%s' does not exist", in.TransitGatewayAttachmentId)
	}
	a.State = "deleted"
	out := withTags(r, a, (*TgwVpcAttachment).view)
	out.State = "deleting"
	return &struct {
		Header
		Attachment TgwVpcAttachment `xml:"transitGatewayVpcAttachment"`
	}{Attachment: out}, nil
}

// ---- peering attachments ----

type tgwInfo struct {
	TransitGatewayId string `xml:"transitGatewayId"`
	OwnerId          string `xml:"ownerId"`
	Region           string `xml:"region"`
}

// TgwPeering peers two transit gateways, which may be in other accounts and regions.
type TgwPeering struct {
	TransitGatewayAttachmentId string  `xml:"transitGatewayAttachmentId"`
	RequesterTgwInfo           tgwInfo `xml:"requesterTgwInfo"`
	AccepterTgwInfo            tgwInfo `xml:"accepterTgwInfo"`
	Options                    struct {
		DynamicRouting string `xml:"dynamicRouting"`
	} `xml:"options"`
	Status struct {
		Code    string `xml:"code"`
		Message string `xml:"message"`
	} `xml:"status"`
	State        string `xml:"state"`
	CreationTime string `xml:"creationTime"`
	TagSet       []Tag  `xml:"tagSet>item" json:"-"`
}

func (p *TgwPeering) view() (string, *[]Tag, map[string][]string) {
	return p.TransitGatewayAttachmentId, &p.TagSet, map[string][]string{"transit-gateway-attachment-id": {p.TransitGatewayAttachmentId},
		"transit-gateway-id": {p.RequesterTgwInfo.TransitGatewayId, p.AccepterTgwInfo.TransitGatewayId}, "state": {p.State},
		"local-owner-id": {p.RequesterTgwInfo.OwnerId}, "remote-owner-id": {p.AccepterTgwInfo.OwnerId}}
}

func (p *TgwPeering) set(state string) {
	p.State, p.Status.Code = state, state
}

// allPeerings lists the peerings of every region, by id; lock held.
func (s *Service) allPeerings() map[string]*TgwPeering {
	all := map[string]*TgwPeering{}
	for _, r := range s.st.Regions {
		for id, p := range r.x().Peerings {
			all[id] = p
		}
	}
	return all
}

// sees reports whether the caller is one side of the peering.
func (p *TgwPeering) sees(c *core.Ctx) bool {
	return p.RequesterTgwInfo.OwnerId == c.Account && p.RequesterTgwInfo.Region == c.Region ||
		p.AccepterTgwInfo.OwnerId == c.Account && p.AccepterTgwInfo.Region == c.Region
}

func (s *Service) createTgwPeering(c *core.Ctx, in *struct {
	TransitGatewayId, PeerTransitGatewayId, PeerAccountId, PeerRegion string
	Options                                                           struct{ DynamicRouting string }
	TagSpecifications                                                 []tagSpec `query:"TagSpecification"`
}) (any, error) {
	for name, v := range map[string]string{"TransitGatewayId": in.TransitGatewayId, "PeerTransitGatewayId": in.PeerTransitGatewayId,
		"PeerAccountId": in.PeerAccountId, "PeerRegion": in.PeerRegion} {
		if v == "" {
			return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter %s", name)
		}
	}
	if !slices.Contains(regions, in.PeerRegion) {
		return nil, invalid("Value (%s) for parameter peerRegion is invalid.", in.PeerRegion)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	if _, err := r.x().tgw(in.TransitGatewayId); err != nil {
		return nil, err
	}
	p := &TgwPeering{TransitGatewayAttachmentId: newID("tgw-attach"), CreationTime: time.Now().UTC().Format(core.ISO),
		RequesterTgwInfo: tgwInfo{in.TransitGatewayId, r.Account, r.Name},
		AccepterTgwInfo:  tgwInfo{in.PeerTransitGatewayId, in.PeerAccountId, in.PeerRegion}}
	p.Options.DynamicRouting = "disable"
	if in.Options.DynamicRouting != "" {
		if err := enabled("options.dynamicRouting", &in.Options.DynamicRouting); err != nil {
			return nil, err
		}
		p.Options.DynamicRouting = in.Options.DynamicRouting
	}
	p.set("pendingAcceptance")
	r.x().Peerings[p.TransitGatewayAttachmentId] = p
	r.tag(p.TransitGatewayAttachmentId, typed(in.TagSpecifications, "transit-gateway-attachment"))
	return &struct {
		Header
		Peering TgwPeering `xml:"transitGatewayPeeringAttachment"`
	}{Peering: withTags(r, p, (*TgwPeering).view)}, nil
}

func (s *Service) describeTgwPeerings(c *core.Ctx, in *struct {
	Ids     []string `query:"TransitGatewayAttachmentIds"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	_, _, served := (&TgwPeering{}).view()
	if err := checkFilters(in.Filters, served); err != nil {
		return nil, err
	}
	seen := map[string]*TgwPeering{}
	for id, p := range s.allPeerings() {
		if p.sees(c) {
			seen[id] = p
		}
	}
	list, err := describe(r, seen, in.Ids, in.Filters, "InvalidTransitGatewayAttachmentID.NotFound", "transit gateway attachment", (*TgwPeering).view)
	return &struct {
		Header
		Peerings []TgwPeering `xml:"transitGatewayPeeringAttachments>item"`
	}{Peerings: list}, err
}

// answerTgwPeering serves Accept, Reject and Delete of a peering attachment.
func (s *Service) answerTgwPeering(to string) func(*core.Ctx, *struct{ TransitGatewayAttachmentId string }) (any, error) {
	return func(c *core.Ctx, in *struct{ TransitGatewayAttachmentId string }) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		r := s.reg(c)
		p := s.allPeerings()[in.TransitGatewayAttachmentId]
		if p == nil || !live(p.State) && to == "deleted" || !p.sees(c) {
			return nil, core.Errorf(400, "InvalidTransitGatewayAttachmentID.NotFound", "The transit gateway attachment '%s' does not exist", in.TransitGatewayAttachmentId)
		}
		if to != "deleted" && p.State != "pendingAcceptance" {
			return nil, core.Errorf(400, "IncorrectState", "Transit gateway attachment %s is in state %s.", p.TransitGatewayAttachmentId, p.State)
		}
		p.set(to)
		out := withTags(r, p, (*TgwPeering).view)
		if to == "deleted" {
			out.State = "deleting"
		}
		return &struct {
			Header
			Peering TgwPeering `xml:"transitGatewayPeeringAttachment"`
		}{Peering: out}, nil
	}
}

// ---- all attachments, route table links ----

// TgwAttachment is an attachment of any kind.
type TgwAttachment struct {
	TransitGatewayAttachmentId string `xml:"transitGatewayAttachmentId"`
	TransitGatewayId           string `xml:"transitGatewayId"`
	TransitGatewayOwnerId      string `xml:"transitGatewayOwnerId"`
	ResourceOwnerId            string `xml:"resourceOwnerId"`
	ResourceType               string `xml:"resourceType"`
	ResourceId                 string `xml:"resourceId"`
	State                      string `xml:"state"`
	Association                *struct {
		TransitGatewayRouteTableId string `xml:"transitGatewayRouteTableId"`
		State                      string `xml:"state"`
	} `xml:"association"`
	CreationTime string `xml:"creationTime"`
	TagSet       []Tag  `xml:"tagSet>item"`
}

// attachments lists the attachments the caller sees; lock held.
func (s *Service) attachments(c *core.Ctx, r *region) []TgwAttachment {
	e := r.x()
	var out []TgwAttachment
	for _, id := range core.SortedKeys(e.VpcAttach) {
		a := e.VpcAttach[id]
		x := TgwAttachment{TransitGatewayAttachmentId: id, TransitGatewayId: a.TransitGatewayId, TransitGatewayOwnerId: r.Account,
			ResourceOwnerId: a.VpcOwnerId, ResourceType: "vpc", ResourceId: a.VpcId, State: a.State, CreationTime: a.CreationTime, TagSet: r.tagSet(id)}
		if t := e.Gateways[a.TransitGatewayId]; t != nil && t.Options.AssociationDefaultRouteTableId != "" && a.State == "available" {
			x.Association = &struct {
				TransitGatewayRouteTableId string `xml:"transitGatewayRouteTableId"`
				State                      string `xml:"state"`
			}{t.Options.AssociationDefaultRouteTableId, "associated"}
		}
		out = append(out, x)
	}
	all := s.allPeerings()
	for _, id := range core.SortedKeys(all) {
		p := all[id]
		if !p.sees(c) {
			continue
		}
		out = append(out, TgwAttachment{TransitGatewayAttachmentId: id, TransitGatewayId: p.RequesterTgwInfo.TransitGatewayId,
			TransitGatewayOwnerId: p.RequesterTgwInfo.OwnerId, ResourceOwnerId: p.AccepterTgwInfo.OwnerId, ResourceType: "peering",
			ResourceId: p.AccepterTgwInfo.TransitGatewayId, State: p.State, CreationTime: p.CreationTime, TagSet: r.tagSet(id)})
	}
	return out
}

func (s *Service) describeTgwAttachments(c *core.Ctx, in *struct {
	Ids     []string `query:"TransitGatewayAttachmentIds"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	attrs := map[string][]string{"transit-gateway-attachment-id": nil, "transit-gateway-id": nil, "resource-type": nil, "resource-id": nil,
		"resource-owner-id": nil, "state": nil, "transit-gateway-owner-id": nil}
	if err := checkFilters(in.Filters, attrs); err != nil {
		return nil, err
	}
	all := s.attachments(c, r)
	for _, id := range in.Ids {
		if !slices.ContainsFunc(all, func(a TgwAttachment) bool { return a.TransitGatewayAttachmentId == id }) {
			return nil, core.Errorf(400, "InvalidTransitGatewayAttachmentID.NotFound", "The transit gateway attachment '%s' does not exist", id)
		}
	}
	out := &struct {
		Header
		Attachments []TgwAttachment `xml:"transitGatewayAttachments>item"`
	}{}
	for _, a := range all {
		if (len(in.Ids) == 0 || slices.Contains(in.Ids, a.TransitGatewayAttachmentId)) && match(in.Filters, map[string][]string{
			"transit-gateway-attachment-id": {a.TransitGatewayAttachmentId}, "transit-gateway-id": {a.TransitGatewayId},
			"resource-type": {a.ResourceType}, "resource-id": {a.ResourceId}, "resource-owner-id": {a.ResourceOwnerId},
			"state": {a.State}, "transit-gateway-owner-id": {a.TransitGatewayOwnerId}}, nil) {
			out.Attachments = append(out.Attachments, a)
		}
	}
	return out, nil
}

// tgwRouteTableLinks serves the associations (default) or propagations of a
// default route table: every available VPC attachment of its gateway, when
// the gateway makes the table its default for them.
func (s *Service) tgwRouteTableLinks(propagations bool) func(*core.Ctx, *struct {
	TransitGatewayRouteTableId string
	Filters                    []filter `query:"Filter"`
}) (any, error) {
	type link struct {
		TransitGatewayAttachmentId string `xml:"transitGatewayAttachmentId"`
		ResourceId                 string `xml:"resourceId"`
		ResourceType               string `xml:"resourceType"`
		State                      string `xml:"state"`
	}
	return func(c *core.Ctx, in *struct {
		TransitGatewayRouteTableId string
		Filters                    []filter `query:"Filter"`
	}) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		e := s.reg(c).x()
		var links []link
		found := false
		for _, t := range e.Gateways {
			if t.RouteTableId != in.TransitGatewayRouteTableId || t.State != "available" {
				continue
			}
			found = true
			linked, state := t.Options.AssociationDefaultRouteTableId, "associated"
			if propagations {
				linked, state = t.Options.PropagationDefaultRouteTableId, "enabled"
			}
			for _, id := range core.SortedKeys(e.VpcAttach) {
				if a := e.VpcAttach[id]; a.TransitGatewayId == t.TransitGatewayId && a.State == "available" && linked == t.RouteTableId {
					l := link{id, a.VpcId, "vpc", state}
					if match(in.Filters, map[string][]string{"transit-gateway-attachment-id": {id}, "resource-id": {a.VpcId}, "resource-type": {"vpc"}, "state": {state}}, nil) {
						links = append(links, l)
					}
				}
			}
		}
		if !found {
			return nil, core.Errorf(400, "InvalidRouteTableID.NotFound", "The transit gateway route table '%s' does not exist", in.TransitGatewayRouteTableId)
		}
		if propagations {
			return &struct {
				Header
				Links []link `xml:"transitGatewayRouteTablePropagations>item"`
			}{Links: links}, nil
		}
		return &struct {
			Header
			Links []link `xml:"associations>item"`
		}{Links: links}, nil
	}
}
