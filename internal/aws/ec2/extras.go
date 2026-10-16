package ec2

// Smaller EC2 features that sit beside the core ones: EBS encryption by
// default, the instance type offerings and VPC CIDR associations. The
// resources of the other extras files (transit gateways, flow logs, hosts,
// created images, instance profile associations) are kept in extras, one per
// account and region, inside region.X.

import (
	"net"
	"slices"

	"github.com/nrndev/pogi/internal/core"
)

const defaultEbsKey = "alias/aws/ebs"

// extras are the resources of one account and region that the extras files own.
type extras struct {
	Gateways   map[string]*TransitGateway   `json:"gateways"`
	VpcAttach  map[string]*TgwVpcAttachment `json:"vpcAttach"`
	Peerings   map[string]*TgwPeering       `json:"peerings"` // kept in the requester's region
	FlowLogs   map[string]*FlowLog          `json:"flowLogs"`
	Hosts      map[string]*Host             `json:"hosts"`
	Dhcp       map[string]*DhcpOptions      `json:"dhcp"`
	Images     map[string]*Image            `json:"images"` // made by CreateImage
	Gone       []string                     `json:"gone"`   // catalog images this region deregistered
	Assocs     map[string]*IipAssociation   `json:"assocs"`
	EbsDefault bool                         `json:"ebsDefault"`
	EbsKey     string                       `json:"ebsKey"`
}

func fill[T any](m *map[string]*T) {
	if *m == nil {
		*m = map[string]*T{}
	}
}

// x returns the region's extras, made on first use; lock held.
func (r *region) x() *extras {
	if r.X == nil {
		r.X = &extras{}
	}
	e := r.X
	fill(&e.Gateways)
	fill(&e.VpcAttach)
	fill(&e.Peerings)
	fill(&e.FlowLogs)
	fill(&e.Hosts)
	fill(&e.Dhcp)
	fill(&e.Images)
	fill(&e.Assocs)
	return e
}

func (s *Service) extraOps() map[string]core.Handler {
	return map[string]core.Handler{
		"EnableEbsEncryptionByDefault":             core.H(s.ebsDefault(func(e *extras) { e.EbsDefault = true })),
		"DisableEbsEncryptionByDefault":            core.H(s.ebsDefault(func(e *extras) { e.EbsDefault = false })),
		"GetEbsEncryptionByDefault":                core.H(s.ebsDefault(func(*extras) {})),
		"GetEbsDefaultKmsKeyId":                    core.H(s.ebsKey("")),
		"ResetEbsDefaultKmsKeyId":                  core.H(s.ebsKey(defaultEbsKey)),
		"ModifyEbsDefaultKmsKeyId":                 core.H(s.modifyEbsKey),
		"DescribeInstanceTypeOfferings":            core.H(s.describeInstanceTypeOfferings),
		"AssociateVpcCidrBlock":                    core.H(s.associateVpcCidrBlock),
		"DisassociateVpcCidrBlock":                 core.H(s.disassociateVpcCidrBlock),
		"CreateImage":                              core.H(s.createImage),
		"DeregisterImage":                          core.H(s.deregisterImage),
		"AssociateIamInstanceProfile":              core.H(s.associateIamInstanceProfile),
		"ReplaceIamInstanceProfileAssociation":     core.H(s.replaceIamInstanceProfile),
		"DisassociateIamInstanceProfile":           core.H(s.disassociateIamInstanceProfile),
		"DescribeIamInstanceProfileAssociations":   core.H(s.describeIamInstanceProfileAssociations),
		"CreateFlowLogs":                           core.H(s.createFlowLogs),
		"DescribeFlowLogs":                         core.H(s.describeFlowLogs),
		"DeleteFlowLogs":                           core.H(s.deleteFlowLogs),
		"AllocateHosts":                            core.H(s.allocateHosts),
		"DescribeHosts":                            core.H(s.describeHosts),
		"ModifyHosts":                              core.H(s.modifyHosts),
		"ReleaseHosts":                             core.H(s.releaseHosts),
		"CreateTransitGateway":                     core.H(s.createTransitGateway),
		"DescribeTransitGateways":                  core.H(s.describeTransitGateways),
		"ModifyTransitGateway":                     core.H(s.modifyTransitGateway),
		"DeleteTransitGateway":                     core.H(s.deleteTransitGateway),
		"CreateTransitGatewayVpcAttachment":        core.H(s.createTgwVpcAttachment),
		"DescribeTransitGatewayVpcAttachments":     core.H(s.describeTgwVpcAttachments),
		"ModifyTransitGatewayVpcAttachment":        core.H(s.modifyTgwVpcAttachment),
		"DeleteTransitGatewayVpcAttachment":        core.H(s.deleteTgwVpcAttachment),
		"CreateTransitGatewayPeeringAttachment":    core.H(s.createTgwPeering),
		"DescribeTransitGatewayPeeringAttachments": core.H(s.describeTgwPeerings),
		"AcceptTransitGatewayPeeringAttachment":    core.H(s.answerTgwPeering("available")),
		"RejectTransitGatewayPeeringAttachment":    core.H(s.answerTgwPeering("rejected")),
		"DeleteTransitGatewayPeeringAttachment":    core.H(s.answerTgwPeering("deleted")),
		"DescribeTransitGatewayAttachments":        core.H(s.describeTgwAttachments),
		"GetTransitGatewayRouteTableAssociations":  core.H(s.tgwRouteTableLinks(false)),
		"GetTransitGatewayRouteTablePropagations":  core.H(s.tgwRouteTableLinks(true)),
	}
}

// extraExists reports whether a taggable resource of the extras files exists; lock held.
func (r *region) extraExists(id string) bool {
	e := r.x()
	switch resourceType(id) {
	case "transit-gateway":
		return e.Gateways[id] != nil
	case "transit-gateway-attachment":
		return e.VpcAttach[id] != nil || e.Peerings[id] != nil
	case "vpc-flow-log":
		return e.FlowLogs[id] != nil
	case "dedicated-host":
		return e.Hosts[id] != nil
	case "dhcp-options":
		return e.Dhcp[id] != nil
	}
	return false
}

// ---- EBS encryption by default ----

// ebsKeyID is the key encrypted volumes use unless they name one.
func (e *extras) ebsKeyID() string {
	if e.EbsKey == "" {
		return defaultEbsKey
	}
	return e.EbsKey
}

// ebsDefault serves Enable, Disable and Get of EBS encryption by default.
func (s *Service) ebsDefault(set func(*extras)) func(*core.Ctx, *struct{}) (any, error) {
	return func(c *core.Ctx, _ *struct{}) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		e := s.reg(c).x()
		set(e)
		return &struct {
			Header
			On bool `xml:"ebsEncryptionByDefault"`
		}{On: e.EbsDefault}, nil
	}
}

// ebsKey serves Get ("") and Reset of the default KMS key.
func (s *Service) ebsKey(reset string) func(*core.Ctx, *struct{}) (any, error) {
	return func(c *core.Ctx, _ *struct{}) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		e := s.reg(c).x()
		if reset != "" {
			e.EbsKey = ""
		}
		return &struct {
			Header
			KmsKeyId string `xml:"kmsKeyId"`
		}{KmsKeyId: e.ebsKeyID()}, nil
	}
}

func (s *Service) modifyEbsKey(c *core.Ctx, in *struct{ KmsKeyId string }) (any, error) {
	if in.KmsKeyId == "" {
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter KmsKeyId")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reg(c).x().EbsKey = in.KmsKeyId
	return &struct {
		Header
		KmsKeyId string `xml:"kmsKeyId"`
	}{KmsKeyId: in.KmsKeyId}, nil
}

// ---- instance type offerings ----

type typeOffering struct {
	InstanceType string `xml:"instanceType"`
	LocationType string `xml:"locationType"`
	Location     string `xml:"location"`
}

// describeInstanceTypeOfferings lists every instance type in every location
// asked for: pogi offers all types everywhere.
func (s *Service) describeInstanceTypeOfferings(c *core.Ctx, in *struct {
	LocationType string
	Filters      []filter `query:"Filter"`
}) (any, error) {
	kind := in.LocationType
	if kind == "" {
		kind = "region"
	}
	var places []string
	switch kind {
	case "region":
		places = []string{c.Region}
	case "availability-zone":
		places = zones(c.Region)
	case "availability-zone-id":
		for _, z := range zones(c.Region) {
			places = append(places, zoneID(z))
		}
	default:
		return nil, invalid("Value (%s) for parameter locationType is invalid.", kind)
	}
	if err := checkFilters(in.Filters, map[string][]string{"instance-type": nil, "location": nil}); err != nil {
		return nil, err
	}
	out := &struct {
		Header
		Offerings []typeOffering `xml:"instanceTypeOfferingSet>item"`
	}{}
	for _, place := range places {
		for _, t := range instanceTypes {
			if match(in.Filters, map[string][]string{"instance-type": {t}, "location": {place}}, nil) {
				out.Offerings = append(out.Offerings, typeOffering{t, kind, place})
			}
		}
	}
	return out, nil
}

// ---- VPC CIDR blocks ----

// holds reports whether one of the VPC's associated CIDR blocks contains n.
func (v *Vpc) holds(n *net.IPNet) bool {
	ones, _ := n.Mask.Size()
	return slices.ContainsFunc(v.CidrAssoc, func(a cidrAssoc) bool {
		_, vn, err := net.ParseCIDR(a.CidrBlock)
		if err != nil {
			return false
		}
		vones, _ := vn.Mask.Size()
		return a.State.State == "associated" && vn.Contains(n.IP) && ones >= vones
	})
}

// cidrs are the VPC's associated IPv4 CIDR blocks.
func (v *Vpc) cidrs() []string {
	var out []string
	for _, a := range v.CidrAssoc {
		out = append(out, a.CidrBlock)
	}
	return out
}

// maxVpcCidrs is the number of IPv4 CIDR blocks a VPC may have.
const maxVpcCidrs = 5

func (s *Service) associateVpcCidrBlock(c *core.Ctx, in *struct {
	VpcId, CidrBlock, Ipv6CidrBlock string
	AmazonProvidedIpv6CidrBlock     bool
}) (any, error) {
	switch {
	case in.AmazonProvidedIpv6CidrBlock || in.Ipv6CidrBlock != "":
		return nil, invalid("IPv6 CIDR blocks are not supported.")
	case in.CidrBlock == "":
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter CidrBlock")
	}
	_, n, err := net.ParseCIDR(in.CidrBlock)
	if err != nil || n.IP.To4() == nil {
		return nil, invalid("Value (%s) for parameter cidrBlock is invalid. This is not a valid CIDR block.", in.CidrBlock)
	}
	if ones, _ := n.Mask.Size(); ones < 16 || ones > 28 {
		return nil, core.Errorf(400, "InvalidVpc.Range", "The CIDR '%s' is invalid.", in.CidrBlock)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	v, err := r.vpc(in.VpcId)
	if err != nil {
		return nil, err
	}
	if len(v.CidrAssoc) >= maxVpcCidrs {
		return nil, core.Errorf(400, "CidrLimitExceeded", "This network '%s' has met its maximum number of allowed CIDRs.", v.VpcId)
	}
	for _, a := range v.CidrAssoc {
		if _, o, _ := net.ParseCIDR(a.CidrBlock); o.Contains(n.IP) || n.Contains(o.IP) {
			return nil, invalid("CIDR block %s overlaps with pre-existing CIDR block %s of %s.", n, a.CidrBlock, v.VpcId)
		}
	}
	a := cidrAssoc{newID("vpc-cidr-assoc"), n.String(), status{"associated"}}
	v.CidrAssoc = append(v.CidrAssoc, a)
	r.setLocalRoute(v.VpcId, a.CidrBlock, true)
	a.State.State = "associating" // EC2 answers before the block is associated
	return &struct {
		Header
		VpcId string    `xml:"vpcId"`
		Assoc cidrAssoc `xml:"cidrBlockAssociation"`
	}{VpcId: v.VpcId, Assoc: a}, nil
}

func (s *Service) disassociateVpcCidrBlock(c *core.Ctx, in *struct{ AssociationId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	for _, v := range r.Vpcs {
		k := slices.IndexFunc(v.CidrAssoc, func(a cidrAssoc) bool { return a.AssociationId == in.AssociationId })
		if k < 0 {
			continue
		}
		a := v.CidrAssoc[k]
		if a.CidrBlock == v.CidrBlock {
			return nil, core.Errorf(400, "OperationNotPermitted", "The vpc CIDR block with association ID %s may not be disassociated. It is the primary IPv4 block of the VPC.", a.AssociationId)
		}
		_, n, _ := net.ParseCIDR(a.CidrBlock)
		for _, sn := range r.Subnets {
			if _, sc, _ := net.ParseCIDR(sn.CidrBlock); sn.VpcId == v.VpcId && n.Contains(sc.IP) {
				return nil, core.Errorf(400, "DependencyViolation", "The CIDR '%s' has dependencies (subnet %s) and cannot be disassociated.", a.CidrBlock, sn.SubnetId)
			}
		}
		v.CidrAssoc = slices.Delete(slices.Clone(v.CidrAssoc), k, k+1)
		r.setLocalRoute(v.VpcId, a.CidrBlock, false)
		a.State.State = "disassociating"
		return &struct {
			Header
			VpcId string    `xml:"vpcId"`
			Assoc cidrAssoc `xml:"cidrBlockAssociation"`
		}{VpcId: v.VpcId, Assoc: a}, nil
	}
	return nil, core.Errorf(400, "InvalidVpcCidrBlockAssociationID.NotFound", "The vpc cidr block association ID '%s' does not exist", in.AssociationId)
}

// ---- shared output pieces ----

// failure is one entry of an Unsuccessful list.
type failure struct {
	ResourceId string `xml:"resourceId"`
	Error      struct {
		Code    string `xml:"code"`
		Message string `xml:"message"`
	} `xml:"error"`
}

func newFailure(id, code, msg string) failure {
	f := failure{ResourceId: id}
	f.Error.Code, f.Error.Message = code, msg
	return f
}

// enabled checks an "enable"/"disable" option value; empty is left alone.
func enabled(name string, vals ...*string) error {
	for _, v := range vals {
		if *v != "" && *v != "enable" && *v != "disable" {
			return invalid("Value (%s) for parameter %s is invalid. Value must be enable or disable.", *v, name)
		}
	}
	return nil
}
