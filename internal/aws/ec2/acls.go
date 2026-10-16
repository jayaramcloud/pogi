package ec2

// Network ACLs: custom ACLs, their entries, and which ACL each subnet uses.
// Every subnet is always in one ACL, its VPC's default one until moved.
// pogi stores the rules; it does not filter traffic with them.

import (
	"net"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

type aclAssoc struct {
	NetworkAclAssociationId string `xml:"networkAclAssociationId"`
	NetworkAclId            string `xml:"networkAclId"`
	SubnetId                string `xml:"subnetId"`
}

type portRange struct {
	From int `xml:"from"`
	To   int `xml:"to"`
}

type icmpCode struct {
	Code int `xml:"code"`
	Type int `xml:"type"`
}

func (a *Acl) view() (string, *[]Tag, map[string][]string) {
	attrs := map[string][]string{"network-acl-id": {a.NetworkAclId}, "vpc-id": {a.VpcId}, "default": {strconv.FormatBool(a.IsDefault)}, "owner-id": {a.OwnerId}}
	declare(attrs, "association.association-id", "association.network-acl-id", "association.subnet-id", "entry.cidr", "entry.ipv6-cidr",
		"entry.egress", "entry.rule-action", "entry.rule-number", "entry.protocol")
	for _, x := range a.Associations {
		attrs["association.association-id"] = append(attrs["association.association-id"], x.NetworkAclAssociationId)
		attrs["association.network-acl-id"] = append(attrs["association.network-acl-id"], x.NetworkAclId)
		attrs["association.subnet-id"] = append(attrs["association.subnet-id"], x.SubnetId)
	}
	for _, e := range a.Entries {
		attrs["entry.cidr"] = append(attrs["entry.cidr"], e.CidrBlock)
		attrs["entry.ipv6-cidr"] = append(attrs["entry.ipv6-cidr"], e.Ipv6CidrBlock)
		attrs["entry.egress"] = append(attrs["entry.egress"], strconv.FormatBool(e.Egress))
		attrs["entry.rule-action"] = append(attrs["entry.rule-action"], e.RuleAction)
		attrs["entry.rule-number"] = append(attrs["entry.rule-number"], strconv.Itoa(e.RuleNumber))
		attrs["entry.protocol"] = append(attrs["entry.protocol"], e.Protocol)
	}
	return a.NetworkAclId, &a.TagSet, attrs
}

// defaultEntries are the two rules every ACL starts with (the default ACL
// also has an allow rule 100 in each direction).
func defaultEntries(allow bool) []aclEntry {
	var out []aclEntry
	for _, egress := range []bool{false, true} {
		if allow {
			out = append(out, aclEntry{RuleNumber: 100, Protocol: "-1", RuleAction: "allow", Egress: egress, CidrBlock: "0.0.0.0/0"})
		}
		out = append(out, aclEntry{RuleNumber: 32767, Protocol: "-1", RuleAction: "deny", Egress: egress, CidrBlock: "0.0.0.0/0"})
	}
	return out
}

func (r *region) hasAclAssoc(subnet string) bool {
	for _, a := range r.Acls {
		if slices.ContainsFunc(a.Associations, func(x aclAssoc) bool { return x.SubnetId == subnet }) {
			return true
		}
	}
	return false
}

// joinDefaultAcl puts a new subnet in its VPC's default ACL; lock held.
func (r *region) joinDefaultAcl(sn *Subnet) {
	for _, k := range core.SortedKeys(r.Acls) {
		if a := r.Acls[k]; a.IsDefault && a.VpcId == sn.VpcId {
			a.Associations = append(a.Associations, aclAssoc{newID("aclassoc"), a.NetworkAclId, sn.SubnetId})
			return
		}
	}
}

func (r *region) acl(id string) (*Acl, error) {
	if a := r.Acls[id]; a != nil {
		return a, nil
	}
	return nil, core.Errorf(400, "InvalidNetworkAclID.NotFound", "The networkAcl ID '%s' does not exist", id)
}

func (s *Service) createAcl(c *core.Ctx, in *struct {
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
	a := &Acl{NetworkAclId: newID("acl"), VpcId: v.VpcId, OwnerId: r.Account, Entries: defaultEntries(false)}
	r.Acls[a.NetworkAclId] = a
	r.tag(a.NetworkAclId, in.TagSpecs)
	return &struct {
		Header
		Acl Acl `xml:"networkAcl"`
	}{Acl: withTags(r, a, (*Acl).view)}, nil
}

func (s *Service) deleteAcl(c *core.Ctx, in *struct{ NetworkAclId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	a, err := r.acl(in.NetworkAclId)
	if err != nil {
		return nil, err
	}
	if a.IsDefault {
		return nil, invalid("cannot delete the default network acl %s", a.NetworkAclId)
	}
	if len(a.Associations) > 0 {
		return nil, core.Errorf(400, "DependencyViolation", "The networkAcl '%s' has dependencies and cannot be deleted.", a.NetworkAclId)
	}
	delete(r.Acls, a.NetworkAclId)
	delete(r.Tags, a.NetworkAclId)
	return ok(), nil
}

// aclProtocols names the protocols EC2 accepts by name; it lists them by number.
var aclProtocols = map[string]string{"all": "-1", "tcp": "6", "udp": "17", "icmp": "1", "icmpv6": "58"}

type aclEntryInput struct {
	NetworkAclId             string
	RuleNumber               *int
	Protocol, RuleAction     string
	Egress                   *bool
	CidrBlock, Ipv6CidrBlock string
	PortRange                *portRange
	Icmp                     *icmpCode
}

// aclEntryOp creates ("create") or replaces ("replace") an entry.
func (s *Service) aclEntryOp(verb string) func(*core.Ctx, *aclEntryInput) (any, error) {
	return func(c *core.Ctx, in *aclEntryInput) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		r := s.reg(c)
		a, err := r.acl(in.NetworkAclId)
		if err != nil {
			return nil, err
		}
		e := aclEntry{Protocol: in.Protocol, RuleAction: in.RuleAction, Egress: in.Egress != nil && *in.Egress, PortRange: in.PortRange, IcmpTypeCode: in.Icmp}
		if in.RuleNumber == nil {
			return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter ruleNumber")
		}
		if e.RuleNumber = *in.RuleNumber; e.RuleNumber < 1 || e.RuleNumber > 32766 {
			return nil, invalid("Value (%d) for parameter ruleNumber is invalid. Rule number must be between 1 and 32766.", e.RuleNumber)
		}
		if e.RuleAction != "allow" && e.RuleAction != "deny" {
			return nil, invalid("Value (%s) for parameter ruleAction is invalid. Must be allow or deny.", e.RuleAction)
		}
		if n, named := aclProtocols[strings.ToLower(e.Protocol)]; named {
			e.Protocol = n
		} else if p, err := strconv.Atoi(e.Protocol); err != nil || p < -1 || p > 255 {
			return nil, invalid("Value (%s) for parameter protocol is invalid.", e.Protocol)
		}
		switch {
		case (in.CidrBlock == "") == (in.Ipv6CidrBlock == ""):
			return nil, core.Errorf(400, "MissingParameter", "The request must contain exactly one of cidrBlock and ipv6CidrBlock")
		case in.CidrBlock != "":
			_, n, err := net.ParseCIDR(in.CidrBlock)
			if err != nil || n.IP.To4() == nil {
				return nil, invalid("Value (%s) for parameter cidrBlock is invalid. This is not a valid CIDR block.", in.CidrBlock)
			}
			e.CidrBlock = n.String()
		default:
			_, n, err := net.ParseCIDR(in.Ipv6CidrBlock)
			if err != nil || n.IP.To4() != nil {
				return nil, invalid("Value (%s) for parameter ipv6CidrBlock is invalid. This is not a valid CIDR block.", in.Ipv6CidrBlock)
			}
			e.Ipv6CidrBlock = n.String()
		}
		i := a.entryIndex(e.RuleNumber, e.Egress)
		switch {
		case verb == "create" && i >= 0:
			return nil, core.Errorf(400, "NetworkAclEntryAlreadyExists", "The network acl entry identified by %d already exists.", e.RuleNumber)
		case verb == "replace" && i < 0:
			return nil, noEntry(e.RuleNumber)
		case i >= 0:
			a.Entries[i] = e
		default:
			a.Entries = append(a.Entries, e)
			slices.SortStableFunc(a.Entries, func(x, y aclEntry) int {
				if x.Egress != y.Egress {
					if x.Egress {
						return 1
					}
					return -1
				}
				return x.RuleNumber - y.RuleNumber
			})
		}
		return ok(), nil
	}
}

func noEntry(n int) error {
	return core.Errorf(400, "InvalidNetworkAclEntry.NotFound", "The network acl entry identified by %d does not exist", n)
}

func (a *Acl) entryIndex(n int, egress bool) int {
	return slices.IndexFunc(a.Entries, func(e aclEntry) bool { return e.RuleNumber == n && e.Egress == egress })
}

func (s *Service) deleteAclEntry(c *core.Ctx, in *struct {
	NetworkAclId string
	RuleNumber   int
	Egress       bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	a, err := r.acl(in.NetworkAclId)
	if err != nil {
		return nil, err
	}
	i := a.entryIndex(in.RuleNumber, in.Egress)
	if i < 0 {
		return nil, noEntry(in.RuleNumber)
	}
	if in.RuleNumber == 32767 {
		return nil, invalid("cannot delete the default rule of network acl %s", a.NetworkAclId)
	}
	a.Entries = slices.Delete(a.Entries, i, i+1)
	return ok(), nil
}

// replaceAclAssociation moves a subnet to another ACL of its VPC.
func (s *Service) replaceAclAssociation(c *core.Ctx, in *struct{ AssociationId, NetworkAclId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	to, err := r.acl(in.NetworkAclId)
	if err != nil {
		return nil, err
	}
	for _, k := range core.SortedKeys(r.Acls) {
		from := r.Acls[k]
		i := slices.IndexFunc(from.Associations, func(x aclAssoc) bool { return x.NetworkAclAssociationId == in.AssociationId })
		if i < 0 {
			continue
		}
		x := from.Associations[i]
		if from.VpcId != to.VpcId {
			return nil, invalid("Network ACL %s and association %s belong to different networks", to.NetworkAclId, in.AssociationId)
		}
		from.Associations = slices.Delete(from.Associations, i, i+1)
		x.NetworkAclAssociationId, x.NetworkAclId = newID("aclassoc"), to.NetworkAclId
		to.Associations = append(to.Associations, x)
		return &struct {
			Header
			AssociationId string `xml:"newAssociationId"`
		}{AssociationId: x.NetworkAclAssociationId}, nil
	}
	return nil, notAssociated(in.AssociationId)
}
