package ec2

// Elastic IPs, NAT gateways and network interfaces. A NAT gateway holds an
// Elastic IP and a network interface of its own; DescribeNetworkInterfaces
// lists those and the primary interface of every instance.

import (
	"cmp"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// ---- Elastic IPs ----

// Address is an Elastic IP.
type Address struct {
	AllocationId            string `xml:"allocationId"`
	PublicIp                string `xml:"publicIp"`
	Domain                  string `xml:"domain"`
	PublicIpv4Pool          string `xml:"publicIpv4Pool"`
	NetworkBorderGroup      string `xml:"networkBorderGroup"`
	AssociationId           string `xml:"associationId,omitempty"`
	InstanceId              string `xml:"instanceId,omitempty"`
	NetworkInterfaceId      string `xml:"networkInterfaceId,omitempty"`
	NetworkInterfaceOwnerId string `xml:"networkInterfaceOwnerId,omitempty"`
	PrivateIpAddress        string `xml:"privateIpAddress,omitempty"`
	TagSet                  []Tag  `xml:"tagSet>item" json:"-"`
}

func (a *Address) view() (string, *[]Tag, map[string][]string) {
	return a.AllocationId, &a.TagSet, map[string][]string{"allocation-id": {a.AllocationId}, "association-id": {a.AssociationId},
		"domain": {a.Domain}, "instance-id": {a.InstanceId}, "network-border-group": {a.NetworkBorderGroup},
		"network-interface-id": {a.NetworkInterfaceId}, "network-interface-owner-id": {a.NetworkInterfaceOwnerId},
		"private-ip-address": {a.PrivateIpAddress}, "public-ip": {a.PublicIp}}
}

// publicDNS is the name EC2 gives a public IPv4 address.
func publicDNS(ip, region string) string {
	if region == "us-east-1" {
		return "ec2-" + strings.ReplaceAll(ip, ".", "-") + ".compute-1.amazonaws.com"
	}
	return "ec2-" + strings.ReplaceAll(ip, ".", "-") + "." + region + ".compute.amazonaws.com"
}

// newPublicIP picks a public address no Elastic IP or instance holds; lock held.
func (r *region) newPublicIP() string {
	for {
		ip := fmt.Sprintf("54.%d.%d.%d", rand.IntN(256), rand.IntN(256), 1+rand.IntN(254))
		used := slices.ContainsFunc(slices.Collect(maps.Values(r.Net.Addresses)), func(a *Address) bool { return a.PublicIp == ip }) ||
			slices.ContainsFunc(slices.Collect(maps.Values(r.Instances)), func(i *Instance) bool { return i.PublicIpAddress == ip })
		if !used {
			return ip
		}
	}
}

func (r *region) address(id string) (*Address, error) {
	if a := r.Net.Addresses[id]; a != nil {
		return a, nil
	}
	return nil, core.Errorf(400, "InvalidAllocationID.NotFound", "The allocation ID '%s' does not exist", id)
}

func (s *Service) allocateAddress(c *core.Ctx, in *struct {
	Domain             string
	NetworkBorderGroup string
	TagSpecs           []tagSpec `query:"TagSpecification"`
}) (any, error) {
	if in.Domain != "" && in.Domain != "vpc" {
		return nil, invalid("Value (%s) for parameter domain is invalid. EC2-Classic is no longer supported.", in.Domain)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	a := &Address{AllocationId: newID("eipalloc"), PublicIp: r.newPublicIP(), Domain: "vpc", PublicIpv4Pool: "amazon",
		NetworkBorderGroup: cmp.Or(in.NetworkBorderGroup, r.Name)}
	r.Net.Addresses[a.AllocationId] = a
	r.tag(a.AllocationId, in.TagSpecs)
	return &struct {
		Header
		PublicIp           string `xml:"publicIp"`
		AllocationId       string `xml:"allocationId"`
		Domain             string `xml:"domain"`
		PublicIpv4Pool     string `xml:"publicIpv4Pool"`
		NetworkBorderGroup string `xml:"networkBorderGroup"`
	}{PublicIp: a.PublicIp, AllocationId: a.AllocationId, Domain: a.Domain, PublicIpv4Pool: a.PublicIpv4Pool, NetworkBorderGroup: a.NetworkBorderGroup}, nil
}

// detach takes an address off whatever it is on, and an instance back to
// having no public address; lock held.
func (r *region) detach(a *Address) {
	if i := r.Instances[a.InstanceId]; i != nil {
		i.PublicIpAddress, i.PublicDnsName = "", ""
	}
	a.AssociationId, a.InstanceId, a.NetworkInterfaceId, a.NetworkInterfaceOwnerId, a.PrivateIpAddress = "", "", "", "", ""
}

// dropInstanceAddresses frees the Elastic IPs of an instance that ended; lock held.
func (r *region) dropInstanceAddresses(instance string) {
	for _, a := range r.Net.Addresses {
		if a.InstanceId == instance {
			a.AssociationId, a.InstanceId, a.NetworkInterfaceId, a.NetworkInterfaceOwnerId, a.PrivateIpAddress = "", "", "", "", ""
		}
	}
}

func (s *Service) associateAddress(c *core.Ctx, in *struct {
	AllocationId, InstanceId, NetworkInterfaceId, PrivateIpAddress string
	AllowReassociation                                             bool
}) (any, error) {
	if in.AllocationId == "" {
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter allocationId")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	a, err := r.address(in.AllocationId)
	if err != nil {
		return nil, err
	}
	var inst *Instance
	var nic *NetworkInterface
	switch {
	case in.InstanceId != "":
		if inst = r.Instances[in.InstanceId]; inst == nil || inst.State.Name == "terminated" {
			return nil, core.Errorf(400, "InvalidInstanceID.NotFound", "The instance ID '%s' does not exist", in.InstanceId)
		}
		nic = r.allEnis()[inst.nics()[0].NetworkInterfaceId]
	case in.NetworkInterfaceId != "":
		if nic = r.allEnis()[in.NetworkInterfaceId]; nic == nil {
			return nil, core.Errorf(400, "InvalidNetworkInterfaceID.NotFound", "The networkInterface ID '%s' does not exist", in.NetworkInterfaceId)
		}
		if nic.Attachment != nil {
			inst = r.Instances[nic.Attachment.InstanceId]
		}
	default:
		return nil, core.Errorf(400, "MissingParameter", "The request must contain either the parameter instanceId or networkInterfaceId")
	}
	ip := cmp.Or(in.PrivateIpAddress, nic.PrivateIpAddress)
	if a.AssociationId != "" && !in.AllowReassociation {
		return nil, core.Errorf(400, "Resource.AlreadyAssociated", "resource %s is already associated with associate-id %s", a.AllocationId, a.AssociationId)
	}
	for _, o := range r.Net.Addresses {
		if o != a && o.NetworkInterfaceId == nic.NetworkInterfaceId && o.PrivateIpAddress == ip {
			return nil, core.Errorf(400, "Resource.AlreadyAssociated", "the private address %s of %s already has Elastic IP %s", ip, nic.NetworkInterfaceId, o.AllocationId)
		}
	}
	r.detach(a)
	a.AssociationId, a.NetworkInterfaceId, a.NetworkInterfaceOwnerId, a.PrivateIpAddress = newID("eipassoc"), nic.NetworkInterfaceId, r.Account, ip
	if inst != nil {
		a.InstanceId = inst.InstanceId
		inst.PublicIpAddress, inst.PublicDnsName = a.PublicIp, publicDNS(a.PublicIp, r.Name)
	}
	return &struct {
		Header
		AssociationId string `xml:"associationId"`
	}{AssociationId: a.AssociationId}, nil
}

func (s *Service) disassociateAddress(c *core.Ctx, in *struct{ AssociationId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	for _, a := range r.Net.Addresses {
		if in.AssociationId != "" && a.AssociationId == in.AssociationId {
			r.detach(a)
			return ok(), nil
		}
	}
	return nil, notAssociated(in.AssociationId)
}

func (s *Service) releaseAddress(c *core.Ctx, in *struct{ AllocationId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	a, err := r.address(in.AllocationId)
	if err != nil {
		return nil, err
	}
	if a.AssociationId != "" {
		return nil, core.Errorf(400, "InvalidIPAddress.InUse", "The address with allocation id '%s' is in use.", a.AllocationId)
	}
	delete(r.Net.Addresses, a.AllocationId)
	delete(r.Tags, a.AllocationId)
	return ok(), nil
}

func (s *Service) describeAddresses(c *core.Ctx, in *struct {
	AllocationIds []string `query:"AllocationId"`
	PublicIps     []string `query:"PublicIp"`
	Filters       []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	ids := slices.Clone(in.AllocationIds)
	for _, ip := range in.PublicIps {
		k := slices.IndexFunc(core.SortedKeys(r.Net.Addresses), func(id string) bool { return r.Net.Addresses[id].PublicIp == ip })
		if k < 0 {
			return nil, core.Errorf(400, "InvalidAddress.NotFound", "Address '%s' not found.", ip)
		}
		ids = append(ids, core.SortedKeys(r.Net.Addresses)[k])
	}
	addrs, err := describe(r, r.Net.Addresses, ids, in.Filters, "InvalidAllocationID.NotFound", "allocation ID", (*Address).view)
	return &struct {
		Header
		Addresses []Address `xml:"addressesSet>item"`
	}{Addresses: addrs}, err
}

// describeAddressesAttribute answers for the domain-name attribute: pogi has
// no reverse DNS records, so each address has none.
func (s *Service) describeAddressesAttribute(c *core.Ctx, in *struct {
	AllocationIds []string `query:"AllocationId"`
	Attribute     string
}) (any, error) {
	if in.Attribute != "" && in.Attribute != "domain-name" {
		return nil, invalid("Value (%s) for parameter attribute is invalid. Unknown attribute.", in.Attribute)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	addrs, err := describe(r, r.Net.Addresses, in.AllocationIds, nil, "InvalidAllocationID.NotFound", "allocation ID", (*Address).view)
	type item struct {
		PublicIp     string `xml:"publicIp"`
		AllocationId string `xml:"allocationId"`
	}
	out := &struct {
		Header
		Items []item `xml:"addressSet>item"`
	}{}
	for _, a := range addrs {
		out.Items = append(out.Items, item{a.PublicIp, a.AllocationId})
	}
	return out, err
}

// ---- network interfaces ----

// NetworkInterface is a network interface in its wire shape. Interfaces made
// for NAT gateways and endpoints are stored; those of instances are made
// from the instance when described.
type NetworkInterface struct {
	Association        *eniAssociation   `xml:"association" json:"-"`
	Attachment         *eniAttachment    `xml:"attachment" json:"-"`
	AvailabilityZone   string            `xml:"availabilityZone"`
	Description        string            `xml:"description"`
	Groups             []GroupIdentifier `xml:"groupSet>item"`
	InterfaceType      string            `xml:"interfaceType"`
	MacAddress         string            `xml:"macAddress"`
	NetworkInterfaceId string            `xml:"networkInterfaceId"`
	OwnerId            string            `xml:"ownerId"`
	PrivateDnsName     string            `xml:"privateDnsName"`
	PrivateIpAddress   string            `xml:"privateIpAddress"`
	PrivateIpAddresses []eniIP           `xml:"privateIpAddressesSet>item"`
	RequesterId        string            `xml:"requesterId,omitempty"`
	RequesterManaged   bool              `xml:"requesterManaged"`
	SourceDestCheck    bool              `xml:"sourceDestCheck"`
	Status             string            `xml:"status"`
	SubnetId           string            `xml:"subnetId"`
	TagSet             []Tag             `xml:"tagSet>item" json:"-"`
	VpcId              string            `xml:"vpcId"`
	Owner              string            `xml:"-"` // the NAT gateway or endpoint it belongs to
}

type eniAssociation struct {
	AllocationId  string `xml:"allocationId,omitempty"`
	AssociationId string `xml:"associationId,omitempty"`
	IpOwnerId     string `xml:"ipOwnerId"`
	PublicDnsName string `xml:"publicDnsName"`
	PublicIp      string `xml:"publicIp"`
}

type eniAttachment struct {
	AttachmentId        string `xml:"attachmentId"`
	AttachTime          string `xml:"attachTime"`
	DeleteOnTermination bool   `xml:"deleteOnTermination"`
	DeviceIndex         int    `xml:"deviceIndex"`
	InstanceId          string `xml:"instanceId"`
	InstanceOwnerId     string `xml:"instanceOwnerId"`
	Status              string `xml:"status"`
}

type eniIP struct {
	PrivateDnsName   string `xml:"privateDnsName"`
	PrivateIpAddress string `xml:"privateIpAddress"`
	Primary          bool   `xml:"primary"`
}

func (n *NetworkInterface) view() (string, *[]Tag, map[string][]string) {
	attrs := map[string][]string{"network-interface-id": {n.NetworkInterfaceId}, "subnet-id": {n.SubnetId}, "vpc-id": {n.VpcId},
		"private-ip-address": {n.PrivateIpAddress}, "addresses.private-ip-address": {n.PrivateIpAddress}, "status": {n.Status},
		"description": {n.Description}, "interface-type": {n.InterfaceType}, "availability-zone": {n.AvailabilityZone},
		"mac-address": {n.MacAddress}, "owner-id": {n.OwnerId}, "requester-managed": {fmt.Sprint(n.RequesterManaged)},
		"private-dns-name": {n.PrivateDnsName}}
	declare(attrs, "group-id", "group-name", "attachment.instance-id", "attachment.status", "attachment.attachment-id", "association.allocation-id",
		"association.association-id", "association.public-ip", "addresses.association.public-ip")
	for _, g := range n.Groups {
		attrs["group-id"] = append(attrs["group-id"], g.GroupId)
		attrs["group-name"] = append(attrs["group-name"], g.GroupName)
	}
	if a := n.Attachment; a != nil {
		attrs["attachment.instance-id"], attrs["attachment.status"] = []string{a.InstanceId}, []string{a.Status}
		attrs["attachment.attachment-id"] = []string{a.AttachmentId}
	}
	if a := n.Association; a != nil {
		attrs["association.allocation-id"], attrs["association.association-id"] = []string{a.AllocationId}, []string{a.AssociationId}
		attrs["association.public-ip"], attrs["addresses.association.public-ip"] = []string{a.PublicIp}, []string{a.PublicIp}
	}
	return n.NetworkInterfaceId, &n.TagSet, attrs
}

// allEnis returns copies of every interface: the stored ones, and the primary
// interface of each instance that has not ended. Each shows the Elastic IP
// (or auto-assigned public address) on it. Lock held.
func (r *region) allEnis() map[string]*NetworkInterface {
	out := map[string]*NetworkInterface{}
	for id, n := range r.Net.Enis {
		cp := *n
		out[id] = &cp
	}
	for _, i := range r.Instances {
		if i.State.Name == "terminated" || i.State.Name == "shutting-down" {
			continue
		}
		in := i.nics()[0]
		n := &NetworkInterface{AvailabilityZone: i.Placement.AvailabilityZone, Groups: in.Groups, InterfaceType: "interface", MacAddress: in.MacAddress,
			NetworkInterfaceId: in.NetworkInterfaceId, OwnerId: r.Account, PrivateDnsName: in.PrivateDnsName, PrivateIpAddress: in.PrivateIpAddress,
			PrivateIpAddresses: []eniIP{{in.PrivateDnsName, in.PrivateIpAddress, true}}, SourceDestCheck: in.SourceDestCheck, Status: "in-use",
			SubnetId: i.SubnetId, VpcId: i.VpcId}
		n.Attachment = &eniAttachment{AttachmentId: in.Attachment.AttachmentId, AttachTime: in.Attachment.AttachTime, DeleteOnTermination: true,
			InstanceId: i.InstanceId, InstanceOwnerId: r.Account, Status: "attached"}
		if i.PublicIpAddress != "" {
			n.Association = &eniAssociation{IpOwnerId: "amazon", PublicDnsName: i.PublicDnsName, PublicIp: i.PublicIpAddress}
		}
		out[n.NetworkInterfaceId] = n
	}
	for _, a := range r.Net.Addresses {
		if n := out[a.NetworkInterfaceId]; n != nil && a.AssociationId != "" {
			n.Association = &eniAssociation{AllocationId: a.AllocationId, AssociationId: a.AssociationId, IpOwnerId: r.Account,
				PublicDnsName: publicDNS(a.PublicIp, r.Name), PublicIp: a.PublicIp}
		}
	}
	return out
}

// newEni makes a stored interface in a subnet, at ip or the next free address; lock held.
func (r *region) newEni(sn *Subnet, ip, desc, kind, owner string, groups []GroupIdentifier) (*NetworkInterface, error) {
	if ip == "" {
		if ip = r.freeIP(sn); ip == "" {
			return nil, core.Errorf(400, "InsufficientFreeAddressesInSubnet", "There are not enough free addresses in subnet '%s' to satisfy the requested number of instances.", sn.SubnetId)
		}
	}
	m := core.RandHex(10)
	n := &NetworkInterface{AvailabilityZone: sn.AvailabilityZone, Description: desc, Groups: groups, InterfaceType: kind,
		MacAddress: fmt.Sprintf("02:%s:%s:%s:%s:%s", m[0:2], m[2:4], m[4:6], m[6:8], m[8:10]), NetworkInterfaceId: newID("eni"), OwnerId: r.Account,
		PrivateDnsName: hostname(ip, r.Name), PrivateIpAddress: ip, RequesterId: "amazon-aws", RequesterManaged: true, SourceDestCheck: false,
		Status: "in-use", SubnetId: sn.SubnetId, VpcId: sn.VpcId, Owner: owner}
	n.PrivateIpAddresses = []eniIP{{n.PrivateDnsName, ip, true}}
	r.Net.Enis[n.NetworkInterfaceId] = n
	sn.AvailableIpAddressCount--
	return n, nil
}

// dropEni deletes a stored interface and frees its address; lock held.
func (r *region) dropEni(id string) {
	n := r.Net.Enis[id]
	if n == nil {
		return
	}
	if sn := r.Subnets[n.SubnetId]; sn != nil {
		sn.AvailableIpAddressCount++
	}
	for _, a := range r.Net.Addresses {
		if a.NetworkInterfaceId == id {
			r.detach(a)
		}
	}
	delete(r.Net.Enis, id)
	delete(r.Tags, id)
}

func (s *Service) describeEnis(c *core.Ctx, in *struct {
	Ids     []string `query:"NetworkInterfaceId"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	enis, err := describe(r, r.allEnis(), in.Ids, in.Filters, "InvalidNetworkInterfaceID.NotFound", "networkInterface ID", (*NetworkInterface).view)
	return &struct {
		Header
		Enis []NetworkInterface `xml:"networkInterfaceSet>item"`
	}{Enis: enis}, err
}

// ---- NAT gateways ----

// NatGateway is a NAT gateway.
type NatGateway struct {
	CreateTime          string       `xml:"createTime"`
	DeleteTime          string       `xml:"deleteTime,omitempty"`
	NatGatewayAddresses []natAddress `xml:"natGatewayAddressSet>item"`
	NatGatewayId        string       `xml:"natGatewayId"`
	State               string       `xml:"state"`
	SubnetId            string       `xml:"subnetId"`
	VpcId               string       `xml:"vpcId"`
	TagSet              []Tag        `xml:"tagSet>item" json:"-"`
	ConnectivityType    string       `xml:"connectivityType"`
	ClientToken         string       `xml:"-"`
}

type natAddress struct {
	AllocationId       string `xml:"allocationId,omitempty"`
	NetworkInterfaceId string `xml:"networkInterfaceId"`
	PrivateIp          string `xml:"privateIp"`
	PublicIp           string `xml:"publicIp,omitempty"`
	IsPrimary          bool   `xml:"isPrimary"`
	Status             string `xml:"status"`
}

func (g *NatGateway) view() (string, *[]Tag, map[string][]string) {
	attrs := map[string][]string{"nat-gateway-id": {g.NatGatewayId}, "state": {g.State}, "subnet-id": {g.SubnetId}, "vpc-id": {g.VpcId}}
	declare(attrs, "nat-gateway-address.allocation-id", "nat-gateway-address.public-ip")
	for _, a := range g.NatGatewayAddresses {
		attrs["nat-gateway-address.allocation-id"] = append(attrs["nat-gateway-address.allocation-id"], a.AllocationId)
		attrs["nat-gateway-address.public-ip"] = append(attrs["nat-gateway-address.public-ip"], a.PublicIp)
	}
	return g.NatGatewayId, &g.TagSet, attrs
}

func (s *Service) createNat(c *core.Ctx, in *struct {
	SubnetId, AllocationId, ConnectivityType, PrivateIpAddress, ClientToken string
	TagSpecs                                                                []tagSpec `query:"TagSpecification"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	out := func(g *NatGateway) any {
		v := withTags(r, g, (*NatGateway).view)
		v.State = "pending" // the first answer, as in EC2; it is available when described
		return &struct {
			Header
			ClientToken string     `xml:"clientToken,omitempty"`
			Nat         NatGateway `xml:"natGateway"`
		}{ClientToken: in.ClientToken, Nat: v}
	}
	if in.ClientToken != "" {
		for _, g := range r.Net.Nats {
			if g.ClientToken == in.ClientToken && g.State != "deleted" {
				return out(g), nil
			}
		}
	}
	sn := r.Subnets[in.SubnetId]
	if sn == nil {
		return nil, core.Errorf(400, "InvalidSubnetID.NotFound", "The subnet ID '%s' does not exist", in.SubnetId)
	}
	kind := cmp.Or(in.ConnectivityType, "public")
	if kind != "public" && kind != "private" {
		return nil, invalid("Value (%s) for parameter connectivityType is invalid.", kind)
	}
	var addr *Address
	switch {
	case kind == "private" && in.AllocationId != "":
		return nil, core.Errorf(400, "InvalidParameterCombination", "A private NAT gateway cannot have an allocation ID")
	case kind == "public" && in.AllocationId == "":
		return nil, core.Errorf(400, "MissingParameter", "The request must contain the parameter AllocationId")
	case kind == "public":
		var err error
		if addr, err = r.address(in.AllocationId); err != nil {
			return nil, err
		}
		if addr.AssociationId != "" {
			return nil, core.Errorf(400, "Resource.AlreadyAssociated", "The specified allocation ID %s is already associated", addr.AllocationId)
		}
	}
	g := &NatGateway{NatGatewayId: newID("nat"), CreateTime: time.Now().UTC().Format(core.ISO), State: "available", SubnetId: sn.SubnetId,
		VpcId: sn.VpcId, ConnectivityType: kind, ClientToken: in.ClientToken}
	eni, err := r.newEni(sn, in.PrivateIpAddress, "Interface for NAT Gateway "+g.NatGatewayId, "nat_gateway", g.NatGatewayId, nil)
	if err != nil {
		return nil, err
	}
	na := natAddress{NetworkInterfaceId: eni.NetworkInterfaceId, PrivateIp: eni.PrivateIpAddress, IsPrimary: true, Status: "succeeded"}
	if addr != nil {
		addr.AssociationId, addr.NetworkInterfaceId, addr.NetworkInterfaceOwnerId, addr.PrivateIpAddress = newID("eipassoc"), eni.NetworkInterfaceId, r.Account, eni.PrivateIpAddress
		na.AllocationId, na.PublicIp = addr.AllocationId, addr.PublicIp
	}
	g.NatGatewayAddresses = []natAddress{na}
	r.Net.Nats[g.NatGatewayId] = g
	r.tag(g.NatGatewayId, in.TagSpecs)
	return out(g), nil
}

func (s *Service) describeNats(c *core.Ctx, in *struct {
	Ids     []string `query:"NatGatewayId"`
	Filters []filter `query:"Filter"`
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	for id, g := range r.Net.Nats { // EC2 forgets a deleted NAT gateway after about an hour
		if t, err := time.Parse(core.ISO, g.DeleteTime); err == nil && time.Since(t) > time.Hour {
			delete(r.Net.Nats, id)
			delete(r.Tags, id)
		}
	}
	nats, err := describe(r, r.Net.Nats, in.Ids, in.Filters, "NatGatewayNotFound", "natGateway ID", (*NatGateway).view)
	return &struct {
		Header
		Nats []NatGateway `xml:"natGatewaySet>item"`
	}{Nats: nats}, err
}

func (s *Service) deleteNat(c *core.Ctx, in *struct{ NatGatewayId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.reg(c)
	g := r.Net.Nats[in.NatGatewayId]
	if g == nil {
		return nil, core.Errorf(400, "NatGatewayNotFound", "The Nat Gateway %s was not found", in.NatGatewayId)
	}
	if g.State != "deleted" {
		for _, a := range g.NatGatewayAddresses {
			r.dropEni(a.NetworkInterfaceId)
		}
		id := g.NatGatewayId
		r.blackhole(func(rt route) bool { return rt.NatGatewayId == id })
		g.State, g.DeleteTime = "deleted", time.Now().UTC().Format(core.ISO)
	}
	return &struct {
		Header
		NatGatewayId string `xml:"natGatewayId"`
	}{NatGatewayId: g.NatGatewayId}, nil
}
