package ec2_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	ec2svc "github.com/nrndev/pogi/internal/aws/ec2"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

// netEnv starts EC2 and makes a VPC with one /24 subnet.
func netEnv(t *testing.T) (*core.Hub, *ec2.Client, string, string) {
	t.Helper()
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { return ec2svc.New(h) })
	c := ec2.NewFromConfig(awstest.Config(t, url))
	vpc := *must(c.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.50.0.0/16")})).Vpc.VpcId
	sn := *must(c.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: &vpc, CidrBlock: aws.String("10.50.1.0/24")})).Subnet.SubnetId
	return hub, c, vpc, sn
}

func tagSpec(rt types.ResourceType) []types.TagSpecification {
	return []types.TagSpecification{{ResourceType: rt, Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}}}
}

// floci #5361, #4266, #1464, #1463: route tables, routes and associations.
func TestRouteTables(t *testing.T) {
	hub, c, vpc, sn := netEnv(t)
	rt := must(c.CreateRouteTable(ctx, &ec2.CreateRouteTableInput{VpcId: &vpc, TagSpecifications: tagSpec(types.ResourceTypeRouteTable)})).RouteTable
	if len(rt.Tags) != 1 || len(rt.Routes) != 1 || *rt.Routes[0].GatewayId != "local" || *rt.Routes[0].DestinationCidrBlock != "10.50.0.0/16" {
		t.Fatalf("new route table = %+v", rt)
	}
	igw := *must(c.CreateInternetGateway(ctx, &ec2.CreateInternetGatewayInput{})).InternetGateway.InternetGatewayId
	_, err := c.CreateRoute(ctx, &ec2.CreateRouteInput{RouteTableId: rt.RouteTableId, DestinationCidrBlock: aws.String("0.0.0.0/0"), GatewayId: &igw})
	wantCode(t, err, "InvalidParameterValue") // not attached to this VPC yet
	must(c.AttachInternetGateway(ctx, &ec2.AttachInternetGatewayInput{InternetGatewayId: &igw, VpcId: &vpc}))
	must(c.CreateRoute(ctx, &ec2.CreateRouteInput{RouteTableId: rt.RouteTableId, DestinationCidrBlock: aws.String("0.0.0.0/0"), GatewayId: &igw}))
	_, err = c.CreateRoute(ctx, &ec2.CreateRouteInput{RouteTableId: rt.RouteTableId, DestinationCidrBlock: aws.String("0.0.0.0/0"), GatewayId: &igw})
	wantCode(t, err, "RouteAlreadyExists")
	_, err = c.CreateRoute(ctx, &ec2.CreateRouteInput{RouteTableId: rt.RouteTableId, DestinationCidrBlock: aws.String("10.9.0.0/16"), GatewayId: aws.String("igw-nope")})
	wantCode(t, err, "InvalidGatewayID.NotFound")

	// the local route stays
	_, err = c.DeleteRoute(ctx, &ec2.DeleteRouteInput{RouteTableId: rt.RouteTableId, DestinationCidrBlock: aws.String("10.50.0.0/16")})
	wantCode(t, err, "InvalidParameterValue")
	_, err = c.DeleteRoute(ctx, &ec2.DeleteRouteInput{RouteTableId: rt.RouteTableId, DestinationCidrBlock: aws.String("10.77.0.0/16")})
	wantCode(t, err, "InvalidRoute.NotFound")

	// dry run on a replace is honoured (4266)
	_, err = c.ReplaceRoute(ctx, &ec2.ReplaceRouteInput{RouteTableId: rt.RouteTableId, DestinationCidrBlock: aws.String("0.0.0.0/0"), GatewayId: aws.String("igw-none"), DryRun: aws.Bool(true)})
	wantCode(t, err, "DryRunOperation")

	// replace the default route with an egress-only gateway for IPv6, and a NAT gateway for IPv4
	eigw := must(c.CreateEgressOnlyInternetGateway(ctx, &ec2.CreateEgressOnlyInternetGatewayInput{VpcId: &vpc})).EgressOnlyInternetGateway
	if !strings.HasPrefix(*eigw.EgressOnlyInternetGatewayId, "eigw-") || *eigw.Attachments[0].VpcId != vpc {
		t.Fatalf("egress-only gateway = %+v", eigw)
	}
	must(c.CreateRoute(ctx, &ec2.CreateRouteInput{RouteTableId: rt.RouteTableId, DestinationIpv6CidrBlock: aws.String("::/0"), EgressOnlyInternetGatewayId: eigw.EgressOnlyInternetGatewayId}))
	alloc := must(c.AllocateAddress(ctx, &ec2.AllocateAddressInput{Domain: types.DomainTypeVpc}))
	nat := must(c.CreateNatGateway(ctx, &ec2.CreateNatGatewayInput{SubnetId: &sn, AllocationId: alloc.AllocationId})).NatGateway
	must(c.ReplaceRoute(ctx, &ec2.ReplaceRouteInput{RouteTableId: rt.RouteTableId, DestinationCidrBlock: aws.String("0.0.0.0/0"), NatGatewayId: nat.NatGatewayId}))
	got := must(c.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{Filters: []types.Filter{filter("route.nat-gateway-id", *nat.NatGatewayId)}})).RouteTables
	if len(got) != 1 || len(got[0].Routes) != 3 || *got[0].Routes[1].NatGatewayId != *nat.NatGatewayId || got[0].Routes[1].GatewayId != nil ||
		*got[0].Routes[2].DestinationIpv6CidrBlock != "::/0" || *got[0].Routes[2].EgressOnlyInternetGatewayId != *eigw.EgressOnlyInternetGatewayId {
		t.Fatalf("routes = %+v", got)
	}
	_, err = c.ReplaceRoute(ctx, &ec2.ReplaceRouteInput{RouteTableId: rt.RouteTableId, DestinationCidrBlock: aws.String("10.88.0.0/16"), NatGatewayId: nat.NatGatewayId})
	wantCode(t, err, "InvalidParameterValue")

	// ten subnets associate at once without losing one (1464)
	done := make(chan error, 10)
	for i := 0; i < 10; i++ {
		s := must(c.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: &vpc, CidrBlock: aws.String(fmt.Sprintf("10.50.%d.0/24", 20+i))}))
		go func() {
			_, err := c.AssociateRouteTable(ctx, &ec2.AssociateRouteTableInput{RouteTableId: rt.RouteTableId, SubnetId: s.Subnet.SubnetId})
			done <- err
		}()
	}
	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if d := must(c.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{RouteTableIds: []string{*rt.RouteTableId}})).RouteTables[0]; len(d.Associations) != 10 {
		t.Fatalf("%d associations", len(d.Associations))
	}
	one := must(c.AssociateRouteTable(ctx, &ec2.AssociateRouteTableInput{RouteTableId: rt.RouteTableId, SubnetId: &sn}))
	_, err = c.AssociateRouteTable(ctx, &ec2.AssociateRouteTableInput{RouteTableId: rt.RouteTableId, SubnetId: &sn})
	wantCode(t, err, "Resource.AlreadyAssociated")
	_, err = c.DeleteRouteTable(ctx, &ec2.DeleteRouteTableInput{RouteTableId: rt.RouteTableId})
	wantCode(t, err, "DependencyViolation")

	// the main route table moves to the new table and back (what aws_main_route_table_association does)
	main := must(c.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{Filters: []types.Filter{filter("vpc-id", vpc), filter("association.main", "true")}})).RouteTables[0]
	_, err = c.DisassociateRouteTable(ctx, &ec2.DisassociateRouteTableInput{AssociationId: main.Associations[0].RouteTableAssociationId})
	wantCode(t, err, "InvalidParameterValue")
	moved := must(c.ReplaceRouteTableAssociation(ctx, &ec2.ReplaceRouteTableAssociationInput{AssociationId: main.Associations[0].RouteTableAssociationId, RouteTableId: rt.RouteTableId}))
	now := must(c.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{Filters: []types.Filter{filter("vpc-id", vpc), filter("association.main", "true")}})).RouteTables
	if len(now) != 1 || *now[0].RouteTableId != *rt.RouteTableId {
		t.Fatalf("main table = %+v", now)
	}
	must(c.ReplaceRouteTableAssociation(ctx, &ec2.ReplaceRouteTableAssociationInput{AssociationId: moved.NewAssociationId, RouteTableId: main.RouteTableId}))
	must(c.DisassociateRouteTable(ctx, &ec2.DisassociateRouteTableInput{AssociationId: one.AssociationId}))
	_, err = c.DisassociateRouteTable(ctx, &ec2.DisassociateRouteTableInput{AssociationId: one.AssociationId})
	wantCode(t, err, "InvalidAssociationID.NotFound")

	roundTrip(t, hub)
	if d := must(c.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{RouteTableIds: []string{*rt.RouteTableId}})).RouteTables[0]; len(d.Associations) != 10 || len(d.Routes) != 3 {
		t.Fatalf("after restore: %+v", d)
	}

	// a deleted NAT gateway leaves a blackhole route, and releases its address
	must(c.DeleteNatGateway(ctx, &ec2.DeleteNatGatewayInput{NatGatewayId: nat.NatGatewayId}))
	if d := must(c.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{RouteTableIds: []string{*rt.RouteTableId}})).RouteTables[0]; d.Routes[1].State != types.RouteStateBlackhole {
		t.Fatalf("route after NAT delete = %+v", d.Routes[1])
	}
	must(c.ReleaseAddress(ctx, &ec2.ReleaseAddressInput{AllocationId: alloc.AllocationId}))
	must(c.DeleteRoute(ctx, &ec2.DeleteRouteInput{RouteTableId: rt.RouteTableId, DestinationCidrBlock: aws.String("0.0.0.0/0")}))
	must(c.DeleteRoute(ctx, &ec2.DeleteRouteInput{RouteTableId: rt.RouteTableId, DestinationIpv6CidrBlock: aws.String("::/0")}))
	_, err = c.DeleteInternetGateway(ctx, &ec2.DeleteInternetGatewayInput{InternetGatewayId: &igw})
	wantCode(t, err, "DependencyViolation")
	must(c.DetachInternetGateway(ctx, &ec2.DetachInternetGatewayInput{InternetGatewayId: &igw, VpcId: &vpc}))
	must(c.DeleteInternetGateway(ctx, &ec2.DeleteInternetGatewayInput{InternetGatewayId: &igw}))
	must(c.DeleteEgressOnlyInternetGateway(ctx, &ec2.DeleteEgressOnlyInternetGatewayInput{EgressOnlyInternetGatewayId: eigw.EgressOnlyInternetGatewayId}))
}

// floci #5236, #1090: a NAT gateway has its Elastic IP and a network interface that DescribeNetworkInterfaces lists.
func TestNatGateways(t *testing.T) {
	hub, c, vpc, sn := netEnv(t)
	alloc := must(c.AllocateAddress(ctx, &ec2.AllocateAddressInput{Domain: types.DomainTypeVpc, TagSpecifications: tagSpec(types.ResourceTypeElasticIp)}))
	_, err := c.CreateNatGateway(ctx, &ec2.CreateNatGatewayInput{SubnetId: &sn})
	wantCode(t, err, "MissingParameter")
	created := must(c.CreateNatGateway(ctx, &ec2.CreateNatGatewayInput{SubnetId: &sn, AllocationId: alloc.AllocationId, TagSpecifications: tagSpec(types.ResourceTypeNatgateway)})).NatGateway
	if created.State != types.NatGatewayStatePending {
		t.Fatalf("first state = %s", created.State)
	}
	nat := must(c.DescribeNatGateways(ctx, &ec2.DescribeNatGatewaysInput{NatGatewayIds: []string{*created.NatGatewayId}})).NatGateways[0]
	if nat.State != types.NatGatewayStateAvailable || len(nat.Tags) != 1 || len(nat.NatGatewayAddresses) != 1 {
		t.Fatalf("nat = %+v", nat)
	}
	a := nat.NatGatewayAddresses[0]
	if *a.PublicIp != *alloc.PublicIp || *a.AllocationId != *alloc.AllocationId || !strings.HasPrefix(*a.PrivateIp, "10.50.1.") {
		t.Fatalf("nat address = %+v, eip = %+v", a, alloc)
	}
	eni := must(c.DescribeNetworkInterfaces(ctx, &ec2.DescribeNetworkInterfacesInput{NetworkInterfaceIds: []string{*a.NetworkInterfaceId}})).NetworkInterfaces[0]
	if *eni.PrivateIpAddress != *a.PrivateIp || *eni.SubnetId != sn || *eni.VpcId != vpc || !*eni.RequesterManaged || eni.InterfaceType != "nat_gateway" ||
		eni.Association == nil || *eni.Association.PublicIp != *alloc.PublicIp {
		t.Fatalf("nat interface = %+v", eni)
	}
	if n := len(must(c.DescribeNatGateways(ctx, &ec2.DescribeNatGatewaysInput{Filter: []types.Filter{filter("vpc-id", vpc), filter("state", "available")}})).NatGateways); n != 1 {
		t.Fatalf("%d nat gateways by filter", n)
	}

	// the address shows what holds it, and cannot be released or reused
	ad := must(c.DescribeAddresses(ctx, &ec2.DescribeAddressesInput{AllocationIds: []string{*alloc.AllocationId}})).Addresses[0]
	if ad.AssociationId == nil || *ad.NetworkInterfaceId != *a.NetworkInterfaceId || ad.InstanceId != nil || len(ad.Tags) != 1 || ad.Domain != types.DomainTypeVpc {
		t.Fatalf("address = %+v", ad)
	}
	_, err = c.ReleaseAddress(ctx, &ec2.ReleaseAddressInput{AllocationId: alloc.AllocationId})
	wantCode(t, err, "InvalidIPAddress.InUse")
	_, err = c.CreateNatGateway(ctx, &ec2.CreateNatGatewayInput{SubnetId: &sn, AllocationId: alloc.AllocationId})
	wantCode(t, err, "Resource.AlreadyAssociated")
	byIP := must(c.DescribeAddresses(ctx, &ec2.DescribeAddressesInput{PublicIps: []string{*alloc.PublicIp}})).Addresses
	if len(byIP) != 1 {
		t.Fatalf("by public ip = %+v", byIP)
	}
	attr := must(c.DescribeAddressesAttribute(ctx, &ec2.DescribeAddressesAttributeInput{AllocationIds: []string{*alloc.AllocationId}, Attribute: types.AddressAttributeNameDomainName}))
	if len(attr.Addresses) != 1 || *attr.Addresses[0].PublicIp != *alloc.PublicIp {
		t.Fatalf("address attribute = %+v", attr.Addresses)
	}
	_, err = c.DeleteSubnet(ctx, &ec2.DeleteSubnetInput{SubnetId: &sn})
	wantCode(t, err, "DependencyViolation")
	_, err = c.DeleteVpc(ctx, &ec2.DeleteVpcInput{VpcId: &vpc})
	wantCode(t, err, "DependencyViolation")

	roundTrip(t, hub)
	if n := must(c.DescribeNatGateways(ctx, &ec2.DescribeNatGatewaysInput{NatGatewayIds: []string{*nat.NatGatewayId}})).NatGateways[0]; len(n.NatGatewayAddresses) != 1 {
		t.Fatalf("after restore: %+v", n)
	}

	must(c.DeleteNatGateway(ctx, &ec2.DeleteNatGatewayInput{NatGatewayId: nat.NatGatewayId}))
	if n := must(c.DescribeNatGateways(ctx, &ec2.DescribeNatGatewaysInput{NatGatewayIds: []string{*nat.NatGatewayId}})).NatGateways[0]; n.State != types.NatGatewayStateDeleted {
		t.Fatalf("state after delete = %s", n.State)
	}
	if n := len(must(c.DescribeNetworkInterfaces(ctx, &ec2.DescribeNetworkInterfacesInput{})).NetworkInterfaces); n != 0 {
		t.Fatalf("%d interfaces left", n)
	}
	_, err = c.DescribeNetworkInterfaces(ctx, &ec2.DescribeNetworkInterfacesInput{NetworkInterfaceIds: []string{*a.NetworkInterfaceId}})
	wantCode(t, err, "InvalidNetworkInterfaceID.NotFound")
	must(c.ReleaseAddress(ctx, &ec2.ReleaseAddressInput{AllocationId: alloc.AllocationId}))
	_, err = c.ReleaseAddress(ctx, &ec2.ReleaseAddressInput{AllocationId: alloc.AllocationId})
	wantCode(t, err, "InvalidAllocationID.NotFound")
	must(c.DeleteSubnet(ctx, &ec2.DeleteSubnetInput{SubnetId: &sn}))
}

// Instances list their primary interface, and an Elastic IP can sit on an instance.
func TestInstanceInterfacesAndElasticIPs(t *testing.T) {
	_, c, _, sn := netEnv(t)
	run := must(c.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String(alpine), MinCount: aws.Int32(1), MaxCount: aws.Int32(1), SubnetId: &sn}))
	id := *run.Instances[0].InstanceId
	nics := must(c.DescribeNetworkInterfaces(ctx, &ec2.DescribeNetworkInterfacesInput{Filters: []types.Filter{filter("attachment.instance-id", id)}})).NetworkInterfaces
	want := run.Instances[0].NetworkInterfaces[0]
	if len(nics) != 1 || *nics[0].NetworkInterfaceId != *want.NetworkInterfaceId || *nics[0].Attachment.InstanceId != id || *nics[0].PrivateIpAddress != *run.Instances[0].PrivateIpAddress ||
		*nics[0].SubnetId != sn || len(nics[0].Groups) != 1 {
		t.Fatalf("instance interface = %+v, instance says %+v", nics, want)
	}
	eip := must(c.AllocateAddress(ctx, &ec2.AllocateAddressInput{}))
	assoc := must(c.AssociateAddress(ctx, &ec2.AssociateAddressInput{AllocationId: eip.AllocationId, InstanceId: &id}))
	if i := describe(t, c, id); *i.PublicIpAddress != *eip.PublicIp {
		t.Fatalf("instance public ip = %v", i.PublicIpAddress)
	}
	nic := must(c.DescribeNetworkInterfaces(ctx, &ec2.DescribeNetworkInterfacesInput{Filters: []types.Filter{filter("association.allocation-id", *eip.AllocationId)}})).NetworkInterfaces
	if len(nic) != 1 || *nic[0].Association.AssociationId != *assoc.AssociationId {
		t.Fatalf("interface by allocation = %+v", nic)
	}
	other := must(c.AllocateAddress(ctx, &ec2.AllocateAddressInput{}))
	_, err := c.AssociateAddress(ctx, &ec2.AssociateAddressInput{AllocationId: eip.AllocationId, InstanceId: &id})
	wantCode(t, err, "Resource.AlreadyAssociated")
	_, err = c.AssociateAddress(ctx, &ec2.AssociateAddressInput{AllocationId: other.AllocationId, InstanceId: &id})
	wantCode(t, err, "Resource.AlreadyAssociated")
	must(c.DisassociateAddress(ctx, &ec2.DisassociateAddressInput{AssociationId: assoc.AssociationId}))
	if i := describe(t, c, id); i.PublicIpAddress != nil {
		t.Fatalf("instance public ip after disassociate = %v", *i.PublicIpAddress)
	}
	must(c.AssociateAddress(ctx, &ec2.AssociateAddressInput{AllocationId: eip.AllocationId, NetworkInterfaceId: want.NetworkInterfaceId}))
	must(c.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{id}}))
	if a := must(c.DescribeAddresses(ctx, &ec2.DescribeAddressesInput{AllocationIds: []string{*eip.AllocationId}})).Addresses[0]; a.AssociationId != nil {
		t.Fatalf("address still on a terminated instance: %+v", a)
	}
	if n := len(must(c.DescribeNetworkInterfaces(ctx, &ec2.DescribeNetworkInterfacesInput{})).NetworkInterfaces); n != 0 {
		t.Fatalf("%d interfaces after terminate", n)
	}
}

// floci #5384, #5345, #1462, #2273: endpoints, service prefix lists and managed prefix lists.
func TestEndpointsAndPrefixLists(t *testing.T) {
	hub, c, vpc, _ := netEnv(t)
	small := *must(c.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: &vpc, CidrBlock: aws.String("10.50.9.0/28"), AvailabilityZone: aws.String("us-east-1b")})).Subnet.SubnetId
	rt := *must(c.CreateRouteTable(ctx, &ec2.CreateRouteTableInput{VpcId: &vpc})).RouteTable.RouteTableId

	// gateway endpoint: a route to the S3 prefix list
	gw := must(c.CreateVpcEndpoint(ctx, &ec2.CreateVpcEndpointInput{VpcId: &vpc, ServiceName: aws.String("com.amazonaws.us-east-1.s3"), RouteTableIds: []string{rt},
		TagSpecifications: tagSpec(types.ResourceTypeVpcEndpoint)})).VpcEndpoint
	if gw.VpcEndpointType != types.VpcEndpointTypeGateway || len(gw.RouteTableIds) != 1 || gw.RouteTableIds[0] != rt || len(gw.Tags) != 1 || gw.PolicyDocument == nil {
		t.Fatalf("gateway endpoint = %+v", gw)
	}
	pl := must(c.DescribePrefixLists(ctx, &ec2.DescribePrefixListsInput{Filters: []types.Filter{filter("prefix-list-name", "com.amazonaws.us-east-1.s3")}})).PrefixLists
	if len(pl) != 1 || len(pl[0].Cidrs) == 0 || !strings.HasPrefix(*pl[0].PrefixListId, "pl-") {
		t.Fatalf("prefix lists = %+v", pl)
	}
	tbl := must(c.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{RouteTableIds: []string{rt}})).RouteTables[0]
	if len(tbl.Routes) != 2 || *tbl.Routes[1].DestinationPrefixListId != *pl[0].PrefixListId || *tbl.Routes[1].GatewayId != *gw.VpcEndpointId {
		t.Fatalf("routes = %+v", tbl.Routes)
	}
	_, err := c.CreateVpcEndpoint(ctx, &ec2.CreateVpcEndpointInput{VpcId: &vpc, ServiceName: aws.String("com.amazonaws.us-east-1.s3"), RouteTableIds: []string{rt}})
	wantCode(t, err, "RouteAlreadyExists")
	_, err = c.CreateVpcEndpoint(ctx, &ec2.CreateVpcEndpointInput{VpcId: &vpc, ServiceName: aws.String("com.amazonaws.us-east-1.sqs")})
	wantCode(t, err, "InvalidParameter") // sqs has no gateway endpoint
	if n := len(must(c.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{RouteTableIds: []string{rt}})).RouteTables[0].Routes); n != 2 {
		t.Fatalf("%d routes", n)
	}

	// interface endpoint in a /28: its address is inside the subnet (5384)
	sg := *must(c.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{GroupName: aws.String("ep"), Description: aws.String("ep"), VpcId: &vpc})).GroupId
	ifc := must(c.CreateVpcEndpoint(ctx, &ec2.CreateVpcEndpointInput{VpcId: &vpc, ServiceName: aws.String("com.amazonaws.us-east-1.sqs"), VpcEndpointType: types.VpcEndpointTypeInterface,
		SubnetIds: []string{small}, SecurityGroupIds: []string{sg}})).VpcEndpoint
	if len(ifc.NetworkInterfaceIds) != 1 || len(ifc.SubnetIds) != 1 || !*ifc.PrivateDnsEnabled || len(ifc.DnsEntries) != 3 || *ifc.Groups[0].GroupId != sg {
		t.Fatalf("interface endpoint = %+v", ifc)
	}
	enis := must(c.DescribeNetworkInterfaces(ctx, &ec2.DescribeNetworkInterfacesInput{Filters: []types.Filter{filter("subnet-id", small)}})).NetworkInterfaces
	if len(enis) != 1 || !strings.HasPrefix(*enis[0].PrivateIpAddress, "10.50.9.") || *enis[0].NetworkInterfaceId != ifc.NetworkInterfaceIds[0] ||
		!strings.Contains(*enis[0].Description, *ifc.VpcEndpointId) || *enis[0].Groups[0].GroupId != sg {
		t.Fatalf("endpoint interfaces = %+v", enis)
	}
	if n := *must(c.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{SubnetIds: []string{small}})).Subnets[0].AvailableIpAddressCount; n != 10 {
		t.Fatalf("free addresses = %d", n)
	}
	_, err = c.CreateVpcEndpoint(ctx, &ec2.CreateVpcEndpointInput{VpcId: &vpc, ServiceName: aws.String("com.amazonaws.us-east-1.ssm"), VpcEndpointType: types.VpcEndpointTypeInterface,
		SubnetIds: []string{small, small}})
	wantCode(t, err, "DuplicateSubnetsInSameZone")
	if n := len(must(c.DescribeNetworkInterfaces(ctx, &ec2.DescribeNetworkInterfacesInput{})).NetworkInterfaces); n != 1 {
		t.Fatalf("a failed create left %d interfaces", n)
	}

	// update: move the gateway endpoint's route, add a subnet to the interface endpoint
	rt2 := *must(c.CreateRouteTable(ctx, &ec2.CreateRouteTableInput{VpcId: &vpc})).RouteTable.RouteTableId
	must(c.ModifyVpcEndpoint(ctx, &ec2.ModifyVpcEndpointInput{VpcEndpointId: gw.VpcEndpointId, AddRouteTableIds: []string{rt2}, RemoveRouteTableIds: []string{rt}}))
	if n := len(must(c.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{RouteTableIds: []string{rt}})).RouteTables[0].Routes); n != 1 {
		t.Fatalf("old table has %d routes", n)
	}
	other := *must(c.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: &vpc, CidrBlock: aws.String("10.50.10.0/24"), AvailabilityZone: aws.String("us-east-1c")})).Subnet.SubnetId
	must(c.ModifyVpcEndpoint(ctx, &ec2.ModifyVpcEndpointInput{VpcEndpointId: ifc.VpcEndpointId, AddSubnetIds: []string{other}}))
	got := must(c.DescribeVpcEndpoints(ctx, &ec2.DescribeVpcEndpointsInput{Filters: []types.Filter{filter("vpc-endpoint-type", "Interface")}})).VpcEndpoints
	if len(got) != 1 || len(got[0].NetworkInterfaceIds) != 2 || len(got[0].DnsEntries) != 4 || got[0].State != "available" {
		t.Fatalf("interface endpoint after modify = %+v", got)
	}
	_, err = c.DeleteVpc(ctx, &ec2.DeleteVpcInput{VpcId: &vpc})
	wantCode(t, err, "DependencyViolation")
	roundTrip(t, hub)

	del := must(c.DeleteVpcEndpoints(ctx, &ec2.DeleteVpcEndpointsInput{VpcEndpointIds: []string{*gw.VpcEndpointId, *ifc.VpcEndpointId, "vpce-nope"}}))
	if len(del.Unsuccessful) != 1 || *del.Unsuccessful[0].ResourceId != "vpce-nope" || *del.Unsuccessful[0].Error.Code != "InvalidVpcEndpointId.NotFound" {
		t.Fatalf("unsuccessful = %+v", del.Unsuccessful)
	}
	if n := len(must(c.DescribeNetworkInterfaces(ctx, &ec2.DescribeNetworkInterfacesInput{})).NetworkInterfaces); n != 0 {
		t.Fatalf("%d interfaces after delete", n)
	}
	if n := len(must(c.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{RouteTableIds: []string{rt2}})).RouteTables[0].Routes); n != 1 {
		t.Fatalf("%d routes after delete", n)
	}
	_, err = c.DescribeVpcEndpoints(ctx, &ec2.DescribeVpcEndpointsInput{VpcEndpointIds: []string{*gw.VpcEndpointId}})
	wantCode(t, err, "InvalidVpcEndpointId.NotFound")

	// managed prefix lists: AWS's names are reserved (2273)
	_, err = c.CreateManagedPrefixList(ctx, &ec2.CreateManagedPrefixListInput{PrefixListName: aws.String("com.amazonaws.probe"), AddressFamily: aws.String("IPv4"), MaxEntries: aws.Int32(5)})
	wantCode(t, err, "InvalidParameterValue")
	mp := must(c.CreateManagedPrefixList(ctx, &ec2.CreateManagedPrefixListInput{PrefixListName: aws.String("mine"), AddressFamily: aws.String("IPv4"), MaxEntries: aws.Int32(2),
		Entries: []types.AddPrefixListEntry{{Cidr: aws.String("10.1.0.0/16"), Description: aws.String("a")}}, TagSpecifications: tagSpec(types.ResourceTypePrefixList)})).PrefixList
	if *mp.Version != 1 || mp.State != types.PrefixListStateCreateComplete || len(mp.Tags) != 1 {
		t.Fatalf("prefix list = %+v", mp)
	}
	_, err = c.ModifyManagedPrefixList(ctx, &ec2.ModifyManagedPrefixListInput{PrefixListId: mp.PrefixListId, CurrentVersion: aws.Int64(1),
		AddEntries: []types.AddPrefixListEntry{{Cidr: aws.String("10.2.0.0/16")}, {Cidr: aws.String("10.3.0.0/16")}}})
	wantCode(t, err, "PrefixListMaxEntriesExceeded")
	_, err = c.ModifyManagedPrefixList(ctx, &ec2.ModifyManagedPrefixListInput{PrefixListId: mp.PrefixListId, CurrentVersion: aws.Int64(7)})
	wantCode(t, err, "IncorrectState")
	up := must(c.ModifyManagedPrefixList(ctx, &ec2.ModifyManagedPrefixListInput{PrefixListId: mp.PrefixListId, CurrentVersion: aws.Int64(1),
		AddEntries: []types.AddPrefixListEntry{{Cidr: aws.String("10.2.0.0/16")}}, RemoveEntries: []types.RemovePrefixListEntry{{Cidr: aws.String("10.1.0.0/16")}}})).PrefixList
	if *up.Version != 2 || up.State != types.PrefixListStateModifyComplete {
		t.Fatalf("modified = %+v", up)
	}
	ents := must(c.GetManagedPrefixListEntries(ctx, &ec2.GetManagedPrefixListEntriesInput{PrefixListId: mp.PrefixListId})).Entries
	if len(ents) != 1 || *ents[0].Cidr != "10.2.0.0/16" {
		t.Fatalf("entries = %+v", ents)
	}
	if all := must(c.DescribeManagedPrefixLists(ctx, &ec2.DescribeManagedPrefixListsInput{})).PrefixLists; len(all) != 3 { // s3, dynamodb, mine
		t.Fatalf("%d managed prefix lists", len(all))
	}
	must(c.CreateRoute(ctx, &ec2.CreateRouteInput{RouteTableId: &rt, DestinationPrefixListId: mp.PrefixListId, VpcPeeringConnectionId: aws.String("pcx-0123456789abcdef0")}))
	must(c.DeleteManagedPrefixList(ctx, &ec2.DeleteManagedPrefixListInput{PrefixListId: mp.PrefixListId}))
	_, err = c.GetManagedPrefixListEntries(ctx, &ec2.GetManagedPrefixListEntriesInput{PrefixListId: mp.PrefixListId})
	wantCode(t, err, "InvalidPrefixListID.NotFound")
}

// floci #1461: network ACLs, their entries and subnet associations.
func TestNetworkAcls(t *testing.T) {
	hub, c, vpc, sn := netEnv(t)
	def := must(c.DescribeNetworkAcls(ctx, &ec2.DescribeNetworkAclsInput{Filters: []types.Filter{filter("vpc-id", vpc), filter("default", "true")}})).NetworkAcls
	if len(def) != 1 || len(def[0].Associations) != 1 || *def[0].Associations[0].SubnetId != sn || len(def[0].Entries) != 4 {
		t.Fatalf("default acl = %+v", def)
	}
	acl := must(c.CreateNetworkAcl(ctx, &ec2.CreateNetworkAclInput{VpcId: &vpc, TagSpecifications: tagSpec(types.ResourceTypeNetworkAcl)})).NetworkAcl
	if *acl.IsDefault || len(acl.Entries) != 2 || len(acl.Tags) != 1 {
		t.Fatalf("new acl = %+v", acl)
	}
	entry := &ec2.CreateNetworkAclEntryInput{NetworkAclId: acl.NetworkAclId, RuleNumber: aws.Int32(100), Protocol: aws.String("tcp"), RuleAction: types.RuleActionAllow,
		Egress: aws.Bool(false), CidrBlock: aws.String("10.0.0.0/8"), PortRange: &types.PortRange{From: aws.Int32(80), To: aws.Int32(443)}}
	must(c.CreateNetworkAclEntry(ctx, entry))
	_, err := c.CreateNetworkAclEntry(ctx, entry)
	wantCode(t, err, "NetworkAclEntryAlreadyExists")
	entry.RuleNumber = aws.Int32(40000)
	_, err = c.CreateNetworkAclEntry(ctx, entry)
	wantCode(t, err, "InvalidParameterValue")
	must(c.CreateNetworkAclEntry(ctx, &ec2.CreateNetworkAclEntryInput{NetworkAclId: acl.NetworkAclId, RuleNumber: aws.Int32(100), Protocol: aws.String("-1"), RuleAction: types.RuleActionDeny,
		Egress: aws.Bool(true), Ipv6CidrBlock: aws.String("::/0")}))
	must(c.ReplaceNetworkAclEntry(ctx, &ec2.ReplaceNetworkAclEntryInput{NetworkAclId: acl.NetworkAclId, RuleNumber: aws.Int32(100), Protocol: aws.String("udp"), RuleAction: types.RuleActionDeny,
		Egress: aws.Bool(false), CidrBlock: aws.String("10.0.0.0/8"), PortRange: &types.PortRange{From: aws.Int32(53), To: aws.Int32(53)}}))
	_, err = c.ReplaceNetworkAclEntry(ctx, &ec2.ReplaceNetworkAclEntryInput{NetworkAclId: acl.NetworkAclId, RuleNumber: aws.Int32(101), Protocol: aws.String("udp"), RuleAction: types.RuleActionDeny,
		Egress: aws.Bool(false), CidrBlock: aws.String("10.0.0.0/8")})
	wantCode(t, err, "InvalidNetworkAclEntry.NotFound")
	got := must(c.DescribeNetworkAcls(ctx, &ec2.DescribeNetworkAclsInput{NetworkAclIds: []string{*acl.NetworkAclId}})).NetworkAcls[0]
	if len(got.Entries) != 4 || *got.Entries[0].RuleNumber != 100 || *got.Entries[0].Protocol != "17" || got.Entries[0].RuleAction != types.RuleActionDeny || *got.Entries[0].PortRange.From != 53 ||
		*got.Entries[1].RuleNumber != 32767 || *got.Entries[1].Egress || !*got.Entries[2].Egress || *got.Entries[2].Ipv6CidrBlock != "::/0" {
		t.Fatalf("entries = %+v", got.Entries)
	}
	must(c.DeleteNetworkAclEntry(ctx, &ec2.DeleteNetworkAclEntryInput{NetworkAclId: acl.NetworkAclId, RuleNumber: aws.Int32(100), Egress: aws.Bool(true)}))
	_, err = c.DeleteNetworkAclEntry(ctx, &ec2.DeleteNetworkAclEntryInput{NetworkAclId: acl.NetworkAclId, RuleNumber: aws.Int32(100), Egress: aws.Bool(true)})
	wantCode(t, err, "InvalidNetworkAclEntry.NotFound")
	_, err = c.DeleteNetworkAclEntry(ctx, &ec2.DeleteNetworkAclEntryInput{NetworkAclId: acl.NetworkAclId, RuleNumber: aws.Int32(32767), Egress: aws.Bool(false)})
	wantCode(t, err, "InvalidParameterValue")

	// move the subnet to the new ACL (what aws_network_acl does), then back
	cur := must(c.DescribeNetworkAcls(ctx, &ec2.DescribeNetworkAclsInput{Filters: []types.Filter{filter("association.subnet-id", sn)}})).NetworkAcls
	if len(cur) != 1 || *cur[0].NetworkAclId != *def[0].NetworkAclId {
		t.Fatalf("acl of subnet = %+v", cur)
	}
	moved := must(c.ReplaceNetworkAclAssociation(ctx, &ec2.ReplaceNetworkAclAssociationInput{AssociationId: cur[0].Associations[0].NetworkAclAssociationId, NetworkAclId: acl.NetworkAclId}))
	roundTrip(t, hub)
	cur = must(c.DescribeNetworkAcls(ctx, &ec2.DescribeNetworkAclsInput{Filters: []types.Filter{filter("association.subnet-id", sn)}})).NetworkAcls
	if len(cur) != 1 || *cur[0].NetworkAclId != *acl.NetworkAclId || *cur[0].Associations[0].NetworkAclAssociationId != *moved.NewAssociationId {
		t.Fatalf("acl of subnet after move = %+v", cur)
	}
	_, err = c.DeleteNetworkAcl(ctx, &ec2.DeleteNetworkAclInput{NetworkAclId: acl.NetworkAclId})
	wantCode(t, err, "DependencyViolation")
	_, err = c.DeleteNetworkAcl(ctx, &ec2.DeleteNetworkAclInput{NetworkAclId: def[0].NetworkAclId})
	wantCode(t, err, "InvalidParameterValue")
	must(c.ReplaceNetworkAclAssociation(ctx, &ec2.ReplaceNetworkAclAssociationInput{AssociationId: moved.NewAssociationId, NetworkAclId: def[0].NetworkAclId}))
	must(c.DeleteNetworkAcl(ctx, &ec2.DeleteNetworkAclInput{NetworkAclId: acl.NetworkAclId}))

	// a new subnet joins the default ACL; deleting it leaves no association behind
	sn2 := *must(c.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: &vpc, CidrBlock: aws.String("10.50.2.0/24")})).Subnet.SubnetId
	if d := must(c.DescribeNetworkAcls(ctx, &ec2.DescribeNetworkAclsInput{NetworkAclIds: []string{*def[0].NetworkAclId}})).NetworkAcls[0]; len(d.Associations) != 2 {
		t.Fatalf("default acl associations = %+v", d.Associations)
	}
	must(c.DeleteSubnet(ctx, &ec2.DeleteSubnetInput{SubnetId: &sn2}))
	if d := must(c.DescribeNetworkAcls(ctx, &ec2.DescribeNetworkAclsInput{NetworkAclIds: []string{*def[0].NetworkAclId}})).NetworkAcls[0]; len(d.Associations) != 1 {
		t.Fatalf("default acl associations after delete = %+v", d.Associations)
	}
}

// An added VPC CIDR block gets a local route in every route table of the VPC, and loses it again.
func TestLocalRouteForAddedCidr(t *testing.T) {
	_, c, vpc, _ := netEnv(t)
	rt := *must(c.CreateRouteTable(ctx, &ec2.CreateRouteTableInput{VpcId: &vpc})).RouteTable.RouteTableId
	routes := func() int {
		return len(must(c.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{RouteTableIds: []string{rt}})).RouteTables[0].Routes)
	}
	o := must(c.AssociateVpcCidrBlock(ctx, &ec2.AssociateVpcCidrBlockInput{VpcId: &vpc, CidrBlock: aws.String("10.251.0.0/20")}))
	if n := routes(); n != 2 {
		t.Fatalf("%d routes after associate", n)
	}
	must(c.DisassociateVpcCidrBlock(ctx, &ec2.DisassociateVpcCidrBlockInput{AssociationId: o.CidrBlockAssociation.AssociationId}))
	if n := routes(); n != 1 {
		t.Fatalf("%d routes after disassociate", n)
	}
}
