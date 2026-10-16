package ec2_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	ec2svc "github.com/nrndev/pogi/internal/aws/ec2"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

// roundTrip snapshots, resets and restores the hub: state must survive Save and Load.
func roundTrip(t *testing.T, h *core.Hub) {
	t.Helper()
	b, err := h.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	h.Reset()
	if err := h.Restore(b); err != nil {
		t.Fatal(err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func filter(name string, values ...string) types.Filter {
	return types.Filter{Name: aws.String(name), Values: values}
}

func wantCode(t *testing.T, err error, code string) {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func TestDefaultVPCAndNetworking(t *testing.T) {
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { return ec2svc.New(h) })
	c := ec2.NewFromConfig(awstest.Config(t, url))

	if r := must(c.DescribeRegions(ctx, &ec2.DescribeRegionsInput{RegionNames: []string{"eu-west-1"}})); len(r.Regions) != 1 || *r.Regions[0].Endpoint != "ec2.eu-west-1.amazonaws.com" {
		t.Fatalf("regions = %+v", r.Regions)
	}
	azs := must(c.DescribeAvailabilityZones(ctx, &ec2.DescribeAvailabilityZonesInput{})).AvailabilityZones
	if len(azs) != 3 || *azs[0].ZoneName != "us-east-1a" || *azs[0].ZoneId != "use1-az1" || azs[0].State != types.AvailabilityZoneStateAvailable {
		t.Fatalf("zones = %+v", azs)
	}

	// The default VPC: what Terraform's data sources and CDK's Vpc.fromLookup read.
	vpcs := must(c.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{Filters: []types.Filter{filter("isDefault", "true")}})).Vpcs
	if len(vpcs) != 1 || *vpcs[0].CidrBlock != "172.31.0.0/16" || !*vpcs[0].IsDefault || vpcs[0].State != types.VpcStateAvailable {
		t.Fatalf("default VPC = %+v", vpcs)
	}
	def := vpcs[0].VpcId
	attrs := must(c.DescribeAccountAttributes(ctx, &ec2.DescribeAccountAttributesInput{AttributeNames: []types.AccountAttributeName{types.AccountAttributeNameDefaultVpc}}))
	if *attrs.AccountAttributes[0].AttributeValues[0].AttributeValue != *def {
		t.Fatalf("account attributes = %+v", attrs.AccountAttributes)
	}
	subnets := must(c.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{Filters: []types.Filter{filter("vpc-id", *def), filter("default-for-az", "true")}})).Subnets
	if len(subnets) != 3 || *subnets[0].AvailableIpAddressCount != 4091 || !*subnets[0].MapPublicIpOnLaunch {
		t.Fatalf("default subnets = %+v", subnets)
	}
	sgs := must(c.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{Filters: []types.Filter{filter("vpc-id", *def), filter("group-name", "default")}})).SecurityGroups
	if len(sgs) != 1 || *sgs[0].IpPermissions[0].UserIdGroupPairs[0].GroupId != *sgs[0].GroupId || *sgs[0].IpPermissionsEgress[0].IpRanges[0].CidrIp != "0.0.0.0/0" {
		t.Fatalf("default security group = %+v", sgs)
	}
	igws := must(c.DescribeInternetGateways(ctx, &ec2.DescribeInternetGatewaysInput{Filters: []types.Filter{filter("attachment.vpc-id", *def)}})).InternetGateways
	tables := must(c.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{Filters: []types.Filter{filter("vpc-id", *def), filter("association.main", "true")}})).RouteTables
	acls := must(c.DescribeNetworkAcls(ctx, &ec2.DescribeNetworkAclsInput{Filters: []types.Filter{filter("vpc-id", *def), filter("default", "true")}})).NetworkAcls
	if len(igws) != 1 || len(tables) != 1 || len(tables[0].Routes) != 2 || *tables[0].Routes[1].GatewayId != *igws[0].InternetGatewayId || len(acls) != 1 {
		t.Fatalf("igws = %+v, route tables = %+v, acls = %+v", igws, tables, acls)
	}

	vpc := must(c.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16"), TagSpecifications: []types.TagSpecification{
		{ResourceType: types.ResourceTypeVpc, Tags: []types.Tag{{Key: aws.String("Name"), Value: aws.String("main")}}}}})).Vpc
	if *vpc.Tags[0].Value != "main" || *vpc.IsDefault {
		t.Fatalf("vpc = %+v", vpc)
	}
	hostnames := func() bool {
		return *must(c.DescribeVpcAttribute(ctx, &ec2.DescribeVpcAttributeInput{VpcId: vpc.VpcId, Attribute: types.VpcAttributeNameEnableDnsHostnames})).EnableDnsHostnames.Value
	}
	if hostnames() {
		t.Fatal("new VPCs have DNS hostnames off")
	}
	must(c.ModifyVpcAttribute(ctx, &ec2.ModifyVpcAttributeInput{VpcId: vpc.VpcId, EnableDnsHostnames: &types.AttributeBooleanValue{Value: aws.Bool(true)}}))
	if !hostnames() {
		t.Fatal("ModifyVpcAttribute did not stick")
	}
	sub := must(c.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: vpc.VpcId, CidrBlock: aws.String("10.0.1.0/24"), AvailabilityZone: aws.String("us-east-1b")})).Subnet
	if *sub.AvailableIpAddressCount != 251 || *sub.AvailabilityZoneId != "use1-az2" {
		t.Fatalf("subnet = %+v", sub)
	}
	_, err := c.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: vpc.VpcId, CidrBlock: aws.String("10.1.0.0/24")})
	wantCode(t, err, "InvalidSubnet.Range")

	sg := must(c.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{GroupName: aws.String("web"), Description: aws.String("web tier"), VpcId: vpc.VpcId}))
	https := types.IpPermission{IpProtocol: aws.String("tcp"), FromPort: aws.Int32(443), ToPort: aws.Int32(443),
		IpRanges: []types.IpRange{{CidrIp: aws.String("0.0.0.0/0")}, {CidrIp: aws.String("10.0.0.0/8")}}}
	must(c.AuthorizeSecurityGroupIngress(ctx, &ec2.AuthorizeSecurityGroupIngressInput{GroupId: sg.GroupId, IpPermissions: []types.IpPermission{https}}))
	_, err = c.AuthorizeSecurityGroupIngress(ctx, &ec2.AuthorizeSecurityGroupIngressInput{GroupId: sg.GroupId, IpPermissions: []types.IpPermission{https}})
	wantCode(t, err, "InvalidPermission.Duplicate")
	must(c.RevokeSecurityGroupEgress(ctx, &ec2.RevokeSecurityGroupEgressInput{GroupId: sg.GroupId,
		IpPermissions: []types.IpPermission{{IpProtocol: aws.String("-1"), IpRanges: []types.IpRange{{CidrIp: aws.String("0.0.0.0/0")}}}}}))
	roundTrip(t, hub)
	web := must(c.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: []string{*sg.GroupId}})).SecurityGroups[0]
	if len(web.IpPermissions) != 1 || len(web.IpPermissions[0].IpRanges) != 2 || *web.IpPermissions[0].FromPort != 443 || len(web.IpPermissionsEgress) != 0 {
		t.Fatalf("web group = %+v", web)
	}

	must(c.CreateTags(ctx, &ec2.CreateTagsInput{Resources: []string{*sub.SubnetId}, Tags: []types.Tag{{Key: aws.String("tier"), Value: aws.String("app")}}}))
	tags := must(c.DescribeTags(ctx, &ec2.DescribeTagsInput{Filters: []types.Filter{filter("resource-type", "subnet")}})).Tags
	if len(tags) != 1 || *tags[0].ResourceId != *sub.SubnetId || *tags[0].Value != "app" {
		t.Fatalf("tags = %+v", tags)
	}
	must(c.DeleteTags(ctx, &ec2.DeleteTagsInput{Resources: []string{*sub.SubnetId}}))
	if l := must(c.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{Filters: []types.Filter{filter("tag:tier", "app")}})).Subnets; len(l) != 0 {
		t.Fatalf("subnets still tagged: %+v", l)
	}

	_, err = c.DeleteVpc(ctx, &ec2.DeleteVpcInput{VpcId: vpc.VpcId})
	wantCode(t, err, "DependencyViolation")
	must(c.DeleteSubnet(ctx, &ec2.DeleteSubnetInput{SubnetId: sub.SubnetId}))
	must(c.DeleteSecurityGroup(ctx, &ec2.DeleteSecurityGroupInput{GroupId: sg.GroupId}))
	must(c.DeleteVpc(ctx, &ec2.DeleteVpcInput{VpcId: vpc.VpcId}))
	_, err = c.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{VpcIds: []string{*vpc.VpcId}})
	wantCode(t, err, "InvalidVpcID.NotFound")

	imgs := must(c.DescribeImages(ctx, &ec2.DescribeImagesInput{Owners: []string{"amazon"}, Filters: []types.Filter{filter("name", "al2023-ami-*-x86_64")}})).Images
	if len(imgs) != 1 || !strings.HasPrefix(*imgs[0].ImageId, "ami-") || imgs[0].State != types.ImageStateAvailable {
		t.Fatalf("images = %+v", imgs)
	}
	if r := must(c.DescribeInstances(ctx, &ec2.DescribeInstancesInput{})); len(r.Reservations) != 0 {
		t.Fatalf("instances = %+v", r.Reservations)
	}
}

// floci #5229: DescribeImages honors the block-device-mapping filters instead of ignoring them.
func TestDescribeImagesBlockDeviceFilter(t *testing.T) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return ec2svc.New(h) })
	c := ec2.NewFromConfig(awstest.Config(t, url))
	n := func(typ string) int {
		return len(must(c.DescribeImages(ctx, &ec2.DescribeImagesInput{Filters: []types.Filter{filter("block-device-mapping.volume-type", typ)}})).Images)
	}
	if n("no-such-type") != 0 || n("gp3") == 0 {
		t.Errorf("volume-type filter: no-such-type matched %d, gp3 matched %d", n("no-such-type"), n("gp3"))
	}
}

func TestKeyPairs(t *testing.T) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return ec2svc.New(h) })
	c := ec2.NewFromConfig(awstest.Config(t, url))

	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	var wire []byte
	for _, part := range [][]byte{[]byte("ssh-ed25519"), pub} {
		wire = append(binary.BigEndian.AppendUint32(wire, uint32(len(part))), part...)
	}
	imported := must(c.ImportKeyPair(ctx, &ec2.ImportKeyPairInput{KeyName: aws.String("laptop"),
		PublicKeyMaterial: []byte("ssh-ed25519 " + base64.StdEncoding.EncodeToString(wire) + " me@laptop")}))
	if !strings.HasPrefix(*imported.KeyPairId, "key-") || len(*imported.KeyFingerprint) != 44 {
		t.Fatalf("imported = %+v", imported)
	}
	created := must(c.CreateKeyPair(ctx, &ec2.CreateKeyPairInput{KeyName: aws.String("deploy")}))
	if !strings.HasPrefix(*created.KeyMaterial, "-----BEGIN RSA PRIVATE KEY-----") || strings.Count(*created.KeyFingerprint, ":") != 19 {
		t.Fatalf("created = %+v", created)
	}
	_, err := c.CreateKeyPair(ctx, &ec2.CreateKeyPairInput{KeyName: aws.String("deploy")})
	wantCode(t, err, "InvalidKeyPair.Duplicate")

	keys := must(c.DescribeKeyPairs(ctx, &ec2.DescribeKeyPairsInput{IncludePublicKey: aws.Bool(true)})).KeyPairs
	if len(keys) != 2 || *keys[1].KeyName != "laptop" || keys[1].KeyType != types.KeyTypeEd25519 || !strings.HasPrefix(*keys[1].PublicKey, "ssh-ed25519 ") {
		t.Fatalf("key pairs = %+v", keys)
	}
	if byID := must(c.DescribeKeyPairs(ctx, &ec2.DescribeKeyPairsInput{KeyPairIds: []string{*created.KeyPairId}})).KeyPairs; len(byID) != 1 || byID[0].PublicKey != nil {
		t.Fatalf("by id = %+v", byID)
	}
	must(c.DeleteKeyPair(ctx, &ec2.DeleteKeyPairInput{KeyName: aws.String("deploy")}))
	_, err = c.DescribeKeyPairs(ctx, &ec2.DescribeKeyPairsInput{KeyNames: []string{"deploy"}})
	wantCode(t, err, "InvalidKeyPair.NotFound")
}

// floci #5440 and #4365: an unknown zone, zone id or filter name is refused, and known filters narrow the answer.
func TestUnknownZonesAndFilters(t *testing.T) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return ec2svc.New(h) })
	c := ec2.NewFromConfig(awstest.Config(t, url))
	az := must(c.DescribeAvailabilityZones(ctx, &ec2.DescribeAvailabilityZonesInput{})).AvailabilityZones[0]
	if r := must(c.DescribeAvailabilityZones(ctx, &ec2.DescribeAvailabilityZonesInput{ZoneNames: []string{*az.ZoneName}})); len(r.AvailabilityZones) != 1 {
		t.Fatalf("by name: %+v", r.AvailabilityZones)
	}
	if r := must(c.DescribeAvailabilityZones(ctx, &ec2.DescribeAvailabilityZonesInput{Filters: []types.Filter{filter("zone-id", *az.ZoneId)}})); len(r.AvailabilityZones) != 1 {
		t.Fatalf("by filter: %+v", r.AvailabilityZones)
	}
	_, err := c.DescribeAvailabilityZones(ctx, &ec2.DescribeAvailabilityZonesInput{ZoneNames: []string{"nope-1z"}})
	wantCode(t, err, "InvalidParameterValue")
	_, err = c.DescribeAvailabilityZones(ctx, &ec2.DescribeAvailabilityZonesInput{ZoneIds: []string{"nope-az1"}})
	wantCode(t, err, "InvalidParameterValue")
	_, err = c.DescribeAvailabilityZones(ctx, &ec2.DescribeAvailabilityZonesInput{Filters: []types.Filter{filter("bogus", "x")}})
	wantCode(t, err, "InvalidParameterValue")

	v := must(c.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.2.0.0/16")})).Vpc
	_, err = c.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{Filters: []types.Filter{filter("not-a-filter", "anything")}})
	wantCode(t, err, "InvalidParameterValue")
	for _, name := range []string{"cidr", "cidr-block"} {
		if r := must(c.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{Filters: []types.Filter{filter(name, "10.2.0.0/16")}})); len(r.Vpcs) != 1 || *r.Vpcs[0].VpcId != *v.VpcId {
			t.Fatalf("filter %s: %+v", name, r.Vpcs)
		}
	}
	if r := must(c.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{Filters: []types.Filter{filter("tag:Name", "none")}})); len(r.Vpcs) != 0 {
		t.Fatalf("tag filter: %+v", r.Vpcs)
	}
}

// floci #3178: instance types describe their network cards and addresses per interface.
func TestInstanceTypeNetworkInfo(t *testing.T) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return ec2svc.New(h) })
	c := ec2.NewFromConfig(awstest.Config(t, url))
	r := must(c.DescribeInstanceTypes(ctx, &ec2.DescribeInstanceTypesInput{InstanceTypes: []types.InstanceType{"m5.large", "t4g.medium"}}))
	if len(r.InstanceTypes) != 2 {
		t.Fatalf("types = %+v", r.InstanceTypes)
	}
	for _, it := range r.InstanceTypes {
		n := it.NetworkInfo
		if n == nil || n.DefaultNetworkCardIndex == nil || len(n.NetworkCards) != 1 || n.Ipv4AddressesPerInterface == nil || *n.Ipv4AddressesPerInterface < 2 || *n.MaximumNetworkInterfaces < 2 {
			t.Errorf("%s: network info = %+v", it.InstanceType, n)
		}
	}
}

// A MaxResults or a volume Size that is out of range is refused with InvalidParameterValue.
func TestNumbersInRange(t *testing.T) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return ec2svc.New(h) })
	c := ec2.NewFromConfig(awstest.Config(t, url))
	code := func(err error) string {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			return ae.ErrorCode()
		}
		return ""
	}
	for _, n := range []int32{-1, 0, 4, 1001} {
		_, err := c.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{MaxResults: aws.Int32(n)})
		if code(err) != "InvalidParameterValue" {
			t.Errorf("DescribeVpcs MaxResults=%d: %v", n, err)
		}
	}
	must(c.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{MaxResults: aws.Int32(5)}))
	for _, n := range []int32{-1, 0, 16385} {
		_, err := c.CreateVolume(ctx, &ec2.CreateVolumeInput{AvailabilityZone: aws.String("us-east-1a"), Size: aws.Int32(n)})
		if code(err) != "InvalidParameterValue" {
			t.Errorf("CreateVolume Size=%d: %v", n, err)
		}
	}
	must(c.CreateVolume(ctx, &ec2.CreateVolumeInput{AvailabilityZone: aws.String("us-east-1a"), Size: aws.Int32(8)}))
}
