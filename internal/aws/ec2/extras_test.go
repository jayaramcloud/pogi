package ec2_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	ec2svc "github.com/nrndev/pogi/internal/aws/ec2"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker/dockertest"
)

// newHub serves EC2 alone, with no container engine.
func newHub(t *testing.T) (*core.Hub, string) {
	t.Helper()
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	return core.ServeTest(t, func(h *core.Hub) core.Service { return ec2svc.New(h) })
}

func vpcWithSubnet(t *testing.T, c *ec2.Client, vpcCidr, subnetCidr string) (vpc, subnet string) {
	t.Helper()
	vpc = *must(c.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String(vpcCidr)})).Vpc.VpcId
	subnet = *must(c.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: &vpc, CidrBlock: aws.String(subnetCidr)})).Subnet.SubnetId
	return
}

// floci #4990, #4982: transit gateways, VPC attachments and peering attachments.
func TestTransitGateways(t *testing.T) {
	hub, url := newHub(t)
	c := ec2.NewFromConfig(awstest.Config(t, url))
	tag := []types.TagSpecification{{ResourceType: types.ResourceTypeTransitGateway, Tags: []types.Tag{{Key: aws.String("Name"), Value: aws.String("hub")}}}}
	a := must(c.CreateTransitGateway(ctx, &ec2.CreateTransitGatewayInput{Description: aws.String("a"), TagSpecifications: tag,
		Options: &types.TransitGatewayRequestOptions{AmazonSideAsn: aws.Int64(64600), DnsSupport: types.DnsSupportValueDisable}})).TransitGateway
	b := must(c.CreateTransitGateway(ctx, &ec2.CreateTransitGatewayInput{})).TransitGateway
	if a.State != types.TransitGatewayStateAvailable || *a.Options.AmazonSideAsn != 64600 || a.Options.DnsSupport != types.DnsSupportValueDisable ||
		a.Options.DefaultRouteTableAssociation != types.DefaultRouteTableAssociationValueEnable || *b.Options.AmazonSideAsn != 64512 ||
		a.Options.AssociationDefaultRouteTableId == nil || !strings.HasPrefix(*a.TransitGatewayId, "tgw-") {
		t.Fatalf("gateways = %+v %+v", a, b)
	}
	_, err := c.CreateTransitGateway(ctx, &ec2.CreateTransitGatewayInput{Options: &types.TransitGatewayRequestOptions{AmazonSideAsn: aws.Int64(10)}})
	wantCode(t, err, "InvalidParameterValue")
	_, err = c.DescribeTransitGateways(ctx, &ec2.DescribeTransitGatewaysInput{TransitGatewayIds: []string{"tgw-nope"}})
	wantCode(t, err, "InvalidTransitGatewayID.NotFound")

	vpc, subnet := vpcWithSubnet(t, c, "10.50.0.0/16", "10.50.1.0/24")
	att := must(c.CreateTransitGatewayVpcAttachment(ctx, &ec2.CreateTransitGatewayVpcAttachmentInput{TransitGatewayId: a.TransitGatewayId,
		VpcId: &vpc, SubnetIds: []string{subnet}})).TransitGatewayVpcAttachment
	if att.State != types.TransitGatewayAttachmentStateAvailable || *att.VpcId != vpc || len(att.SubnetIds) != 1 || att.Options.DnsSupport != types.DnsSupportValueEnable {
		t.Fatalf("attachment = %+v", att)
	}
	_, err = c.CreateTransitGatewayVpcAttachment(ctx, &ec2.CreateTransitGatewayVpcAttachmentInput{TransitGatewayId: a.TransitGatewayId, VpcId: &vpc, SubnetIds: []string{subnet}})
	wantCode(t, err, "DuplicateTransitGatewayAttachment")
	_, err = c.DeleteTransitGateway(ctx, &ec2.DeleteTransitGatewayInput{TransitGatewayId: a.TransitGatewayId})
	wantCode(t, err, "IncorrectState")

	// what Terraform reads: the attachment, its default route table association, the gateway
	all := must(c.DescribeTransitGatewayAttachments(ctx, &ec2.DescribeTransitGatewayAttachmentsInput{})).TransitGatewayAttachments
	if len(all) != 1 || all[0].ResourceType != types.TransitGatewayAttachmentResourceTypeVpc || all[0].Association == nil ||
		*all[0].Association.TransitGatewayRouteTableId != *a.Options.AssociationDefaultRouteTableId {
		t.Fatalf("attachments = %+v", all)
	}
	assoc := must(c.GetTransitGatewayRouteTableAssociations(ctx, &ec2.GetTransitGatewayRouteTableAssociationsInput{TransitGatewayRouteTableId: a.Options.AssociationDefaultRouteTableId})).Associations
	if len(assoc) != 1 || *assoc[0].TransitGatewayAttachmentId != *att.TransitGatewayAttachmentId {
		t.Fatalf("associations = %+v", assoc)
	}
	mod := must(c.ModifyTransitGateway(ctx, &ec2.ModifyTransitGatewayInput{TransitGatewayId: a.TransitGatewayId, Description: aws.String("renamed"),
		Options: &types.ModifyTransitGatewayOptions{AddTransitGatewayCidrBlocks: []string{"10.99.0.0/24"}, AutoAcceptSharedAttachments: types.AutoAcceptSharedAttachmentsValueEnable}})).TransitGateway
	if *mod.Description != "renamed" || len(mod.Options.TransitGatewayCidrBlocks) != 1 || mod.Options.AutoAcceptSharedAttachments != types.AutoAcceptSharedAttachmentsValueEnable {
		t.Fatalf("modified = %+v", mod)
	}
	sub2 := *must(c.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: &vpc, CidrBlock: aws.String("10.50.2.0/24"), AvailabilityZone: aws.String("us-east-1b")})).Subnet.SubnetId
	mv := must(c.ModifyTransitGatewayVpcAttachment(ctx, &ec2.ModifyTransitGatewayVpcAttachmentInput{TransitGatewayAttachmentId: att.TransitGatewayAttachmentId,
		AddSubnetIds: []string{sub2}})).TransitGatewayVpcAttachment
	if len(mv.SubnetIds) != 2 {
		t.Fatalf("modified attachment = %+v", mv)
	}

	peer := must(c.CreateTransitGatewayPeeringAttachment(ctx, &ec2.CreateTransitGatewayPeeringAttachmentInput{TransitGatewayId: a.TransitGatewayId,
		PeerTransitGatewayId: b.TransitGatewayId, PeerAccountId: aws.String("000000000000"), PeerRegion: aws.String("us-east-1")})).TransitGatewayPeeringAttachment
	if peer.State != types.TransitGatewayAttachmentStatePendingAcceptance || *peer.RequesterTgwInfo.TransitGatewayId != *a.TransitGatewayId || *peer.AccepterTgwInfo.Region != "us-east-1" {
		t.Fatalf("peering = %+v", peer)
	}
	roundTrip(t, hub)
	acc := must(c.AcceptTransitGatewayPeeringAttachment(ctx, &ec2.AcceptTransitGatewayPeeringAttachmentInput{TransitGatewayAttachmentId: peer.TransitGatewayAttachmentId})).TransitGatewayPeeringAttachment
	if acc.State != types.TransitGatewayAttachmentStateAvailable {
		t.Fatalf("accepted = %+v", acc)
	}
	_, err = c.AcceptTransitGatewayPeeringAttachment(ctx, &ec2.AcceptTransitGatewayPeeringAttachmentInput{TransitGatewayAttachmentId: peer.TransitGatewayAttachmentId})
	wantCode(t, err, "IncorrectState")
	if n := len(must(c.DescribeTransitGatewayPeeringAttachments(ctx, &ec2.DescribeTransitGatewayPeeringAttachmentsInput{})).TransitGatewayPeeringAttachments); n != 1 {
		t.Fatalf("%d peerings", n)
	}

	must(c.DeleteTransitGatewayPeeringAttachment(ctx, &ec2.DeleteTransitGatewayPeeringAttachmentInput{TransitGatewayAttachmentId: peer.TransitGatewayAttachmentId}))
	must(c.DeleteTransitGatewayVpcAttachment(ctx, &ec2.DeleteTransitGatewayVpcAttachmentInput{TransitGatewayAttachmentId: att.TransitGatewayAttachmentId}))
	d := must(c.DeleteTransitGateway(ctx, &ec2.DeleteTransitGatewayInput{TransitGatewayId: a.TransitGatewayId})).TransitGateway
	if d.State != types.TransitGatewayStateDeleting {
		t.Fatalf("deleted = %+v", d)
	}
	g := must(c.DescribeTransitGateways(ctx, &ec2.DescribeTransitGatewaysInput{Filters: []types.Filter{filter("state", "available")}})).TransitGateways
	if len(g) != 1 || *g[0].TransitGatewayId != *b.TransitGatewayId {
		t.Fatalf("available gateways = %+v", g)
	}
}

// floci #4426, #4370: flow logs keep their LogFormat.
func TestFlowLogs(t *testing.T) {
	hub, url := newHub(t)
	c := ec2.NewFromConfig(awstest.Config(t, url))
	vpc, _ := vpcWithSubnet(t, c, "10.51.0.0/16", "10.51.1.0/24")
	f := "${version} ${srcaddr} ${dstaddr}"
	o := must(c.CreateFlowLogs(ctx, &ec2.CreateFlowLogsInput{ResourceIds: []string{vpc, "vpc-nope"}, ResourceType: types.FlowLogsResourceTypeVpc,
		TrafficType: types.TrafficTypeAll, LogDestinationType: types.LogDestinationTypeS3, LogDestination: aws.String("arn:aws:s3:::bucket"), LogFormat: &f,
		TagSpecifications: []types.TagSpecification{{ResourceType: types.ResourceTypeVpcFlowLog, Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}}}}))
	if len(o.FlowLogIds) != 1 || len(o.Unsuccessful) != 1 || *o.Unsuccessful[0].ResourceId != "vpc-nope" {
		t.Fatalf("created = %+v", o)
	}
	roundTrip(t, hub)
	d := must(c.DescribeFlowLogs(ctx, &ec2.DescribeFlowLogsInput{})).FlowLogs
	if len(d) != 1 || *d[0].LogFormat != f || *d[0].FlowLogStatus != "ACTIVE" || d[0].DestinationOptions == nil || d[0].DestinationOptions.FileFormat != types.DestinationFileFormatPlainText ||
		*d[0].MaxAggregationInterval != 600 || len(d[0].Tags) != 1 {
		t.Fatalf("described = %+v", d)
	}
	def := must(c.CreateFlowLogs(ctx, &ec2.CreateFlowLogsInput{ResourceIds: []string{vpc}, ResourceType: types.FlowLogsResourceTypeVpc,
		TrafficType: types.TrafficTypeReject, LogGroupName: aws.String("flows")}))
	if got := must(c.DescribeFlowLogs(ctx, &ec2.DescribeFlowLogsInput{FlowLogIds: def.FlowLogIds})).FlowLogs; len(got) != 1 || !strings.HasPrefix(*got[0].LogFormat, "${version} ${account-id}") {
		t.Fatalf("default format = %+v", got)
	}
	_, err := c.CreateFlowLogs(ctx, &ec2.CreateFlowLogsInput{ResourceIds: []string{vpc}, ResourceType: types.FlowLogsResourceTypeVpc, TrafficType: types.TrafficTypeAll,
		LogDestinationType: types.LogDestinationTypeS3})
	wantCode(t, err, "InvalidParameterValue")
	del := must(c.DeleteFlowLogs(ctx, &ec2.DeleteFlowLogsInput{FlowLogIds: append(o.FlowLogIds, "fl-nope")}))
	if len(del.Unsuccessful) != 1 || *del.Unsuccessful[0].Error.Code != "InvalidFlowLogId.NotFound" || len(must(c.DescribeFlowLogs(ctx, &ec2.DescribeFlowLogsInput{})).FlowLogs) != 1 {
		t.Fatalf("deleted = %+v", del)
	}
}

// floci #4784: dedicated hosts.
func TestDedicatedHosts(t *testing.T) {
	_, url := newHub(t)
	c := ec2.NewFromConfig(awstest.Config(t, url))
	o := must(c.AllocateHosts(ctx, &ec2.AllocateHostsInput{AvailabilityZone: aws.String("us-east-1a"), InstanceFamily: aws.String("m5"), Quantity: aws.Int32(2)}))
	if len(o.HostIds) != 2 || !strings.HasPrefix(o.HostIds[0], "h-") {
		t.Fatalf("hosts = %v", o.HostIds)
	}
	h := must(c.DescribeHosts(ctx, &ec2.DescribeHostsInput{HostIds: o.HostIds[:1]})).Hosts
	if len(h) != 1 || h[0].State != types.AllocationStateAvailable || *h[0].HostProperties.InstanceFamily != "m5" || h[0].AutoPlacement != types.AutoPlacementOff {
		t.Fatalf("host = %+v", h)
	}
	m := must(c.ModifyHosts(ctx, &ec2.ModifyHostsInput{HostIds: append(o.HostIds[:1:1], "h-nope"), AutoPlacement: types.AutoPlacementOn}))
	if len(m.Successful) != 1 || len(m.Unsuccessful) != 1 {
		t.Fatalf("modified = %+v", m)
	}
	if h := must(c.DescribeHosts(ctx, &ec2.DescribeHostsInput{HostIds: o.HostIds[:1]})).Hosts; h[0].AutoPlacement != types.AutoPlacementOn {
		t.Fatalf("host = %+v", h)
	}
	must(c.ReleaseHosts(ctx, &ec2.ReleaseHostsInput{HostIds: o.HostIds[:1]}))
	if h := must(c.DescribeHosts(ctx, &ec2.DescribeHostsInput{Filter: []types.Filter{filter("state", "released")}})).Hosts; len(h) != 1 {
		t.Fatalf("released = %+v", h)
	}
	_, err := c.AllocateHosts(ctx, &ec2.AllocateHostsInput{AvailabilityZone: aws.String("mars-1a"), InstanceFamily: aws.String("m5"), Quantity: aws.Int32(1)})
	wantCode(t, err, "InvalidParameterValue")
}

// floci #5370: encryption by default applies to new volumes.
func TestEbsEncryptionByDefault(t *testing.T) {
	hub, url := newHub(t)
	c := ec2.NewFromConfig(awstest.Config(t, url))
	vol := func() types.Volume {
		v := must(c.CreateVolume(ctx, &ec2.CreateVolumeInput{AvailabilityZone: aws.String("us-east-1a"), Size: aws.Int32(1)}))
		return must(c.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{VolumeIds: []string{*v.VolumeId}})).Volumes[0]
	}
	if *must(c.GetEbsEncryptionByDefault(ctx, &ec2.GetEbsEncryptionByDefaultInput{})).EbsEncryptionByDefault || *vol().Encrypted {
		t.Fatal("encrypted before it was enabled")
	}
	if !*must(c.EnableEbsEncryptionByDefault(ctx, &ec2.EnableEbsEncryptionByDefaultInput{})).EbsEncryptionByDefault {
		t.Fatal("not enabled")
	}
	roundTrip(t, hub)
	v := vol()
	if !*v.Encrypted || *v.KmsKeyId != "alias/aws/ebs" {
		t.Fatalf("volume = %+v", v)
	}
	run := must(c.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String(alpine), MinCount: aws.Int32(1), MaxCount: aws.Int32(1)}))
	root := must(c.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{Filters: []types.Filter{filter("attachment.instance-id", *run.Instances[0].InstanceId)}})).Volumes[0]
	if !*root.Encrypted {
		t.Fatalf("root volume = %+v", root)
	}
	must(c.ModifyEbsDefaultKmsKeyId(ctx, &ec2.ModifyEbsDefaultKmsKeyIdInput{KmsKeyId: aws.String("alias/mine")}))
	if k := *must(c.GetEbsDefaultKmsKeyId(ctx, &ec2.GetEbsDefaultKmsKeyIdInput{})).KmsKeyId; k != "alias/mine" || *vol().KmsKeyId != "alias/mine" {
		t.Fatalf("key = %s", k)
	}
	must(c.DisableEbsEncryptionByDefault(ctx, &ec2.DisableEbsEncryptionByDefaultInput{}))
	if *vol().Encrypted {
		t.Fatal("still encrypted after Disable")
	}
}

// floci #4778: the nano sizes, and DescribeInstanceTypeOfferings.
func TestInstanceTypeOfferings(t *testing.T) {
	_, url := newHub(t)
	c := ec2.NewFromConfig(awstest.Config(t, url))
	for _, it := range []types.InstanceType{"t2.nano", "t2.small", "t3.nano", "t3a.nano"} {
		if n := len(must(c.DescribeInstanceTypes(ctx, &ec2.DescribeInstanceTypesInput{InstanceTypes: []types.InstanceType{it}})).InstanceTypes); n != 1 {
			t.Errorf("DescribeInstanceTypes %s: %d", it, n)
		}
		if n := len(must(c.DescribeInstanceTypeOfferings(ctx, &ec2.DescribeInstanceTypeOfferingsInput{Filters: []types.Filter{filter("instance-type", string(it))}})).InstanceTypeOfferings); n != 1 {
			t.Errorf("DescribeInstanceTypeOfferings %s: %d", it, n)
		}
	}
	az := must(c.DescribeInstanceTypeOfferings(ctx, &ec2.DescribeInstanceTypeOfferingsInput{LocationType: types.LocationTypeAvailabilityZone,
		Filters: []types.Filter{filter("instance-type", "t3.nano"), filter("location", "us-east-1a")}})).InstanceTypeOfferings
	if len(az) != 1 || *az[0].Location != "us-east-1a" || az[0].LocationType != types.LocationTypeAvailabilityZone {
		t.Fatalf("offerings = %+v", az)
	}
}

// floci #3241: AssociateVpcCidrBlock answers with a nested CidrBlockState, and the block takes subnets.
func TestAssociateVpcCidrBlock(t *testing.T) {
	_, url := newHub(t)
	c := ec2.NewFromConfig(awstest.Config(t, url))
	vpc, _ := vpcWithSubnet(t, c, "10.52.0.0/16", "10.52.1.0/24")
	o := must(c.AssociateVpcCidrBlock(ctx, &ec2.AssociateVpcCidrBlockInput{VpcId: &vpc, CidrBlock: aws.String("10.250.44.0/22")}))
	if o.CidrBlockAssociation == nil || o.CidrBlockAssociation.CidrBlockState == nil || o.CidrBlockAssociation.CidrBlockState.State == "" || *o.VpcId != vpc {
		t.Fatalf("association = %+v", o)
	}
	v := must(c.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{Filters: []types.Filter{filter("cidr-block-association.cidr-block", "10.250.44.0/22")}})).Vpcs
	if len(v) != 1 || len(v[0].CidrBlockAssociationSet) != 2 || v[0].CidrBlockAssociationSet[1].CidrBlockState.State != types.VpcCidrBlockStateCodeAssociated {
		t.Fatalf("vpcs = %+v", v)
	}
	_, err := c.AssociateVpcCidrBlock(ctx, &ec2.AssociateVpcCidrBlockInput{VpcId: &vpc, CidrBlock: aws.String("10.52.200.0/24")})
	wantCode(t, err, "InvalidParameterValue")
	sn := must(c.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: &vpc, CidrBlock: aws.String("10.250.44.0/24")})).Subnet
	_, err = c.DisassociateVpcCidrBlock(ctx, &ec2.DisassociateVpcCidrBlockInput{AssociationId: o.CidrBlockAssociation.AssociationId})
	wantCode(t, err, "DependencyViolation")
	must(c.DeleteSubnet(ctx, &ec2.DeleteSubnetInput{SubnetId: sn.SubnetId}))
	must(c.DisassociateVpcCidrBlock(ctx, &ec2.DisassociateVpcCidrBlockInput{AssociationId: o.CidrBlockAssociation.AssociationId}))
	_, err = c.DisassociateVpcCidrBlock(ctx, &ec2.DisassociateVpcCidrBlockInput{AssociationId: v[0].CidrBlockAssociationSet[0].AssociationId})
	wantCode(t, err, "OperationNotPermitted")
}

// floci #4095: CreateImage captures an instance, DeregisterImage removes the image and its docker image.
func TestCreateAndDeregisterImage(t *testing.T) {
	engine := dockertest.Start(t, 0, false)
	hub, _, c := serve(t)
	run := must(c.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String(alpine), MinCount: aws.Int32(1), MaxCount: aws.Int32(1)}))
	id := *run.Instances[0].InstanceId

	_, err := c.CreateImage(ctx, &ec2.CreateImageInput{InstanceId: aws.String("i-0"), Name: aws.String("nothing")})
	wantCode(t, err, "InvalidInstanceID.NotFound")
	img := must(c.CreateImage(ctx, &ec2.CreateImageInput{InstanceId: &id, Name: aws.String("golden"), Description: aws.String("d"),
		TagSpecifications: []types.TagSpecification{{ResourceType: types.ResourceTypeImage, Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}}}}))
	_, err = c.CreateImage(ctx, &ec2.CreateImageInput{InstanceId: &id, Name: aws.String("golden")})
	wantCode(t, err, "InvalidAMIName.Duplicate")
	roundTrip(t, hub)
	d := must(c.DescribeImages(ctx, &ec2.DescribeImagesInput{Owners: []string{"self"}})).Images
	if len(d) != 1 || *d[0].ImageId != *img.ImageId || d[0].State != types.ImageStateAvailable || *d[0].Name != "golden" || len(d[0].BlockDeviceMappings) != 1 ||
		*d[0].BlockDeviceMappings[0].Ebs.VolumeSize != 8 || len(d[0].Tags) != 1 {
		t.Fatalf("images = %+v", d)
	}
	if got := engine.Called("commit "); len(got) != 1 || !strings.HasSuffix(got[0], " pogi-ami:"+*img.ImageId) {
		t.Fatalf("docker commit calls = %v", got)
	}
	if r := must(c.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: img.ImageId, MinCount: aws.Int32(1), MaxCount: aws.Int32(1)})); *r.Instances[0].ImageId != *img.ImageId {
		t.Fatalf("instance from the image = %+v", r.Instances[0])
	}
	fromImage := func() bool { // the containers start in the background
		for _, call := range engine.Called("create ") {
			if strings.HasSuffix(call, " pogi-ami:"+*img.ImageId) {
				return true
			}
		}
		return false
	}
	for n := 0; !fromImage(); n++ {
		if n > 100 {
			t.Fatalf("the second instance did not start from the image: %v", engine.Called("create "))
		}
		time.Sleep(20 * time.Millisecond)
	}

	must(c.DeregisterImage(ctx, &ec2.DeregisterImageInput{ImageId: img.ImageId}))
	if got := engine.Called("rmi "); len(got) != 1 || got[0] != "rmi pogi-ami:"+*img.ImageId {
		t.Fatalf("docker image calls = %v", got)
	}
	_, err = c.DeregisterImage(ctx, &ec2.DeregisterImageInput{ImageId: img.ImageId})
	wantCode(t, err, "InvalidAMIID.NotFound")
	// a catalog image goes from this region only
	must(c.DeregisterImage(ctx, &ec2.DeregisterImageInput{ImageId: aws.String(alpine)}))
	_, err = c.DescribeImages(ctx, &ec2.DescribeImagesInput{ImageIds: []string{alpine}})
	wantCode(t, err, "InvalidAMIID.NotFound")
}

// floci #1094: IAM instance profile associations.
func TestIamInstanceProfileAssociations(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, _, c := serve(t)
	with := must(c.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String(alpine), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
		IamInstanceProfile: &types.IamInstanceProfileSpecification{Name: aws.String("web")}})).Instances[0]
	without := must(c.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String(alpine), MinCount: aws.Int32(1), MaxCount: aws.Int32(1)})).Instances[0]

	d := must(c.DescribeIamInstanceProfileAssociations(ctx, &ec2.DescribeIamInstanceProfileAssociationsInput{})).IamInstanceProfileAssociations
	if len(d) != 1 || *d[0].InstanceId != *with.InstanceId || d[0].State != types.IamInstanceProfileAssociationStateAssociated ||
		!strings.HasSuffix(*d[0].IamInstanceProfile.Arn, ":instance-profile/web") {
		t.Fatalf("launch association = %+v", d)
	}
	_, err := c.AssociateIamInstanceProfile(ctx, &ec2.AssociateIamInstanceProfileInput{InstanceId: with.InstanceId, IamInstanceProfile: &types.IamInstanceProfileSpecification{Name: aws.String("web")}})
	wantCode(t, err, "IncorrectState")
	_, err = c.AssociateIamInstanceProfile(ctx, &ec2.AssociateIamInstanceProfileInput{InstanceId: without.InstanceId, IamInstanceProfile: &types.IamInstanceProfileSpecification{Name: aws.String("nope")}})
	wantCode(t, err, "InvalidParameterValue")

	a := must(c.AssociateIamInstanceProfile(ctx, &ec2.AssociateIamInstanceProfileInput{InstanceId: without.InstanceId,
		IamInstanceProfile: &types.IamInstanceProfileSpecification{Name: aws.String("web")}})).IamInstanceProfileAssociation
	if *a.InstanceId != *without.InstanceId || !strings.HasPrefix(*a.AssociationId, "iip-assoc-") {
		t.Fatalf("association = %+v", a)
	}
	if i := describe(t, c, *without.InstanceId); i.IamInstanceProfile == nil || !strings.HasSuffix(*i.IamInstanceProfile.Arn, ":instance-profile/web") {
		t.Fatalf("instance after Associate = %+v", i.IamInstanceProfile)
	}
	if n := len(must(c.DescribeIamInstanceProfileAssociations(ctx, &ec2.DescribeIamInstanceProfileAssociationsInput{
		Filters: []types.Filter{filter("instance-id", *without.InstanceId)}})).IamInstanceProfileAssociations); n != 1 {
		t.Fatalf("%d associations of the instance", n)
	}
	r := must(c.ReplaceIamInstanceProfileAssociation(ctx, &ec2.ReplaceIamInstanceProfileAssociationInput{AssociationId: a.AssociationId,
		IamInstanceProfile: &types.IamInstanceProfileSpecification{Name: aws.String("web")}})).IamInstanceProfileAssociation
	if *r.AssociationId == *a.AssociationId {
		t.Fatal("Replace kept the association id")
	}
	_, err = c.DisassociateIamInstanceProfile(ctx, &ec2.DisassociateIamInstanceProfileInput{AssociationId: a.AssociationId})
	wantCode(t, err, "InvalidAssociationID.NotFound")
	must(c.DisassociateIamInstanceProfile(ctx, &ec2.DisassociateIamInstanceProfileInput{AssociationId: r.AssociationId}))
	if i := describe(t, c, *without.InstanceId); i.IamInstanceProfile != nil {
		t.Fatalf("instance after Disassociate = %+v", i.IamInstanceProfile)
	}
}
