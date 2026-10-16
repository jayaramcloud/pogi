package cloudformation_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfn "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// deploy creates a stack and waits for it to settle.
func deploy(t *testing.T, c *cfn.Client, name, body string) types.Stack {
	t.Helper()
	if _, err := c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String(name), TemplateBody: aws.String(body),
		Capabilities: []types.Capability{types.CapabilityCapabilityIam, types.CapabilityCapabilityNamedIam}}); err != nil {
		t.Fatal(err)
	}
	return settle(t, c, name)
}

// redeploy updates a stack and waits for it to settle.
func redeploy(t *testing.T, c *cfn.Client, name, body string) types.Stack {
	t.Helper()
	if _, err := c.UpdateStack(ctx, &cfn.UpdateStackInput{StackName: aws.String(name), TemplateBody: aws.String(body),
		Capabilities: []types.Capability{types.CapabilityCapabilityIam, types.CapabilityCapabilityNamedIam}}); err != nil {
		t.Fatal(err)
	}
	return settle(t, c, name)
}

func wantStatus(t *testing.T, c *cfn.Client, st types.Stack, want types.StackStatus) {
	t.Helper()
	if st.StackStatus != want {
		var why []string
		for _, e := range must(c.DescribeStackEvents(ctx, &cfn.DescribeStackEventsInput{StackName: st.StackId})).StackEvents {
			if strings.HasSuffix(string(e.ResourceStatus), "_FAILED") {
				why = append(why, aws.ToString(e.LogicalResourceId)+": "+aws.ToString(e.ResourceStatusReason))
			}
		}
		t.Fatalf("stack %s, want %s: %v", st.StackStatus, want, why)
	}
}

func remove(t *testing.T, c *cfn.Client, name string) {
	t.Helper()
	id := aws.ToString(must(c.DescribeStacks(ctx, &cfn.DescribeStacksInput{StackName: aws.String(name)})).Stacks[0].StackId)
	must(c.DeleteStack(ctx, &cfn.DeleteStackInput{StackName: aws.String(name)}))
	if st := settle(t, c, id); st.StackStatus != types.StackStatusDeleteComplete {
		t.Fatalf("delete: %s %s", st.StackStatus, aws.ToString(st.StackStatusReason))
	}
}

const vpcStack = `{"Resources":{
"Vpc":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"%s","EnableDnsHostnames":true,"EnableDnsSupport":true,"Tags":[{"Key":"Name","Value":"main"}]}},
"Sub":{"Type":"AWS::EC2::Subnet","Properties":{"VpcId":{"Ref":"Vpc"},"CidrBlock":"10.0.1.0/24","MapPublicIpOnLaunch":true,"Tags":[{"Key":"Name","Value":"pub"}]}},
"Priv":{"Type":"AWS::EC2::Subnet","Properties":{"VpcId":{"Ref":"Vpc"},"CidrBlock":"10.0.2.0/24"}},
"Igw":{"Type":"AWS::EC2::InternetGateway"},
"Att":{"Type":"AWS::EC2::VPCGatewayAttachment","Properties":{"VpcId":{"Ref":"Vpc"},"InternetGatewayId":{"Ref":"Igw"}}},
"Rt":{"Type":"AWS::EC2::RouteTable","Properties":{"VpcId":{"Ref":"Vpc"}}},
"Default":{"Type":"AWS::EC2::Route","DependsOn":"Att","Properties":{"RouteTableId":{"Ref":"Rt"},"DestinationCidrBlock":"0.0.0.0/0","GatewayId":{"Ref":"Igw"}}},
"Assoc":{"Type":"AWS::EC2::SubnetRouteTableAssociation","Properties":{"RouteTableId":{"Ref":"Rt"},"SubnetId":{"Ref":"Sub"}}},
"Eip":{"Type":"AWS::EC2::EIP","DependsOn":"Att","Properties":{"Domain":"vpc"}},
"Nat":{"Type":"AWS::EC2::NatGateway","Properties":{"SubnetId":{"Ref":"Sub"},"AllocationId":{"Fn::GetAtt":["Eip","AllocationId"]}}},
"Sg":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"web","VpcId":{"Ref":"Vpc"},
 "SecurityGroupIngress":[{"IpProtocol":"tcp","FromPort":80,"ToPort":80,"CidrIp":"0.0.0.0/0"}],"SecurityGroupEgress":[{"IpProtocol":"tcp","FromPort":443,"ToPort":443,"CidrIp":"10.0.0.0/8"}]}},
"Ssh":{"Type":"AWS::EC2::SecurityGroupIngress","Properties":{"GroupId":{"Ref":"Sg"},"IpProtocol":"tcp","FromPort":22,"ToPort":22,"SourceSecurityGroupId":{"Ref":"Sg"}}},
"Acl":{"Type":"AWS::EC2::NetworkAcl","Properties":{"VpcId":{"Ref":"Vpc"},"Tags":[{"Key":"Name","Value":"acl"}]}},
"AclIn":{"Type":"AWS::EC2::NetworkAclEntry","Properties":{"NetworkAclId":{"Ref":"Acl"},"RuleNumber":100,"Protocol":6,"RuleAction":"allow","CidrBlock":"0.0.0.0/0","PortRange":{"From":80,"To":80}}},
"AclAssoc":{"Type":"AWS::EC2::SubnetNetworkAclAssociation","Properties":{"SubnetId":{"Ref":"Priv"},"NetworkAclId":{"Ref":"Acl"}}},
"S3":{"Type":"AWS::EC2::VPCEndpoint","Properties":{"VpcId":{"Ref":"Vpc"},"ServiceName":"com.amazonaws.us-east-1.s3","RouteTableIds":[{"Ref":"Rt"}]}},
"Lt":{"Type":"AWS::EC2::LaunchTemplate","Properties":{"LaunchTemplateName":"lt-%s","LaunchTemplateData":{"ImageId":"ami-12345678","InstanceType":"t3.micro","UserData":"IyEvYmluL3NoCmVjaG8gaGk=","SecurityGroupIds":[{"Ref":"Sg"}]}}}},
"Outputs":{"VpcId":{"Value":{"Ref":"Vpc"}},"Cidr":{"Value":{"Fn::GetAtt":["Vpc","CidrBlock"]}},"DefaultSg":{"Value":{"Fn::GetAtt":["Vpc","DefaultSecurityGroup"]}},"DefaultAcl":{"Value":{"Fn::GetAtt":["Vpc","DefaultNetworkAcl"]}},
"SubnetId":{"Value":{"Ref":"Sub"}},"Ip":{"Value":{"Ref":"Eip"}},"Alloc":{"Value":{"Fn::GetAtt":["Eip","AllocationId"]}},"NatId":{"Value":{"Ref":"Nat"}},"LtLatest":{"Value":{"Fn::GetAtt":["Lt","LatestVersionNumber"]}}}}`

// floci #5222, #1976, #2013, #1297, #5502, #4658: a VPC template makes real EC2 resources, updates them and removes them.
func TestVPCStack(t *testing.T) {
	cfg := start(t)
	c, e := cfn.NewFromConfig(cfg), ec2.NewFromConfig(cfg)
	st := deploy(t, c, "net", fmt.Sprintf(vpcStack, "10.0.0.0/16", "a"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)

	v := must(e.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{Filters: []ec2types.Filter{{Name: aws.String("tag:Name"), Values: []string{"main"}}}})).Vpcs
	if len(v) != 1 || aws.ToString(v[0].VpcId) != out["VpcId"] || out["Cidr"] != "10.0.0.0/16" {
		t.Fatalf("vpcs %+v outputs %v", v, out)
	}
	if h := must(e.DescribeVpcAttribute(ctx, &ec2.DescribeVpcAttributeInput{VpcId: v[0].VpcId, Attribute: ec2types.VpcAttributeNameEnableDnsHostnames})); !aws.ToBool(h.EnableDnsHostnames.Value) {
		t.Fatal("EnableDnsHostnames is false")
	}
	if g := must(e.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: []string{out["DefaultSg"]}})).SecurityGroups; len(g) != 1 || aws.ToString(g[0].GroupName) != "default" {
		t.Fatalf("default security group %v", g)
	}
	if a := must(e.DescribeNetworkAcls(ctx, &ec2.DescribeNetworkAclsInput{NetworkAclIds: []string{out["DefaultAcl"]}})).NetworkAcls; len(a) != 1 || !aws.ToBool(a[0].IsDefault) {
		t.Fatalf("default acl %v", a)
	}
	if s := must(e.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{SubnetIds: []string{out["SubnetId"]}})).Subnets; len(s) != 1 || !aws.ToBool(s[0].MapPublicIpOnLaunch) {
		t.Fatalf("subnet %+v", s)
	}
	if r := must(e.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{Filters: []ec2types.Filter{{Name: aws.String("association.subnet-id"), Values: []string{out["SubnetId"]}}}})).RouteTables; len(r) != 1 ||
		!hasRoute(r[0], "0.0.0.0/0") || !hasRoute(r[0], "pl-") {
		t.Fatalf("route table of the public subnet %+v", r)
	}
	if n := must(e.DescribeNatGateways(ctx, &ec2.DescribeNatGatewaysInput{NatGatewayIds: []string{out["NatId"]}})).NatGateways; len(n) != 1 ||
		aws.ToString(n[0].NatGatewayAddresses[0].PublicIp) != out["Ip"] || aws.ToString(n[0].NatGatewayAddresses[0].AllocationId) != out["Alloc"] {
		t.Fatalf("nat gateway %+v", n)
	}
	sg := must(e.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{Filters: []ec2types.Filter{{Name: aws.String("description"), Values: []string{"web"}}}})).SecurityGroups
	if len(sg) != 1 || len(sg[0].IpPermissions) != 2 || len(sg[0].IpPermissionsEgress) != 1 || aws.ToInt32(sg[0].IpPermissionsEgress[0].FromPort) != 443 {
		t.Fatalf("security group %+v", sg)
	}
	if a := must(e.DescribeNetworkAcls(ctx, &ec2.DescribeNetworkAclsInput{Filters: []ec2types.Filter{{Name: aws.String("tag:Name"), Values: []string{"acl"}}}})).NetworkAcls; len(a) != 1 ||
		len(a[0].Associations) != 1 || len(a[0].Entries) != 3 {
		t.Fatalf("acl %+v", a)
	}
	if ep := must(e.DescribeVpcEndpoints(ctx, &ec2.DescribeVpcEndpointsInput{})).VpcEndpoints; len(ep) != 1 || len(ep[0].RouteTableIds) != 1 {
		t.Fatalf("endpoint %+v", ep)
	}
	ver := must(e.DescribeLaunchTemplateVersions(ctx, &ec2.DescribeLaunchTemplateVersionsInput{LaunchTemplateName: aws.String("lt-a")})).LaunchTemplateVersions
	if len(ver) != 1 || aws.ToString(ver[0].LaunchTemplateData.UserData) != "IyEvYmluL3NoCmVjaG8gaGk=" || len(ver[0].LaunchTemplateData.SecurityGroupIds) != 1 || out["LtLatest"] != "1" {
		t.Fatalf("launch template versions %+v", ver)
	}

	// New launch template data is a new version, a new CIDR replaces the VPC (and so everything in it).
	next := strings.Replace(fmt.Sprintf(vpcStack, "10.0.0.0/16", "a"), `"InstanceType":"t3.micro"`, `"InstanceType":"t3.small"`, 1)
	st = redeploy(t, c, "net", next)
	wantStatus(t, c, st, types.StackStatusUpdateComplete)
	if outputs(st)["LtLatest"] != "2" {
		t.Fatalf("launch template latest version %s", outputs(st)["LtLatest"])
	}
	st = redeploy(t, c, "net", strings.Replace(next, "10.0.0.0/16", "10.0.0.0/8", 1))
	wantStatus(t, c, st, types.StackStatusUpdateRollbackComplete)
	if got := must(e.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{Filters: []ec2types.Filter{{Name: aws.String("isDefault"), Values: []string{"false"}}}})).Vpcs; len(got) != 1 || aws.ToString(got[0].VpcId) != out["VpcId"] {
		t.Fatalf("vpcs after the failed update: %+v", got)
	}

	remove(t, c, "net")
	if got := must(e.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{Filters: []ec2types.Filter{{Name: aws.String("isDefault"), Values: []string{"false"}}}})).Vpcs; len(got) != 0 {
		t.Fatalf("vpcs after DeleteStack: %+v", got)
	}
	if a := must(e.DescribeAddresses(ctx, &ec2.DescribeAddressesInput{})).Addresses; len(a) != 0 {
		t.Fatalf("addresses after DeleteStack: %+v", a)
	}
}

func hasRoute(t ec2types.RouteTable, dest string) bool {
	for _, r := range t.Routes {
		if strings.HasPrefix(aws.ToString(r.DestinationCidrBlock), dest) || strings.HasPrefix(aws.ToString(r.DestinationPrefixListId), dest) {
			return true
		}
	}
	return false
}

// floci #5502: a changed CIDR replaces the VPC (the old one goes), tags and DNS settings change in place.
func TestVPCReplace(t *testing.T) {
	cfg := start(t)
	c, e := cfn.NewFromConfig(cfg), ec2.NewFromConfig(cfg)
	one := `{"Resources":{"Vpc":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"%s","EnableDnsHostnames":%s,"Tags":[{"Key":"env","Value":"%s"}]}}}}`
	mine := func() []ec2types.Vpc {
		return must(e.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{Filters: []ec2types.Filter{{Name: aws.String("isDefault"), Values: []string{"false"}}}})).Vpcs
	}
	wantStatus(t, c, deploy(t, c, "vpc", fmt.Sprintf(one, "10.2.0.0/16", "false", "a")), types.StackStatusCreateComplete)
	id := aws.ToString(mine()[0].VpcId)
	wantStatus(t, c, redeploy(t, c, "vpc", fmt.Sprintf(one, "10.2.0.0/16", "true", "b")), types.StackStatusUpdateComplete)
	v := mine()
	if len(v) != 1 || aws.ToString(v[0].VpcId) != id || len(v[0].Tags) != 1 || aws.ToString(v[0].Tags[0].Value) != "b" {
		t.Fatalf("in place update: %+v", v)
	}
	if h := must(e.DescribeVpcAttribute(ctx, &ec2.DescribeVpcAttributeInput{VpcId: aws.String(id), Attribute: ec2types.VpcAttributeNameEnableDnsHostnames})); !aws.ToBool(h.EnableDnsHostnames.Value) {
		t.Fatal("EnableDnsHostnames not updated")
	}
	wantStatus(t, c, redeploy(t, c, "vpc", fmt.Sprintf(one, "10.3.0.0/16", "true", "b")), types.StackStatusUpdateComplete)
	if v := mine(); len(v) != 1 || aws.ToString(v[0].VpcId) == id || aws.ToString(v[0].CidrBlock) != "10.3.0.0/16" {
		t.Fatalf("replacement: %+v", v)
	}
	remove(t, c, "vpc")
	if v := mine(); len(v) != 0 {
		t.Fatalf("after DeleteStack: %+v", v)
	}
}

// Flow logs, transit gateways, prefix lists, key pairs, egress-only gateways, hosts and CIDR blocks.
func TestEC2MoreTypes(t *testing.T) {
	cfg := start(t)
	c, e := cfn.NewFromConfig(cfg), ec2.NewFromConfig(cfg)
	tpl := `{"Resources":{"Vpc":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"10.6.0.0/16"}},
"Sub":{"Type":"AWS::EC2::Subnet","Properties":{"VpcId":{"Ref":"Vpc"},"CidrBlock":"10.6.1.0/24","AvailabilityZone":"us-east-1a"}},
"Lg":{"Type":"AWS::Logs::LogGroup"},
"Flow":{"Type":"AWS::EC2::FlowLog","Properties":{"ResourceId":{"Ref":"Vpc"},"ResourceType":"VPC","TrafficType":"ALL","LogDestinationType":"cloud-watch-logs","LogGroupName":{"Ref":"Lg"},"DeliverLogsPermissionArn":"arn:aws:iam::000000000000:role/flow"}},
"Tgw":{"Type":"AWS::EC2::TransitGateway","Properties":{"Description":"%s","AmazonSideAsn":64513}},
"Att":{"Type":"AWS::EC2::TransitGatewayAttachment","Properties":{"TransitGatewayId":{"Ref":"Tgw"},"VpcId":{"Ref":"Vpc"},"SubnetIds":[{"Ref":"Sub"}]}},
"Eigw":{"Type":"AWS::EC2::EgressOnlyInternetGateway","Properties":{"VpcId":{"Ref":"Vpc"}}},
"Pl":{"Type":"AWS::EC2::PrefixList","Properties":{"PrefixListName":"office","AddressFamily":"IPv4","MaxEntries":5,"Entries":[{"Cidr":"192.0.2.0/24","Description":"hq"}%s]}},
"Key":{"Type":"AWS::EC2::KeyPair","Properties":{"KeyName":"deploy"}},
"Host":{"Type":"AWS::EC2::Host","Properties":{"AvailabilityZone":"us-east-1a","InstanceType":"m5.large"}},
"Cidr":{"Type":"AWS::EC2::VPCCidrBlock","Properties":{"VpcId":{"Ref":"Vpc"},"CidrBlock":"10.7.0.0/16"}}},
"Outputs":{"Tgw":{"Value":{"Ref":"Tgw"}},"Pl":{"Value":{"Ref":"Pl"}}}}`
	st := deploy(t, c, "more", fmt.Sprintf(tpl, "one", ""))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)
	if f := must(e.DescribeFlowLogs(ctx, &ec2.DescribeFlowLogsInput{})).FlowLogs; len(f) != 1 {
		t.Fatalf("flow logs %+v", f)
	}
	if g := must(e.DescribeTransitGateways(ctx, &ec2.DescribeTransitGatewaysInput{TransitGatewayIds: []string{out["Tgw"]}})).TransitGateways; len(g) != 1 || aws.ToString(g[0].Description) != "one" {
		t.Fatalf("transit gateways %+v", g)
	}
	if a := must(e.DescribeTransitGatewayVpcAttachments(ctx, &ec2.DescribeTransitGatewayVpcAttachmentsInput{})).TransitGatewayVpcAttachments; len(a) != 1 {
		t.Fatalf("attachments %+v", a)
	}
	if g := must(e.DescribeEgressOnlyInternetGateways(ctx, &ec2.DescribeEgressOnlyInternetGatewaysInput{})).EgressOnlyInternetGateways; len(g) != 1 {
		t.Fatalf("egress-only gateways %+v", g)
	}
	if k := must(e.DescribeKeyPairs(ctx, &ec2.DescribeKeyPairsInput{KeyNames: []string{"deploy"}})).KeyPairs; len(k) != 1 {
		t.Fatalf("key pairs %+v", k)
	}
	if h := must(e.DescribeHosts(ctx, &ec2.DescribeHostsInput{})).Hosts; len(h) != 1 {
		t.Fatalf("hosts %+v", h)
	}
	wantStatus(t, c, redeploy(t, c, "more", fmt.Sprintf(tpl, "two", `,{"Cidr":"198.51.100.0/24"}`)), types.StackStatusUpdateComplete)
	if g := must(e.DescribeTransitGateways(ctx, &ec2.DescribeTransitGatewaysInput{TransitGatewayIds: []string{out["Tgw"]}})).TransitGateways; aws.ToString(g[0].Description) != "two" {
		t.Fatalf("transit gateway after update %+v", g)
	}
	if en := must(e.GetManagedPrefixListEntries(ctx, &ec2.GetManagedPrefixListEntriesInput{PrefixListId: aws.String(out["Pl"])})).Entries; len(en) != 2 {
		t.Fatalf("prefix list entries %+v", en)
	}
	remove(t, c, "more")
	if g := must(e.DescribeTransitGateways(ctx, &ec2.DescribeTransitGatewaysInput{Filters: []ec2types.Filter{{Name: aws.String("state"), Values: []string{"available"}}}})).TransitGateways; len(g) != 0 {
		t.Fatalf("transit gateways after DeleteStack %+v", g)
	}
}
