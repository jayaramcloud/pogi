package cloudformation_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	cfn "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
)

// An instance, a volume attached to it, DHCP options and an Elastic IP association are real EC2 resources.
func TestInstanceStack(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent") // no container engine: instances are metadata only
	cfg := start(t)
	c, e := cfn.NewFromConfig(cfg), ec2.NewFromConfig(cfg)
	tpl := `{"Resources":{
"Vpc":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"10.4.0.0/16"}},
"Sub":{"Type":"AWS::EC2::Subnet","Properties":{"VpcId":{"Ref":"Vpc"},"CidrBlock":"10.4.1.0/24","AvailabilityZone":"us-east-1a"}},
"Sg":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"i","VpcId":{"Ref":"Vpc"}}},
"Dhcp":{"Type":"AWS::EC2::DHCPOptions","Properties":{"DomainName":"corp.example","DomainNameServers":["10.4.0.2"],"Tags":[{"Key":"Name","Value":"d"}]}},
"DhcpAssoc":{"Type":"AWS::EC2::VPCDHCPOptionsAssociation","Properties":{"VpcId":{"Ref":"Vpc"},"DhcpOptionsId":{"Ref":"Dhcp"}}},
"I":{"Type":"AWS::EC2::Instance","Properties":{"ImageId":"ami-12345678","InstanceType":"%s","SubnetId":{"Ref":"Sub"},"SecurityGroupIds":[{"Ref":"Sg"}],"UserData":"IyEvYmluL3NoCmVjaG8gaGk=","Tags":[{"Key":"Name","Value":"%s"}]}},
"Vol":{"Type":"AWS::EC2::Volume","Properties":{"AvailabilityZone":"us-east-1a","Size":%s,"VolumeType":"gp3"}},
"Att":{"Type":"AWS::EC2::VolumeAttachment","Properties":{"InstanceId":{"Ref":"I"},"VolumeId":{"Ref":"Vol"},"Device":"/dev/sdf"}},
"Eip":{"Type":"AWS::EC2::EIP"},
"EipAssoc":{"Type":"AWS::EC2::EIPAssociation","Properties":{"AllocationId":{"Fn::GetAtt":["Eip","AllocationId"]},"InstanceId":{"Ref":"I"}}}},
"Outputs":{"Id":{"Value":{"Ref":"I"}},"Ip":{"Value":{"Fn::GetAtt":["I","PrivateIp"]}},"Az":{"Value":{"Fn::GetAtt":["I","AvailabilityZone"]}},"Vol":{"Value":{"Ref":"Vol"}},"Eip":{"Value":{"Ref":"Eip"}}}}`
	st := deploy(t, c, "inst", fmt.Sprintf(tpl, "t3.micro", "a", "8"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)
	inst := func() ec2types.Instance {
		r := must(e.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{out["Id"]}})).Reservations
		return r[0].Instances[0]
	}
	i := inst()
	if i.State.Name != ec2types.InstanceStateNameRunning || i.InstanceType != ec2types.InstanceTypeT3Micro || aws.ToString(i.PrivateIpAddress) != out["Ip"] ||
		out["Az"] != "us-east-1a" || len(i.SecurityGroups) != 1 || aws.ToString(i.PublicIpAddress) != out["Eip"] {
		t.Fatalf("instance %+v outputs %v", i, out)
	}
	if v := must(e.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{VolumeIds: []string{out["Vol"]}})).Volumes; len(v) != 1 || len(v[0].Attachments) != 1 || aws.ToString(v[0].Attachments[0].InstanceId) != out["Id"] {
		t.Fatalf("volume %+v", v)
	}
	d := must(e.DescribeDhcpOptions(ctx, &ec2.DescribeDhcpOptionsInput{Filters: []ec2types.Filter{{Name: aws.String("tag:Name"), Values: []string{"d"}}}})).DhcpOptions
	if len(d) != 1 || len(d[0].DhcpConfigurations) != 2 {
		t.Fatalf("dhcp options %+v", d)
	}
	if v := must(e.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{Filters: []ec2types.Filter{{Name: aws.String("dhcp-options-id"), Values: []string{aws.ToString(d[0].DhcpOptionsId)}}}})).Vpcs; len(v) != 1 {
		t.Fatalf("vpc using the dhcp options: %+v", v)
	}

	// a new type is a stop and start of the same instance, a bigger volume grows in place, tags change in place
	wantStatus(t, c, redeploy(t, c, "inst", fmt.Sprintf(tpl, "t3.small", "b", "16")), types.StackStatusUpdateComplete)
	i = inst()
	if i.InstanceType != ec2types.InstanceTypeT3Small || i.State.Name != ec2types.InstanceStateNameRunning || aws.ToString(i.Tags[0].Value) != "b" {
		t.Fatalf("instance after update %+v", i)
	}
	if v := must(e.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{VolumeIds: []string{out["Vol"]}})).Volumes; aws.ToInt32(v[0].Size) != 16 {
		t.Fatalf("volume after update %+v", v)
	}
	remove(t, c, "inst")
	if r := must(e.DescribeInstances(ctx, &ec2.DescribeInstancesInput{Filters: []ec2types.Filter{{Name: aws.String("instance-state-name"), Values: []string{"running", "stopped", "pending"}}}})).Reservations; len(r) != 0 {
		t.Fatalf("instances after DeleteStack: %+v", r)
	}
	if v := must(e.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{})).Volumes; len(v) != 0 {
		t.Fatalf("volumes after DeleteStack: %+v", v)
	}
}

// A launch configuration, and DB subnet, parameter and cluster parameter groups with a DB instance.
func TestLaunchConfigAndDatabase(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	cfg := start(t)
	c, a, r := cfn.NewFromConfig(cfg), autoscaling.NewFromConfig(cfg), rds.NewFromConfig(cfg)
	tpl := `{"Resources":{` + netHead + `,
"Lc":{"Type":"AWS::AutoScaling::LaunchConfiguration","Properties":{"ImageId":"ami-12345678","InstanceType":"t3.micro","InstanceMonitoring":true}},
"Asg":{"Type":"AWS::AutoScaling::AutoScalingGroup","Properties":{"MinSize":"0","MaxSize":"1","LaunchConfigurationName":{"Ref":"Lc"},"VPCZoneIdentifier":[{"Ref":"S1"}]}},
"Sg":{"Type":"AWS::RDS::DBSubnetGroup","Properties":{"DBSubnetGroupDescription":"db","SubnetIds":[{"Ref":"S1"},{"Ref":"S2"}]}},
"Pg":{"Type":"AWS::RDS::DBParameterGroup","Properties":{"Family":"postgres17","Description":"pg","Parameters":{"max_connections":"%s"}}},
"Cpg":{"Type":"AWS::RDS::DBClusterParameterGroup","Properties":{"Family":"aurora-postgresql16","Description":"cpg","Parameters":{"timezone":"UTC"}}},
"Db":{"Type":"AWS::RDS::DBInstance","Properties":{"Engine":"postgres","DBInstanceClass":"db.t3.micro","AllocatedStorage":"20","MasterUsername":"admin","MasterUserPassword":"password123",
 "DBSubnetGroupName":{"Ref":"Sg"},"DBParameterGroupName":{"Ref":"Pg"},"BackupRetentionPeriod":%s,"Tags":[{"Key":"env","Value":"dev"}]}}},
"Outputs":{"Host":{"Value":{"Fn::GetAtt":["Db","Endpoint.Address"]}},"Id":{"Value":{"Ref":"Db"}}}}`
	st := deploy(t, c, "db", fmt.Sprintf(tpl, "100", "1"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)
	if l := must(a.DescribeLaunchConfigurations(ctx, &autoscaling.DescribeLaunchConfigurationsInput{})).LaunchConfigurations; len(l) != 1 || !aws.ToBool(l[0].InstanceMonitoring.Enabled) {
		t.Fatalf("launch configurations %+v", l)
	}
	if g := must(a.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{})).AutoScalingGroups; len(g) != 1 || aws.ToString(g[0].LaunchConfigurationName) == "" {
		t.Fatalf("groups %+v", g)
	}
	db := must(r.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{})).DBInstances
	if len(db) != 1 || aws.ToString(db[0].DBInstanceIdentifier) != out["Id"] || db[0].DBSubnetGroup == nil || aws.ToString(db[0].DBParameterGroups[0].DBParameterGroupName) == "" {
		t.Fatalf("db instances %+v", db)
	}
	if p := must(r.DescribeDBParameterGroups(ctx, &rds.DescribeDBParameterGroupsInput{})).DBParameterGroups; len(p) < 1 {
		t.Fatalf("parameter groups %+v", p)
	}
	if p := must(r.DescribeDBClusterParameterGroups(ctx, &rds.DescribeDBClusterParameterGroupsInput{})).DBClusterParameterGroups; len(p) < 1 {
		t.Fatalf("cluster parameter groups %+v", p)
	}
	wantStatus(t, c, redeploy(t, c, "db", fmt.Sprintf(tpl, "200", "5")), types.StackStatusUpdateComplete)
	db = must(r.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{})).DBInstances
	if len(db) != 1 || aws.ToString(db[0].DBInstanceIdentifier) != out["Id"] || aws.ToInt32(db[0].BackupRetentionPeriod) != 5 {
		t.Fatalf("db instance after update %+v", db)
	}
	remove(t, c, "db")
	if db := must(r.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{})).DBInstances; len(db) != 0 {
		t.Fatalf("db instances after DeleteStack: %+v", db)
	}
	if l := must(a.DescribeLaunchConfigurations(ctx, &autoscaling.DescribeLaunchConfigurationsInput{})).LaunchConfigurations; len(l) != 0 {
		t.Fatalf("launch configurations after DeleteStack: %+v", l)
	}
}
