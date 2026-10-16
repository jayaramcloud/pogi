package cloudformation

import (
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// EC2 instances, volumes, DHCP options and Elastic IP associations, and Auto Scaling launch configurations.

// instanceState is the state name of an instance ("" when it is not found).
func instanceState(c *pctx, id string) (string, *xnode) {
	r, err := c.ec2("DescribeInstances", map[string]any{"InstanceId": []string{id}})
	if err != nil {
		return "", nil
	}
	return r.get("reservationSet", "item", "instancesSet", "item", "instanceState").text("name"), r
}

// instanceAttrs waits for an instance to leave "pending" and reads what Fn::GetAtt gives.
func instanceAttrs(c *pctx, id string) (map[string]any, error) {
	var r *xnode
	if !waitFor(30*time.Second, func() bool {
		var st string
		st, r = instanceState(c, id)
		return st != "pending"
	}) {
		return nil, core.Errorf(500, "Timeout", "instance %s is still pending", id)
	}
	return map[string]any{"AvailabilityZone": r.findText("availabilityZone"), "PrivateDnsName": r.findText("privateDnsName"), "PrivateIp": r.findText("privateIpAddress"),
		"PublicDnsName": r.findText("dnsName"), "PublicIp": r.findText("ipAddress")}, nil
}

// instanceWait waits for an instance to reach a state.
func instanceWait(c *pctx, id, want string) error {
	if !waitFor(30*time.Second, func() bool { st, _ := instanceState(c, id); return st == want }) {
		return core.Errorf(500, "Timeout", "instance %s did not reach %s", id, want)
	}
	return nil
}

// runInput is the RunInstances form of an AWS::EC2::Instance's properties.
func runInput(p map[string]any) map[string]any {
	in := pick(p, "ImageId", "InstanceType", "KeyName", "SubnetId", "PrivateIpAddress", "UserData", "DisableApiTermination", "EbsOptimized",
		"InstanceInitiatedShutdownBehavior", "CreditSpecification", "MetadataOptions", "LaunchTemplate", "SecurityGroupIds=SecurityGroupId",
		"SecurityGroups=SecurityGroup", "BlockDeviceMappings=BlockDeviceMapping")
	in["MinCount"], in["MaxCount"] = 1, 1
	if n := str(p["IamInstanceProfile"]); n != "" {
		in["IamInstanceProfile"] = map[string]any{"Name": n}
	}
	if m, ok := p["Monitoring"]; ok {
		in["Monitoring"] = map[string]any{"Enabled": m}
	}
	if az := p["AvailabilityZone"]; az != nil || p["Tenancy"] != nil {
		in["Placement"] = pick(p, "AvailabilityZone", "Tenancy")
	}
	var nics []any
	for _, n := range listMaps(p["NetworkInterfaces"]) {
		nic := pick(n, "DeviceIndex", "SubnetId", "PrivateIpAddress", "AssociatePublicIpAddress")
		if g := list(n["GroupSet"]); len(g) > 0 {
			nic["SecurityGroupId"] = g
		}
		nics = append(nics, nic)
	}
	if nics != nil {
		in["NetworkInterface"] = nics
	}
	in["TagSpecification"] = ec2Tags("instance", p["Tags"])
	return in
}

func init() {
	register("AWS::EC2::Instance", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			r, err := c.ec2("RunInstances", runInput(p))
			if err != nil {
				return "", nil, err
			}
			id := r.findText("instanceId")
			attrs, err := instanceAttrs(c, id)
			return id, attrs, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "InstanceType", "UserData", "SecurityGroupIds", "DisableApiTermination", "InstanceInitiatedShutdownBehavior", "EbsOptimized", "Monitoring", "Tags") {
				return nil, true, nil
			}
			var attrs map[string]any
			// a new type or user data needs a stopped instance: stop, change, start (some interruptions)
			if !equal(old["InstanceType"], p["InstanceType"]) || !equal(old["UserData"], p["UserData"]) {
				if _, err := c.ec2("StopInstances", map[string]any{"InstanceId": []string{phys}}); err != nil {
					return nil, false, err
				}
				if err := instanceWait(c, phys, "stopped"); err != nil {
					return nil, false, err
				}
				in := map[string]any{"InstanceId": phys}
				if p["InstanceType"] != nil {
					in["InstanceType"] = map[string]any{"Value": p["InstanceType"]}
				}
				if p["UserData"] != nil {
					in["UserData"] = map[string]any{"Value": p["UserData"]}
				}
				_, err := c.ec2("ModifyInstanceAttribute", in)
				if _, e := c.ec2("StartInstances", map[string]any{"InstanceId": []string{phys}}); err == nil {
					err = e
				}
				if err != nil {
					return nil, false, err
				}
				var e error
				if attrs, e = instanceAttrs(c, phys); e != nil {
					return nil, false, e
				}
			}
			for _, a := range []struct{ prop, attr string }{{"DisableApiTermination", "DisableApiTermination"}, {"InstanceInitiatedShutdownBehavior", "InstanceInitiatedShutdownBehavior"}, {"EbsOptimized", "EbsOptimized"}} {
				if v := p[a.prop]; v != nil && !equal(old[a.prop], v) {
					if _, err := c.ec2("ModifyInstanceAttribute", map[string]any{"InstanceId": phys, a.attr: map[string]any{"Value": v}}); err != nil {
						return nil, false, err
					}
				}
			}
			if !equal(old["SecurityGroupIds"], p["SecurityGroupIds"]) {
				if _, err := c.ec2("ModifyInstanceAttribute", map[string]any{"InstanceId": phys, "GroupId": list(p["SecurityGroupIds"])}); err != nil {
					return nil, false, err
				}
			}
			return attrs, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			if _, err := c.ec2("TerminateInstances", map[string]any{"InstanceId": []string{phys}}); err != nil {
				return err
			}
			// subnets and groups cannot go while the instance still holds its network interface
			waitFor(30*time.Second, func() bool { st, _ := instanceState(c, phys); return st == "terminated" || st == "" })
			return nil
		},
	})

	volumeInPlace := []string{"Size", "VolumeType", "Iops", "Throughput", "Tags"}
	register("AWS::EC2::Volume", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "AvailabilityZone", "Size", "VolumeType", "Iops", "Throughput", "Encrypted", "KmsKeyId", "SnapshotId", "MultiAttachEnabled")
			in["TagSpecification"] = ec2Tags("volume", p["Tags"])
			r, err := c.ec2("CreateVolume", in)
			if err != nil {
				return "", nil, err
			}
			id := r.findText("volumeId")
			return id, map[string]any{"VolumeId": id}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, volumeInPlace...) {
				return nil, true, nil
			}
			if differs(pick(old, volumeInPlace[:4]...), pick(p, volumeInPlace[:4]...)) {
				in := pick(p, volumeInPlace[:4]...)
				in["VolumeId"] = phys
				if _, err := c.ec2("ModifyVolume", in); err != nil {
					return nil, false, err
				}
			}
			return nil, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteVolume", map[string]any{"VolumeId": phys})
			return err
		},
	})
	register("AWS::EC2::VolumeAttachment", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			_, err := c.ec2("AttachVolume", pick(p, "Device", "InstanceId", "VolumeId"))
			return c.name(64, false), nil, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DetachVolume", pick(p, "VolumeId", "InstanceId", "Device"))
			// a volume that is already free (its instance ended first) is detached enough
			if err != nil && strings.Contains(err.Error(), "available") {
				return nil
			}
			return err
		},
	})

	register("AWS::EC2::DHCPOptions", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var cfg []any
			for _, k := range []struct{ prop, key string }{{"DomainName", "domain-name"}, {"DomainNameServers", "domain-name-servers"}, {"NetbiosNameServers", "netbios-name-servers"},
				{"NetbiosNodeType", "netbios-node-type"}, {"NtpServers", "ntp-servers"}} {
				if v, ok := p[k.prop]; ok {
					vals := list(v)
					if len(vals) == 0 {
						vals = []string{str(v)}
					}
					cfg = append(cfg, map[string]any{"Key": k.key, "Value": vals})
				}
			}
			r, err := c.ec2("CreateDhcpOptions", map[string]any{"DhcpConfiguration": cfg, "TagSpecification": ec2Tags("dhcp-options", p["Tags"])})
			if err != nil {
				return "", nil, err
			}
			id := r.findText("dhcpOptionsId")
			return id, map[string]any{"DhcpOptionsId": id}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Tags") {
				return nil, true, nil
			}
			return nil, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteDhcpOptions", map[string]any{"DhcpOptionsId": phys})
			return err
		},
	})
	register("AWS::EC2::VPCDHCPOptionsAssociation", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			_, err := c.ec2("AssociateDhcpOptions", pick(p, "DhcpOptionsId", "VpcId"))
			return c.name(64, false), nil, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("AssociateDhcpOptions", map[string]any{"DhcpOptionsId": "default", "VpcId": p["VpcId"]})
			return err
		},
	})

	register("AWS::EC2::EIPAssociation", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			if p["AllocationId"] == nil {
				return "", nil, validation("pogi supports VPC Elastic IPs only: AWS::EC2::EIPAssociation needs an AllocationId")
			}
			r, err := c.ec2("AssociateAddress", pick(p, "AllocationId", "InstanceId", "NetworkInterfaceId", "PrivateIpAddress", "AllowReassociation"))
			if err != nil {
				return "", nil, err
			}
			id := r.findText("associationId")
			return id, map[string]any{"Id": id}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DisassociateAddress", map[string]any{"AssociationId": phys})
			return err
		},
	})

	register("AWS::AutoScaling::LaunchConfiguration", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["LaunchConfigurationName"]), c.name(255, false))
			in := pick(p, "ImageId", "InstanceType", "KeyName", "UserData", "SecurityGroups", "KernelId", "RamDiskId=RamdiskId", "SpotPrice", "BlockDeviceMappings",
				"IamInstanceProfile", "EbsOptimized", "AssociatePublicIpAddress", "PlacementTenancy", "MetadataOptions")
			in["LaunchConfigurationName"] = name
			if m, ok := p["InstanceMonitoring"]; ok {
				in["InstanceMonitoring"] = map[string]any{"Enabled": m}
			}
			_, err := c.query("autoscaling", "CreateLaunchConfiguration", in)
			return name, map[string]any{"LaunchConfigurationName": name}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.query("autoscaling", "DeleteLaunchConfiguration", map[string]any{"LaunchConfigurationName": phys})
			return err
		},
	})
}
