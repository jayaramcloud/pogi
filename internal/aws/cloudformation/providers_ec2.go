package cloudformation

import "time"

// EC2 networking resources: VPCs, subnets, route tables, gateways, Elastic IPs,
// NAT gateways, security groups, network ACLs, VPC endpoints and launch templates.

func filters(kv ...string) []any {
	var out []any
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, map[string]any{"Name": kv[i], "Value": []string{kv[i+1]}})
	}
	return out
}

// vpcAttrs reads what Fn::GetAtt gives for a VPC.
func vpcAttrs(c *pctx, id string) (map[string]any, error) {
	v, err := c.ec2("DescribeVpcs", map[string]any{"VpcId": []string{id}})
	if err != nil {
		return nil, err
	}
	attrs := map[string]any{"VpcId": id, "CidrBlock": v.findText("cidrBlock"), "Ipv6CidrBlocks": []any{}}
	assoc := []any{}
	for _, a := range v.items("vpcSet", "item", "cidrBlockAssociationSet") {
		assoc = append(assoc, a.text("associationId"))
	}
	attrs["CidrBlockAssociations"] = assoc
	sg, err := c.ec2("DescribeSecurityGroups", map[string]any{"Filter": filters("vpc-id", id, "group-name", "default")})
	if err != nil {
		return nil, err
	}
	attrs["DefaultSecurityGroup"] = sg.findText("groupId")
	acl, err := c.ec2("DescribeNetworkAcls", map[string]any{"Filter": filters("vpc-id", id, "default", "true")})
	if err != nil {
		return nil, err
	}
	attrs["DefaultNetworkAcl"] = acl.findText("networkAclId")
	return attrs, nil
}

func setVpcDNS(c *pctx, id string, p map[string]any, reset bool) error {
	for _, a := range []struct {
		prop string
		def  bool
	}{{"EnableDnsSupport", true}, {"EnableDnsHostnames", false}} {
		v, ok := p[a.prop]
		if !ok {
			if !reset {
				continue
			}
			v = a.def
		}
		if _, err := c.ec2("ModifyVpcAttribute", map[string]any{"VpcId": id, a.prop: map[string]any{"Value": str(v)}}); err != nil {
			return err
		}
	}
	return nil
}

// subnetAttrs reads what Fn::GetAtt gives for a subnet.
func subnetAttrs(c *pctx, id string) (map[string]any, error) {
	sn, err := c.ec2("DescribeSubnets", map[string]any{"SubnetId": []string{id}})
	if err != nil {
		return nil, err
	}
	attrs := map[string]any{"SubnetId": id, "AvailabilityZone": sn.findText("availabilityZone"), "VpcId": sn.findText("vpcId"),
		"CidrBlock": sn.findText("cidrBlock"), "Ipv6CidrBlocks": []any{}, "OutpostArn": ""}
	acl, err := c.ec2("DescribeNetworkAcls", map[string]any{"Filter": filters("association.subnet-id", id)})
	if err != nil {
		return nil, err
	}
	for _, a := range acl.items("networkAclSet", "item", "associationSet") {
		if a.text("subnetId") == id {
			attrs["NetworkAclAssociationId"] = a.text("networkAclAssociationId")
		}
	}
	return attrs, nil
}

func setSubnetFlags(c *pctx, id string, p map[string]any, reset bool) error {
	for _, prop := range []string{"MapPublicIpOnLaunch", "AssignIpv6AddressOnCreation"} {
		v, ok := p[prop]
		if !ok {
			if !reset {
				continue
			}
			v = false
		}
		if _, err := c.ec2("ModifySubnetAttribute", map[string]any{"SubnetId": id, prop: map[string]any{"Value": str(v)}}); err != nil {
			return err
		}
	}
	return nil
}

// sgPerms turns CloudFormation security group rules into IpPermissions.
func sgPerms(rules any) []any {
	var out []any
	for _, r := range listMaps(rules) {
		perm := pick(r, "IpProtocol", "FromPort", "ToPort")
		desc := r["Description"]
		ref := func(key string, extra string) {
			g := map[string]any{"Description": desc}
			g[key] = r[extra]
			perm[map[string]string{"GroupId": "Groups", "PrefixListId": "PrefixListIds"}[key]] = []any{g}
		}
		switch {
		case r["CidrIp"] != nil:
			perm["IpRanges"] = []any{map[string]any{"CidrIp": r["CidrIp"], "Description": desc}}
		case r["CidrIpv6"] != nil:
			perm["Ipv6Ranges"] = []any{map[string]any{"CidrIpv6": r["CidrIpv6"], "Description": desc}}
		case r["SourceSecurityGroupId"] != nil:
			ref("GroupId", "SourceSecurityGroupId")
		case r["DestinationSecurityGroupId"] != nil:
			ref("GroupId", "DestinationSecurityGroupId")
		case r["SourcePrefixListId"] != nil:
			ref("PrefixListId", "SourcePrefixListId")
		case r["DestinationPrefixListId"] != nil:
			ref("PrefixListId", "DestinationPrefixListId")
		case r["SourceSecurityGroupName"] != nil:
			perm["Groups"] = []any{map[string]any{"GroupName": r["SourceSecurityGroupName"], "UserId": r["SourceSecurityGroupOwnerId"], "Description": desc}}
		}
		out = append(out, perm)
	}
	return out
}

var allowAll = map[string]any{"IpProtocol": "-1", "IpRanges": []any{map[string]any{"CidrIp": "0.0.0.0/0"}}}

// sgRules are the ingress and egress rules a group has in a template: a group
// that lists no egress rules keeps the allow-all rule EC2 gives every new group.
func sgRules(p map[string]any) (ingress, egress []any) {
	egress = []any{allowAll}
	if p["SecurityGroupEgress"] != nil {
		egress = sgPerms(p["SecurityGroupEgress"])
	}
	return sgPerms(p["SecurityGroupIngress"]), egress
}

// sgChange authorizes or revokes the rules in now that are not in old.
func sgChange(c *pctx, group, action string, old, now []any) error {
	known := map[string]bool{}
	for _, r := range old {
		known[jsonString(r)] = true
	}
	var rest []any
	for _, r := range now {
		if !known[jsonString(r)] {
			rest = append(rest, r)
		}
	}
	if len(rest) == 0 {
		return nil
	}
	_, err := c.ec2(action, map[string]any{"GroupId": group, "IpPermissions": rest})
	return err
}

func sgUpdate(c *pctx, group string, old, p map[string]any) error {
	oi, oe := sgRules(old)
	ni, ne := sgRules(p)
	for _, step := range []struct {
		action   string
		from, to []any
	}{{"RevokeSecurityGroupIngress", ni, oi}, {"RevokeSecurityGroupEgress", ne, oe}, {"AuthorizeSecurityGroupIngress", oi, ni}, {"AuthorizeSecurityGroupEgress", oe, ne}} {
		if err := sgChange(c, group, step.action, step.from, step.to); err != nil {
			return err
		}
	}
	return nil
}

// sgRule is AWS::EC2::SecurityGroupIngress and ...Egress: one rule added to a group.
func sgRule(egress bool) *provider {
	add, del := "AuthorizeSecurityGroupIngress", "RevokeSecurityGroupIngress"
	if egress {
		add, del = "AuthorizeSecurityGroupEgress", "RevokeSecurityGroupEgress"
	}
	call := func(c *pctx, action string, p map[string]any) error {
		in := pick(p, "GroupId", "GroupName")
		in["IpPermissions"] = sgPerms([]any{p})
		_, err := c.ec2(action, in)
		return err
	}
	return &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			id := c.name(64, false)
			return id, map[string]any{"Id": id}, call(c, add, p)
		},
		del: func(c *pctx, phys string, p map[string]any) error { return call(c, del, p) },
	}
}

// ltParams renames CloudFormation's LaunchTemplateData members to the names the EC2 API takes for its lists.
func ltParams(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, e := range x {
			if n, ok := map[string]string{"SecurityGroupIds": "SecurityGroupId", "SecurityGroups": "SecurityGroup", "BlockDeviceMappings": "BlockDeviceMapping",
				"NetworkInterfaces": "NetworkInterface", "TagSpecifications": "TagSpecification", "Tags": "Tag", "Groups": "SecurityGroupId",
				"ElasticGpuSpecifications": "ElasticGpuSpecification", "ElasticInferenceAccelerators": "ElasticInferenceAccelerator",
				"LicenseSpecifications": "LicenseSpecification"}[k]; ok {
				k = n
			}
			out[k] = ltParams(e)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = ltParams(e)
		}
		return out
	}
	return v
}

func ltTags(p map[string]any) any {
	if specs, _ := p["TagSpecifications"].([]any); len(specs) > 0 {
		return ltParams(specs)
	}
	return nil
}

func ltAttrs(c *pctx, id string) (map[string]any, error) {
	r, err := c.ec2("DescribeLaunchTemplates", map[string]any{"LaunchTemplateId": []string{id}})
	if err != nil {
		return nil, err
	}
	return map[string]any{"LaunchTemplateId": id, "LatestVersionNumber": r.findText("latestVersionNumber"),
		"DefaultVersionNumber": r.findText("defaultVersionNumber")}, nil
}

func init() {
	register("AWS::EC2::VPC", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "CidrBlock", "InstanceTenancy")
			in["TagSpecification"] = ec2Tags("vpc", p["Tags"])
			r, err := c.ec2("CreateVpc", in)
			if err != nil {
				return "", nil, err
			}
			id := r.findText("vpcId")
			if err := setVpcDNS(c, id, p, false); err != nil {
				return "", nil, err
			}
			attrs, err := vpcAttrs(c, id)
			return id, attrs, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "EnableDnsSupport", "EnableDnsHostnames", "Tags") {
				return nil, true, nil
			}
			if err := setVpcDNS(c, phys, p, true); err != nil {
				return nil, false, err
			}
			return nil, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteVpc", map[string]any{"VpcId": phys})
			return err
		},
	})

	register("AWS::EC2::Subnet", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "VpcId", "CidrBlock", "AvailabilityZone", "AvailabilityZoneId")
			in["TagSpecification"] = ec2Tags("subnet", p["Tags"])
			r, err := c.ec2("CreateSubnet", in)
			if err != nil {
				return "", nil, err
			}
			id := r.findText("subnetId")
			if err := setSubnetFlags(c, id, p, false); err != nil {
				return "", nil, err
			}
			attrs, err := subnetAttrs(c, id)
			return id, attrs, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "MapPublicIpOnLaunch", "AssignIpv6AddressOnCreation", "Tags") {
				return nil, true, nil
			}
			if err := setSubnetFlags(c, phys, p, true); err != nil {
				return nil, false, err
			}
			return nil, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteSubnet", map[string]any{"SubnetId": phys})
			return err
		},
	})

	register("AWS::EC2::RouteTable", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			r, err := c.ec2("CreateRouteTable", map[string]any{"VpcId": p["VpcId"], "TagSpecification": ec2Tags("route-table", p["Tags"])})
			if err != nil {
				return "", nil, err
			}
			id := r.findText("routeTableId")
			return id, map[string]any{"RouteTableId": id}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Tags") {
				return nil, true, nil
			}
			return nil, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteRouteTable", map[string]any{"RouteTableId": phys})
			return err
		},
	})

	routeKeys := []string{"RouteTableId", "DestinationCidrBlock", "DestinationIpv6CidrBlock", "DestinationPrefixListId", "GatewayId", "NatGatewayId", "InstanceId",
		"NetworkInterfaceId", "VpcPeeringConnectionId", "TransitGatewayId", "EgressOnlyInternetGatewayId"}
	// A route has no id of its own: CloudFormation names it by table and destination.
	routeID := func(p map[string]any) string {
		return str(p["RouteTableId"]) + "|" + or(str(p["DestinationCidrBlock"]), or(str(p["DestinationIpv6CidrBlock"]), str(p["DestinationPrefixListId"])))
	}
	register("AWS::EC2::Route", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			_, err := c.ec2("CreateRoute", pick(p, routeKeys...))
			return routeID(p), nil, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if routeID(old) != routeID(p) {
				return nil, true, nil
			}
			_, err := c.ec2("ReplaceRoute", pick(p, routeKeys...))
			return nil, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteRoute", pick(p, "RouteTableId", "DestinationCidrBlock", "DestinationIpv6CidrBlock", "DestinationPrefixListId"))
			return err
		},
	})

	register("AWS::EC2::SubnetRouteTableAssociation", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			r, err := c.ec2("AssociateRouteTable", pick(p, "RouteTableId", "SubnetId"))
			if err != nil {
				return "", nil, err
			}
			id := r.findText("associationId")
			return id, map[string]any{"Id": id}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DisassociateRouteTable", map[string]any{"AssociationId": phys})
			return err
		},
	})

	register("AWS::EC2::InternetGateway", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			r, err := c.ec2("CreateInternetGateway", map[string]any{"TagSpecification": ec2Tags("internet-gateway", p["Tags"])})
			if err != nil {
				return "", nil, err
			}
			id := r.findText("internetGatewayId")
			return id, map[string]any{"InternetGatewayId": id}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Tags") {
				return nil, true, nil
			}
			return nil, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteInternetGateway", map[string]any{"InternetGatewayId": phys})
			return err
		},
	})

	register("AWS::EC2::VPCGatewayAttachment", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			if p["VpnGatewayId"] != nil {
				return "", nil, validation("pogi does not support VPN gateways yet: AWS::EC2::VPCGatewayAttachment takes an InternetGatewayId")
			}
			_, err := c.ec2("AttachInternetGateway", pick(p, "InternetGatewayId", "VpcId"))
			return c.name(64, false), nil, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DetachInternetGateway", pick(p, "InternetGatewayId", "VpcId"))
			return err
		},
	})

	register("AWS::EC2::EIP", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "Domain", "NetworkBorderGroup")
			in["TagSpecification"] = ec2Tags("elastic-ip", p["Tags"])
			r, err := c.ec2("AllocateAddress", in)
			if err != nil {
				return "", nil, err
			}
			ip, alloc := r.findText("publicIp"), r.findText("allocationId")
			if p["InstanceId"] != nil {
				if _, err := c.ec2("AssociateAddress", map[string]any{"AllocationId": alloc, "InstanceId": p["InstanceId"]}); err != nil {
					c.ec2("ReleaseAddress", map[string]any{"AllocationId": alloc})
					return "", nil, err
				}
			}
			// Ref of an EIP is its public address.
			return ip, map[string]any{"PublicIp": ip, "AllocationId": alloc}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "InstanceId", "Tags") {
				return nil, true, nil
			}
			r, err := c.ec2("DescribeAddresses", map[string]any{"PublicIp": []string{phys}})
			if err != nil {
				return nil, false, err
			}
			alloc := r.findText("allocationId")
			if !equal(old["InstanceId"], p["InstanceId"]) {
				if a := r.findText("associationId"); a != "" {
					if _, err := c.ec2("DisassociateAddress", map[string]any{"AssociationId": a}); err != nil {
						return nil, false, err
					}
				}
				if p["InstanceId"] != nil {
					if _, err := c.ec2("AssociateAddress", map[string]any{"AllocationId": alloc, "InstanceId": p["InstanceId"]}); err != nil {
						return nil, false, err
					}
				}
			}
			return nil, false, c.ec2Retag(alloc, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			r, err := c.ec2("DescribeAddresses", map[string]any{"PublicIp": []string{phys}})
			if err != nil {
				return err
			}
			if a := r.findText("associationId"); a != "" {
				if _, err := c.ec2("DisassociateAddress", map[string]any{"AssociationId": a}); err != nil {
					return err
				}
			}
			_, err = c.ec2("ReleaseAddress", map[string]any{"AllocationId": r.findText("allocationId")})
			return err
		},
	})

	register("AWS::EC2::NatGateway", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "SubnetId", "AllocationId", "ConnectivityType", "PrivateIpAddress")
			in["TagSpecification"] = ec2Tags("natgateway", p["Tags"])
			r, err := c.ec2("CreateNatGateway", in)
			if err != nil {
				return "", nil, err
			}
			id := r.findText("natGatewayId")
			// CloudFormation waits until the gateway is available; describing it moves it on.
			for range 20 {
				d, err := c.ec2("DescribeNatGateways", map[string]any{"NatGatewayId": []string{id}})
				if err != nil {
					return id, nil, err
				}
				if d.findText("state") != "pending" {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			return id, map[string]any{"NatGatewayId": id}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Tags") {
				return nil, true, nil
			}
			return nil, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteNatGateway", map[string]any{"NatGatewayId": phys})
			return err
		},
	})

	register("AWS::EC2::SecurityGroup", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["GroupName"]), c.name(255, false))
			in := map[string]any{"GroupName": name, "Description": or(str(p["GroupDescription"]), name), "VpcId": p["VpcId"],
				"TagSpecification": ec2Tags("security-group", p["Tags"])}
			r, err := c.ec2("CreateSecurityGroup", in)
			if err != nil {
				return "", nil, err
			}
			id := r.findText("groupId")
			if err := sgUpdate(c, id, map[string]any{}, p); err != nil {
				c.ec2("DeleteSecurityGroup", map[string]any{"GroupId": id})
				return "", nil, err
			}
			return id, map[string]any{"GroupId": id, "VpcId": or(str(p["VpcId"]), r.findText("vpcId"))}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "SecurityGroupIngress", "SecurityGroupEgress", "Tags") {
				return nil, true, nil
			}
			if err := sgUpdate(c, phys, old, p); err != nil {
				return nil, false, err
			}
			return nil, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteSecurityGroup", map[string]any{"GroupId": phys})
			return err
		},
	})
	register("AWS::EC2::SecurityGroupIngress", sgRule(false))
	register("AWS::EC2::SecurityGroupEgress", sgRule(true))

	register("AWS::EC2::LaunchTemplate", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := map[string]any{"LaunchTemplateName": or(str(p["LaunchTemplateName"]), c.name(128, false)), "VersionDescription": p["VersionDescription"],
				"LaunchTemplateData": ltParams(p["LaunchTemplateData"]), "TagSpecification": ltTags(p)}
			r, err := c.ec2("CreateLaunchTemplate", in)
			if err != nil {
				return "", nil, err
			}
			id := r.findText("launchTemplateId")
			attrs, err := ltAttrs(c, id)
			return id, attrs, err
		},
		// New launch template data is a new version of the same template.
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "LaunchTemplateData", "VersionDescription", "TagSpecifications") {
				return nil, true, nil
			}
			if equal(old["LaunchTemplateData"], p["LaunchTemplateData"]) {
				return nil, false, nil
			}
			if _, err := c.ec2("CreateLaunchTemplateVersion", map[string]any{"LaunchTemplateId": phys, "VersionDescription": p["VersionDescription"],
				"LaunchTemplateData": ltParams(p["LaunchTemplateData"])}); err != nil {
				return nil, false, err
			}
			attrs, err := ltAttrs(c, phys)
			return attrs, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteLaunchTemplate", map[string]any{"LaunchTemplateId": phys})
			return err
		},
	})

	register("AWS::EC2::VPCEndpoint", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "VpcId", "ServiceName", "VpcEndpointType", "IpAddressType", "PrivateDnsEnabled", "DnsOptions",
				"RouteTableIds=RouteTableId", "SubnetIds=SubnetId", "SecurityGroupIds=SecurityGroupId")
			if d, ok := p["PolicyDocument"]; ok {
				in["PolicyDocument"] = jsonString(d)
			}
			in["TagSpecification"] = ec2Tags("vpc-endpoint", p["Tags"])
			r, err := c.ec2("CreateVpcEndpoint", in)
			if err != nil {
				return "", nil, err
			}
			id := r.findText("vpcEndpointId")
			attrs := map[string]any{"Id": id, "CreationTimestamp": r.findText("creationTimestamp"), "DnsEntries": []any{}, "NetworkInterfaceIds": []any{}}
			var dns, enis []any
			for _, d := range r.items("vpcEndpoint", "dnsEntrySet") {
				dns = append(dns, d.text("hostedZoneId")+":"+d.text("dnsName"))
			}
			for _, e := range r.items("vpcEndpoint", "networkInterfaceIdSet") {
				enis = append(enis, e.Text)
			}
			if dns != nil {
				attrs["DnsEntries"] = dns
			}
			if enis != nil {
				attrs["NetworkInterfaceIds"] = enis
			}
			return id, attrs, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "PolicyDocument", "RouteTableIds", "SubnetIds", "SecurityGroupIds", "PrivateDnsEnabled", "Tags") {
				return nil, true, nil
			}
			in := map[string]any{"VpcEndpointId": phys}
			for _, l := range []struct{ prop, add, rm string }{{"RouteTableIds", "AddRouteTableId", "RemoveRouteTableId"},
				{"SubnetIds", "AddSubnetId", "RemoveSubnetId"}, {"SecurityGroupIds", "AddSecurityGroupId", "RemoveSecurityGroupId"}} {
				o, n := list(old[l.prop]), list(p[l.prop])
				if add := minus(n, o); len(add) > 0 {
					in[l.add] = add
				}
				if rm := minus(o, n); len(rm) > 0 {
					in[l.rm] = rm
				}
			}
			if p["PrivateDnsEnabled"] != nil {
				in["PrivateDnsEnabled"] = p["PrivateDnsEnabled"]
			}
			if d, ok := p["PolicyDocument"]; ok {
				in["PolicyDocument"] = jsonString(d)
			} else if old["PolicyDocument"] != nil {
				in["ResetPolicy"] = true
			}
			if _, err := c.ec2("ModifyVpcEndpoint", in); err != nil {
				return nil, false, err
			}
			return nil, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteVpcEndpoints", map[string]any{"VpcEndpointId": []string{phys}})
			return err
		},
	})

	register("AWS::EC2::NetworkAcl", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			r, err := c.ec2("CreateNetworkAcl", map[string]any{"VpcId": p["VpcId"], "TagSpecification": ec2Tags("network-acl", p["Tags"])})
			if err != nil {
				return "", nil, err
			}
			id := r.findText("networkAclId")
			return id, map[string]any{"Id": id}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Tags") {
				return nil, true, nil
			}
			return nil, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteNetworkAcl", map[string]any{"NetworkAclId": phys})
			return err
		},
	})

	aclEntry := func(p map[string]any) map[string]any {
		in := pick(p, "NetworkAclId", "RuleNumber", "Protocol", "RuleAction", "CidrBlock", "Ipv6CidrBlock", "PortRange", "Icmp")
		in["Egress"] = str(p["Egress"]) == "true"
		return in
	}
	register("AWS::EC2::NetworkAclEntry", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			_, err := c.ec2("CreateNetworkAclEntry", aclEntry(p))
			return c.name(64, false), nil, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Protocol", "RuleAction", "CidrBlock", "Ipv6CidrBlock", "PortRange", "Icmp") {
				return nil, true, nil
			}
			_, err := c.ec2("ReplaceNetworkAclEntry", aclEntry(p))
			return nil, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			in := pick(p, "NetworkAclId", "RuleNumber")
			in["Egress"] = str(p["Egress"]) == "true"
			_, err := c.ec2("DeleteNetworkAclEntry", in)
			return err
		},
	})

	// Associating a subnet with an ACL replaces the association it has (the VPC's default one at first).
	register("AWS::EC2::SubnetNetworkAclAssociation", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			subnet := str(p["SubnetId"])
			cur, err := c.ec2("DescribeNetworkAcls", map[string]any{"Filter": filters("association.subnet-id", subnet)})
			if err != nil {
				return "", nil, err
			}
			from := ""
			for _, a := range cur.items("networkAclSet", "item", "associationSet") {
				if a.text("subnetId") == subnet {
					from = a.text("networkAclAssociationId")
				}
			}
			r, err := c.ec2("ReplaceNetworkAclAssociation", map[string]any{"AssociationId": from, "NetworkAclId": p["NetworkAclId"]})
			if err != nil {
				return "", nil, err
			}
			id := r.findText("newAssociationId")
			return id, map[string]any{"AssociationId": id}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			acl, err := c.ec2("DescribeNetworkAcls", map[string]any{"Filter": filters("association.association-id", phys)})
			if err != nil {
				return err
			}
			vpc := acl.findText("vpcId")
			if vpc == "" {
				return nil // the subnet or ACL is already gone
			}
			def, err := c.ec2("DescribeNetworkAcls", map[string]any{"Filter": filters("vpc-id", vpc, "default", "true")})
			if err != nil {
				return err
			}
			_, err = c.ec2("ReplaceNetworkAclAssociation", map[string]any{"AssociationId": phys, "NetworkAclId": def.findText("networkAclId")})
			return err
		},
	})
}

// minus lists what is in a and not in b.
func minus(a, b []string) []string {
	var out []string
	for _, x := range a {
		found := false
		for _, y := range b {
			found = found || x == y
		}
		if !found {
			out = append(out, x)
		}
	}
	return out
}
