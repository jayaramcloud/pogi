package cloudformation

// EC2 flow logs, transit gateways and their attachments, egress-only gateways, prefix lists,
// key pairs, dedicated hosts and VPC CIDR blocks.

func init() {
	register("AWS::EC2::FlowLog", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "ResourceType", "TrafficType", "LogDestinationType", "LogDestination", "LogGroupName", "DeliverLogsPermissionArn", "LogFormat",
				"MaxAggregationInterval", "DestinationOptions")
			in["ResourceId"] = []string{str(p["ResourceId"])}
			in["TagSpecification"] = ec2Tags("vpc-flow-log", p["Tags"])
			r, err := c.ec2("CreateFlowLogs", in)
			if err != nil {
				return "", nil, err
			}
			id := r.text("flowLogIdSet", "item")
			return id, map[string]any{"Id": id}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Tags") {
				return nil, true, nil
			}
			return nil, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteFlowLogs", map[string]any{"FlowLogId": []string{phys}})
			return err
		},
	})

	tgwOptions := []string{"AmazonSideAsn", "AutoAcceptSharedAttachments", "DefaultRouteTableAssociation", "DefaultRouteTablePropagation", "DnsSupport", "MulticastSupport",
		"VpnEcmpSupport", "TransitGatewayCidrBlocks", "SecurityGroupReferencingSupport"}
	register("AWS::EC2::TransitGateway", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := map[string]any{"Description": p["Description"], "Options": pick(p, tgwOptions...), "TagSpecification": ec2Tags("transit-gateway", p["Tags"])}
			r, err := c.ec2("CreateTransitGateway", in)
			if err != nil {
				return "", nil, err
			}
			id := r.findText("transitGatewayId")
			return id, map[string]any{"Id": id, "TransitGatewayArn": r.findText("transitGatewayArn")}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			// the side ASN and the CIDR blocks are fixed when the gateway is made
			if differs(old, p, "Description", "AutoAcceptSharedAttachments", "DefaultRouteTableAssociation", "DefaultRouteTablePropagation", "DnsSupport", "MulticastSupport",
				"VpnEcmpSupport", "SecurityGroupReferencingSupport", "Tags") {
				return nil, true, nil
			}
			opts := pick(p, "AutoAcceptSharedAttachments", "DefaultRouteTableAssociation", "DefaultRouteTablePropagation", "DnsSupport", "MulticastSupport", "VpnEcmpSupport",
				"SecurityGroupReferencingSupport")
			if _, err := c.ec2("ModifyTransitGateway", map[string]any{"TransitGatewayId": phys, "Description": p["Description"], "Options": opts}); err != nil {
				return nil, false, err
			}
			return nil, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteTransitGateway", map[string]any{"TransitGatewayId": phys})
			return err
		},
	})
	register("AWS::EC2::TransitGatewayAttachment", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "TransitGatewayId", "VpcId", "SubnetIds", "Options")
			in["TagSpecification"] = ec2Tags("transit-gateway-attachment", p["Tags"])
			r, err := c.ec2("CreateTransitGatewayVpcAttachment", in)
			if err != nil {
				return "", nil, err
			}
			id := r.findText("transitGatewayAttachmentId")
			return id, map[string]any{"Id": id}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "SubnetIds", "Options", "Tags") {
				return nil, true, nil
			}
			in := map[string]any{"TransitGatewayAttachmentId": phys, "Options": p["Options"]}
			if add := minus(list(p["SubnetIds"]), list(old["SubnetIds"])); len(add) > 0 {
				in["AddSubnetIds"] = add
			}
			if rm := minus(list(old["SubnetIds"]), list(p["SubnetIds"])); len(rm) > 0 {
				in["RemoveSubnetIds"] = rm
			}
			if _, err := c.ec2("ModifyTransitGatewayVpcAttachment", in); err != nil {
				return nil, false, err
			}
			return nil, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteTransitGatewayVpcAttachment", map[string]any{"TransitGatewayAttachmentId": phys})
			return err
		},
	})
	register("AWS::EC2::TransitGatewayPeeringAttachment", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "TransitGatewayId", "PeerTransitGatewayId", "PeerAccountId", "PeerRegion")
			in["TagSpecification"] = ec2Tags("transit-gateway-attachment", p["Tags"])
			r, err := c.ec2("CreateTransitGatewayPeeringAttachment", in)
			if err != nil {
				return "", nil, err
			}
			id := r.findText("transitGatewayAttachmentId")
			return id, map[string]any{"TransitGatewayAttachmentId": id}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Tags") {
				return nil, true, nil
			}
			return nil, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteTransitGatewayPeeringAttachment", map[string]any{"TransitGatewayAttachmentId": phys})
			return err
		},
	})

	register("AWS::EC2::EgressOnlyInternetGateway", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			r, err := c.ec2("CreateEgressOnlyInternetGateway", map[string]any{"VpcId": p["VpcId"]})
			if err != nil {
				return "", nil, err
			}
			id := r.findText("egressOnlyInternetGatewayId")
			return id, map[string]any{"Id": id}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteEgressOnlyInternetGateway", map[string]any{"EgressOnlyInternetGatewayId": phys})
			return err
		},
	})

	entries := func(v any) []map[string]string {
		var out []map[string]string
		for _, e := range listMaps(v) {
			out = append(out, map[string]string{"Cidr": str(e["Cidr"]), "Description": str(e["Description"])})
		}
		return out
	}
	register("AWS::EC2::PrefixList", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "PrefixListName", "AddressFamily", "MaxEntries")
			in["Entry"] = entries(p["Entries"])
			in["TagSpecification"] = ec2Tags("prefix-list", p["Tags"])
			r, err := c.ec2("CreateManagedPrefixList", in)
			if err != nil {
				return "", nil, err
			}
			id := r.findText("prefixListId")
			return id, map[string]any{"PrefixListId": id, "Arn": r.findText("prefixListArn"), "OwnerId": r.findText("ownerId"), "Version": r.findText("version")}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Entries", "MaxEntries", "Tags") {
				return nil, true, nil
			}
			in := map[string]any{"PrefixListId": phys}
			oe, ne := map[string]bool{}, map[string]bool{}
			var add, rm []map[string]string
			for _, e := range entries(old["Entries"]) {
				oe[e["Cidr"]] = true
			}
			for _, e := range entries(p["Entries"]) {
				ne[e["Cidr"]] = true
				if !oe[e["Cidr"]] {
					add = append(add, e)
				}
			}
			for _, e := range entries(old["Entries"]) {
				if !ne[e["Cidr"]] {
					rm = append(rm, map[string]string{"Cidr": e["Cidr"]})
				}
			}
			if len(add)+len(rm) > 0 || !equal(old["MaxEntries"], p["MaxEntries"]) {
				if len(add) > 0 {
					in["AddEntry"] = add
				}
				if len(rm) > 0 {
					in["RemoveEntry"] = rm
				}
				if p["MaxEntries"] != nil {
					in["MaxEntries"] = p["MaxEntries"]
				}
				if _, err := c.ec2("ModifyManagedPrefixList", in); err != nil {
					return nil, false, err
				}
			}
			return nil, false, c.ec2Retag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteManagedPrefixList", map[string]any{"PrefixListId": phys})
			return err
		},
	})

	register("AWS::EC2::KeyPair", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["KeyName"]), c.name(255, false))
			var r *xnode
			var err error
			if p["PublicKeyMaterial"] != nil {
				r, err = c.ec2("ImportKeyPair", map[string]any{"KeyName": name, "PublicKeyMaterial": p["PublicKeyMaterial"], "TagSpecification": ec2Tags("key-pair", p["Tags"])})
			} else {
				in := map[string]any{"KeyName": name, "KeyType": p["KeyType"], "TagSpecification": ec2Tags("key-pair", p["Tags"])}
				r, err = c.ec2("CreateKeyPair", in)
			}
			if err != nil {
				return "", nil, err
			}
			return name, map[string]any{"KeyPairId": r.findText("keyPairId"), "KeyFingerprint": r.findText("keyFingerprint")}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DeleteKeyPair", map[string]any{"KeyName": phys})
			return err
		},
	})

	register("AWS::EC2::Host", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "AvailabilityZone", "InstanceType", "InstanceFamily", "AutoPlacement", "HostRecovery", "HostMaintenance")
			in["Quantity"] = 1
			in["TagSpecification"] = ec2Tags("dedicated-host", p["HostTags"])
			r, err := c.ec2("AllocateHosts", in)
			if err != nil {
				return "", nil, err
			}
			return r.text("hostIdSet", "item"), nil, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "AutoPlacement", "HostRecovery", "HostMaintenance", "InstanceType", "HostTags") {
				return nil, true, nil
			}
			in := pick(p, "AutoPlacement", "HostRecovery", "HostMaintenance", "InstanceType")
			in["HostId"] = []string{phys}
			if _, err := c.ec2("ModifyHosts", in); err != nil {
				return nil, false, err
			}
			return nil, false, c.ec2Retag(phys, old["HostTags"], p["HostTags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("ReleaseHosts", map[string]any{"HostId": []string{phys}})
			return err
		},
	})

	register("AWS::EC2::VPCCidrBlock", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			r, err := c.ec2("AssociateVpcCidrBlock", pick(p, "VpcId", "CidrBlock"))
			if err != nil {
				return "", nil, err
			}
			id := r.findText("associationId")
			return id, map[string]any{"Id": id}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.ec2("DisassociateVpcCidrBlock", map[string]any{"AssociationId": phys})
			return err
		},
	})
}
