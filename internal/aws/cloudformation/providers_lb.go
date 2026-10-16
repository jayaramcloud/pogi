package cloudformation

import (
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Load balancers (ELB v2), Auto Scaling groups and RDS clusters: the services
// that speak the Query protocol, where a list is sent as Name.N.

func (c *pctx) elb(action string, params map[string]any) (*xnode, error) {
	return c.query("elasticloadbalancing", action, params)
}

// elbName is the name CloudFormation makes for a load balancer or target group: at most 32 letters, digits and hyphens.
func elbName(c *pctx) string {
	base := c.stack + "-" + c.logical
	if len(base) > 19 {
		base = base[:19]
	}
	return strings.Trim(base, "-") + "-" + core.RandString(12, core.UpperNum)
}

// elbRetag makes a resource's tags match the new ones.
func (c *pctx) elbRetag(arn string, old, now any) error {
	o, n := tagMap(old), tagMap(now)
	var gone []string
	for _, k := range core.SortedKeys(o) {
		if _, ok := n[k]; !ok {
			gone = append(gone, k)
		}
	}
	var add []map[string]string
	for _, k := range core.SortedKeys(n) {
		if v, ok := o[k]; !ok || v != n[k] {
			add = append(add, map[string]string{"Key": k, "Value": n[k]})
		}
	}
	if len(gone) > 0 {
		if _, err := c.elb("RemoveTags", map[string]any{"ResourceArns": []string{arn}, "TagKeys": gone}); err != nil {
			return err
		}
	}
	if len(add) > 0 {
		_, err := c.elb("AddTags", map[string]any{"ResourceArns": []string{arn}, "Tags": add})
		return err
	}
	return nil
}

// elbAttrs sets the attributes a template lists, when it lists any.
func (c *pctx) elbAttrs(action, idKey, arn string, attrs any) error {
	if l, _ := attrs.([]any); len(l) > 0 {
		_, err := c.elb(action, map[string]any{idKey: arn, "Attributes": l})
		return err
	}
	return nil
}

func lbAttrs(c *pctx, arn string) (map[string]any, error) {
	r, err := c.elb("DescribeLoadBalancers", map[string]any{"LoadBalancerArns": []string{arn}})
	if err != nil {
		return nil, err
	}
	var sgs []any
	for _, m := range r.items("DescribeLoadBalancersResult", "LoadBalancers", "member", "SecurityGroups") {
		sgs = append(sgs, strings.TrimSpace(m.Text))
	}
	if sgs == nil {
		sgs = []any{}
	}
	return map[string]any{"DNSName": r.findText("DNSName"), "CanonicalHostedZoneID": r.findText("CanonicalHostedZoneId"),
		"LoadBalancerName": r.findText("LoadBalancerName"), "LoadBalancerFullName": arn[strings.Index(arn, "loadbalancer/")+len("loadbalancer/"):],
		"SecurityGroups": sgs}, nil
}

func tgAttrs(c *pctx, arn string) (map[string]any, error) {
	r, err := c.elb("DescribeTargetGroups", map[string]any{"TargetGroupArns": []string{arn}})
	if err != nil {
		return nil, err
	}
	var lbs []any
	for _, m := range r.items("DescribeTargetGroupsResult", "TargetGroups", "member", "LoadBalancerArns") {
		lbs = append(lbs, strings.TrimSpace(m.Text))
	}
	if lbs == nil {
		lbs = []any{}
	}
	return map[string]any{"TargetGroupArn": arn, "TargetGroupName": r.findText("TargetGroupName"), "TargetGroupFullName": arn[strings.Index(arn, "targetgroup/"):],
		"LoadBalancerArns": lbs}, nil
}

// targetsOf lists the targets of a template as Id[:Port] keys with their API form.
func targetsOf(v any) map[string]any {
	out := map[string]any{}
	for _, t := range listMaps(v) {
		out[str(t["Id"])+":"+str(t["Port"])] = pick(t, "Id", "Port", "AvailabilityZone")
	}
	return out
}

func init() {
	register("AWS::ElasticLoadBalancingV2::LoadBalancer", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "Type", "Scheme", "IpAddressType", "Subnets", "SecurityGroups", "SubnetMappings", "Tags")
			in["Name"] = or(str(p["Name"]), elbName(c))
			r, err := c.elb("CreateLoadBalancer", in)
			if err != nil {
				return "", nil, err
			}
			arn := r.findText("LoadBalancerArn")
			if err := c.elbAttrs("ModifyLoadBalancerAttributes", "LoadBalancerArn", arn, p["LoadBalancerAttributes"]); err != nil {
				c.elb("DeleteLoadBalancer", map[string]any{"LoadBalancerArn": arn})
				return "", nil, err
			}
			attrs, err := lbAttrs(c, arn)
			return arn, attrs, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Subnets", "SubnetMappings", "SecurityGroups", "IpAddressType", "LoadBalancerAttributes", "Tags") {
				return nil, true, nil
			}
			if !equal(old["Subnets"], p["Subnets"]) || !equal(old["SubnetMappings"], p["SubnetMappings"]) {
				if _, err := c.elb("SetSubnets", pick(map[string]any{"LoadBalancerArn": phys, "Subnets": p["Subnets"], "SubnetMappings": p["SubnetMappings"]},
					"LoadBalancerArn", "Subnets", "SubnetMappings")); err != nil {
					return nil, false, err
				}
			}
			if !equal(old["SecurityGroups"], p["SecurityGroups"]) {
				if _, err := c.elb("SetSecurityGroups", map[string]any{"LoadBalancerArn": phys, "SecurityGroups": p["SecurityGroups"]}); err != nil {
					return nil, false, err
				}
			}
			if !equal(old["IpAddressType"], p["IpAddressType"]) && p["IpAddressType"] != nil {
				if _, err := c.elb("SetIpAddressType", map[string]any{"LoadBalancerArn": phys, "IpAddressType": p["IpAddressType"]}); err != nil {
					return nil, false, err
				}
			}
			if err := c.elbAttrs("ModifyLoadBalancerAttributes", "LoadBalancerArn", phys, p["LoadBalancerAttributes"]); err != nil {
				return nil, false, err
			}
			if err := c.elbRetag(phys, old["Tags"], p["Tags"]); err != nil {
				return nil, false, err
			}
			attrs, err := lbAttrs(c, phys)
			return attrs, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.elb("DeleteLoadBalancer", map[string]any{"LoadBalancerArn": phys})
			return err
		},
	})

	tgHealth := []string{"HealthCheckEnabled", "HealthCheckIntervalSeconds", "HealthCheckPath", "HealthCheckPort", "HealthCheckProtocol", "HealthCheckTimeoutSeconds",
		"HealthyThresholdCount", "UnhealthyThresholdCount", "Matcher"}
	register("AWS::ElasticLoadBalancingV2::TargetGroup", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, append([]string{"Port", "Protocol", "ProtocolVersion", "VpcId", "TargetType", "IpAddressType", "Tags"}, tgHealth...)...)
			in["Name"] = or(str(p["Name"]), elbName(c))
			r, err := c.elb("CreateTargetGroup", in)
			if err != nil {
				return "", nil, err
			}
			arn := r.findText("TargetGroupArn")
			fail := func(err error) (string, map[string]any, error) {
				c.elb("DeleteTargetGroup", map[string]any{"TargetGroupArn": arn})
				return "", nil, err
			}
			if err := c.elbAttrs("ModifyTargetGroupAttributes", "TargetGroupArn", arn, p["TargetGroupAttributes"]); err != nil {
				return fail(err)
			}
			if ts := listMaps(p["Targets"]); len(ts) > 0 {
				if _, err := c.elb("RegisterTargets", map[string]any{"TargetGroupArn": arn, "Targets": p["Targets"]}); err != nil {
					return fail(err)
				}
			}
			attrs, err := tgAttrs(c, arn)
			return arn, attrs, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, append([]string{"TargetGroupAttributes", "Targets", "Tags"}, tgHealth...)...) {
				return nil, true, nil
			}
			if h := pick(p, tgHealth...); differs(pick(old, tgHealth...), h) {
				h["TargetGroupArn"] = phys
				if _, err := c.elb("ModifyTargetGroup", h); err != nil {
					return nil, false, err
				}
			}
			if err := c.elbAttrs("ModifyTargetGroupAttributes", "TargetGroupArn", phys, p["TargetGroupAttributes"]); err != nil {
				return nil, false, err
			}
			ot, nt := targetsOf(old["Targets"]), targetsOf(p["Targets"])
			var add, rm []any
			for k, t := range nt {
				if _, ok := ot[k]; !ok {
					add = append(add, t)
				}
			}
			for k, t := range ot {
				if _, ok := nt[k]; !ok {
					rm = append(rm, t)
				}
			}
			if len(rm) > 0 {
				if _, err := c.elb("DeregisterTargets", map[string]any{"TargetGroupArn": phys, "Targets": rm}); err != nil {
					return nil, false, err
				}
			}
			if len(add) > 0 {
				if _, err := c.elb("RegisterTargets", map[string]any{"TargetGroupArn": phys, "Targets": add}); err != nil {
					return nil, false, err
				}
			}
			return nil, false, c.elbRetag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.elb("DeleteTargetGroup", map[string]any{"TargetGroupArn": phys})
			return err
		},
	})

	listenerKeys := []string{"Port", "Protocol", "SslPolicy", "Certificates", "DefaultActions", "AlpnPolicy", "MutualAuthentication"}
	register("AWS::ElasticLoadBalancingV2::Listener", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, append([]string{"LoadBalancerArn", "Tags"}, listenerKeys...)...)
			r, err := c.elb("CreateListener", in)
			if err != nil {
				return "", nil, err
			}
			arn := r.findText("ListenerArn")
			if err := c.elbAttrs("ModifyListenerAttributes", "ListenerArn", arn, p["ListenerAttributes"]); err != nil {
				c.elb("DeleteListener", map[string]any{"ListenerArn": arn})
				return "", nil, err
			}
			return arn, map[string]any{"ListenerArn": arn}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, append([]string{"ListenerAttributes", "Tags"}, listenerKeys...)...) {
				return nil, true, nil
			}
			if differs(pick(old, listenerKeys...), pick(p, listenerKeys...)) {
				in := pick(p, listenerKeys...)
				in["ListenerArn"] = phys
				if _, err := c.elb("ModifyListener", in); err != nil {
					return nil, false, err
				}
			}
			if err := c.elbAttrs("ModifyListenerAttributes", "ListenerArn", phys, p["ListenerAttributes"]); err != nil {
				return nil, false, err
			}
			return nil, false, c.elbRetag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.elb("DeleteListener", map[string]any{"ListenerArn": phys})
			return err
		},
	})

	register("AWS::ElasticLoadBalancingV2::ListenerRule", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			r, err := c.elb("CreateRule", pick(p, "ListenerArn", "Priority", "Conditions", "Actions"))
			if err != nil {
				return "", nil, err
			}
			arn := r.findText("RuleArn")
			return arn, map[string]any{"RuleArn": arn, "IsDefault": false}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Priority", "Conditions", "Actions") {
				return nil, true, nil
			}
			if differs(pick(old, "Conditions", "Actions"), pick(p, "Conditions", "Actions")) {
				in := pick(p, "Conditions", "Actions")
				in["RuleArn"] = phys
				if _, err := c.elb("ModifyRule", in); err != nil {
					return nil, false, err
				}
			}
			if !equal(old["Priority"], p["Priority"]) {
				if _, err := c.elb("SetRulePriorities", map[string]any{"RulePriorities": []any{map[string]any{"RuleArn": phys, "Priority": p["Priority"]}}}); err != nil {
					return nil, false, err
				}
			}
			return nil, false, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.elb("DeleteRule", map[string]any{"RuleArn": phys})
			return err
		},
	})

	// ---- Auto Scaling ----
	asgKeys := []string{"MinSize", "MaxSize", "DesiredCapacity", "HealthCheckType", "HealthCheckGracePeriod", "LaunchTemplate", "MixedInstancesPolicy",
		"LaunchConfigurationName", "TerminationPolicies", "NewInstancesProtectedFromScaleIn", "MaxInstanceLifetime", "CapacityRebalance", "DefaultInstanceWarmup",
		"PlacementGroup", "ServiceLinkedRoleARN", "Context", "AvailabilityZones"}
	// asgInput is the API form of a template's group properties.
	asgInput := func(p map[string]any) map[string]any {
		in := pick(p, asgKeys...)
		if v, ok := p["Cooldown"]; ok {
			in["DefaultCooldown"] = v
		}
		if z := list(p["VPCZoneIdentifier"]); len(z) > 0 {
			in["VPCZoneIdentifier"] = strings.Join(z, ",")
		}
		return in
	}
	asgTags := func(name string, tags any) []any {
		var out []any
		for _, t := range listMaps(tags) {
			out = append(out, map[string]any{"ResourceId": name, "ResourceType": "auto-scaling-group", "Key": t["Key"], "Value": t["Value"],
				"PropagateAtLaunch": or2(t["PropagateAtLaunch"], false)})
		}
		return out
	}
	register("AWS::AutoScaling::AutoScalingGroup", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["AutoScalingGroupName"]), c.name(255, false))
			in := asgInput(p)
			in["AutoScalingGroupName"] = name
			for k, v := range pick(p, "TargetGroupARNs", "LoadBalancerNames", "LifecycleHookSpecificationList") {
				in[k] = v
			}
			if t := asgTags(name, p["Tags"]); len(t) > 0 {
				in["Tags"] = t
			}
			if _, err := c.query("autoscaling", "CreateAutoScalingGroup", in); err != nil {
				return "", nil, err
			}
			return name, map[string]any{"AutoScalingGroupName": name}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, append([]string{"Cooldown", "VPCZoneIdentifier", "TargetGroupARNs", "Tags"}, asgKeys...)...) {
				return nil, true, nil
			}
			in := asgInput(p)
			in["AutoScalingGroupName"] = phys
			if _, err := c.query("autoscaling", "UpdateAutoScalingGroup", in); err != nil {
				return nil, false, err
			}
			ot, nt := list(old["TargetGroupARNs"]), list(p["TargetGroupARNs"])
			for _, s := range []struct {
				action string
				arns   []string
			}{{"DetachLoadBalancerTargetGroups", minus(ot, nt)}, {"AttachLoadBalancerTargetGroups", minus(nt, ot)}} {
				if len(s.arns) > 0 {
					if _, err := c.query("autoscaling", s.action, map[string]any{"AutoScalingGroupName": phys, "TargetGroupARNs": s.arns}); err != nil {
						return nil, false, err
					}
				}
			}
			if !equal(old["Tags"], p["Tags"]) {
				gone := map[string]bool{}
				for _, t := range listMaps(old["Tags"]) {
					gone[str(t["Key"])] = true
				}
				for _, t := range listMaps(p["Tags"]) {
					delete(gone, str(t["Key"]))
				}
				var rm []any
				for _, t := range asgTags(phys, old["Tags"]) {
					if gone[str(t.(map[string]any)["Key"])] {
						rm = append(rm, t)
					}
				}
				if len(rm) > 0 {
					if _, err := c.query("autoscaling", "DeleteTags", map[string]any{"Tags": rm}); err != nil {
						return nil, false, err
					}
				}
				if t := asgTags(phys, p["Tags"]); len(t) > 0 {
					if _, err := c.query("autoscaling", "CreateOrUpdateTags", map[string]any{"Tags": t}); err != nil {
						return nil, false, err
					}
				}
			}
			return nil, false, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.query("autoscaling", "DeleteAutoScalingGroup", map[string]any{"AutoScalingGroupName": phys, "ForceDelete": true})
			return err
		},
	})

	// ---- RDS clusters ----
	clusterCreate := []string{"Engine", "EngineVersion", "EngineMode", "DatabaseName", "MasterUsername", "MasterUserPassword", "DBClusterParameterGroupName", "DBSubnetGroupName",
		"KmsKeyId", "StorageType", "NetworkType", "DBClusterInstanceClass", "PreferredBackupWindow", "PreferredMaintenanceWindow", "Port", "AllocatedStorage",
		"BackupRetentionPeriod", "StorageEncrypted", "DeletionProtection", "CopyTagsToSnapshot", "EnableIAMDatabaseAuthentication", "EnableHttpEndpoint",
		"VpcSecurityGroupIds", "AvailabilityZones", "EnableCloudwatchLogsExports", "ServerlessV2ScalingConfiguration"}
	// the members ModifyDBCluster takes: the rest are fixed when the cluster is made
	clusterModify := []string{"EngineVersion", "MasterUserPassword", "DBClusterParameterGroupName", "PreferredBackupWindow", "PreferredMaintenanceWindow", "StorageType",
		"DBClusterInstanceClass", "AllocatedStorage", "BackupRetentionPeriod", "DeletionProtection", "CopyTagsToSnapshot", "EnableIAMDatabaseAuthentication",
		"EnableHttpEndpoint", "VpcSecurityGroupIds", "ServerlessV2ScalingConfiguration"}
	clusterAttrs := func(c *pctx, id string) (map[string]any, error) {
		r, err := c.query("rds", "DescribeDBClusters", map[string]any{"DBClusterIdentifier": id})
		if err != nil {
			return nil, err
		}
		return map[string]any{"Endpoint.Address": r.findText("Endpoint"), "Endpoint.Port": r.findText("Port"), "ReadEndpoint.Address": r.findText("ReaderEndpoint"),
			"DBClusterArn": r.findText("DBClusterArn"), "DBClusterResourceId": r.findText("DbClusterResourceId")}, nil
	}
	register("AWS::RDS::DBCluster", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			id := or(str(p["DBClusterIdentifier"]), c.name(63, true))
			in := pick(p, clusterCreate...)
			in["DBClusterIdentifier"] = id
			in["Tags"] = p["Tags"]
			if p["SourceDBClusterIdentifier"] != nil || p["SnapshotIdentifier"] != nil {
				return "", nil, validation("pogi does not support restoring an AWS::RDS::DBCluster from a snapshot or a source cluster yet")
			}
			if _, err := c.query("rds", "CreateDBCluster", in); err != nil {
				return "", nil, err
			}
			attrs, err := clusterAttrs(c, id)
			return id, attrs, err
		},
		// Engine, EngineMode, StorageEncrypted, KmsKeyId, the name, the subnet group and the zones are fixed when a cluster is made: changing one replaces it.
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, append([]string{"EnableCloudwatchLogsExports", "Tags"}, clusterModify...)...) {
				return nil, true, nil
			}
			in := pick(p, clusterModify...)
			ol, nl := list(old["EnableCloudwatchLogsExports"]), list(p["EnableCloudwatchLogsExports"])
			if add, rm := minus(nl, ol), minus(ol, nl); len(add)+len(rm) > 0 {
				in["CloudwatchLogsExportConfiguration"] = map[string]any{"EnableLogTypes": add, "DisableLogTypes": rm}
			}
			in["DBClusterIdentifier"], in["ApplyImmediately"] = phys, true
			if len(in) > 2 {
				if _, err := c.query("rds", "ModifyDBCluster", in); err != nil {
					return nil, false, err
				}
			}
			attrs, err := clusterAttrs(c, phys)
			if err != nil {
				return nil, false, err
			}
			return attrs, false, rdsRetag(c, str(attrs["DBClusterArn"]), old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.query("rds", "DeleteDBCluster", map[string]any{"DBClusterIdentifier": phys, "SkipFinalSnapshot": true})
			return err
		},
	})
}

// rdsRetag makes the tags of an RDS resource match the new ones.
func rdsRetag(c *pctx, arn string, old, now any) error { return retag(c, "rds", arn, old, now) }

// retag makes the tags of a resource of an RDS-style Query service (RDS, ElastiCache) match the new ones.
func retag(c *pctx, sign, arn string, old, now any) error {
	o, n := tagMap(old), tagMap(now)
	var gone []string
	for _, k := range core.SortedKeys(o) {
		if _, ok := n[k]; !ok {
			gone = append(gone, k)
		}
	}
	var add []map[string]string
	for _, k := range core.SortedKeys(n) {
		if v, ok := o[k]; !ok || v != n[k] {
			add = append(add, map[string]string{"Key": k, "Value": n[k]})
		}
	}
	if len(gone) > 0 {
		if _, err := c.query(sign, "RemoveTagsFromResource", map[string]any{"ResourceName": arn, "TagKeys": gone}); err != nil {
			return err
		}
	}
	if len(add) > 0 {
		_, err := c.query(sign, "AddTagsToResource", map[string]any{"ResourceName": arn, "Tags": add})
		return err
	}
	return nil
}
