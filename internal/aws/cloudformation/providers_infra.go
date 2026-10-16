package cloudformation

import (
	"strings"
	"time"
)

// Cloud Map, CloudTrail, CodeBuild, CodePipeline, CodeDeploy, MemoryDB and Verified Permissions.

const (
	sdT       = "Route53AutoNaming_v20170314"
	trailT    = "CloudTrail_20131101"
	buildT    = "CodeBuild_20161006"
	pipelineT = "CodePipeline_20150709"
	deployT   = "CodeDeploy_20141006"
	memT      = "AmazonMemoryDB"
	avpT      = "VerifiedPermissions"
)

// camelKeep is camel with the members named in keep left as they are (the API spells them with a capital).
func camelKeep(v any, keep ...string) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, e := range x {
			if !contains(keep, k) {
				k = lowerFirst(k)
			}
			out[k] = camelKeep(e, keep...)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = camelKeep(e, keep...)
		}
		return out
	}
	return v
}

func init() {
	// ---- Cloud Map ----
	namespace := func(op string) *provider {
		return &provider{
			create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
				in := pick(p, "Name", "Vpc", "Description", "Properties")
				if t := tagList(p["Tags"]); len(t) > 0 {
					in["Tags"] = t
				}
				var out struct{ OperationId string }
				if err := c.call(sdT, op, in, &out); err != nil {
					return "", nil, err
				}
				var id string
				var oerr error
				waitFor(30*time.Second, func() bool {
					var o struct {
						Operation struct {
							Status, ErrorMessage string
							Targets              map[string]string
						}
					}
					if oerr = c.call(sdT, "GetOperation", map[string]any{"OperationId": out.OperationId}, &o); oerr != nil {
						return true
					}
					id = o.Operation.Targets["NAMESPACE"]
					if o.Operation.Status == "FAIL" {
						oerr = validation("%s", o.Operation.ErrorMessage)
					}
					return o.Operation.Status == "SUCCESS" || o.Operation.Status == "FAIL"
				})
				if oerr != nil {
					return "", nil, oerr
				}
				var ns struct {
					Namespace struct {
						Arn        string
						Properties struct{ DnsProperties struct{ HostedZoneId string } }
					}
				}
				if err := c.call(sdT, "GetNamespace", map[string]any{"Id": id}, &ns); err != nil {
					return "", nil, err
				}
				return id, map[string]any{"Id": id, "Arn": ns.Namespace.Arn, "HostedZoneId": ns.Namespace.Properties.DnsProperties.HostedZoneId}, nil
			},
			del: func(c *pctx, phys string, p map[string]any) error {
				return c.call(sdT, "DeleteNamespace", map[string]any{"Id": phys}, nil)
			},
		}
	}
	register("AWS::ServiceDiscovery::PrivateDnsNamespace", namespace("CreatePrivateDnsNamespace"))
	register("AWS::ServiceDiscovery::PublicDnsNamespace", namespace("CreatePublicDnsNamespace"))
	register("AWS::ServiceDiscovery::HttpNamespace", namespace("CreateHttpNamespace"))
	register("AWS::ServiceDiscovery::Service", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "Name", "NamespaceId", "DnsConfig", "HealthCheckConfig", "HealthCheckCustomConfig", "Description", "Type")
			if in["Name"] == nil {
				in["Name"] = c.name(64, true)
			}
			if t := tagList(p["Tags"]); len(t) > 0 {
				in["Tags"] = t
			}
			var out struct {
				Service struct{ Id, Arn, Name string }
			}
			if err := c.call(sdT, "CreateService", in, &out); err != nil {
				return "", nil, err
			}
			s := out.Service
			return s.Id, map[string]any{"Id": s.Id, "Arn": s.Arn, "Name": s.Name}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Description", "DnsConfig", "HealthCheckConfig", "Tags") {
				return nil, true, nil
			}
			return nil, false, c.call(sdT, "UpdateService", map[string]any{"Id": phys, "Service": pick(p, "Description", "DnsConfig", "HealthCheckConfig")}, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(sdT, "DeleteService", map[string]any{"Id": phys}, nil)
		},
	})
	register("AWS::ServiceDiscovery::Instance", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			id := or(str(p["InstanceId"]), c.name(64, true))
			return id, nil, c.call(sdT, "RegisterInstance", map[string]any{"ServiceId": p["ServiceId"], "InstanceId": id, "Attributes": p["InstanceAttributes"]}, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(sdT, "DeregisterInstance", map[string]any{"ServiceId": p["ServiceId"], "InstanceId": phys}, nil)
		},
	})

	// ---- CloudTrail ----
	trailLogging := func(c *pctx, name string, p map[string]any) error {
		op := "StopLogging"
		if str(p["IsLogging"]) == "true" {
			op = "StartLogging"
		}
		return c.call(trailT, op, map[string]any{"Name": name}, nil)
	}
	trailSelectors := func(c *pctx, name string, p map[string]any) error {
		if s := p["EventSelectors"]; s != nil {
			if err := c.call(trailT, "PutEventSelectors", map[string]any{"TrailName": name, "EventSelectors": s}, nil); err != nil {
				return err
			}
		}
		if s := p["InsightSelectors"]; s != nil {
			return c.call(trailT, "PutInsightSelectors", map[string]any{"TrailName": name, "InsightSelectors": s}, nil)
		}
		return nil
	}
	trailKeys := []string{"S3BucketName", "S3KeyPrefix", "SnsTopicName", "IncludeGlobalServiceEvents", "IsMultiRegionTrail", "EnableLogFileValidation",
		"CloudWatchLogsLogGroupArn", "CloudWatchLogsRoleArn", "KMSKeyId=KmsKeyId", "IsOrganizationTrail"}
	register("AWS::CloudTrail::Trail", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["TrailName"]), c.name(128, false))
			in := pick(p, trailKeys...)
			in["Name"] = name
			if t := tagList(p["Tags"]); len(t) > 0 {
				in["TagsList"] = t
			}
			var out struct{ TrailARN, SnsTopicARN string }
			if err := c.call(trailT, "CreateTrail", in, &out); err != nil {
				return "", nil, err
			}
			if err := trailSelectors(c, name, p); err != nil {
				c.call(trailT, "DeleteTrail", map[string]any{"Name": name}, nil)
				return "", nil, err
			}
			if err := trailLogging(c, name, p); err != nil {
				return "", nil, err
			}
			return name, map[string]any{"Arn": out.TrailARN, "SnsTopicArn": out.SnsTopicARN}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["TrailName"]) != str(p["TrailName"]) {
				return nil, true, nil
			}
			in := pick(p, trailKeys...)
			in["Name"] = phys
			var out struct{ TrailARN, SnsTopicARN string }
			if err := c.call(trailT, "UpdateTrail", in, &out); err != nil {
				return nil, false, err
			}
			if err := trailSelectors(c, phys, p); err != nil {
				return nil, false, err
			}
			return map[string]any{"Arn": out.TrailARN, "SnsTopicArn": out.SnsTopicARN}, false, trailLogging(c, phys, p)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(trailT, "DeleteTrail", map[string]any{"Name": phys}, nil)
		},
	})

	// ---- CodeBuild ----
	projectBody := func(p map[string]any) map[string]any {
		in := camel(p, false).(map[string]any)
		if src, ok := in["source"].(map[string]any); ok {
			if b, ok := src["buildSpec"]; ok {
				src["buildspec"] = b
				delete(src, "buildSpec")
			}
		}
		return in
	}
	register("AWS::CodeBuild::Project", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := projectBody(p)
			name := or(str(p["Name"]), c.name(255, false))
			in["name"] = name
			var out struct{ Project struct{ Arn string } }
			if err := c.call(buildT, "CreateProject", in, &out); err != nil {
				return "", nil, err
			}
			return name, map[string]any{"Arn": out.Project.Arn}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["Name"]) != str(p["Name"]) {
				return nil, true, nil
			}
			in := projectBody(p)
			in["name"] = phys
			var out struct{ Project struct{ Arn string } }
			err := c.call(buildT, "UpdateProject", in, &out)
			return map[string]any{"Arn": out.Project.Arn}, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(buildT, "DeleteProject", map[string]any{"name": phys}, nil)
		},
	})

	// ---- CodePipeline ----
	pipelineBody := func(p map[string]any, name string) map[string]any {
		pl := camel(pick(p, "RoleArn", "ArtifactStore", "ArtifactStores", "Stages", "RestartExecutionOnUpdate", "Variables", "Triggers"), false).(map[string]any)
		// an action's configuration holds the user's own names and string values
		stages, _ := pl["stages"].([]any)
		for i, st := range listMaps(p["Stages"]) {
			acts := listMaps(st["Actions"])
			out, _ := stages[i].(map[string]any)["actions"].([]any)
			for j, a := range acts {
				if cfg, ok := a["Configuration"].(map[string]any); ok {
					m := map[string]any{}
					for k, v := range cfg {
						m[k] = str(v)
					}
					out[j].(map[string]any)["configuration"] = m
				}
			}
		}
		pl["name"] = name
		return pl
	}
	register("AWS::CodePipeline::Pipeline", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["Name"]), c.name(100, false))
			in := map[string]any{"pipeline": pipelineBody(p, name)}
			if t := tagList(p["Tags"]); len(t) > 0 {
				in["tags"] = camel(any(toAny(t)), false)
			}
			var out struct{ Pipeline struct{ Version int } }
			if err := c.call(pipelineT, "CreatePipeline", in, &out); err != nil {
				return "", nil, err
			}
			return name, map[string]any{"Version": out.Pipeline.Version}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["Name"]) != str(p["Name"]) {
				return nil, true, nil
			}
			var out struct{ Pipeline struct{ Version int } }
			err := c.call(pipelineT, "UpdatePipeline", map[string]any{"pipeline": pipelineBody(p, phys)}, &out)
			return map[string]any{"Version": out.Pipeline.Version}, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(pipelineT, "DeletePipeline", map[string]any{"name": phys}, nil)
		},
	})

	// ---- CodeDeploy ----
	register("AWS::CodeDeploy::Application", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["ApplicationName"]), c.name(100, false))
			in := map[string]any{"applicationName": name, "computePlatform": p["ComputePlatform"]}
			if t := tagList(p["Tags"]); len(t) > 0 {
				in["tags"] = t
			}
			return name, nil, c.call(deployT, "CreateApplication", in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(deployT, "DeleteApplication", map[string]any{"applicationName": phys}, nil)
		},
	})
	register("AWS::CodeDeploy::DeploymentConfig", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["DeploymentConfigName"]), c.name(100, false))
			in := map[string]any{}
			for k, v := range p {
				in[k] = v
			}
			in["DeploymentConfigName"] = name
			return name, nil, c.call(deployT, "CreateDeploymentConfig", in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(deployT, "DeleteDeploymentConfig", map[string]any{"deploymentConfigName": phys}, nil)
		},
	})
	groupBody := func(p map[string]any) map[string]any {
		in := camelKeep(p, "Key", "Value", "Type").(map[string]any)
		if t, ok := p["Tags"]; ok { // tags of the group itself
			in["tags"] = tagList(t)
		}
		return in
	}
	register("AWS::CodeDeploy::DeploymentGroup", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := groupBody(p)
			name := or(str(p["DeploymentGroupName"]), c.name(100, false))
			in["deploymentGroupName"] = name
			var out struct{ DeploymentGroupId string }
			if err := c.call(deployT, "CreateDeploymentGroup", in, &out); err != nil {
				return "", nil, err
			}
			// Ref of a deployment group is its name (a pipeline's deploy action takes it as one)
			return name, map[string]any{"Id": out.DeploymentGroupId}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(pick(old, "ApplicationName", "DeploymentGroupName"), pick(p, "ApplicationName", "DeploymentGroupName")) {
				return nil, true, nil
			}
			in := groupBody(p)
			in["currentDeploymentGroupName"] = or(str(p["DeploymentGroupName"]), str(old["DeploymentGroupName"]))
			delete(in, "deploymentGroupName")
			return nil, false, c.call(deployT, "UpdateDeploymentGroup", in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(deployT, "DeleteDeploymentGroup", map[string]any{"applicationName": p["ApplicationName"], "deploymentGroupName": phys}, nil)
		},
	})

	// ---- MemoryDB ----
	register("AWS::MemoryDB::SubnetGroup", simple(spec{target: memT, create: "CreateSubnetGroup", update: "UpdateSubnetGroup", del: "DeleteSubnetGroup", name: "SubnetGroupName", max: 255, lower: true,
		inPlace: []string{"Description", "SubnetIds", "Tags"},
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			return map[string]any{"Description": p["Description"], "SubnetIds": p["SubnetIds"], "Tags": tagList(p["Tags"])}
		},
		attrs: func(c *pctx, phys string, _, _ map[string]any) map[string]any {
			return map[string]any{"Arn": c.arn("memorydb", "subnetgroup/"+phys), "SubnetGroupName": phys}
		}}))
	register("AWS::MemoryDB::ParameterGroup", simple(spec{target: memT, create: "CreateParameterGroup", del: "DeleteParameterGroup", name: "ParameterGroupName", max: 255, lower: true,
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			return map[string]any{"Family": p["Family"], "Description": p["Description"], "Tags": tagList(p["Tags"])}
		},
		attrs: func(c *pctx, phys string, _, _ map[string]any) map[string]any {
			return map[string]any{"Arn": c.arn("memorydb", "parametergroup/"+phys), "ParameterGroupName": phys}
		}}))
	register("AWS::MemoryDB::User", simple(spec{target: memT, create: "CreateUser", del: "DeleteUser", name: "UserName", max: 40,
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			return map[string]any{"AccessString": p["AccessString"], "AuthenticationMode": p["AuthenticationMode"], "Tags": tagList(p["Tags"])}
		},
		attrs: func(c *pctx, phys string, _, _ map[string]any) map[string]any {
			return map[string]any{"Arn": c.arn("memorydb", "user/"+phys), "UserName": phys}
		}}))
	register("AWS::MemoryDB::ACL", simple(spec{target: memT, create: "CreateACL", del: "DeleteACL", name: "ACLName", max: 40, lower: true,
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			return map[string]any{"UserNames": p["UserNames"], "Tags": tagList(p["Tags"])}
		},
		attrs: func(c *pctx, phys string, _, _ map[string]any) map[string]any {
			return map[string]any{"Arn": c.arn("memorydb", "acl/"+phys)}
		}}))
	register("AWS::MemoryDB::Cluster", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := strings.ToLower(or(str(p["ClusterName"]), c.name(40, true)))
			in := pick(p, "NodeType", "ACLName", "SubnetGroupName", "NumShards", "NumReplicasPerShard", "ParameterGroupName", "SecurityGroupIds", "Port", "TLSEnabled",
				"EngineVersion", "Description", "MaintenanceWindow", "SnapshotRetentionLimit", "SnapshotWindow", "KmsKeyId", "Engine", "AutoMinorVersionUpgrade", "DataTiering", "SnsTopicArn")
			in["ClusterName"], in["Tags"] = name, tagList(p["Tags"])
			if err := c.call(memT, "CreateCluster", in, nil); err != nil {
				return "", nil, err
			}
			var out struct {
				Clusters []struct {
					ARN, Status     string
					ClusterEndpoint struct {
						Address string
						Port    int
					}
				}
			}
			waitFor(2*time.Minute, func() bool {
				if c.call(memT, "DescribeClusters", map[string]any{"ClusterName": name}, &out) != nil || len(out.Clusters) == 0 {
					return true
				}
				return out.Clusters[0].Status == "available"
			})
			attrs := map[string]any{"Status": "available"}
			if len(out.Clusters) > 0 {
				cl := out.Clusters[0]
				attrs = map[string]any{"ARN": cl.ARN, "Status": cl.Status, "ClusterEndpoint.Address": cl.ClusterEndpoint.Address, "ClusterEndpoint.Port": str(float64(cl.ClusterEndpoint.Port))}
			}
			return name, attrs, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			inPlace := []string{"Description", "SecurityGroupIds", "ACLName", "ParameterGroupName", "MaintenanceWindow", "SnapshotRetentionLimit", "SnapshotWindow", "NodeType",
				"EngineVersion", "NumReplicasPerShard", "NumShards", "SnsTopicArn", "Tags"}
			if differs(old, p, inPlace...) {
				return nil, true, nil
			}
			in := pick(p, inPlace[:len(inPlace)-1]...)
			in["ClusterName"] = phys
			return nil, false, c.call(memT, "UpdateCluster", in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(memT, "DeleteCluster", map[string]any{"ClusterName": phys}, nil)
		},
	})

	// ---- Verified Permissions ----
	register("AWS::VerifiedPermissions::PolicyStore", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := map[string]any{"ValidationSettings": p["ValidationSettings"], "Description": p["Description"]}
			var out struct{ PolicyStoreId, Arn string }
			if err := c.call(avpT, "CreatePolicyStore", in, &out); err != nil {
				return "", nil, err
			}
			if sch, ok := p["Schema"].(map[string]any); ok && sch["CedarJson"] != nil {
				if err := c.call(avpT, "PutSchema", map[string]any{"PolicyStoreId": out.PolicyStoreId, "Definition": map[string]any{"CedarJson": str(sch["CedarJson"])}}, nil); err != nil {
					c.call(avpT, "DeletePolicyStore", map[string]any{"PolicyStoreId": out.PolicyStoreId}, nil)
					return "", nil, err
				}
			}
			return out.PolicyStoreId, map[string]any{"Arn": out.Arn, "PolicyStoreId": out.PolicyStoreId}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(avpT, "DeletePolicyStore", map[string]any{"PolicyStoreId": phys}, nil)
		},
	})
	register("AWS::VerifiedPermissions::Policy", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct{ PolicyId string }
			if err := c.call(avpT, "CreatePolicy", pick(p, "PolicyStoreId", "Definition"), &out); err != nil {
				return "", nil, err
			}
			return out.PolicyId, map[string]any{"PolicyId": out.PolicyId}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(avpT, "DeletePolicy", map[string]any{"PolicyStoreId": p["PolicyStoreId"], "PolicyId": phys}, nil)
		},
	})
}

func toAny(t []map[string]string) []any {
	out := make([]any, len(t))
	for i, e := range t {
		m := map[string]any{}
		for k, v := range e {
			m[k] = v
		}
		out[i] = m
	}
	return out
}
