package cloudformation

// AWS Batch, Organizations, and the DocumentDB and Neptune types that the RDS service runs.

const (
	batchSvc = "batch"
	orgT     = "AWSOrganizationsV20161128"
)

// engineAlias maps a type that RDS serves to its RDS type and the engine that it sets.
var engineAlias = map[string][2]string{
	"AWS::DocDB::DBCluster":                 {"AWS::RDS::DBCluster", "docdb"},
	"AWS::DocDB::DBInstance":                {"AWS::RDS::DBInstance", "docdb"},
	"AWS::DocDB::DBSubnetGroup":             {"AWS::RDS::DBSubnetGroup", ""},
	"AWS::DocDB::DBClusterParameterGroup":   {"AWS::RDS::DBClusterParameterGroup", ""},
	"AWS::Neptune::DBCluster":               {"AWS::RDS::DBCluster", "neptune"},
	"AWS::Neptune::DBInstance":              {"AWS::RDS::DBInstance", "neptune"},
	"AWS::Neptune::DBSubnetGroup":           {"AWS::RDS::DBSubnetGroup", ""},
	"AWS::Neptune::DBClusterParameterGroup": {"AWS::RDS::DBClusterParameterGroup", ""},
	"AWS::Neptune::DBParameterGroup":        {"AWS::RDS::DBParameterGroup", ""},
}

// aliasProvider is the RDS provider with the engine set when the template does not.
func aliasProvider(typ string) *provider {
	a, ok := engineAlias[typ]
	base := providers[a[0]]
	if !ok || base == nil {
		return nil
	}
	if a[1] == "" {
		return base
	}
	withEngine := func(p map[string]any) map[string]any {
		out := map[string]any{"Engine": a[1]}
		for k, v := range p {
			out[k] = v
		}
		return out
	}
	return &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) { return base.create(c, withEngine(p)) },
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			return base.update(c, phys, withEngine(old), withEngine(p))
		},
		del: base.del,
	}
}

func init() {
	// ---- Batch ----
	bt := func(c *pctx, op string, in map[string]any) (map[string]any, error) {
		out := map[string]any{}
		return out, c.rest(batchSvc, "POST", "/v1/"+op, in, &out)
	}
	tagsOf := func(p map[string]any, in map[string]any) map[string]any {
		if t := tagMap(p["Tags"]); len(t) > 0 {
			in["tags"] = t
		}
		return in
	}
	register("AWS::Batch::ComputeEnvironment", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "Type", "State", "ServiceRole", "ComputeResources", "UnmanagedvCpus", "EksConfiguration", "ReplaceComputeEnvironment")
			in["ComputeEnvironmentName"] = or(str(p["ComputeEnvironmentName"]), c.name(128, false))
			out, err := bt(c, "createcomputeenvironment", tagsOf(p, in))
			arn := dig(out, "computeEnvironmentArn")
			return arn, map[string]any{"ComputeEnvironmentArn": arn}, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "State", "ServiceRole", "ComputeResources", "UnmanagedvCpus", "Tags") {
				return nil, true, nil
			}
			in := pick(p, "State", "ServiceRole", "ComputeResources", "UnmanagedvCpus")
			in["ComputeEnvironment"] = phys
			_, err := bt(c, "updatecomputeenvironment", in)
			return nil, false, err
		},
		// an environment is disabled before it is deleted
		del: func(c *pctx, phys string, p map[string]any) error {
			bt(c, "updatecomputeenvironment", map[string]any{"ComputeEnvironment": phys, "State": "DISABLED"})
			_, err := bt(c, "deletecomputeenvironment", map[string]any{"ComputeEnvironment": phys})
			return err
		},
	})
	register("AWS::Batch::JobQueue", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "State", "Priority", "ComputeEnvironmentOrder", "SchedulingPolicyArn")
			in["JobQueueName"] = or(str(p["JobQueueName"]), c.name(128, false))
			out, err := bt(c, "createjobqueue", tagsOf(p, in))
			arn := dig(out, "jobQueueArn")
			return arn, map[string]any{"JobQueueArn": arn}, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "State", "Priority", "ComputeEnvironmentOrder", "SchedulingPolicyArn", "Tags") {
				return nil, true, nil
			}
			in := pick(p, "State", "Priority", "ComputeEnvironmentOrder", "SchedulingPolicyArn")
			in["JobQueue"] = phys
			_, err := bt(c, "updatejobqueue", in)
			return nil, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			bt(c, "updatejobqueue", map[string]any{"JobQueue": phys, "State": "DISABLED"})
			_, err := bt(c, "deletejobqueue", map[string]any{"JobQueue": phys})
			return err
		},
	})
	register("AWS::Batch::JobDefinition", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "Type", "ContainerProperties", "Parameters", "RetryStrategy", "Timeout", "PlatformCapabilities", "PropagateTags", "NodeProperties", "EksProperties", "SchedulingPriority")
			in["JobDefinitionName"] = or(str(p["JobDefinitionName"]), c.name(128, false))
			out, err := bt(c, "registerjobdefinition", tagsOf(p, in))
			arn := dig(out, "jobDefinitionArn")
			return arn, nil, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := bt(c, "deregisterjobdefinition", map[string]any{"JobDefinition": phys})
			return err
		},
	})
	register("AWS::Batch::SchedulingPolicy", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "FairsharePolicy")
			in["Name"] = or(str(p["Name"]), c.name(128, false))
			out, err := bt(c, "createschedulingpolicy", tagsOf(p, in))
			arn := dig(out, "arn")
			return arn, map[string]any{"Arn": arn}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := bt(c, "deleteschedulingpolicy", map[string]any{"Arn": phys})
			return err
		},
	})

	// ---- Organizations ----
	register("AWS::Organizations::OrganizationalUnit", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "ParentId", "Name")
			in["Tags"] = tagList(p["Tags"])
			var out struct{ OrganizationalUnit struct{ Id, Arn string } }
			if err := c.call(orgT, "CreateOrganizationalUnit", in, &out); err != nil {
				return "", nil, err
			}
			return out.OrganizationalUnit.Id, map[string]any{"Id": out.OrganizationalUnit.Id, "Arn": out.OrganizationalUnit.Arn}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Name") {
				return nil, true, nil
			}
			return nil, false, c.call(orgT, "UpdateOrganizationalUnit", map[string]any{"OrganizationalUnitId": phys, "Name": p["Name"]}, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(orgT, "DeleteOrganizationalUnit", map[string]any{"OrganizationalUnitId": phys}, nil)
		},
	})
	register("AWS::Organizations::Policy", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "Name", "Type", "Description")
			in["Content"], in["Tags"] = jsonString(p["Content"]), tagList(p["Tags"])
			var out struct {
				Policy struct {
					PolicySummary struct {
						Id, Arn    string
						AwsManaged bool
					}
				}
			}
			if err := c.call(orgT, "CreatePolicy", in, &out); err != nil {
				return "", nil, err
			}
			s := out.Policy.PolicySummary
			for _, t := range list(p["TargetIds"]) {
				if err := c.call(orgT, "AttachPolicy", map[string]any{"PolicyId": s.Id, "TargetId": t}, nil); err != nil {
					return "", nil, err
				}
			}
			return s.Id, map[string]any{"Id": s.Id, "Arn": s.Arn, "AwsManaged": s.AwsManaged}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Name", "Description", "Content", "TargetIds", "Tags") {
				return nil, true, nil
			}
			in := pick(p, "Name", "Description")
			in["PolicyId"], in["Content"] = phys, jsonString(p["Content"])
			if err := c.call(orgT, "UpdatePolicy", in, nil); err != nil {
				return nil, false, err
			}
			for _, t := range minus(list(old["TargetIds"]), list(p["TargetIds"])) {
				c.call(orgT, "DetachPolicy", map[string]any{"PolicyId": phys, "TargetId": t}, nil)
			}
			for _, t := range minus(list(p["TargetIds"]), list(old["TargetIds"])) {
				if err := c.call(orgT, "AttachPolicy", map[string]any{"PolicyId": phys, "TargetId": t}, nil); err != nil {
					return nil, false, err
				}
			}
			return nil, false, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			for _, t := range list(p["TargetIds"]) {
				c.call(orgT, "DetachPolicy", map[string]any{"PolicyId": phys, "TargetId": t}, nil)
			}
			return c.call(orgT, "DeletePolicy", map[string]any{"PolicyId": phys}, nil)
		},
	})
}
