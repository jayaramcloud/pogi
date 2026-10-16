package cloudformation

import (
	"strings"
	"time"
)

// Redshift, Redshift Serverless and Timestream for InfluxDB.

const (
	rsSvc   = "redshift"
	rsSlT   = "RedshiftServerless"
	influxT = "AmazonTimestreamInfluxDB"
)

func init() {
	clusterCreate := []string{"NodeType", "ClusterType", "MasterUsername", "MasterUserPassword", "DBName", "ClusterParameterGroupName", "ClusterSubnetGroupName", "AvailabilityZone",
		"PreferredMaintenanceWindow", "ClusterVersion", "KmsKeyId", "NumberOfNodes", "Port", "AutomatedSnapshotRetentionPeriod", "ManualSnapshotRetentionPeriod", "AllowVersionUpgrade",
		"PubliclyAccessible", "Encrypted", "EnhancedVpcRouting", "VpcSecurityGroupIds", "ClusterSecurityGroups", "IamRoles", "Tags"}
	clusterModify := []string{"NodeType", "ClusterType", "NumberOfNodes", "MasterUserPassword", "ClusterParameterGroupName", "PreferredMaintenanceWindow", "ClusterVersion",
		"AutomatedSnapshotRetentionPeriod", "AllowVersionUpgrade", "PubliclyAccessible", "EnhancedVpcRouting", "VpcSecurityGroupIds", "Port"}
	clusterAttrs := func(c *pctx, id string) (map[string]any, error) {
		var r *xnode
		var err error
		waitFor(2*time.Minute, func() bool {
			if r, err = c.query(rsSvc, "DescribeClusters", map[string]any{"ClusterIdentifier": id}); err != nil {
				return true
			}
			s := r.findText("ClusterStatus")
			return s == "available" || s == "failed"
		})
		if err != nil {
			return nil, err
		}
		ep := r.find("Endpoint")
		return map[string]any{"Endpoint.Address": ep.text("Address"), "Endpoint.Port": ep.text("Port"), "Id": id}, nil
	}
	register("AWS::Redshift::Cluster", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			id := or(str(p["ClusterIdentifier"]), c.name(63, true))
			in := pick(p, clusterCreate...)
			in["ClusterIdentifier"] = id
			if _, err := c.query(rsSvc, "CreateCluster", in); err != nil {
				return "", nil, err
			}
			attrs, err := clusterAttrs(c, id)
			return id, attrs, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, append([]string{"Tags"}, clusterModify...)...) {
				return nil, true, nil
			}
			in := pick(p, clusterModify...)
			in["ClusterIdentifier"] = phys
			if _, err := c.query(rsSvc, "ModifyCluster", in); err != nil {
				return nil, false, err
			}
			attrs, err := clusterAttrs(c, phys)
			return attrs, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.query(rsSvc, "DeleteCluster", map[string]any{"ClusterIdentifier": phys, "SkipFinalClusterSnapshot": true})
			return err
		},
	})
	register("AWS::Redshift::ClusterSubnetGroup", simple(spec{sign: rsSvc, create: "CreateClusterSubnetGroup", update: "ModifyClusterSubnetGroup", del: "DeleteClusterSubnetGroup",
		name: "ClusterSubnetGroupName", max: 255, lower: true, inPlace: []string{"Description", "SubnetIds"},
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			return map[string]any{"Description": p["Description"], "SubnetIds": p["SubnetIds"], "Tags": p["Tags"]}
		}}))
	register("AWS::Redshift::ClusterParameterGroup", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["ParameterGroupName"]), c.name(255, true))
			in := map[string]any{"ParameterGroupName": name, "ParameterGroupFamily": p["ParameterGroupFamily"], "Description": p["Description"], "Tags": p["Tags"]}
			if _, err := c.query(rsSvc, "CreateClusterParameterGroup", in); err != nil {
				return "", nil, err
			}
			if ps := p["Parameters"]; ps != nil {
				if _, err := c.query(rsSvc, "ModifyClusterParameterGroup", map[string]any{"ParameterGroupName": name, "Parameters": ps}); err != nil {
					c.query(rsSvc, "DeleteClusterParameterGroup", map[string]any{"ParameterGroupName": name})
					return "", nil, err
				}
			}
			return strings.ToLower(name), nil, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Parameters", "Tags") {
				return nil, true, nil
			}
			if p["Parameters"] != nil && !equal(old["Parameters"], p["Parameters"]) {
				_, err := c.query(rsSvc, "ModifyClusterParameterGroup", map[string]any{"ParameterGroupName": phys, "Parameters": p["Parameters"]})
				return nil, false, err
			}
			return nil, false, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.query(rsSvc, "DeleteClusterParameterGroup", map[string]any{"ParameterGroupName": phys})
			return err
		},
	})

	// ---- Redshift Serverless ----
	register("AWS::RedshiftServerless::Namespace", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := strings.ToLower(or(str(p["NamespaceName"]), c.name(64, true)))
			in := pick(p, "AdminUsername", "AdminUserPassword", "DbName", "DefaultIamRoleArn", "KmsKeyId", "IamRoles", "LogExports", "Tags")
			in["NamespaceName"] = name
			var out struct {
				Namespace struct{ NamespaceArn, NamespaceId, Status string }
			}
			if err := c.call(rsSlT, "CreateNamespace", in, &out); err != nil {
				return "", nil, err
			}
			n := out.Namespace
			return name, map[string]any{"Namespace.NamespaceName": name, "Namespace.NamespaceArn": n.NamespaceArn, "Namespace.NamespaceId": n.NamespaceId, "Namespace.Status": n.Status}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			inPlace := []string{"AdminUsername", "AdminUserPassword", "DefaultIamRoleArn", "KmsKeyId", "IamRoles", "LogExports", "Tags"}
			if differs(old, p, inPlace...) {
				return nil, true, nil
			}
			in := pick(p, inPlace[:len(inPlace)-1]...)
			in["NamespaceName"] = phys
			return nil, false, c.call(rsSlT, "UpdateNamespace", in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(rsSlT, "DeleteNamespace", map[string]any{"NamespaceName": phys}, nil)
		},
	})
	register("AWS::RedshiftServerless::Workgroup", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := strings.ToLower(or(str(p["WorkgroupName"]), c.name(64, true)))
			in := pick(p, "NamespaceName", "BaseCapacity", "MaxCapacity", "Port", "EnhancedVpcRouting", "PubliclyAccessible", "ConfigParameters", "SecurityGroupIds", "SubnetIds", "TrackName", "Tags")
			in["WorkgroupName"] = name
			var out struct {
				Workgroup struct {
					WorkgroupArn, WorkgroupId, Status string
					Endpoint                          struct {
						Address string
						Port    int
					}
				}
			}
			if err := c.call(rsSlT, "CreateWorkgroup", in, &out); err != nil {
				return "", nil, err
			}
			w := out.Workgroup
			return name, map[string]any{"Workgroup.WorkgroupName": name, "Workgroup.WorkgroupArn": w.WorkgroupArn, "Workgroup.WorkgroupId": w.WorkgroupId, "Workgroup.Status": w.Status,
				"Workgroup.Endpoint.Address": w.Endpoint.Address, "Workgroup.Endpoint.Port": str(float64(w.Endpoint.Port))}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			inPlace := []string{"BaseCapacity", "MaxCapacity", "Port", "EnhancedVpcRouting", "PubliclyAccessible", "ConfigParameters", "SecurityGroupIds", "SubnetIds", "TrackName", "Tags"}
			if differs(old, p, inPlace...) {
				return nil, true, nil
			}
			in := pick(p, inPlace[:len(inPlace)-1]...)
			in["WorkgroupName"] = phys
			return nil, false, c.call(rsSlT, "UpdateWorkgroup", in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(rsSlT, "DeleteWorkgroup", map[string]any{"WorkgroupName": phys}, nil)
		},
	})

	// ---- Timestream for InfluxDB ----
	register("AWS::Timestream::InfluxDBInstance", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "Username", "Password", "Organization", "Bucket", "DbInstanceType", "AllocatedStorage", "VpcSubnetIds", "VpcSecurityGroupIds", "DbStorageType", "DeploymentType",
				"PubliclyAccessible", "DbParameterGroupIdentifier", "LogDeliveryConfiguration", "Port", "NetworkType")
			in["Name"] = or(str(p["Name"]), c.name(40, false))
			if t := tagMap(p["Tags"]); len(t) > 0 {
				in["Tags"] = t
			}
			var out struct{ Id, Arn string }
			if err := c.call(influxT, "CreateDbInstance", in, &out); err != nil {
				return "", nil, err
			}
			var d struct {
				Status, Endpoint string
				Port             int
			}
			waitFor(2*time.Minute, func() bool {
				return c.call(influxT, "GetDbInstance", map[string]any{"Identifier": out.Id}, &d) != nil || d.Status == "AVAILABLE" || d.Status == "FAILED"
			})
			return out.Id, map[string]any{"Id": out.Id, "Arn": out.Arn, "Endpoint": d.Endpoint, "Status": d.Status, "Port": str(float64(d.Port))}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			inPlace := []string{"DbInstanceType", "AllocatedStorage", "DbStorageType", "DeploymentType", "DbParameterGroupIdentifier", "LogDeliveryConfiguration", "Port", "Tags"}
			if differs(old, p, inPlace...) {
				return nil, true, nil
			}
			in := pick(p, inPlace[:len(inPlace)-1]...)
			in["Identifier"] = phys
			return nil, false, c.call(influxT, "UpdateDbInstance", in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(influxT, "DeleteDbInstance", map[string]any{"Identifier": phys}, nil)
		},
	})
}
