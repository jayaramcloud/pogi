package cloudformation

import (
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// RDS DB instances, subnet groups and parameter groups.

func dbInstance(c *pctx, id string) (*xnode, error) {
	r, err := c.query("rds", "DescribeDBInstances", map[string]any{"DBInstanceIdentifier": id})
	if err != nil {
		return nil, err
	}
	return r.find("DBInstance"), nil
}

func dbInstanceAttrs(c *pctx, id string) (map[string]any, error) {
	var n *xnode
	var err error
	// CloudFormation waits until the instance is available; a real engine takes a while to start.
	var status string
	waitFor(2*time.Minute, func() bool {
		if n, err = dbInstance(c, id); err != nil {
			return true
		}
		status = n.text("DBInstanceStatus")
		return status == "available" || status == "failed"
	})
	if err != nil {
		return nil, err
	}
	if status == "failed" {
		return nil, core.Errorf(500, "DBInstanceFailed", "DB instance %s failed to start", id)
	}
	e := n.get("Endpoint")
	return map[string]any{"Endpoint.Address": e.text("Address"), "Endpoint.Port": e.text("Port"), "Endpoint.HostedZoneId": e.text("HostedZoneId"),
		"DBInstanceArn": n.text("DBInstanceArn"), "DbiResourceId": n.text("DbiResourceId")}, nil
}

func init() {
	instanceCreate := []string{"DBInstanceClass", "Engine", "EngineVersion", "DBName", "MasterUsername", "MasterUserPassword", "DBClusterIdentifier", "DBSubnetGroupName",
		"DBParameterGroupName", "AvailabilityZone", "StorageType", "PreferredBackupWindow", "PreferredMaintenanceWindow", "CACertificateIdentifier", "KmsKeyId",
		"NetworkType", "MonitoringRoleArn", "Port", "AllocatedStorage", "Iops", "MonitoringInterval", "PerformanceInsightsRetentionPeriod", "MaxAllocatedStorage",
		"BackupRetentionPeriod", "PubliclyAccessible", "StorageEncrypted", "MultiAZ", "DeletionProtection", "CopyTagsToSnapshot", "EnableIAMDatabaseAuthentication",
		"EnablePerformanceInsights", "AutoMinorVersionUpgrade", "EnableCloudwatchLogsExports", "VPCSecurityGroups=VpcSecurityGroupIds"}
	// what ModifyDBInstance takes; the rest is fixed when the instance is made and a change replaces it
	instanceModify := []string{"DBInstanceClass", "EngineVersion", "MasterUserPassword", "DBParameterGroupName", "StorageType", "PreferredBackupWindow",
		"PreferredMaintenanceWindow", "CACertificateIdentifier", "NetworkType", "MonitoringRoleArn", "AllocatedStorage", "Iops", "MonitoringInterval",
		"PerformanceInsightsRetentionPeriod", "MaxAllocatedStorage", "BackupRetentionPeriod", "PubliclyAccessible", "MultiAZ", "DeletionProtection",
		"CopyTagsToSnapshot", "EnableIAMDatabaseAuthentication", "EnablePerformanceInsights", "AutoMinorVersionUpgrade", "VPCSecurityGroups", "Port"}
	register("AWS::RDS::DBInstance", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			id := or(str(p["DBInstanceIdentifier"]), c.name(63, true))
			in := pick(p, instanceCreate...)
			in["DBInstanceIdentifier"], in["Tags"] = id, p["Tags"]
			if p["SourceDBInstanceIdentifier"] != nil || p["DBSnapshotIdentifier"] != nil {
				return "", nil, validation("pogi does not support restoring or replicating an AWS::RDS::DBInstance yet")
			}
			if _, err := c.query("rds", "CreateDBInstance", in); err != nil {
				return "", nil, err
			}
			attrs, err := dbInstanceAttrs(c, id)
			return id, attrs, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, append([]string{"EnableCloudwatchLogsExports", "Tags"}, instanceModify...)...) {
				return nil, true, nil
			}
			in := pick(p, instanceModify...)
			if v, ok := in["VPCSecurityGroups"]; ok {
				in["VpcSecurityGroupIds"] = v
				delete(in, "VPCSecurityGroups")
			}
			if v, ok := in["Port"]; ok {
				in["DBPortNumber"] = v
				delete(in, "Port")
			}
			ol, nl := list(old["EnableCloudwatchLogsExports"]), list(p["EnableCloudwatchLogsExports"])
			if add, rm := minus(nl, ol), minus(ol, nl); len(add)+len(rm) > 0 {
				in["CloudwatchLogsExportConfiguration"] = map[string]any{"EnableLogTypes": add, "DisableLogTypes": rm}
			}
			in["DBInstanceIdentifier"], in["ApplyImmediately"] = phys, true
			if _, err := c.query("rds", "ModifyDBInstance", in); err != nil {
				return nil, false, err
			}
			attrs, err := dbInstanceAttrs(c, phys)
			if err != nil {
				return nil, false, err
			}
			return attrs, false, rdsRetag(c, str(attrs["DBInstanceArn"]), old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.query("rds", "DeleteDBInstance", map[string]any{"DBInstanceIdentifier": phys, "SkipFinalSnapshot": true})
			return err
		},
	})

	subnetGroupArn := func(c *pctx, name string) string { return c.arn("rds", "subgrp:"+name) }
	register("AWS::RDS::DBSubnetGroup", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["DBSubnetGroupName"]), c.name(255, true))
			in := pick(p, "DBSubnetGroupDescription", "SubnetIds", "Tags")
			in["DBSubnetGroupName"] = name
			_, err := c.query("rds", "CreateDBSubnetGroup", in)
			return strings.ToLower(name), nil, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "DBSubnetGroupDescription", "SubnetIds", "Tags") {
				return nil, true, nil
			}
			if differs(pick(old, "DBSubnetGroupDescription", "SubnetIds"), pick(p, "DBSubnetGroupDescription", "SubnetIds")) {
				in := pick(p, "DBSubnetGroupDescription", "SubnetIds")
				in["DBSubnetGroupName"] = phys
				if _, err := c.query("rds", "ModifyDBSubnetGroup", in); err != nil {
					return nil, false, err
				}
			}
			return nil, false, rdsRetag(c, subnetGroupArn(c, phys), old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.query("rds", "DeleteDBSubnetGroup", map[string]any{"DBSubnetGroupName": phys})
			return err
		},
	})

	// A parameter group's values are a map in CloudFormation and a list in the API.
	paramList := func(v any) []any {
		m, _ := v.(map[string]any)
		var out []any
		for _, k := range core.SortedKeys(m) {
			out = append(out, map[string]any{"ParameterName": k, "ParameterValue": str(m[k]), "ApplyMethod": "pending-reboot"})
		}
		return out
	}
	paramGroup := func(cluster bool) *provider {
		kind, nameKey, prop := "DBParameterGroup", "DBParameterGroupName", "DBParameterGroupName"
		if cluster {
			kind, nameKey, prop = "DBClusterParameterGroup", "DBClusterParameterGroupName", "DBClusterParameterGroupName"
		}
		arnKind := map[bool]string{false: "pg:", true: "cluster-pg:"}[cluster]
		return &provider{
			create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
				name := or(str(p[prop]), c.name(255, true))
				in := map[string]any{nameKey: name, "DBParameterGroupFamily": p["Family"], "Description": p["Description"], "Tags": p["Tags"]}
				if _, err := c.query("rds", "Create"+kind, in); err != nil {
					return "", nil, err
				}
				if ps := paramList(p["Parameters"]); len(ps) > 0 {
					if _, err := c.query("rds", "Modify"+kind, map[string]any{nameKey: name, "Parameters": ps}); err != nil {
						c.query("rds", "Delete"+kind, map[string]any{nameKey: name})
						return "", nil, err
					}
				}
				return strings.ToLower(name), nil, nil
			},
			update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
				if differs(old, p, "Parameters", "Tags") {
					return nil, true, nil
				}
				if !equal(old["Parameters"], p["Parameters"]) {
					if _, err := c.query("rds", "Reset"+kind, map[string]any{nameKey: phys, "ResetAllParameters": true}); err != nil {
						return nil, false, err
					}
					if ps := paramList(p["Parameters"]); len(ps) > 0 {
						if _, err := c.query("rds", "Modify"+kind, map[string]any{nameKey: phys, "Parameters": ps}); err != nil {
							return nil, false, err
						}
					}
				}
				return nil, false, rdsRetag(c, c.arn("rds", arnKind+phys), old["Tags"], p["Tags"])
			},
			del: func(c *pctx, phys string, p map[string]any) error {
				_, err := c.query("rds", "Delete"+kind, map[string]any{nameKey: phys})
				return err
			},
		}
	}
	register("AWS::RDS::DBParameterGroup", paramGroup(false))
	register("AWS::RDS::DBClusterParameterGroup", paramGroup(true))
}
