package cloudformation

import (
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// ElastiCache clusters, replication groups, subnet and parameter groups, users and user groups.

const ecache = "elasticache"

// cacheReady waits for a cluster or replication group to be available and returns its description.
func cacheReady(c *pctx, op, idKey, id, statusKey string) (*xnode, error) {
	var n *xnode
	var err error
	waitFor(2*time.Minute, func() bool {
		var r *xnode
		if r, err = c.query(ecache, op, map[string]any{idKey: id}); err != nil {
			return true
		}
		n = r
		s := n.findText(statusKey)
		return s == "available" || s == "create-failed"
	})
	return n, err
}

func endpoint(n *xnode, name string) (addr, port string) {
	e := n.find(name)
	return e.text("Address"), e.text("Port")
}

func init() {
	cacheCreate := []string{"Engine", "EngineVersion", "CacheNodeType", "NumCacheNodes", "CacheParameterGroupName", "CacheSubnetGroupName", "PreferredAvailabilityZone",
		"PreferredMaintenanceWindow", "SnapshotRetentionLimit", "AutoMinorVersionUpgrade", "Port", "VpcSecurityGroupIds=SecurityGroupIds", "Tags", "SnapshotWindow"}
	clusterModify := []string{"EngineVersion", "NumCacheNodes", "CacheParameterGroupName", "SnapshotRetentionLimit", "AutoMinorVersionUpgrade", "PreferredMaintenanceWindow",
		"VpcSecurityGroupIds", "SnapshotWindow", "CacheNodeType"}
	register("AWS::ElastiCache::CacheCluster", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			id := or(str(p["ClusterName"]), c.name(50, true))
			in := pick(p, cacheCreate...)
			in["CacheClusterId"] = id
			if _, err := c.query(ecache, "CreateCacheCluster", in); err != nil {
				return "", nil, err
			}
			n, err := cacheReady(c, "DescribeCacheClusters", "CacheClusterId", id, "CacheClusterStatus")
			if err != nil {
				return "", nil, err
			}
			ra, rp := endpoint(n.get("CacheClusters", "member", "CacheNodes", "member"), "Endpoint")
			ca, cp := endpoint(n, "ConfigurationEndpoint")
			return id, map[string]any{"RedisEndpoint.Address": ra, "RedisEndpoint.Port": rp, "ConfigurationEndpoint.Address": ca, "ConfigurationEndpoint.Port": cp,
				"Id": id}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, append([]string{"Tags"}, clusterModify...)...) {
				return nil, true, nil
			}
			in := pick(p, clusterModify...)
			if v, ok := in["VpcSecurityGroupIds"]; ok {
				in["SecurityGroupIds"] = v
				delete(in, "VpcSecurityGroupIds")
			}
			in["CacheClusterId"], in["ApplyImmediately"] = phys, true
			if _, err := c.query(ecache, "ModifyCacheCluster", in); err != nil {
				return nil, false, err
			}
			return nil, false, retag(c, ecache, c.arn("elasticache", "cluster:"+phys), old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.query(ecache, "DeleteCacheCluster", map[string]any{"CacheClusterId": phys})
			return err
		},
	})

	groupCreate := []string{"ReplicationGroupDescription", "Engine", "EngineVersion", "CacheNodeType", "NumCacheClusters", "NumNodeGroups", "ReplicasPerNodeGroup",
		"AutomaticFailoverEnabled", "MultiAZEnabled", "CacheParameterGroupName", "CacheSubnetGroupName", "SecurityGroupIds", "Port", "AtRestEncryptionEnabled",
		"TransitEncryptionEnabled", "SnapshotRetentionLimit", "SnapshotWindow", "PreferredMaintenanceWindow", "AutoMinorVersionUpgrade", "KmsKeyId", "UserGroupIds", "Tags",
		"NodeGroupConfiguration"}
	groupModify := []string{"ReplicationGroupDescription", "EngineVersion", "CacheNodeType", "AutomaticFailoverEnabled", "MultiAZEnabled", "CacheParameterGroupName",
		"SecurityGroupIds", "SnapshotRetentionLimit", "SnapshotWindow", "PreferredMaintenanceWindow", "AutoMinorVersionUpgrade"}
	register("AWS::ElastiCache::ReplicationGroup", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			id := or(str(p["ReplicationGroupId"]), c.name(40, true))
			in := pick(p, groupCreate...)
			in["ReplicationGroupId"] = id
			if _, err := c.query(ecache, "CreateReplicationGroup", in); err != nil {
				return "", nil, err
			}
			n, err := cacheReady(c, "DescribeReplicationGroups", "ReplicationGroupId", id, "Status")
			if err != nil {
				return "", nil, err
			}
			pa, pp := endpoint(n.find("NodeGroup"), "PrimaryEndpoint")
			ra, rp := endpoint(n.find("NodeGroup"), "ReaderEndpoint")
			ca, cp := endpoint(n, "ConfigurationEndpoint")
			return id, map[string]any{"PrimaryEndPoint.Address": pa, "PrimaryEndPoint.Port": pp, "ReaderEndPoint.Address": ra, "ReaderEndPoint.Port": rp,
				"ConfigurationEndPoint.Address": ca, "ConfigurationEndPoint.Port": cp}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, append([]string{"Tags"}, groupModify...)...) {
				return nil, true, nil
			}
			in := pick(p, groupModify...)
			in["ReplicationGroupId"], in["ApplyImmediately"] = phys, true
			if _, err := c.query(ecache, "ModifyReplicationGroup", in); err != nil {
				return nil, false, err
			}
			return nil, false, retag(c, ecache, c.arn("elasticache", "replicationgroup:"+phys), old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.query(ecache, "DeleteReplicationGroup", map[string]any{"ReplicationGroupId": phys})
			return err
		},
	})

	register("AWS::ElastiCache::SubnetGroup", simple(spec{sign: ecache, create: "CreateCacheSubnetGroup", update: "ModifyCacheSubnetGroup", del: "DeleteCacheSubnetGroup", name: "CacheSubnetGroupName",
		max: 255, lower: true, inPlace: []string{"Description", "SubnetIds"},
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			return map[string]any{"CacheSubnetGroupDescription": p["Description"], "SubnetIds": p["SubnetIds"], "Tags": p["Tags"]}
		}}))
	register("AWS::ElastiCache::ParameterGroup", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["CacheParameterGroupName"]), c.name(255, true))
			in := map[string]any{"CacheParameterGroupName": name, "CacheParameterGroupFamily": p["CacheParameterGroupFamily"], "Description": p["Description"], "Tags": p["Tags"]}
			if _, err := c.query(ecache, "CreateCacheParameterGroup", in); err != nil {
				return "", nil, err
			}
			if err := setCacheParams(c, name, p["Properties"]); err != nil {
				c.query(ecache, "DeleteCacheParameterGroup", map[string]any{"CacheParameterGroupName": name})
				return "", nil, err
			}
			return strings.ToLower(name), nil, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Properties", "Tags") {
				return nil, true, nil
			}
			if !equal(old["Properties"], p["Properties"]) {
				if _, err := c.query(ecache, "ResetCacheParameterGroup", map[string]any{"CacheParameterGroupName": phys, "ResetAllParameters": true}); err != nil {
					return nil, false, err
				}
				if err := setCacheParams(c, phys, p["Properties"]); err != nil {
					return nil, false, err
				}
			}
			return nil, false, retag(c, ecache, c.arn("elasticache", "parametergroup:"+phys), old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.query(ecache, "DeleteCacheParameterGroup", map[string]any{"CacheParameterGroupName": phys})
			return err
		},
	})
	register("AWS::ElastiCache::User", simple(spec{sign: ecache, create: "CreateUser", update: "ModifyUser", del: "DeleteUser", name: "UserId", max: 40, lower: false,
		inPlace: []string{"AccessString", "Passwords", "NoPasswordRequired", "AuthenticationMode"},
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			return pick(p, "UserName", "Engine", "AccessString", "Passwords", "NoPasswordRequired", "AuthenticationMode", "Tags")
		},
		attrs: func(c *pctx, phys string, p, _ map[string]any) map[string]any {
			return map[string]any{"Arn": c.arn("elasticache", "user:"+phys), "UserId": phys}
		}}))
	register("AWS::ElastiCache::UserGroup", simple(spec{sign: ecache, create: "CreateUserGroup", update: "ModifyUserGroup", del: "DeleteUserGroup", name: "UserGroupId", max: 40, lower: true,
		build: func(c *pctx, p map[string]any, _ string) map[string]any { return pick(p, "Engine", "UserIds", "Tags") },
		attrs: func(c *pctx, phys string, p, _ map[string]any) map[string]any {
			return map[string]any{"Arn": c.arn("elasticache", "usergroup:"+phys)}
		}}))
}

func setCacheParams(c *pctx, name string, props any) error {
	m, _ := props.(map[string]any)
	var ps []any
	for _, k := range core.SortedKeys(m) {
		ps = append(ps, map[string]any{"ParameterName": k, "ParameterValue": str(m[k])})
	}
	if len(ps) == 0 {
		return nil
	}
	_, err := c.query(ecache, "ModifyCacheParameterGroup", map[string]any{"CacheParameterGroupName": name, "ParameterNameValues": ps})
	return err
}
