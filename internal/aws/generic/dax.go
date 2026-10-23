package generic

import (
	"net"
	"slices"
	"strconv"
	"strings"
)

// DAX: clusters, subnet groups and parameter groups are the engine's, with a few overrides.
// A cluster is available at once. Its discovery endpoint and node endpoints are pogi's own
// address (an http:// URL), so a plain DynamoDB client pointed at that URL reads and writes
// pogi's DynamoDB tables. The DAX client protocol (dax://, port 8111) is not spoken.
//
// ponytail: subnet IDs are looked up in pogi's EC2 to fill the subnet group's VPC and
// zones; IDs EC2 does not know are accepted as they are.

const daxDefaultGroup = "default.dax1.0"

type daxParam struct{ name, desc, def string }

var daxParams = []daxParam{
	{"query-ttl-millis", "Duration in milliseconds for queries to remain cached", "300000"},
	{"record-ttl-millis", "Duration in milliseconds for records to remain cached", "300000"},
}

func init() {
	Override("dax", "CreateCluster", func(c *Call) (map[string]any, error) {
		name := str(c.In["ClusterName"])
		if c.Get("Cluster", name) != nil {
			return nil, c.Exists("Cluster already exists: %s", name)
		}
		if n := daxInt(c.In["ReplicationFactor"]); n < 1 {
			return nil, c.Invalid("ReplicationFactor must be at least 1, got %d.", n)
		}
		group := str(c.In["ParameterGroupName"])
		if group == "" {
			group = daxDefaultGroup
		} else if group != daxDefaultGroup && c.Get("ParameterGroup", group) == nil {
			return nil, c.Err("ParameterGroupNotFoundFault", "Parameter group %s not found.", group)
		}
		if sg := str(c.In["SubnetGroupName"]); sg != "" && c.Get("SubnetGroup", sg) == nil {
			return nil, c.Err("SubnetGroupNotFoundFault", "Subnet group %s not found.", sg)
		}
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		r.Data["ParameterGroup"] = map[string]any{"ParameterGroupName": group, "ParameterApplyStatus": "in-sync", "NodeIdsToReboot": []any{}}
		if sg := str(c.In["SubnetGroupName"]); sg != "" {
			r.Data["SubnetGroup"] = sg
		}
		r.Data["SSEDescription"] = map[string]any{"Status": map[bool]string{true: "ENABLED", false: "DISABLED"}[daxSSE(c.In["SSESpecification"])]}
		r.Data["ClusterEndpointEncryptionType"] = orDefault(c.In["ClusterEndpointEncryptionType"], "NONE")
		r.Data["NetworkType"] = orDefault(c.In["NetworkType"], "ipv4")
		r.Data["PreferredMaintenanceWindow"] = orDefault(c.In["PreferredMaintenanceWindow"], "sun:05:00-sun:06:00")
		if topic := str(c.In["NotificationTopicArn"]); topic != "" {
			r.Data["NotificationConfiguration"] = map[string]any{"TopicArn": topic, "TopicStatus": "active"}
		}
		r.Data["Nodes"] = []any{}
		daxScale(c, r, daxInt(c.In["ReplicationFactor"]), nil)
		c.Put(r)
		return c.Fill(r), nil
	})

	Override("dax", "IncreaseReplicationFactor", func(c *Call) (map[string]any, error) {
		r, err := daxCluster(c)
		if err != nil {
			return nil, err
		}
		if n, cur := daxInt(c.In["NewReplicationFactor"]), len(asList(r.Data["Nodes"])); n <= cur {
			return nil, c.Invalid("NewReplicationFactor must be more than the current %d.", cur)
		}
		daxScale(c, r, daxInt(c.In["NewReplicationFactor"]), asList(c.In["AvailabilityZones"]))
		c.Put(r)
		return c.Fill(r), nil
	})

	Override("dax", "DecreaseReplicationFactor", func(c *Call) (map[string]any, error) {
		r, err := daxCluster(c)
		if err != nil {
			return nil, err
		}
		n, cur := daxInt(c.In["NewReplicationFactor"]), asList(r.Data["Nodes"])
		if n < 1 || n >= len(cur) {
			return nil, c.Invalid("NewReplicationFactor must be at least 1 and less than the current %d.", len(cur))
		}
		remove := asList(c.In["NodeIdsToRemove"])
		for _, id := range remove {
			if daxNode(cur, str(id)) < 0 {
				return nil, c.Err("NodeNotFoundFault", "Node %s not found in cluster %s.", str(id), r.Name)
			}
		}
		if len(remove) > len(cur)-n {
			return nil, c.Invalid("NodeIdsToRemove names %d nodes but only %d must go.", len(remove), len(cur)-n)
		}
		daxScale(c, r, n, remove)
		c.Put(r)
		return c.Fill(r), nil
	})

	Override("dax", "RebootNode", func(c *Call) (map[string]any, error) {
		r, err := daxCluster(c)
		if err != nil {
			return nil, err
		}
		if daxNode(asList(r.Data["Nodes"]), str(c.In["NodeId"])) < 0 {
			return nil, c.Err("NodeNotFoundFault", "Node %s not found in cluster %s.", str(c.In["NodeId"]), r.Name)
		}
		return c.Fill(r), nil
	})

	// UpdateCluster may switch the parameter group.
	Override("dax", "UpdateCluster", func(c *Call) (map[string]any, error) {
		if g := str(c.In["ParameterGroupName"]); g != "" && g != daxDefaultGroup && c.Get("ParameterGroup", g) == nil {
			return nil, c.Err("ParameterGroupNotFoundFault", "Parameter group %s not found.", g)
		}
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		if g := str(c.In["ParameterGroupName"]); g != "" {
			r := c.Res
			r.Data["ParameterGroup"] = map[string]any{"ParameterGroupName": g, "ParameterApplyStatus": "in-sync", "NodeIdsToReboot": []any{}}
			c.Put(r)
			return c.Fill(r), nil
		}
		return out, nil
	})

	Override("dax", "DescribeDefaultParameters", func(c *Call) (map[string]any, error) {
		return map[string]any{"Parameters": daxParamList(nil, "")}, nil
	})
	Override("dax", "DescribeParameters", func(c *Call) (map[string]any, error) {
		g := str(c.In["ParameterGroupName"])
		var vals map[string]any
		if g != daxDefaultGroup {
			r := c.Get("ParameterGroup", g)
			if r == nil {
				return nil, c.Err("ParameterGroupNotFoundFault", "Parameter group %s not found.", g)
			}
			vals, _ = r.Data["Values"].(map[string]any)
		}
		return map[string]any{"Parameters": daxParamList(vals, str(c.In["Source"]))}, nil
	})
	Override("dax", "UpdateParameterGroup", func(c *Call) (map[string]any, error) {
		g := str(c.In["ParameterGroupName"])
		r := c.Get("ParameterGroup", g)
		if r == nil {
			return nil, c.Err("ParameterGroupNotFoundFault", "Parameter group %s not found.", g)
		}
		vals, _ := r.Data["Values"].(map[string]any)
		if vals == nil {
			vals = map[string]any{}
		}
		for _, p := range asList(c.In["ParameterNameValues"]) {
			m := s2Map(p)
			name, v := str(m["ParameterName"]), str(m["ParameterValue"])
			if !slices.ContainsFunc(daxParams, func(d daxParam) bool { return d.name == name }) {
				return nil, c.Invalid("Parameter %s is not a DAX parameter.", name)
			}
			if n, err := strconv.ParseInt(v, 10, 64); err != nil || n < 0 {
				return nil, c.Invalid("Value %q of %s must be a whole number of milliseconds, 0 or more.", v, name)
			}
			vals[name] = v
		}
		r.Data["Values"] = vals
		c.Put(r)
		return map[string]any{"ParameterGroup": map[string]any{"ParameterGroupName": g, "Description": r.Data["Description"]}}, nil
	})

	// A parameter group a cluster uses stays.
	Override("dax", "DeleteParameterGroup", func(c *Call) (map[string]any, error) {
		g := str(c.In["ParameterGroupName"])
		for _, cl := range c.List("Cluster") {
			if str(s2Map(cl.Data["ParameterGroup"])["ParameterGroupName"]) == g {
				return nil, c.Err("InvalidParameterGroupStateFault", "Parameter group %s is used by cluster %s.", g, cl.Name)
			}
		}
		return c.Default()
	})

	// A subnet group fills its VPC and subnet zones from pogi's EC2 where it knows the subnets.
	Override("dax", "CreateSubnetGroup", func(c *Call) (map[string]any, error) {
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		ids := asList(c.In["SubnetIds"])
		var sn struct {
			Subnets []struct {
				ID  string `xml:"subnetId"`
				VPC string `xml:"vpcId"`
				AZ  string `xml:"availabilityZone"`
			} `xml:"subnetSet>item"`
		}
		known := map[string]string{}
		r := c.Res
		if len(ids) > 0 && ec2Describe(c, "DescribeSubnets", "SubnetId", ids, &sn) {
			for _, s := range sn.Subnets {
				known[s.ID] = s.AZ
				r.Data["VpcId"] = s.VPC
			}
		}
		subnets := []any{}
		for _, id := range ids {
			subnets = append(subnets, map[string]any{"SubnetIdentifier": id, "SubnetAvailabilityZone": known[str(id)], "SupportedNetworkTypes": []any{"ipv4"}})
		}
		r.Data["Subnets"] = subnets
		r.Data["SupportedNetworkTypes"] = []any{"ipv4"}
		c.Put(r)
		return c.Fill(r), nil
	})
	Override("dax", "DeleteSubnetGroup", func(c *Call) (map[string]any, error) {
		g := str(c.In["SubnetGroupName"])
		for _, cl := range c.List("Cluster") {
			if str(cl.Data["SubnetGroup"]) == g {
				return nil, c.Err("SubnetGroupInUseFault", "Subnet group %s is used by cluster %s.", g, cl.Name)
			}
		}
		return c.Default()
	})
}

// daxCluster is the cluster the call names.
func daxCluster(c *Call) (*Resource, error) {
	r := c.Get("Cluster", str(c.In["ClusterName"]))
	if r == nil {
		return nil, c.Err("ClusterNotFoundFault", "Cluster not found: %s", str(c.In["ClusterName"]))
	}
	return r, nil
}

func daxNode(nodes []any, id string) int {
	return slices.IndexFunc(nodes, func(n any) bool { return str(s2Map(n)["NodeId"]) == id })
}

func daxSSE(spec any) bool { return s2Map(spec)["Enabled"] == true }

// daxScale makes the cluster have n nodes: it drops the nodes named in remove (then the newest) or adds
// nodes with the next free letter, and sets the endpoints and counts.
func daxScale(c *Call, r *Resource, n int, extra []any) {
	nodes := asList(r.Data["Nodes"])
	if n < len(nodes) {
		for _, id := range extra {
			if i := daxNode(nodes, str(id)); i >= 0 {
				nodes = slices.Delete(nodes, i, i+1)
			}
		}
		nodes = nodes[:n]
	}
	host, port := "localhost", 7644
	if c.Core != nil && c.Core.Host != "" {
		h, p, err := net.SplitHostPort(c.Core.Host)
		if err != nil {
			h = c.Core.Host
		} else if v, err := strconv.Atoi(p); err == nil {
			port = v
		}
		host = h
	}
	ep := map[string]any{"Address": host, "Port": float64(port), "URL": "http://" + net.JoinHostPort(host, strconv.Itoa(port))}
	zones := asList(extra)
	if len(zones) == 0 {
		zones = asList(c.In["AvailabilityZones"])
	}
	for len(nodes) < n {
		i := 0
		for daxNode(nodes, r.Name+"-"+daxSuffix(i)) >= 0 {
			i++
		}
		az := c.Region + string(rune('a'+len(nodes)%3))
		if len(zones) > 0 {
			az = str(zones[len(nodes)%len(zones)])
		}
		nodes = append(nodes, map[string]any{"NodeId": r.Name + "-" + daxSuffix(i), "Endpoint": ep, "NodeStatus": "available",
			"AvailabilityZone": az, "ParameterGroupStatus": "in-sync", "NodeCreateTime": c.now().Format("2006-01-02T15:04:05Z")})
	}
	r.Data["Nodes"] = nodes
	r.Data["ClusterDiscoveryEndpoint"] = ep
	r.Data["Status"] = "available"
	r.Data["TotalNodes"], r.Data["ActiveNodes"] = float64(len(nodes)), float64(len(nodes))
}

func daxSuffix(i int) string {
	if i < 26 {
		return string(rune('a' + i))
	}
	return strings.Repeat("z", i/26) + string(rune('a'+i%26))
}

func daxParamList(vals map[string]any, source string) []any {
	out := []any{}
	for _, p := range daxParams {
		v := p.def
		if s, ok := vals[p.name].(string); ok {
			v = s
		}
		src := "user"
		if source != "" && source != src {
			continue
		}
		out = append(out, map[string]any{"ParameterName": p.name, "ParameterType": "DEFAULT", "ParameterValue": v, "NodeTypeSpecificValues": []any{},
			"Description": p.desc, "Source": src, "DataType": "integer", "AllowedValues": "0-", "IsModifiable": "TRUE", "ChangeType": "IMMEDIATE"})
	}
	return out
}

func daxInt(v any) int { return int(asFloat(v)) }
