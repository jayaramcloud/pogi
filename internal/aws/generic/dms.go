package generic

import (
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// DMS: replication instances (creating, available), endpoints, replication
// tasks (creating, ready; started: starting, running, then stopped for a
// full-load task) and subnet groups. Metadata only, nothing is migrated.
func init() {
	arnTemplates["dms/ReplicationInstance"] = "rep:{tail}"
	arnTemplates["dms/Endpoint"] = "endpoint:{tail}"
	arnTemplates["dms/ReplicationTask"] = "task:{tail}"
	arnTemplates["dms/ReplicationSubnetGroup"] = "subgrp:{ReplicationSubnetGroupIdentifier}"
	arnTemplates["dms/EventSubscription"] = "es:{SubscriptionName}"

	Derive("dms", "ReplicationInstance", func(r *Resource) map[string]any {
		s := Step(r, "ri", "creating", "available")
		if r.Data["_phase"] == "modify" {
			s = Step(r, "ri", "modifying", "available")
		}
		return map[string]any{"ReplicationInstanceStatus": s}
	})
	Derive("dms", "Endpoint", func(r *Resource) map[string]any { return map[string]any{"Status": "active"} })
	Derive("dms", "ReplicationTask", dmsTaskState)
	Derive("dms", "ReplicationSubnetGroup", func(r *Resource) map[string]any {
		var subnets []any
		for i, id := range asList(r.Data["SubnetIds"]) {
			subnets = append(subnets, map[string]any{"SubnetIdentifier": id, "SubnetAvailabilityZone": map[string]any{"Name": "us-east-1" + string(rune('a'+i%3))}, "SubnetStatus": "Active"})
		}
		return map[string]any{"SubnetGroupStatus": "Complete", "Subnets": orEmpty(subnets)}
	})

	Override("dms", "CreateEndpoint", func(c *Call) (map[string]any, error) {
		if err := dmsDup(c, "Endpoint", "EndpointIdentifier"); err != nil {
			return nil, err
		}
		return c.Default()
	})
	Override("dms", "CreateReplicationInstance", func(c *Call) (map[string]any, error) {
		if err := dmsDup(c, "ReplicationInstance", "ReplicationInstanceIdentifier"); err != nil {
			return nil, err
		}
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		for k, v := range map[string]any{"EngineVersion": "3.5.4", "AllocatedStorage": 50.0, "MultiAZ": false, "PubliclyAccessible": true, "AutoMinorVersionUpgrade": true,
			"AvailabilityZone": c.Region + "a", "NetworkType": "IPV4", "ReplicationInstancePrivateIpAddresses": []any{"10.0.0.10"}, "ReplicationInstancePublicIpAddresses": []any{"52.0.0.10"}} {
			if r.Data[k] == nil {
				r.Data[k] = v
			}
		}
		Begin(r, "ri")
		c.Put(r)
		return c.Fill(r), nil
	})
	Override("dms", "ModifyReplicationInstance", func(c *Call) (map[string]any, error) {
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		r.Data["_phase"] = "modify"
		Begin(r, "ri")
		c.Put(r)
		return c.Fill(r), nil
	})
	Override("dms", "CreateReplicationTask", func(c *Call) (map[string]any, error) {
		if err := dmsDup(c, "ReplicationTask", "ReplicationTaskIdentifier"); err != nil {
			return nil, err
		}
		for _, k := range []string{"SourceEndpointArn", "TargetEndpointArn"} {
			if c.Svc.byARN(c.Account, str(c.In[k])) == nil {
				return nil, c.NotFound("Endpoint %s not found.", str(c.In[k]))
			}
		}
		if c.Svc.byARN(c.Account, str(c.In["ReplicationInstanceArn"])) == nil {
			return nil, c.NotFound("Replication instance %s not found.", str(c.In["ReplicationInstanceArn"]))
		}
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		Begin(r, "task")
		c.Put(r)
		return c.Fill(r), nil
	})
	for _, op := range []string{"StartReplicationTask", "StopReplicationTask"} {
		Override("dms", op, func(c *Call) (map[string]any, error) {
			r := c.Svc.byARN(c.Account, str(c.In["ReplicationTaskArn"]))
			if r == nil || r.Type != "ReplicationTask" {
				return nil, c.NotFound("Task %s not found.", str(c.In["ReplicationTaskArn"]))
			}
			st := str(dmsTaskState(r)["Status"])
			if c.Op == "StartReplicationTask" {
				if st == "starting" || st == "running" || st == "stopping" { // a task still creating is let through: nothing here is real work
					return nil, core.Errorf(400, "InvalidResourceStateFault", "Task is %s; only a ready or stopped task can be started.", st)
				}
				r.Data["_phase"] = "start"
			} else {
				if st != "starting" && st != "running" {
					return nil, core.Errorf(400, "InvalidResourceStateFault", "Task is %s, not running.", st)
				}
				r.Data["_phase"] = "stop"
			}
			Begin(r, "task")
			c.Put(r)
			return c.Fill(r), nil
		})
	}
	Override("dms", "DeleteReplicationTask", func(c *Call) (map[string]any, error) {
		if r := c.Svc.byARN(c.Account, str(c.In["ReplicationTaskArn"])); r != nil {
			if st := str(dmsTaskState(r)["Status"]); st == "starting" || st == "running" {
				return nil, core.Errorf(400, "InvalidResourceStateFault", "Task is %s; stop it first.", st)
			}
		}
		out, err := c.Default()
		if err == nil {
			if t, ok := out["ReplicationTask"].(map[string]any); ok {
				t["Status"] = "deleting"
			}
		}
		return out, err
	})
	Override("dms", "DeleteReplicationInstance", func(c *Call) (map[string]any, error) {
		for _, t := range c.List("ReplicationTask") {
			if t.Data["ReplicationInstanceArn"] == c.In["ReplicationInstanceArn"] {
				return nil, core.Errorf(400, "InvalidResourceStateFault", "Replication instance is used by task %s.", str(t.Data["ReplicationTaskIdentifier"]))
			}
		}
		out, err := c.Default()
		if err == nil {
			if i, ok := out["ReplicationInstance"].(map[string]any); ok {
				i["ReplicationInstanceStatus"] = "deleting"
			}
		}
		return out, err
	})

	// Describe calls filter on {Name, Values}, and an empty answer is an empty list.
	dmsFilter := func(typ, listMember string, keys map[string]string) {
		Override("dms", "Describe"+listMember, func(c *Call) (map[string]any, error) {
			var items []any
		next:
			for _, r := range c.List(typ) {
				for _, f := range asList(c.In["Filters"]) {
					fm := f.(map[string]any)
					name := str(fm["Name"])
					got := str(r.Data[keys[name]])
					if strings.HasSuffix(name, "-arn") {
						got = r.ARN
					}
					if !slices.Contains(asList(fm["Values"]), any(got)) {
						continue next
					}
				}
				items = append(items, c.fillItem(c.op.out.Members[listMember].sh.Member.sh, r, listMember))
			}
			page, next, err := pageOf(c, items, "Marker", "MaxRecords")
			if err != nil {
				return nil, err
			}
			out := map[string]any{listMember: page}
			if next != "" {
				out["Marker"] = next
			}
			return out, nil
		})
	}
	dmsFilter("ReplicationInstance", "ReplicationInstances", map[string]string{"replication-instance-id": "ReplicationInstanceIdentifier", "replication-instance-class": "ReplicationInstanceClass", "engine-version": "EngineVersion"})
	dmsFilter("Endpoint", "Endpoints", map[string]string{"endpoint-id": "EndpointIdentifier", "endpoint-type": "EndpointType", "engine-name": "EngineName"})
	dmsFilter("ReplicationTask", "ReplicationTasks", map[string]string{"replication-task-id": "ReplicationTaskIdentifier", "migration-type": "MigrationType"})
	dmsFilter("ReplicationSubnetGroup", "ReplicationSubnetGroups", map[string]string{"replication-subnet-group-id": "ReplicationSubnetGroupIdentifier"})
}

// dmsDup refuses a second resource with the same identifier: these are keyed by the ARN made up for them.
func dmsDup(c *Call, typ, key string) error {
	for _, r := range c.List(typ) {
		if r.Data[key] == c.In[key] {
			return c.Exists("%s %s already exists.", typ, str(c.In[key]))
		}
	}
	return nil
}

// dmsTaskState is a task's status, and its statistics.
func dmsTaskState(r *Resource) map[string]any {
	var s string
	switch r.Data["_phase"] {
	case "start":
		if r.Data["MigrationType"] == "full-load" {
			s = Step(r, "task", "starting", "running", "stopped")
		} else {
			s = Step(r, "task", "starting", "running")
		}
	case "stop":
		s = Step(r, "task", "stopping", "stopped")
	default:
		s = Step(r, "task", "creating", "ready")
	}
	out := map[string]any{"Status": s}
	if s == "stopped" && r.Data["_phase"] == "start" {
		out["StopReason"] = "Stop Reason FULL_LOAD_ONLY_FINISHED"
		out["ReplicationTaskStats"] = map[string]any{"FullLoadProgressPercent": 100.0, "TablesLoaded": 1.0, "TablesLoading": 0.0, "TablesQueued": 0.0, "TablesErrored": 0.0}
	}
	return out
}
