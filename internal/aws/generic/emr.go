package generic

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// EMR: clusters go STARTING, WAITING (or RUNNING while a step is open, and
// TERMINATED once the steps are done on a cluster that does not stay alive);
// steps go PENDING, RUNNING, COMPLETED. Metadata only, nothing runs. A cluster
// is a "Cluster" resource keeping its RunJobFlow input and its steps; its
// state is worked out when it is read.
func init() {
	Override("emr", "RunJobFlow", func(c *Call) (map[string]any, error) {
		now := c.now()
		id := "j-" + core.RandString(13, core.UpperNum)
		data := map[string]any{}
		for k, v := range c.In {
			if k != "Tags" && k != "Steps" {
				data[k] = v
			}
		}
		inst, _ := c.In["Instances"].(map[string]any)
		data["_keep"] = inst["KeepJobFlowAliveWhenNoSteps"] == true
		var steps []any
		for _, s := range asList(c.In["Steps"]) {
			steps = append(steps, emrNewStep(s, now))
		}
		data["_steps"] = orEmpty(steps)
		r := &Resource{Type: "Cluster", Key: id, ID: id, IDs: map[string]string{"ClusterId": id}, ARN: c.arnOf("cluster/"+id, nil), Account: c.Account, Region: c.Region,
			Data: data, Tags: tagMap(c.In["Tags"]), Created: now, Updated: now}
		if len(r.Tags) == 0 {
			r.Tags = nil
		}
		c.Put(r)
		return map[string]any{"JobFlowId": id, "ClusterArn": r.ARN}, nil
	})
	// EMR has no not-found error: a missing thing is an InvalidRequestException.
	for _, op := range []string{"DescribeSecurityConfiguration", "DeleteSecurityConfiguration", "DescribeStudio", "DeleteStudio", "UpdateStudio"} {
		Override("emr", op, func(c *Call) (map[string]any, error) {
			out, err := c.Default()
			if err != nil {
				return nil, c.Invalid("%s", core.AsError(err).Msg)
			}
			return out, nil
		})
	}
	Override("emr", "DescribeCluster", func(c *Call) (map[string]any, error) {
		r, err := emrFind(c, str(c.In["ClusterId"]))
		if err != nil {
			return nil, err
		}
		return map[string]any{"Cluster": emrClusterOut(r)}, nil
	})
	Override("emr", "ListClusters", func(c *Call) (map[string]any, error) {
		want := asList(c.In["ClusterStates"])
		var out []any
		for _, r := range c.List("Cluster") {
			st, _, _ := emrState(r)
			if len(want) > 0 && !slices.Contains(want, any(st)) {
				continue
			}
			full := emrClusterOut(r)
			out = append(out, map[string]any{"Id": r.ID, "Name": full["Name"], "Status": full["Status"], "ClusterArn": r.ARN, "NormalizedInstanceHours": 0.0})
		}
		return map[string]any{"Clusters": orEmpty(out)}, nil
	})
	Override("emr", "AddJobFlowSteps", func(c *Call) (map[string]any, error) {
		r, err := emrFind(c, str(c.In["JobFlowId"]))
		if err != nil {
			return nil, err
		}
		if st, _, _ := emrState(r); st == "TERMINATING" || st == "TERMINATED" {
			return nil, core.Errorf(400, "ValidationException", "A job flow that is shutting down, terminated, or finished may not be modified.")
		}
		steps := asList(r.Data["_steps"])
		var ids []any
		for _, s := range asList(c.In["Steps"]) {
			ns := emrNewStep(s, c.now())
			steps = append(steps, ns)
			ids = append(ids, ns["Id"])
		}
		r.Data["_steps"] = steps
		c.Put(r)
		return map[string]any{"StepIds": orEmpty(ids)}, nil
	})
	Override("emr", "ListSteps", func(c *Call) (map[string]any, error) {
		r, err := emrFind(c, str(c.In["ClusterId"]))
		if err != nil {
			return nil, err
		}
		states, ids := asList(c.In["StepStates"]), asList(c.In["StepIds"])
		var out []any
		for _, s := range slices.Backward(asList(r.Data["_steps"])) { // newest first, like EMR
			so := emrStepOut(r, s.(map[string]any))
			if len(states) > 0 && !slices.Contains(states, any(so["Status"].(map[string]any)["State"])) || len(ids) > 0 && !slices.Contains(ids, so["Id"]) {
				continue
			}
			out = append(out, so)
		}
		return map[string]any{"Steps": orEmpty(out)}, nil
	})
	Override("emr", "DescribeStep", func(c *Call) (map[string]any, error) {
		r, err := emrFind(c, str(c.In["ClusterId"]))
		if err != nil {
			return nil, err
		}
		for _, s := range asList(r.Data["_steps"]) {
			if m := s.(map[string]any); m["Id"] == c.In["StepId"] {
				return map[string]any{"Step": emrStepOut(r, m)}, nil
			}
		}
		return nil, c.Invalid("Step id '%s' is not valid.", str(c.In["StepId"]))
	})
	Override("emr", "CancelSteps", func(c *Call) (map[string]any, error) {
		r, err := emrFind(c, str(c.In["ClusterId"]))
		if err != nil {
			return nil, err
		}
		var infos []any
		for _, id := range asList(c.In["StepIds"]) {
			info := map[string]any{"StepId": id, "Status": "FAILED", "Reason": "No step with this id exists."}
			for _, s := range asList(r.Data["_steps"]) {
				if m := s.(map[string]any); m["Id"] == id {
					if emrStepOut(r, m)["Status"].(map[string]any)["State"] == "PENDING" {
						m["canceled"] = true
						info["Status"], info["Reason"] = "SUBMITTED", nil
						delete(info, "Reason")
					} else {
						info["Reason"] = "Step is not pending."
					}
				}
			}
			infos = append(infos, info)
		}
		c.Put(r)
		return map[string]any{"CancelStepsInfoList": infos}, nil
	})
	Override("emr", "TerminateJobFlows", func(c *Call) (map[string]any, error) {
		var rs []*Resource
		for _, id := range asList(c.In["JobFlowIds"]) {
			r, err := emrFind(c, str(id))
			if err != nil {
				return nil, err
			}
			if r.Data["TerminationProtected"] == true || emrInstances(r)["TerminationProtected"] == true {
				return nil, core.Errorf(400, "ValidationException", "Could not shut down one or more job flows since they are termination protected.")
			}
			rs = append(rs, r)
		}
		for _, r := range rs {
			if r.Data["_term"] == nil {
				Begin(r, "term")
				r.Data["_term"] = true
				c.Put(r)
			}
		}
		return map[string]any{}, nil
	})
	emrSet := func(op, member, key string) {
		Override("emr", op, func(c *Call) (map[string]any, error) {
			for _, id := range asList(c.In["JobFlowIds"]) {
				r, err := emrFind(c, str(id))
				if err != nil {
					return nil, err
				}
				r.Data[key] = c.In[member]
				c.Put(r)
			}
			return map[string]any{}, nil
		})
	}
	emrSet("SetTerminationProtection", "TerminationProtected", "TerminationProtected")
	emrSet("SetVisibleToAllUsers", "VisibleToAllUsers", "VisibleToAllUsers")
	emrSet("SetKeepJobFlowAliveWhenNoSteps", "KeepJobFlowAliveWhenNoSteps", "_keep")
	Override("emr", "ModifyCluster", func(c *Call) (map[string]any, error) {
		r, err := emrFind(c, str(c.In["ClusterId"]))
		if err != nil {
			return nil, err
		}
		if v, ok := c.In["StepConcurrencyLevel"]; ok {
			r.Data["StepConcurrencyLevel"] = v
			c.Put(r)
		}
		return map[string]any{"StepConcurrencyLevel": r.Data["StepConcurrencyLevel"]}, nil
	})
	Override("emr", "ListInstanceGroups", func(c *Call) (map[string]any, error) {
		r, err := emrFind(c, str(c.In["ClusterId"]))
		if err != nil {
			return nil, err
		}
		return map[string]any{"InstanceGroups": orEmpty(emrGroups(r))}, nil
	})
	Override("emr", "ListInstanceFleets", func(c *Call) (map[string]any, error) {
		if _, err := emrFind(c, str(c.In["ClusterId"])); err != nil {
			return nil, err
		}
		return map[string]any{"InstanceFleets": []any{}}, nil
	})
	Override("emr", "ListInstances", func(c *Call) (map[string]any, error) {
		r, err := emrFind(c, str(c.In["ClusterId"]))
		if err != nil {
			return nil, err
		}
		var out []any
		for _, g := range emrGroups(r) {
			g := g.(map[string]any)
			for i := range int(g["RunningInstanceCount"].(float64)) {
				out = append(out, map[string]any{"Id": fmt.Sprintf("ci-%s%d", g["Id"].(string)[3:], i), "Ec2InstanceId": fmt.Sprintf("i-%s%d", strings.ToLower(g["Id"].(string)[3:]), i),
					"InstanceGroupId": g["Id"], "InstanceType": g["InstanceType"], "Market": g["Market"], "Status": map[string]any{"State": "RUNNING"}})
			}
		}
		return map[string]any{"Instances": orEmpty(out)}, nil
	})
	Override("emr", "ListBootstrapActions", func(c *Call) (map[string]any, error) {
		r, err := emrFind(c, str(c.In["ClusterId"]))
		if err != nil {
			return nil, err
		}
		var out []any
		for _, b := range asList(r.Data["BootstrapActions"]) {
			m, _ := b.(map[string]any)
			sb, _ := m["ScriptBootstrapAction"].(map[string]any)
			out = append(out, map[string]any{"Name": m["Name"], "ScriptPath": sb["Path"], "Args": orEmpty(asList(sb["Args"]))})
		}
		return map[string]any{"BootstrapActions": orEmpty(out)}, nil
	})
}

func emrFind(c *Call, id string) (*Resource, error) {
	if r := c.Get("Cluster", id); r != nil {
		return r, nil
	}
	return nil, c.Invalid("Cluster id '%s' is not valid.", id)
}

func emrInstances(r *Resource) map[string]any {
	m, _ := r.Data["Instances"].(map[string]any)
	return m
}

func emrNewStep(cfg any, now time.Time) map[string]any {
	m, _ := cfg.(map[string]any)
	return map[string]any{"Id": "s-" + core.RandString(13, core.UpperNum), "Name": m["Name"], "ActionOnFailure": orDefault(m["ActionOnFailure"], "CONTINUE"),
		"Config": m["HadoopJarStep"], "Added": now.Format(time.RFC3339Nano)}
}

func orDefault(v, def any) any {
	if v == nil || v == "" {
		return def
	}
	return v
}

// emrStepState is where a step is in PENDING, RUNNING, COMPLETED.
func emrStepState(r *Resource, s map[string]any) string {
	if s["canceled"] == true {
		return "CANCELLED"
	}
	if Step(r, "cluster", "STARTING", "WAITING") == "STARTING" {
		return "PENDING"
	}
	t, _ := time.Parse(time.RFC3339Nano, str(s["Added"]))
	return Step(&Resource{Created: t}, "s", "PENDING", "RUNNING", "COMPLETED")
}

func emrStepOut(r *Resource, s map[string]any) map[string]any {
	state := emrStepState(r, s)
	added, _ := time.Parse(time.RFC3339Nano, str(s["Added"]))
	tl := map[string]any{"CreationDateTime": added.Format(timeFmt)}
	if state == "RUNNING" || state == "COMPLETED" {
		tl["StartDateTime"] = added.Format(timeFmt)
	}
	if state == "COMPLETED" {
		tl["EndDateTime"] = time.Now().UTC().Format(timeFmt)
	}
	cfg := orDefault(s["Config"], map[string]any{})
	return map[string]any{"Id": s["Id"], "Name": s["Name"], "ActionOnFailure": s["ActionOnFailure"], "Config": cfg,
		"Status": map[string]any{"State": state, "Timeline": tl}}
}

// emrState is the cluster's state and, for the final ones, why.
func emrState(r *Resource) (state, code, msg string) {
	if r.Data["_term"] != nil {
		return Step(r, "term", "TERMINATING", "TERMINATED"), "USER_REQUEST", "Terminated by user request"
	}
	if Step(r, "cluster", "STARTING", "WAITING") == "STARTING" {
		return "STARTING", "", ""
	}
	for _, s := range asList(r.Data["_steps"]) {
		if st := emrStepState(r, s.(map[string]any)); st != "COMPLETED" && st != "CANCELLED" {
			return "RUNNING", "", ""
		}
	}
	if r.Data["_keep"] != true {
		return "TERMINATED", "ALL_STEPS_COMPLETED", "Steps completed"
	}
	return "WAITING", "", "Cluster ready after last step completed."
}

func emrClusterOut(r *Resource) map[string]any {
	state, code, msg := emrState(r)
	tl := map[string]any{"CreationDateTime": r.Created.Format(timeFmt)}
	if state != "STARTING" {
		tl["ReadyDateTime"] = r.Created.Add(time.Duration(lifecycleDelay.Load())).Format(timeFmt)
	}
	if state == "TERMINATED" {
		tl["EndDateTime"] = time.Now().UTC().Format(timeFmt)
	}
	reason := map[string]any{}
	if code != "" {
		reason["Code"] = code
	}
	if msg != "" {
		reason["Message"] = msg
	}
	inst := emrInstances(r)
	out := map[string]any{
		"Id": r.ID, "Name": r.Data["Name"], "ClusterArn": r.ARN, "ReleaseLabel": r.Data["ReleaseLabel"], "LogUri": r.Data["LogUri"],
		"Status":                  map[string]any{"State": state, "StateChangeReason": reason, "Timeline": tl},
		"AutoTerminate":           r.Data["_keep"] != true,
		"TerminationProtected":    r.Data["TerminationProtected"] == true || inst["TerminationProtected"] == true,
		"VisibleToAllUsers":       r.Data["VisibleToAllUsers"] != false,
		"Applications":            orEmpty(asList(r.Data["Applications"])),
		"Configurations":          orEmpty(asList(r.Data["Configurations"])),
		"Tags":                    []any{},
		"InstanceCollectionType":  "INSTANCE_GROUP",
		"NormalizedInstanceHours": 0.0,
		"ScaleDownBehavior":       orDefault(r.Data["ScaleDownBehavior"], "TERMINATE_AT_TASK_COMPLETION"),
		"ServiceRole":             r.Data["ServiceRole"],
		"StepConcurrencyLevel":    orDefault(r.Data["StepConcurrencyLevel"], 1.0),
		"Ec2InstanceAttributes": map[string]any{"Ec2KeyName": inst["Ec2KeyName"], "Ec2SubnetId": inst["Ec2SubnetId"], "IamInstanceProfile": r.Data["JobFlowRole"],
			"EmrManagedMasterSecurityGroup": inst["EmrManagedMasterSecurityGroup"], "EmrManagedSlaveSecurityGroup": inst["EmrManagedSlaveSecurityGroup"]},
	}
	if state != "STARTING" {
		out["MasterPublicDnsName"] = "ec2-" + strings.ToLower(r.ID[2:]) + ".compute.localhost"
	}
	if len(r.Tags) > 0 {
		var tags []any
		for _, k := range core.SortedKeys(r.Tags) {
			tags = append(tags, map[string]any{"Key": k, "Value": r.Tags[k]})
		}
		out["Tags"] = tags
	}
	for k, v := range out {
		if v == nil {
			delete(out, k)
		}
	}
	return out
}

// emrGroups are the cluster's instance groups, from its instance groups or its master and core settings.
func emrGroups(r *Resource) []any {
	inst := emrInstances(r)
	var groups []map[string]any
	for _, g := range asList(inst["InstanceGroups"]) {
		groups = append(groups, g.(map[string]any))
	}
	if len(groups) == 0 && str(inst["MasterInstanceType"]) != "" {
		groups = append(groups, map[string]any{"Name": "Master", "InstanceRole": "MASTER", "InstanceType": inst["MasterInstanceType"], "InstanceCount": 1.0})
		if n, _ := inst["InstanceCount"].(float64); n > 1 {
			groups = append(groups, map[string]any{"Name": "Core", "InstanceRole": "CORE", "InstanceType": orDefault(inst["SlaveInstanceType"], inst["MasterInstanceType"]), "InstanceCount": n - 1})
		}
	}
	state, _, _ := emrState(r)
	gs := map[string]string{"STARTING": "PROVISIONING", "WAITING": "RUNNING", "RUNNING": "RUNNING", "TERMINATING": "TERMINATING", "TERMINATED": "TERMINATED"}[state]
	var out []any
	for i, g := range groups {
		n, _ := g["InstanceCount"].(float64)
		running := n
		if gs != "RUNNING" {
			running = 0
		}
		out = append(out, map[string]any{"Id": fmt.Sprintf("ig-%s%d", r.ID[2:11], i), "Name": g["Name"], "Market": orDefault(g["Market"], "ON_DEMAND"), "InstanceGroupType": g["InstanceRole"],
			"InstanceType": g["InstanceType"], "RequestedInstanceCount": n, "RunningInstanceCount": running, "Status": map[string]any{"State": gs}})
	}
	return out
}
