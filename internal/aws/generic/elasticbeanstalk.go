package generic

import (
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Elastic Beanstalk: applications, versions (source bundle in S3), configuration
// templates and environments, as metadata. An environment goes Launching to
// Ready (Health Grey to Green) with a CNAME; no containers run.
func init() {
	Derive("elasticbeanstalk", "Environment", ebEnvState)

	// An S3 source bundle has to exist.
	Override("elasticbeanstalk", "CreateApplicationVersion", func(c *Call) (map[string]any, error) {
		if sb, _ := c.In["SourceBundle"].(map[string]any); sb != nil {
			bucket, key := str(sb["S3Bucket"]), str(sb["S3Key"])
			if st, _, _ := c.Hub().Do(c.Account, c.Region, "s3", http.MethodHead, "/"+bucket+"/"+key, nil, nil); st >= 300 {
				return nil, core.Errorf(400, "InvalidParameterValue", "Unable to find the source bundle s3://%s/%s", bucket, key)
			}
		}
		if c.Get("Application", str(c.In["ApplicationName"])) == nil {
			return nil, ebNoApp(str(c.In["ApplicationName"]))
		}
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		r.ARN = c.arnOf("applicationversion/"+str(c.In["ApplicationName"])+"/"+str(c.In["VersionLabel"]), nil)
		r.Data["Status"] = "Processed"
		c.Put(r)
		return c.Fill(r), nil
	})
	Override("elasticbeanstalk", "CreateConfigurationTemplate", func(c *Call) (map[string]any, error) {
		if c.Get("Application", str(c.In["ApplicationName"])) == nil {
			return nil, ebNoApp(str(c.In["ApplicationName"]))
		}
		return c.Default()
	})
	for _, op := range []string{"DescribeApplications", "DescribeApplicationVersions"} {
		Override("elasticbeanstalk", op, func(c *Call) (map[string]any, error) {
			out, err := c.Default()
			if err != nil { // a name that matches nothing is an empty list here
				return map[string]any{c.Op[len("Describe"):]: []any{}}, nil
			}
			if c.Op == "DescribeApplications" {
				ebApps(c, out)
			}
			return out, nil
		})
	}
	Override("elasticbeanstalk", "CreateApplication", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		if a, ok := out["Application"].(map[string]any); ok {
			ebApp(c, a)
		}
		return out, nil
	})
	for _, op := range []string{"UpdateApplication", "DeleteApplication", "DeleteApplicationVersion", "UpdateApplicationVersion", "DeleteConfigurationTemplate", "UpdateConfigurationTemplate"} {
		Override("elasticbeanstalk", op, func(c *Call) (map[string]any, error) {
			out, err := c.Default()
			if err != nil {
				return nil, core.Errorf(400, "InvalidParameterValue", "No %s found: %s", c.typeName(), core.AsError(err).Msg)
			}
			if a, ok := out["Application"].(map[string]any); ok {
				ebApp(c, a)
			}
			return out, nil
		})
	}

	// ---- environments
	Override("elasticbeanstalk", "CreateEnvironment", func(c *Call) (map[string]any, error) {
		app, name := str(c.In["ApplicationName"]), str(c.In["EnvironmentName"])
		if c.Get("Application", app) == nil {
			return nil, ebNoApp(app)
		}
		if old := c.Get("Environment", name); old != nil {
			if ebEnvPhase(old) != "Terminated" {
				return nil, core.Errorf(400, "InvalidParameterValue", "Environment %s already exists.", name)
			}
			c.Svc.Delete(c.Account, c.Region, "Environment", name) // a terminated name can be used again
		}
		if v := str(c.In["VersionLabel"]); v != "" && c.Get("ApplicationVersion", app, v) == nil {
			return nil, core.Errorf(400, "InvalidParameterValue", "No Application Version named '%s' found.", v)
		}
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		prefix := str(c.In["CNAMEPrefix"])
		if prefix == "" {
			prefix = strings.ToLower(name)
		}
		r.ARN = c.arnOf("environment/"+app+"/"+name, nil)
		r.Data["EnvironmentId"] = "e-" + randHex(10)
		r.Data["CNAME"] = prefix + "." + c.Region + ".elasticbeanstalk.localhost"
		r.Data["EndpointURL"] = "awseb-e-" + randHex(10) + "-stack-AWSEBLoadBalancer-" + randHex(8) + "." + c.Region + ".elb.localhost"
		r.Data["_phase"] = "launch"
		if r.Data["Tier"] == nil {
			r.Data["Tier"] = map[string]any{"Name": "WebServer", "Type": "Standard", "Version": "1.0"}
		}
		Begin(r, "env")
		c.Put(r)
		return c.Fill(r), nil
	})
	Override("elasticbeanstalk", "DescribeEnvironments", func(c *Call) (map[string]any, error) {
		names, ids := asList(c.In["EnvironmentNames"]), asList(c.In["EnvironmentIds"])
		var items []any
		for _, r := range c.List("Environment") {
			switch {
			case len(names) > 0 && !slices.Contains(names, any(r.Name)),
				len(ids) > 0 && !slices.Contains(ids, r.Data["EnvironmentId"]),
				str(c.In["ApplicationName"]) != "" && str(r.Data["ApplicationName"]) != str(c.In["ApplicationName"]),
				str(c.In["VersionLabel"]) != "" && str(r.Data["VersionLabel"]) != str(c.In["VersionLabel"]),
				c.In["IncludeDeleted"] != true && ebEnvPhase(r) == "Terminated":
				continue
			}
			items = append(items, c.fillItem(c.op.out.Members["Environments"].sh.Member.sh, r, "Environments"))
		}
		return map[string]any{"Environments": orEmpty(items)}, nil
	})
	Override("elasticbeanstalk", "UpdateEnvironment", func(c *Call) (map[string]any, error) {
		r, err := ebEnv(c)
		if err != nil {
			return nil, err
		}
		if t := str(c.In["TemplateName"]); t != "" && c.Get("ConfigurationTemplate", str(r.Data["ApplicationName"]), t) == nil {
			return nil, core.Errorf(400, "InvalidParameterValue", "No Configuration Template named '%s' found.", t)
		}
		if v := str(c.In["VersionLabel"]); v != "" && c.Get("ApplicationVersion", str(r.Data["ApplicationName"]), v) == nil {
			return nil, core.Errorf(400, "InvalidParameterValue", "No Application Version named '%s' found.", v)
		}
		c.In["EnvironmentName"] = r.Name
		delete(c.In, "EnvironmentId")
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r = c.Res
		r.Data["_phase"] = "update"
		Begin(r, "env")
		c.Put(r)
		return c.Fill(r), nil
	})
	Override("elasticbeanstalk", "TerminateEnvironment", func(c *Call) (map[string]any, error) {
		r, err := ebEnv(c)
		if err != nil {
			return nil, err
		}
		r.Data["_phase"] = "terminate"
		Begin(r, "env")
		c.Put(r)
		return c.Fill(r), nil
	})
	for _, op := range []string{"RebuildEnvironment", "RestartAppServer", "AbortEnvironmentUpdate"} {
		Override("elasticbeanstalk", op, func(c *Call) (map[string]any, error) {
			r, err := ebEnv(c)
			if err != nil {
				return nil, err
			}
			if c.Op == "RebuildEnvironment" {
				r.Data["_phase"] = "update"
				Begin(r, "env")
				c.Put(r)
			}
			return map[string]any{}, nil
		})
	}
	Override("elasticbeanstalk", "DescribeEnvironmentHealth", func(c *Call) (map[string]any, error) {
		r, err := ebEnv(c)
		if err != nil {
			return nil, err
		}
		d := ebEnvState(r)
		return map[string]any{"EnvironmentName": r.Name, "HealthStatus": d["HealthStatus"], "Status": d["Status"], "Color": d["Health"],
			"Causes": []any{}, "RefreshedAt": time.Now().UTC().Format(time.RFC3339)}, nil
	})
	Override("elasticbeanstalk", "DescribeEnvironmentResources", func(c *Call) (map[string]any, error) {
		r, err := ebEnv(c)
		if err != nil {
			return nil, err
		}
		return map[string]any{"EnvironmentResources": map[string]any{"EnvironmentName": r.Name, "AutoScalingGroups": []any{}, "Instances": []any{},
			"LaunchConfigurations": []any{}, "LaunchTemplates": []any{}, "LoadBalancers": []any{}, "Queues": []any{}, "Triggers": []any{}}}, nil
	})
	Override("elasticbeanstalk", "DescribeInstancesHealth", func(c *Call) (map[string]any, error) {
		return map[string]any{"InstanceHealthList": []any{}, "RefreshedAt": time.Now().UTC().Format(time.RFC3339)}, nil
	})
	Override("elasticbeanstalk", "DescribeEvents", func(c *Call) (map[string]any, error) {
		return map[string]any{"Events": []any{}}, nil
	})
	Override("elasticbeanstalk", "ValidateConfigurationSettings", func(c *Call) (map[string]any, error) {
		return map[string]any{"Messages": []any{}}, nil
	})
	Override("elasticbeanstalk", "ListAvailableSolutionStacks", func(c *Call) (map[string]any, error) {
		stacks := []any{"64bit Amazon Linux 2023 v4.3.0 running Python 3.11", "64bit Amazon Linux 2023 v6.1.0 running Node.js 20", "64bit Amazon Linux 2023 v4.0.0 running Docker"}
		return map[string]any{"SolutionStacks": stacks, "SolutionStackDetails": []any{}}, nil
	})
	Override("elasticbeanstalk", "CheckDNSAvailability", func(c *Call) (map[string]any, error) {
		cname := str(c.In["CNAMEPrefix"]) + "." + c.Region + ".elasticbeanstalk.localhost"
		for _, r := range c.List("Environment") {
			if r.Data["CNAME"] == cname && ebEnvPhase(r) != "Terminated" {
				return map[string]any{"Available": false}, nil
			}
		}
		return map[string]any{"Available": true, "FullyQualifiedCNAME": cname}, nil
	})

	// ---- configuration: what the environment or template stores
	Override("elasticbeanstalk", "DescribeConfigurationSettings", func(c *Call) (map[string]any, error) {
		app := str(c.In["ApplicationName"])
		var r *Resource
		switch {
		case str(c.In["TemplateName"]) != "":
			r = c.Get("ConfigurationTemplate", app, str(c.In["TemplateName"]))
		case str(c.In["EnvironmentName"]) != "":
			r = c.Get("Environment", str(c.In["EnvironmentName"]))
			if r != nil && str(r.Data["ApplicationName"]) != app {
				r = nil
			}
		}
		if r == nil {
			return nil, core.Errorf(400, "InvalidParameterValue", "No Environment or Configuration Template found for application %s", app)
		}
		s := map[string]any{"ApplicationName": app, "DeploymentStatus": "deployed", "DateCreated": r.Created.Format(timeFmt), "DateUpdated": r.Updated.Format(timeFmt),
			"OptionSettings": orEmpty(asList(r.Data["OptionSettings"])), "SolutionStackName": r.Data["SolutionStackName"], "PlatformArn": r.Data["PlatformArn"]}
		if r.Type == "Environment" {
			s["EnvironmentName"] = r.Name
		} else {
			s["TemplateName"] = r.Name
			delete(s, "DeploymentStatus")
		}
		if d := r.Data["Description"]; d != nil {
			s["Description"] = d
		}
		return map[string]any{"ConfigurationSettings": []any{s}}, nil
	})

	// ---- tags
	Override("elasticbeanstalk", "UpdateTagsForResource", func(c *Call) (map[string]any, error) {
		r := c.Svc.byARN(c.Account, str(c.In["ResourceArn"]))
		if r == nil {
			return nil, core.Errorf(400, "ResourceNotFoundException", "The resource %s doesn't exist.", str(c.In["ResourceArn"]))
		}
		var remove []string
		for _, k := range asList(c.In["TagsToRemove"]) {
			remove = append(remove, str(k))
		}
		applyTags(r, tagMap(c.In["TagsToAdd"]), remove)
		c.Put(r)
		return map[string]any{}, nil
	})
	Override("elasticbeanstalk", "ListTagsForResource", func(c *Call) (map[string]any, error) {
		r := c.Svc.byARN(c.Account, str(c.In["ResourceArn"]))
		if r == nil {
			return nil, core.Errorf(400, "ResourceNotFoundException", "The resource %s doesn't exist.", str(c.In["ResourceArn"]))
		}
		return map[string]any{"ResourceArn": r.ARN, "ResourceTags": tagsAs(c.op.out.Members["ResourceTags"].sh, r.Tags)}, nil
	})
}

func ebNoApp(name string) error {
	return core.Errorf(400, "InvalidParameterValue", "No Application named '%s' found.", name)
}

// ebEnv finds the environment a request names, by name or id.
func ebEnv(c *Call) (*Resource, error) {
	name, id := str(c.In["EnvironmentName"]), str(c.In["EnvironmentId"])
	for _, r := range c.List("Environment") {
		if (name != "" && r.Name == name) || (id != "" && r.Data["EnvironmentId"] == id) {
			return r, nil
		}
	}
	return nil, core.Errorf(400, "InvalidParameterValue", "No Environment found for EnvironmentName = '%s'.", name+id)
}

// ebEnvPhase is the name of the environment's status.
func ebEnvPhase(r *Resource) string { return str(ebEnvState(r)["Status"]) }

func ebEnvState(r *Resource) map[string]any {
	var status, health, hs string
	switch r.Data["_phase"] {
	case "terminate":
		status = Step(r, "env", "Terminating", "Terminated")
		health, hs = "Grey", "Unknown"
	case "update":
		status = Step(r, "env", "Updating", "Ready")
	default:
		status = Step(r, "env", "Launching", "Ready")
	}
	if health == "" {
		health, hs = "Grey", "Pending"
		if status == "Ready" {
			health, hs = "Green", "Ok"
		}
	}
	return map[string]any{"Status": status, "Health": health, "HealthStatus": hs}
}

// ebApp adds the names of an application's versions and templates.
func ebApp(c *Call, a map[string]any) {
	app := str(a["ApplicationName"])
	var versions, templates []any
	for _, v := range c.List("ApplicationVersion") {
		if v.IDs["ApplicationName"] == app {
			versions = append(versions, v.IDs["VersionLabel"])
		}
	}
	for _, t := range c.List("ConfigurationTemplate") {
		if t.IDs["ApplicationName"] == app {
			templates = append(templates, t.IDs["TemplateName"])
		}
	}
	a["Versions"], a["ConfigurationTemplates"] = orEmpty(versions), orEmpty(templates)
}

func ebApps(c *Call, out map[string]any) {
	for _, a := range asList(out["Applications"]) {
		ebApp(c, a.(map[string]any))
	}
}
