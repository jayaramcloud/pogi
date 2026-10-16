package cloudformation

import (
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/nrndev/pogi/internal/core"
)

// ECS clusters, task definitions and services. ECS speaks JSON with lowerCamel
// names; CloudFormation uses PascalCase for the same members.

const ecsTarget = "AmazonEC2ContainerServiceV20141113"

// lowerFirst turns a PascalCase name into lowerCamel: Cpu -> cpu, EFSVolume -> efsVolume.
func lowerFirst(s string) string {
	if s == "FSxWindowsFileServerVolumeConfiguration" {
		return "fsxWindowsFileServerVolumeConfiguration"
	}
	r := []rune(s)
	n := 0
	for n < len(r) && unicode.IsUpper(r[n]) {
		n++
	}
	if n > 1 && n < len(r) {
		n-- // the last capital starts the next word
	}
	for i := 0; i < n; i++ {
		r[i] = unicode.ToLower(r[i])
	}
	return string(r)
}

// ecsInts are members that are numbers in the ECS API but may arrive as strings in a template.
var ecsInts = map[string]bool{"desiredCount": true, "healthCheckGracePeriodSeconds": true, "maximumPercent": true, "minimumHealthyPercent": true,
	"containerPort": true, "hostPort": true, "memory": true, "memoryReservation": true, "cpu": true, "startTimeout": true, "stopTimeout": true,
	"interval": true, "timeout": true, "retries": true, "startPeriod": true, "softLimit": true, "hardLimit": true, "weight": true, "base": true,
	"sizeInGiB": true, "maxSwap": true, "swappiness": true, "sharedMemorySize": true}

// userKeys hold maps whose keys are the user's own: they keep their spelling.
var userKeys = map[string]bool{"options": true, "dockerLabels": true, "driverOpts": true, "labels": true, "secretOptions": true}

// camel converts the member names of a template value to lowerCamel; ints turns numeric strings into numbers for the members in ecsInts.
func camel(v any, ints bool) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, e := range x {
			k = lowerFirst(k)
			switch {
			case userKeys[k]:
				out[k] = e
			case ints && ecsInts[k]:
				out[k] = number(e)
			default:
				out[k] = camel(e, ints)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = camel(e, ints)
		}
		return out
	}
	return v
}

func number(v any) any {
	if s, ok := v.(string); ok {
		if n, err := strconv.ParseFloat(s, 64); err == nil {
			return n
		}
	}
	return v
}

// waitFor polls f until it is true or d has passed, and says which.
func waitFor(d time.Duration, f func() bool) bool {
	for end := time.Now().Add(d); ; time.Sleep(100 * time.Millisecond) {
		if f() {
			return true
		}
		if time.Now().After(end) {
			return false
		}
	}
}

func (c *pctx) ecs(op string, in map[string]any, out any) error {
	return c.call(ecsTarget, op, in, out)
}

// ecsTags makes a resource's tags match the new ones.
func (c *pctx) ecsRetag(arn string, old, now any) error {
	o, n := tagMap(old), tagMap(now)
	var gone []string
	for k := range o {
		if _, ok := n[k]; !ok {
			gone = append(gone, k)
		}
	}
	var add []map[string]string
	for _, k := range core.SortedKeys(n) {
		if v, ok := o[k]; !ok || v != n[k] {
			add = append(add, map[string]string{"key": k, "value": n[k]})
		}
	}
	if len(gone) > 0 {
		if err := c.ecs("UntagResource", map[string]any{"resourceArn": arn, "tagKeys": gone}, nil); err != nil {
			return err
		}
	}
	if len(add) > 0 {
		return c.ecs("TagResource", map[string]any{"resourceArn": arn, "tags": add}, nil)
	}
	return nil
}

func init() {
	register("AWS::ECS::Cluster", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := camel(p, false).(map[string]any)
			name := or(str(p["ClusterName"]), c.name(255, false))
			in["clusterName"] = name
			if s, ok := in["clusterSettings"]; ok {
				in["settings"] = s
				delete(in, "clusterSettings")
			}
			var out struct{ Cluster struct{ ClusterArn string } }
			if err := c.ecs("CreateCluster", in, &out); err != nil {
				return "", nil, err
			}
			return name, map[string]any{"Arn": out.Cluster.ClusterArn}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "ClusterSettings", "Configuration", "ServiceConnectDefaults", "CapacityProviders", "DefaultCapacityProviderStrategy", "Tags") {
				return nil, true, nil
			}
			in := camel(pick(p, "ClusterSettings", "Configuration", "ServiceConnectDefaults"), false).(map[string]any)
			if s, ok := in["clusterSettings"]; ok {
				in["settings"] = s
				delete(in, "clusterSettings")
			}
			in["cluster"] = phys
			var out struct{ Cluster struct{ ClusterArn string } }
			if len(in) > 1 {
				if err := c.ecs("UpdateCluster", in, &out); err != nil {
					return nil, false, err
				}
			}
			if !equal(old["CapacityProviders"], p["CapacityProviders"]) || !equal(old["DefaultCapacityProviderStrategy"], p["DefaultCapacityProviderStrategy"]) {
				if err := c.ecs("PutClusterCapacityProviders", map[string]any{"cluster": phys, "capacityProviders": or2(p["CapacityProviders"], []any{}),
					"defaultCapacityProviderStrategy": or2(camel(p["DefaultCapacityProviderStrategy"], true), []any{})}, nil); err != nil {
					return nil, false, err
				}
			}
			arn := c.arn("ecs", "cluster/"+phys)
			return nil, false, c.ecsRetag(arn, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.ecs("DeleteCluster", map[string]any{"cluster": phys}, nil)
		},
	})

	register("AWS::ECS::TaskDefinition", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := camel(p, false).(map[string]any)
			in["containerDefinitions"] = camel(p["ContainerDefinitions"], true)
			in["family"] = or(str(p["Family"]), c.name(255, false))
			var out struct {
				TaskDefinition struct{ TaskDefinitionArn string }
			}
			if err := c.ecs("RegisterTaskDefinition", in, &out); err != nil {
				return "", nil, err
			}
			arn := out.TaskDefinition.TaskDefinitionArn
			return arn, map[string]any{"TaskDefinitionArn": arn}, nil
		},
		// A task definition revision never changes: any change makes a new one.
		del: func(c *pctx, phys string, p map[string]any) error {
			err := c.ecs("DeregisterTaskDefinition", map[string]any{"taskDefinition": phys}, nil)
			// a revision that is already gone (state lost across a restart) is as good as deregistered
			if err != nil && (strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "Unable to describe")) {
				return nil
			}
			return err
		},
	})

	// The create-only members of a service: a change replaces it.
	svcInPlace := []string{"TaskDefinition", "DesiredCount", "NetworkConfiguration", "DeploymentConfiguration", "LoadBalancers", "HealthCheckGracePeriodSeconds",
		"EnableExecuteCommand", "PlatformVersion", "CapacityProviderStrategy", "PropagateTags", "EnableECSManagedTags", "ServiceRegistries", "PlacementConstraints",
		"PlacementStrategies", "ServiceConnectConfiguration", "Tags"}
	svcState := func(c *pctx, cluster, name string) (running, pending, desired, failed int, status string) {
		var out struct {
			Services []struct {
				RunningCount, PendingCount, DesiredCount int
				Status                                   string
				Deployments                              []struct{ FailedTasks int }
			}
		}
		if c.ecs("DescribeServices", map[string]any{"cluster": cluster, "services": []string{name}}, &out) == nil && len(out.Services) > 0 {
			s := out.Services[0]
			for _, d := range s.Deployments {
				failed += d.FailedTasks
			}
			return s.RunningCount, s.PendingCount, s.DesiredCount, failed, s.Status
		}
		return 0, 0, 0, 0, ""
	}
	register("AWS::ECS::Service", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := camel(p, true).(map[string]any)
			name := or(str(p["ServiceName"]), c.name(255, false))
			in["serviceName"] = name
			if _, ok := p["DesiredCount"]; !ok && str(p["SchedulingStrategy"]) != "DAEMON" {
				in["desiredCount"] = 1
			}
			var out struct{ Service struct{ ServiceArn string } }
			if err := c.ecs("CreateService", in, &out); err != nil {
				return "", nil, err
			}
			arn := out.Service.ServiceArn
			// CloudFormation waits for the tasks to run.
			// ponytail: stops waiting at the first failed task, or after 30s, and keeps going; AWS fails the resource.
			cluster := or(str(p["Cluster"]), "default")
			waitFor(30*time.Second, func() bool {
				r, pe, d, f, _ := svcState(c, cluster, name)
				return (r >= d && pe == 0) || f > 0
			})
			return arn, map[string]any{"ServiceArn": arn, "Name": name}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, svcInPlace...) {
				return nil, true, nil
			}
			in := camel(pick(p, "TaskDefinition", "DesiredCount", "NetworkConfiguration", "DeploymentConfiguration", "LoadBalancers",
				"HealthCheckGracePeriodSeconds", "EnableExecuteCommand", "PlatformVersion", "CapacityProviderStrategy", "PropagateTags",
				"EnableECSManagedTags", "ServiceRegistries", "PlacementConstraints", "PlacementStrategies", "ServiceConnectConfiguration"), true).(map[string]any)
			if s, ok := in["placementStrategies"]; ok {
				in["placementStrategy"] = s
				delete(in, "placementStrategies")
			}
			in["cluster"], in["service"] = or(str(p["Cluster"]), "default"), phys
			if err := c.ecs("UpdateService", in, nil); err != nil {
				return nil, false, err
			}
			return nil, false, c.ecsRetag(phys, old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			cluster := or(str(p["Cluster"]), "default")
			if err := c.ecs("DeleteService", map[string]any{"cluster": cluster, "service": phys, "force": true}, nil); err != nil {
				return err
			}
			// wait for the service to drain: its cluster cannot go before
			waitFor(30*time.Second, func() bool {
				_, _, _, _, st := svcState(c, cluster, lastSegment(phys))
				return st == "INACTIVE" || st == ""
			})
			return nil
		},
	})
}

func or2(v, def any) any {
	if v == nil {
		return def
	}
	return v
}
