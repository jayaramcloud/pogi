package codedeploy

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"go.yaml.in/yaml/v3"
)

// ECS blue/green deployments run on the engine of Lambda deployments (see
// step): the lifecycle events are the ECS ones, and where Lambda moves an
// alias, they create a replacement task set and move the production
// listener's forward weights between the group's two target groups.

const (
	ecsService = "AmazonEC2ContainerServiceV20141113"
	elbService = "ElasticLoadBalancing_v10"
)

// ECSDeploy is what an ECS deployment knows about its service, listeners
// and task sets.
type ECSDeploy struct {
	Cluster, Service              string
	TaskDefinition, ContainerName string
	ContainerPort                 int
	PlatformVersion               string
	Network, Capacity             json.RawMessage // in the API's shape, from the AppSpec
	ProdListener, TestListener    string
	Groups                        [2]string     // ARNs of the group's two target groups
	Blue, Green                   string        // target group serving now, and the replacement's; set by Install
	BlueSet, GreenSet             string        // task set IDs, original and replacement
	Keep                          bool          // KEEP_ALIVE: the original task set stays
	Wait                          time.Duration // how long the original task set lives on after the shift
	End                           time.Time     // when the original task set ends; set once traffic has moved
	TestMoved                     bool          // the test listener points at the replacement
}

// ecsAppSpec is the part of an ECS AppSpec file CodeDeploy acts on.
type ecsAppSpec struct {
	Resources []map[string]struct {
		Type       string `yaml:"Type"`
		Properties struct {
			TaskDefinition   string `yaml:"TaskDefinition"`
			LoadBalancerInfo struct {
				ContainerName string `yaml:"ContainerName"`
				ContainerPort int    `yaml:"ContainerPort"`
			} `yaml:"LoadBalancerInfo"`
			PlatformVersion      string `yaml:"PlatformVersion"`
			NetworkConfiguration struct {
				AwsvpcConfiguration struct {
					Subnets        []string `yaml:"Subnets"`
					SecurityGroups []string `yaml:"SecurityGroups"`
					AssignPublicIp string   `yaml:"AssignPublicIp"`
				} `yaml:"AwsvpcConfiguration"`
			} `yaml:"NetworkConfiguration"`
			CapacityProviderStrategy []struct {
				Base             int    `yaml:"Base"`
				CapacityProvider string `yaml:"CapacityProvider"`
				Weight           int    `yaml:"Weight"`
			} `yaml:"CapacityProviderStrategy"`
		} `yaml:"Properties"`
	} `yaml:"Resources"`
	Hooks []map[string]string `yaml:"Hooks"`
}

// ecsEvents are the lifecycle events of an ECS deployment, in order. Only
// the ones that are not in pogiEvents run a hook.
var (
	ecsEvents  = []string{"BeforeInstall", "Install", "AfterInstall", "AllowTestTraffic", "AfterAllowTestTraffic", "BeforeAllowTraffic", "AllowTraffic", "AfterAllowTraffic"}
	pogiEvents = []string{"Install", "AllowTestTraffic", "AllowTraffic"}
)

// ecsGroup is the part of an ECS deployment group CodeDeploy checks.
type ecsGroup struct {
	ECSServices      []struct{ ServiceName, ClusterName string }
	LoadBalancerInfo struct {
		TargetGroupPairInfoList []struct {
			TargetGroups                       []struct{ Name string }
			ProdTrafficRoute, TestTrafficRoute struct{ ListenerArns []string }
		}
	}
	DeploymentStyle                  struct{ DeploymentType, DeploymentOption string }
	BlueGreenDeploymentConfiguration struct {
		DeploymentReadyOption struct {
			ActionOnTimeout   string
			WaitTimeInMinutes int
		}
		TerminateBlueInstancesOnDeploymentSuccess struct {
			Action                       string
			TerminationWaitTimeInMinutes int
		}
	}
}

func readGroup(g map[string]any) *ecsGroup {
	var in ecsGroup
	b, _ := json.Marshal(g)
	json.Unmarshal(b, &in)
	return &in
}

// groupArns finds the ARNs of target groups by name.
func (s *Service) groupArns(account, region string, names []string) ([2]string, error) {
	var out struct {
		TargetGroups []struct{ TargetGroupName, TargetGroupArn string }
	}
	var arns [2]string
	if err := s.hub.Call(account, region, elbService, "DescribeTargetGroups", map[string]any{"Names": names}, &out); err != nil {
		return arns, err
	}
	for i, n := range names {
		j := slices.IndexFunc(out.TargetGroups, func(t struct{ TargetGroupName, TargetGroupArn string }) bool { return t.TargetGroupName == n })
		if j < 0 {
			return arns, fmt.Errorf("target group %s was not found", n)
		}
		arns[i] = out.TargetGroups[j].TargetGroupArn
	}
	return arns, nil
}

// checkECSGroup refuses an ECS deployment group CodeDeploy would refuse: one
// service that exists and uses the CODE_DEPLOY controller, one pair of target
// groups with a production listener (and optionally a test listener), and
// blue/green settings with allowed values. Lock held.
func (s *Service) checkECSGroup(c *core.Ctx, g map[string]any) error {
	in := readGroup(g)
	switch n := len(in.ECSServices); {
	case n == 0:
		return invalid("InvalidECSServiceException", "An ECS service (ecsServices) is required for an ECS deployment group.")
	case n > 1:
		return invalid("ECSServiceMappingLimitExceededException", "An ECS deployment group can have only one ECS service.")
	}
	svc := in.ECSServices[0]
	var out struct {
		Services []struct {
			ServiceName, Status  string
			DeploymentController struct{ Type string }
		}
	}
	err := s.hub.Call(c.Account, c.Region, ecsService, "DescribeServices", map[string]any{"cluster": svc.ClusterName, "services": []string{svc.ServiceName}}, &out)
	switch {
	case err != nil || len(out.Services) == 0 || out.Services[0].Status != "ACTIVE":
		return invalid("InvalidECSServiceException", "The ECS service %s was not found in cluster %s.", svc.ServiceName, svc.ClusterName)
	case out.Services[0].DeploymentController.Type != "CODE_DEPLOY":
		return invalid("InvalidECSServiceException", "The ECS service %s does not use the CODE_DEPLOY deployment controller.", svc.ServiceName)
	}
	pairs := in.LoadBalancerInfo.TargetGroupPairInfoList
	if len(pairs) != 1 {
		return invalid("InvalidTargetGroupPairException", "An ECS deployment group needs exactly one target group pair in loadBalancerInfo.")
	}
	p := pairs[0]
	if len(p.TargetGroups) != 2 || p.TargetGroups[0].Name == "" || p.TargetGroups[0].Name == p.TargetGroups[1].Name {
		return invalid("InvalidTargetGroupPairException", "The target group pair needs two different target groups.")
	}
	if len(p.ProdTrafficRoute.ListenerArns) != 1 || len(p.TestTrafficRoute.ListenerArns) > 1 ||
		len(p.TestTrafficRoute.ListenerArns) == 1 && p.TestTrafficRoute.ListenerArns[0] == p.ProdTrafficRoute.ListenerArns[0] {
		return invalid("InvalidTargetGroupPairException", "The target group pair needs one production listener, and at most one other listener for test traffic.")
	}
	if _, err := s.groupArns(c.Account, c.Region, []string{p.TargetGroups[0].Name, p.TargetGroups[1].Name}); err != nil {
		return invalid("InvalidTargetGroupPairException", "%v", err)
	}
	if in.DeploymentStyle.DeploymentType != "BLUE_GREEN" || in.DeploymentStyle.DeploymentOption != "WITH_TRAFFIC_CONTROL" {
		return invalid("InvalidDeploymentStyleException", "An ECS deployment group must use the BLUE_GREEN deployment type with WITH_TRAFFIC_CONTROL.")
	}
	bg := in.BlueGreenDeploymentConfiguration
	ready, stop := bg.DeploymentReadyOption, bg.TerminateBlueInstancesOnDeploymentSuccess
	switch {
	case !slices.Contains([]string{"", "CONTINUE_DEPLOYMENT", "STOP_DEPLOYMENT"}, ready.ActionOnTimeout) || ready.WaitTimeInMinutes < 0:
		return invalid("InvalidBlueGreenDeploymentConfigurationException", "deploymentReadyOption needs the action CONTINUE_DEPLOYMENT or STOP_DEPLOYMENT and a wait of 0 or more minutes.")
	case !slices.Contains([]string{"", "TERMINATE", "KEEP_ALIVE"}, stop.Action) || stop.TerminationWaitTimeInMinutes < 0 || stop.TerminationWaitTimeInMinutes > 2880:
		return invalid("InvalidBlueGreenDeploymentConfigurationException", "terminateBlueInstancesOnDeploymentSuccess needs the action TERMINATE or KEEP_ALIVE and a wait of 0 to 2880 minutes.")
	}
	return nil
}

// ecsShift reads an ECS deployment's AppSpec and its group and makes the
// progress record the engine runs. Called without the lock.
func (s *Service) ecsShift(c *core.Ctx, g map[string]any, revision json.RawMessage, routing *Routing) (*Shift, error) {
	text, err := s.appSpecText(c, revision)
	if err != nil {
		return nil, err
	}
	var spec ecsAppSpec
	if err := yaml.Unmarshal([]byte(text), &spec); err != nil || len(spec.Resources) != 1 || len(spec.Resources[0]) != 1 {
		return nil, invalid("InvalidRevisionException", "The AppSpec must name one AWS::ECS::Service resource (%v)", err)
	}
	for _, r := range spec.Resources[0] {
		pr := r.Properties
		if r.Type != "AWS::ECS::Service" || pr.TaskDefinition == "" || pr.LoadBalancerInfo.ContainerName == "" || pr.LoadBalancerInfo.ContainerPort <= 0 {
			return nil, invalid("InvalidRevisionException", "The AppSpec resource must have the type AWS::ECS::Service, a TaskDefinition and a LoadBalancerInfo with ContainerName and ContainerPort.")
		}
		in := readGroup(g)
		pair, svc, stop := in.LoadBalancerInfo.TargetGroupPairInfoList[0], in.ECSServices[0], in.BlueGreenDeploymentConfiguration.TerminateBlueInstancesOnDeploymentSuccess
		e := &ECSDeploy{Cluster: svc.ClusterName, Service: svc.ServiceName, TaskDefinition: pr.TaskDefinition, ContainerName: pr.LoadBalancerInfo.ContainerName,
			ContainerPort: pr.LoadBalancerInfo.ContainerPort, PlatformVersion: pr.PlatformVersion, ProdListener: pair.ProdTrafficRoute.ListenerArns[0],
			Keep: stop.Action == "KEEP_ALIVE", Wait: time.Duration(stop.TerminationWaitTimeInMinutes) * time.Minute}
		if e.Groups, err = s.groupArns(c.Account, c.Region, []string{pair.TargetGroups[0].Name, pair.TargetGroups[1].Name}); err != nil {
			return nil, invalid("InvalidTargetGroupPairException", "%v", err)
		}
		if l := pair.TestTrafficRoute.ListenerArns; len(l) == 1 {
			e.TestListener = l[0]
		}
		if n := pr.NetworkConfiguration.AwsvpcConfiguration; len(n.Subnets) > 0 {
			e.Network, _ = json.Marshal(map[string]any{"awsvpcConfiguration": map[string]any{"subnets": n.Subnets, "securityGroups": n.SecurityGroups, "assignPublicIp": n.AssignPublicIp}})
		}
		if len(pr.CapacityProviderStrategy) > 0 {
			var list []map[string]any
			for _, cp := range pr.CapacityProviderStrategy {
				list = append(list, map[string]any{"capacityProvider": cp.CapacityProvider, "weight": cp.Weight, "base": cp.Base})
			}
			e.Capacity, _ = json.Marshal(list)
		}
		l := &Shift{ECS: e, Routing: routing, Hooks: map[string]string{}}
		for _, h := range spec.Hooks {
			for event, fn := range h {
				if !slices.Contains(ecsEvents, event) || slices.Contains(pogiEvents, event) {
					return nil, invalid("InvalidRevisionException", "%s is not a lifecycle event that runs a hook in an ECS deployment.", event)
				}
				l.Hooks[event] = fn
			}
		}
		for _, name := range ecsEvents {
			l.Events = append(l.Events, &Event{LifecycleEventName: name, Status: "Pending"})
		}
		return l, nil
	}
	return nil, nil
}

// forward is a listener's default action sending w percent to the green
// target group and the rest to the blue one.
func forward(blue, green string, w int) []map[string]any {
	return []map[string]any{{"Type": "forward", "ForwardConfig": map[string]any{"TargetGroups": []map[string]any{
		{"TargetGroupArn": blue, "Weight": 100 - w}, {"TargetGroupArn": green, "Weight": w}}}}}
}

func (s *Service) setListener(account, region, arn string, acts []map[string]any) error {
	return s.hub.Call(account, region, elbService, "ModifyListener", map[string]any{"ListenerArn": arn, "DefaultActions": acts}, nil)
}

type taskSet struct {
	Id, Status, StabilityStatus                      string
	ComputedDesiredCount, PendingCount, RunningCount int
}

// taskSets describes the deployment service's task sets with these IDs.
func (s *Service) taskSets(account, region string, e *ECSDeploy, ids ...string) ([]taskSet, error) {
	var out struct{ TaskSets []taskSet }
	err := s.hub.Call(account, region, ecsService, "DescribeTaskSets", map[string]any{"cluster": e.Cluster, "service": e.Service, "taskSets": ids}, &out)
	return out.TaskSets, err
}

func (s *Service) ecsCall(account, region string, e *ECSDeploy, op string, in map[string]any, out any) error {
	in["cluster"], in["service"] = e.Cluster, e.Service
	return s.hub.Call(account, region, ecsService, op, in, out)
}

// ecsWork does the Install and AllowTestTraffic events; it reports whether
// the event is complete. Lock held.
func (s *Service) ecsWork(account, region string, d *Deployment, ev *Event, now time.Time) (bool, error) {
	e := d.Lambda.ECS
	if ev.LifecycleEventName == "AllowTestTraffic" {
		if err := s.setListener(account, region, e.TestListener, forward(e.Blue, e.Green, 100)); err != nil {
			return false, fmt.Errorf("pointing the test listener at the replacement task set: %w", err)
		}
		e.TestMoved = true
		return true, nil
	}
	if e.GreenSet == "" {
		if err := s.ecsInstall(account, region, d); err != nil {
			return false, err
		}
	}
	sets, err := s.taskSets(account, region, e, e.GreenSet)
	switch {
	case err != nil:
		return false, err
	case len(sets) == 1 && sets[0].StabilityStatus == "STEADY_STATE":
		return true, nil
	case now.Sub(time.Time(*ev.StartTime)) > time.Hour:
		return false, fmt.Errorf("the replacement task set did not reach a steady state in an hour")
	}
	return false, nil
}

// ecsInstall finds which target group serves production traffic and which
// task set is the original, then creates the replacement task set behind the
// other target group.
func (s *Service) ecsInstall(account, region string, d *Deployment) error {
	e := d.Lambda.ECS
	var ls struct {
		Listeners []struct {
			DefaultActions []struct {
				Type, TargetGroupArn string
				ForwardConfig        struct {
					TargetGroups []struct {
						TargetGroupArn string
						Weight         *int
					}
				}
			}
		}
	}
	if err := s.hub.Call(account, region, elbService, "DescribeListeners", map[string]any{"ListenerArns": []string{e.ProdListener}}, &ls); err != nil {
		return fmt.Errorf("reading the production listener: %w", err)
	}
	var serving []string // the pair's target groups that get traffic
	for _, l := range ls.Listeners {
		for _, a := range l.DefaultActions {
			if a.Type == "forward" && len(a.ForwardConfig.TargetGroups) == 0 && slices.Contains(e.Groups[:], a.TargetGroupArn) {
				serving = append(serving, a.TargetGroupArn)
			}
			for _, t := range a.ForwardConfig.TargetGroups {
				if a.Type == "forward" && (t.Weight == nil || *t.Weight > 0) && slices.Contains(e.Groups[:], t.TargetGroupArn) {
					serving = append(serving, t.TargetGroupArn)
				}
			}
		}
	}
	slices.Sort(serving)
	if serving = slices.Compact(serving); len(serving) != 1 {
		return fmt.Errorf("the production listener must send all traffic to one of the deployment group's two target groups")
	}
	e.Blue = serving[0]
	e.Green = e.Groups[0]
	if e.Green == e.Blue {
		e.Green = e.Groups[1]
	}
	var svc struct {
		Services []struct{ TaskSets []taskSet }
	}
	if err := s.hub.Call(account, region, ecsService, "DescribeServices", map[string]any{"cluster": e.Cluster, "services": []string{e.Service}}, &svc); err != nil || len(svc.Services) == 0 {
		return fmt.Errorf("the ECS service %s was not found in cluster %s", e.Service, e.Cluster)
	}
	for _, ts := range svc.Services[0].TaskSets {
		if ts.Status == "PRIMARY" {
			e.BlueSet = ts.Id
		}
	}
	if e.BlueSet == "" {
		return fmt.Errorf("the ECS service %s has no primary task set", e.Service)
	}
	in := map[string]any{"taskDefinition": e.TaskDefinition, "externalId": d.DeploymentID, "scale": map[string]any{"value": 100, "unit": "PERCENT"},
		"loadBalancers": []map[string]any{{"targetGroupArn": e.Green, "containerName": e.ContainerName, "containerPort": e.ContainerPort}}}
	if e.PlatformVersion != "" {
		in["platformVersion"] = e.PlatformVersion
	}
	if e.Network != nil {
		in["networkConfiguration"] = e.Network
	}
	if e.Capacity != nil {
		in["capacityProviderStrategy"] = e.Capacity
	}
	var out struct{ TaskSet taskSet }
	if err := s.ecsCall(account, region, e, "CreateTaskSet", in, &out); err != nil {
		return fmt.Errorf("creating the replacement task set: %w", err)
	}
	e.GreenSet = out.TaskSet.Id
	return nil
}

// routeECS puts weight percent of production traffic on the replacement
// target group, and the replacement task set becomes PRIMARY at 100. Weight
// 0 rolls everything back: traffic and the primary task set return to the
// original, and the replacement task set goes. Lock held.
func (s *Service) routeECS(account, region string, e *ECSDeploy, weight int) error {
	if e.Blue == "" { // it failed before Install found anything to restore
		return nil
	}
	if weight > 0 {
		if err := s.setListener(account, region, e.ProdListener, forward(e.Blue, e.Green, weight)); err != nil {
			return err
		}
		if weight < 100 {
			return nil
		}
		return s.ecsCall(account, region, e, "UpdateServicePrimaryTaskSet", map[string]any{"primaryTaskSet": e.GreenSet}, nil)
	}
	errs := []error{s.setListener(account, region, e.ProdListener, forward(e.Blue, e.Green, 0))}
	if e.TestMoved {
		errs = append(errs, s.setListener(account, region, e.TestListener, forward(e.Blue, e.Green, 0)))
	}
	if e.BlueSet != "" {
		errs = append(errs, s.ecsCall(account, region, e, "UpdateServicePrimaryTaskSet", map[string]any{"primaryTaskSet": e.BlueSet}, nil))
	}
	if e.GreenSet != "" {
		errs = append(errs, s.deleteSet(account, region, e, e.GreenSet))
	}
	return errors.Join(errs...)
}

// deleteSet drains a task set; one that is already gone is fine.
func (s *Service) deleteSet(account, region string, e *ECSDeploy, id string) error {
	err := s.ecsCall(account, region, e, "DeleteTaskSet", map[string]any{"taskSet": id, "force": true}, nil)
	if ce := (*core.Error)(nil); errors.As(err, &ce) && ce.Code == "TaskSetNotFoundException" {
		return nil
	}
	return err
}

// ecsTerminate ends the original task set, unless the group keeps it.
func (s *Service) ecsTerminate(account, region string, e *ECSDeploy) error {
	if e.Keep || e.BlueSet == "" {
		return nil
	}
	return s.deleteSet(account, region, e, e.BlueSet)
}

// ecsTarget is the one target of an ECS deployment: the service, with its
// task sets and how much traffic each gets.
func (s *Service) ecsTarget(c *core.Ctx, d *Deployment) map[string]any {
	l, e := d.Lambda, d.Lambda.ECS
	sets := []map[string]any{}
	for _, side := range []struct {
		label, id, group string
		weight           int
	}{
		{"Blue", e.BlueSet, e.Blue, 100 - l.Weight}, {"Green", e.GreenSet, e.Green, l.Weight}} {
		if side.id == "" {
			continue
		}
		if got, err := s.taskSets(c.Account, c.Region, e, side.id); err == nil && len(got) == 1 {
			sets = append(sets, map[string]any{"identifer": got[0].Id, "desiredCount": got[0].ComputedDesiredCount, "pendingCount": got[0].PendingCount,
				"runningCount": got[0].RunningCount, "status": got[0].Status, "trafficWeight": float64(side.weight), "taskSetLabel": side.label,
				"targetGroup": map[string]string{"name": groupName(side.group)}})
		}
	}
	return map[string]any{"deploymentId": d.DeploymentID, "targetId": d.targetID(), "targetArn": c.ARN("ecs", "service/"+e.Cluster+"/"+e.Service),
		"status": d.targetStatus(), "lastUpdatedAt": core.Time(cmp.Or(l.Updated, time.Time(d.CreateTime))), "lifecycleEvents": l.Events, "taskSetsInfo": sets}
}

// groupName is the name in a target group ARN (targetgroup/name/id).
func groupName(arn string) string {
	if p := strings.Split(arn, "/"); len(p) > 1 {
		return p[len(p)-2]
	}
	return arn
}
