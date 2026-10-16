// Package autoscaling emulates Amazon EC2 Auto Scaling over the Query
// protocol: groups launching from launch templates, mixed instances policies
// or launch configurations, kept at their desired capacity through EC2
// (hub.Call) and registered with ELBv2 target groups; simple, step and target
// tracking policies run by CloudWatch alarms or ExecutePolicy; scheduled
// actions, lifecycle hooks (stored), suspended processes, tags and warm pools
// (metadata). Application Auto Scaling shares its step and tracking math.
package autoscaling

import (
	"cmp"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/events"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Auto Scaling emulator.
type Service struct {
	hub  *core.Hub
	rpc  *core.RPC
	mu   sync.Mutex
	rec  sync.Mutex // one reconciliation at a time: they call EC2 outside mu
	st   state
	stop func()
}

type state struct {
	Groups  map[string]*group               `json:"groups"`  // account/region/name
	Configs map[string]*LaunchConfiguration `json:"configs"` // account/region/name
}

// group is a group with what the API returns separately.
type group struct {
	Group
	Account    string             `json:"account"`
	Region     string             `json:"region"`
	Policies   map[string]*Policy `json:"policies"`
	Actions    map[string]*action `json:"actions"`
	Hooks      map[string]*Hook   `json:"hooks"`
	Activities []Activity         `json:"activities,omitempty"`
}

// Group is an Auto Scaling group in its wire shape; CreateAutoScalingGroup
// decodes into it.
type Group struct {
	AutoScalingGroupName             string
	AutoScalingGroupARN              string
	LaunchConfigurationName          string
	LaunchTemplate                   *TemplateSpec
	MixedInstancesPolicy             *MixedPolicy
	MinSize                          int
	MaxSize                          int
	DesiredCapacity                  int
	DefaultCooldown                  int
	AvailabilityZones                []string
	LoadBalancerNames                []string
	TargetGroupARNs                  []string
	HealthCheckType                  string
	HealthCheckGracePeriod           int
	Instances                        []Instance
	CreatedTime                      core.Time
	SuspendedProcesses               []Process
	PlacementGroup                   string
	VPCZoneIdentifier                string
	EnabledMetrics                   []EnabledMetric
	Tags                             []Tag
	TerminationPolicies              []string
	NewInstancesProtectedFromScaleIn bool
	ServiceLinkedRoleARN             string
	MaxInstanceLifetime              int
	CapacityRebalance                bool
	WarmPoolConfiguration            *WarmPool
	WarmPoolSize                     int
	Context                          string
	DesiredCapacityType              string
	DefaultInstanceWarmup            int
	InstanceMaintenancePolicy        *struct{ MinHealthyPercentage, MaxHealthyPercentage int }
}

// TemplateSpec names a launch template and version.
type TemplateSpec struct {
	LaunchTemplateId, LaunchTemplateName, Version string
}

// MixedPolicy is a mixed instances policy.
// ponytail: launches use its template and first override; no spot/on-demand split.
type MixedPolicy struct {
	LaunchTemplate *struct {
		LaunchTemplateSpecification *TemplateSpec
		Overrides                   []struct {
			InstanceType, WeightedCapacity string
			LaunchTemplateSpecification    *TemplateSpec
		}
	}
	InstancesDistribution *struct {
		OnDemandAllocationStrategy, SpotAllocationStrategy, SpotMaxPrice             string
		OnDemandBaseCapacity, OnDemandPercentageAboveBaseCapacity, SpotInstancePools *int
	}
}

// Instance is a member of a group.
type Instance struct {
	InstanceId              string
	InstanceType            string
	AvailabilityZone        string
	LifecycleState          string
	HealthStatus            string
	LaunchConfigurationName string
	LaunchTemplate          *TemplateSpec
	ProtectedFromScaleIn    bool
}

// Tag is a group tag.
type Tag struct {
	ResourceId, ResourceType, Key, Value string
	PropagateAtLaunch                    bool
}

// Process is a suspended process; EnabledMetric a group metric collected.
type (
	Process       struct{ ProcessName, SuspensionReason string }
	EnabledMetric struct{ Metric, Granularity string }
)

// WarmPool is a warm pool configuration (metadata: it holds no instances).
type WarmPool struct {
	MaxGroupPreparedCapacity *int
	MinSize                  int
	PoolState                string
	Status                   string
	InstanceReusePolicy      *struct{ ReuseOnScaleIn bool }
}

// Activity is a scaling activity.
type Activity struct {
	ActivityId           string
	AutoScalingGroupName string
	AutoScalingGroupARN  string
	Description          string
	Cause                string
	StartTime            core.Time
	EndTime              core.Time
	StatusCode           string
	StatusMessage        string
	Progress             int
	Details              string
}

// Hook is a lifecycle hook; pogi stores it, transitions do not wait for it.
type Hook struct {
	LifecycleHookName, AutoScalingGroupName, LifecycleTransition string
	NotificationTargetARN, RoleARN, NotificationMetadata         string
	HeartbeatTimeout, GlobalTimeout                              int
	DefaultResult                                                string
}

// Action is a scheduled action in its wire shape.
type Action struct {
	AutoScalingGroupName, ScheduledActionName, ScheduledActionARN string
	Time, StartTime, EndTime                                      *core.Time
	Recurrence                                                    string
	MinSize, MaxSize, DesiredCapacity                             *int
	TimeZone                                                      string
}

type action struct {
	Action
	Next time.Time `json:"next"` // zero: never again
}

// Policy is a scaling policy in its wire shape.
type Policy struct {
	AutoScalingGroupName        string
	PolicyName                  string
	PolicyARN                   string
	PolicyType                  string
	AdjustmentType              string
	MinAdjustmentMagnitude      *int
	ScalingAdjustment           *int
	Cooldown                    *int
	StepAdjustments             []StepAdjustment
	MetricAggregationType       string
	EstimatedInstanceWarmup     *int
	Alarms                      []Alarm
	TargetTrackingConfiguration *Tracking
	Enabled                     bool
}

// LaunchConfiguration is a launch configuration in its wire shape.
type LaunchConfiguration struct {
	LaunchConfigurationName, LaunchConfigurationARN string
	ImageId, KeyName, InstanceType, UserData        string
	SecurityGroups                                  []string
	KernelId, RamdiskId, SpotPrice                  string
	BlockDeviceMappings                             []struct {
		DeviceName, VirtualName string
		Ebs                     *struct {
			SnapshotId, VolumeType         string
			VolumeSize, Iops, Throughput   int
			DeleteOnTermination, Encrypted *bool
		}
		NoDevice *bool
	}
	InstanceMonitoring       *struct{ Enabled bool }
	IamInstanceProfile       string
	CreatedTime              core.Time
	EbsOptimized             bool
	AssociatePublicIpAddress *bool
	PlacementTenancy         string
	MetadataOptions          *struct {
		HttpTokens, HttpEndpoint string
		HttpPutResponseHopLimit  int
	}
}

// New creates the Auto Scaling service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{XMLNS: "http://autoscaling.amazonaws.com/doc/2011-01-01/", Ops: map[string]core.Handler{
		"CreateAutoScalingGroup":              core.H(s.createGroup),
		"DescribeAutoScalingGroups":           core.H(s.describeGroups),
		"UpdateAutoScalingGroup":              core.H(s.updateGroup),
		"DeleteAutoScalingGroup":              core.H(s.deleteGroup),
		"SetDesiredCapacity":                  core.H(s.setDesiredCapacity),
		"DescribeAutoScalingInstances":        core.H(s.describeInstances),
		"AttachInstances":                     core.H(s.attachInstances),
		"DetachInstances":                     core.H(s.detachInstances),
		"TerminateInstanceInAutoScalingGroup": core.H(s.terminateInstance),
		"SetInstanceProtection":               core.H(s.setInstanceProtection),
		"SetInstanceHealth":                   core.H(s.setInstanceHealth),
		"AttachLoadBalancerTargetGroups":      core.H(s.targetGroups(true)),
		"DetachLoadBalancerTargetGroups":      core.H(s.targetGroups(false)),
		"DescribeLoadBalancerTargetGroups":    core.H(s.describeTargetGroups),
		"AttachLoadBalancers":                 core.H(s.loadBalancers(true)),
		"DetachLoadBalancers":                 core.H(s.loadBalancers(false)),
		"DescribeLoadBalancers":               core.H(s.describeLoadBalancers),
		"DescribeScalingActivities":           core.H(s.describeActivities),
		"CreateLaunchConfiguration":           core.H(s.createLaunchConfiguration),
		"DescribeLaunchConfigurations":        core.H(s.describeLaunchConfigurations),
		"DeleteLaunchConfiguration":           core.H(s.deleteLaunchConfiguration),
		"PutScalingPolicy":                    core.H(s.putPolicy),
		"DescribePolicies":                    core.H(s.describePolicies),
		"DeletePolicy":                        core.H(s.deletePolicy),
		"ExecutePolicy":                       core.H(s.executePolicy),
		"PutScheduledUpdateGroupAction":       core.H(s.putAction),
		"DescribeScheduledActions":            core.H(s.describeActions),
		"DeleteScheduledAction":               core.H(s.deleteAction),
		"PutLifecycleHook":                    core.H(s.putHook),
		"DescribeLifecycleHooks":              core.H(s.describeHooks),
		"DeleteLifecycleHook":                 core.H(s.deleteHook),
		"CompleteLifecycleAction":             core.H(s.lifecycleAction),
		"RecordLifecycleActionHeartbeat":      core.H(s.lifecycleAction),
		"SuspendProcesses":                    core.H(s.processes(true)),
		"ResumeProcesses":                     core.H(s.processes(false)),
		"EnableMetricsCollection":             core.H(s.metrics(true)),
		"DisableMetricsCollection":            core.H(s.metrics(false)),
		"CreateOrUpdateTags":                  core.H(s.tags(true)),
		"DeleteTags":                          core.H(s.tags(false)),
		"DescribeTags":                        core.H(s.describeTags),
		"PutWarmPool":                         core.H(s.putWarmPool),
		"DescribeWarmPool":                    core.H(s.describeWarmPool),
		"DeleteWarmPool":                      core.H(s.deleteWarmPool),
		"DescribeAccountLimits":               core.H(s.describeAccountLimits),
	}}
	hub.OnDeliver("autoscaling", s.deliver)
	s.stop = core.Every(10*time.Second, s.Tick)
	return s
}

func (s *Service) Info() core.Info {
	// The smithy service name takes pogi's in-process JSON calls (hub.Call).
	return core.Info{Name: "autoscaling", Cloud: core.AWS, Title: "Auto Scaling", Signing: []string{"autoscaling"},
		Targets: []string{"AutoScaling_2011_01_01"}, Protocol: "query"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

// Close stops the clock.
func (s *Service) Close() error {
	s.stop()
	return nil
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Groups: map[string]*group{}, Configs: map[string]*LaunchConfiguration{}}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := state{Groups: map[string]*group{}, Configs: map[string]*LaunchConfiguration{}}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	for _, g := range st.Groups {
		g.init()
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func (g *group) init() *group {
	if g.Policies == nil {
		g.Policies, g.Actions, g.Hooks = map[string]*Policy{}, map[string]*action{}, map[string]*Hook{}
	}
	return g
}

func key(account, region, name string) string { return account + "/" + region + "/" + name }

func invalid(format string, a ...any) error { return core.Errorf(400, "ValidationError", format, a...) }

// group finds a group; lock held.
func (s *Service) group(c *core.Ctx, name string) (*group, error) {
	if g := s.st.Groups[key(c.Account, c.Region, name)]; g != nil {
		return g, nil
	}
	return nil, invalid("AutoScalingGroup name not found - AutoScalingGroup %s not found", name)
}

func (g *group) suspended(process string) bool {
	return slices.ContainsFunc(g.SuspendedProcesses, func(p Process) bool { return p.ProcessName == process })
}

// view is a copy of the group safe to encode after unlocking.
func (g *group) view() Group {
	v := g.Group
	v.Instances, v.Tags, v.SuspendedProcesses, v.EnabledMetrics = slices.Clone(v.Instances), slices.Clone(v.Tags),
		slices.Clone(v.SuspendedProcesses), slices.Clone(v.EnabledMetrics)
	return v
}

// record adds a scaling activity, newest first; lock held.
func (g *group) record(desc, cause, status, msg string) Activity {
	now := time.Now().UTC()
	a := Activity{ActivityId: core.UUID(), AutoScalingGroupName: g.AutoScalingGroupName, AutoScalingGroupARN: g.AutoScalingGroupARN,
		Description: desc, Cause: "At " + now.Format(time.RFC3339) + " " + cause + ".", StartTime: core.Time(now), EndTime: core.Time(now),
		StatusCode: status, StatusMessage: msg, Progress: 100, Details: "{}"}
	g.Activities = append([]Activity{a}, g.Activities[:min(len(g.Activities), 99)]...)
	return a
}

// ---- groups ----

var groupName = regexp.MustCompile(`^[\x{0020}-\x{D7FF}\x{E000}-\x{FFFD}]{1,255}$`)

func (s *Service) createGroup(c *core.Ctx, in *struct {
	Group
	DesiredCapacity                *int
	LifecycleHookSpecificationList []Hook
}) (any, error) {
	g := (&group{Group: in.Group, Account: c.Account, Region: c.Region}).init()
	n := g.AutoScalingGroupName
	g.DesiredCapacity = g.MinSize
	if in.DesiredCapacity != nil {
		g.DesiredCapacity = *in.DesiredCapacity
	}
	sources := 0
	for _, set := range []bool{g.LaunchTemplate != nil, g.MixedInstancesPolicy != nil, g.LaunchConfigurationName != ""} {
		if set {
			sources++
		}
	}
	switch {
	case !groupName.MatchString(n) || strings.Contains(n, ":"):
		return nil, invalid("1 validation error detected: Value '%s' at 'autoScalingGroupName' failed to satisfy constraint", n)
	case sources != 1:
		return nil, invalid("Valid requests must contain either LaunchTemplate, LaunchConfigurationName, InstanceId or MixedInstancesPolicy parameter.")
	case g.MinSize > g.MaxSize:
		return nil, invalid("Max bound, %d, must be greater than or equal to min bound, %d", g.MaxSize, g.MinSize)
	case g.DesiredCapacity < g.MinSize || g.DesiredCapacity > g.MaxSize:
		return nil, invalid("Desired capacity:%d must be between the specified min size:%d and max size:%d", g.DesiredCapacity, g.MinSize, g.MaxSize)
	case len(g.AvailabilityZones) == 0 && g.VPCZoneIdentifier == "":
		return nil, invalid("At least one Availability Zone or VPC Subnet is required.")
	}
	g.AutoScalingGroupARN = c.ARN("autoscaling", "autoScalingGroup:"+core.UUID()+":autoScalingGroupName/"+n)
	g.DefaultCooldown, g.HealthCheckType = cmp.Or(g.DefaultCooldown, 300), cmp.Or(g.HealthCheckType, "EC2")
	g.ServiceLinkedRoleARN = cmp.Or(g.ServiceLinkedRoleARN, c.GlobalARN("iam", "role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling"))
	g.CreatedTime, g.Instances, g.SuspendedProcesses, g.EnabledMetrics, g.WarmPoolConfiguration = core.Time(time.Now().UTC()), nil, nil, nil, nil
	if len(g.TerminationPolicies) == 0 {
		g.TerminationPolicies = []string{"Default"}
	}
	for i := range g.Tags {
		g.Tags[i].ResourceId, g.Tags[i].ResourceType = n, "auto-scaling-group"
	}
	for _, h := range in.LifecycleHookSpecificationList {
		h.AutoScalingGroupName = n
		g.Hooks[h.LifecycleHookName] = hookDefaults(h)
	}
	s.mu.Lock()
	if g.LaunchConfigurationName != "" && s.st.Configs[key(c.Account, c.Region, g.LaunchConfigurationName)] == nil {
		s.mu.Unlock()
		return nil, invalid("Launch configuration name not found - Launch configuration %s not found", g.LaunchConfigurationName)
	}
	if s.st.Groups[key(c.Account, c.Region, n)] != nil {
		s.mu.Unlock()
		return nil, core.Errorf(400, "AlreadyExists", "AutoScalingGroup by this name already exists - A group with the name %s already exists", n)
	}
	s.st.Groups[key(c.Account, c.Region, n)] = g
	s.mu.Unlock()
	s.reconcile(c.Account, c.Region, n, fmt.Sprintf("a user request created an AutoScalingGroup changing the desired capacity from 0 to %d", g.DesiredCapacity))
	return nil, nil
}

type filter struct {
	Name   string
	Values []string
}

// tagged applies tag filters (tag-key, tag-value, tag:<key>) to a group.
func tagged(fs []filter, tags []Tag) bool {
	for _, f := range fs {
		if !slices.ContainsFunc(tags, func(t Tag) bool {
			k, ok := strings.CutPrefix(f.Name, "tag:")
			return f.Name == "tag-key" && slices.Contains(f.Values, t.Key) || f.Name == "tag-value" && slices.Contains(f.Values, t.Value) ||
				ok && t.Key == k && slices.Contains(f.Values, t.Value)
		}) {
			return false
		}
	}
	return true
}

func (s *Service) describeGroups(c *core.Ctx, in *struct {
	AutoScalingGroupNames []string
	Filters               []filter
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Group{}
	for _, k := range core.SortedKeys(s.st.Groups) {
		g := s.st.Groups[k]
		if g.Account == c.Account && g.Region == c.Region && (len(in.AutoScalingGroupNames) == 0 || slices.Contains(in.AutoScalingGroupNames,
			g.AutoScalingGroupName)) && tagged(in.Filters, g.Tags) {
			out = append(out, g.view())
		}
	}
	return map[string]any{"AutoScalingGroups": out}, nil
}

func (s *Service) updateGroup(c *core.Ctx, in *struct {
	AutoScalingGroupName                                                        string
	LaunchConfigurationName, HealthCheckType, PlacementGroup, VPCZoneIdentifier *string
	ServiceLinkedRoleARN, Context, DesiredCapacityType                          *string
	LaunchTemplate                                                              *TemplateSpec
	MixedInstancesPolicy                                                        *MixedPolicy
	MinSize, MaxSize, DesiredCapacity, DefaultCooldown, HealthCheckGracePeriod  *int
	MaxInstanceLifetime, DefaultInstanceWarmup                                  *int
	NewInstancesProtectedFromScaleIn, CapacityRebalance                         *bool
	AvailabilityZones, TerminationPolicies                                      []string
	InstanceMaintenancePolicy                                                   *struct{ MinHealthyPercentage, MaxHealthyPercentage int }
}) (any, error) {
	s.mu.Lock()
	g, err := s.group(c, in.AutoScalingGroupName)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	next := g.Group
	switch { // a new source replaces the old one
	case in.LaunchTemplate != nil:
		next.LaunchConfigurationName, next.MixedInstancesPolicy = "", nil
	case in.MixedInstancesPolicy != nil:
		next.LaunchConfigurationName, next.LaunchTemplate = "", nil
	case in.LaunchConfigurationName != nil:
		next.LaunchTemplate, next.MixedInstancesPolicy = nil, nil
	}
	src, dst := reflect.ValueOf(in).Elem(), reflect.ValueOf(&next).Elem()
	for i := 1; i < src.NumField(); i++ { // set fields, by name
		if f := src.Field(i); !f.IsZero() {
			t := dst.FieldByName(src.Type().Field(i).Name)
			if f.Kind() == reflect.Pointer && t.Kind() != reflect.Pointer {
				f = f.Elem()
			}
			t.Set(f)
		}
	}
	if in.DesiredCapacity == nil { // new bounds move the desired capacity into them
		next.DesiredCapacity = min(max(next.DesiredCapacity, next.MinSize), next.MaxSize)
	}
	switch {
	case next.MinSize > next.MaxSize:
		err = invalid("Max bound, %d, must be greater than or equal to min bound, %d", next.MaxSize, next.MinSize)
	case next.DesiredCapacity < next.MinSize || next.DesiredCapacity > next.MaxSize:
		err = invalid("Desired capacity:%d must be between the specified min size:%d and max size:%d", next.DesiredCapacity, next.MinSize, next.MaxSize)
	case next.LaunchConfigurationName != "" && s.st.Configs[key(c.Account, c.Region, next.LaunchConfigurationName)] == nil:
		err = invalid("Launch configuration name not found - Launch configuration %s not found", next.LaunchConfigurationName)
	}
	old := g.DesiredCapacity
	if err == nil {
		g.Group = next
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	s.reconcile(c.Account, c.Region, in.AutoScalingGroupName,
		fmt.Sprintf("a user request update of AutoScalingGroup constraints changing the desired capacity from %d to %d", old, next.DesiredCapacity))
	return nil, nil
}

func (s *Service) deleteGroup(c *core.Ctx, in *struct {
	AutoScalingGroupName string
	ForceDelete          bool
}) (any, error) {
	s.rec.Lock()
	defer s.rec.Unlock()
	s.mu.Lock()
	g, err := s.group(c, in.AutoScalingGroupName)
	if err == nil && len(g.Instances) > 0 && !in.ForceDelete {
		err = core.Errorf(400, "ResourceInUse", "You cannot delete an AutoScalingGroup while there are instances or pending Spot instance request(s) still in the group.")
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	delete(s.st.Groups, key(c.Account, c.Region, in.AutoScalingGroupName))
	var alarms []Alarm
	for _, p := range g.Policies {
		alarms = append(alarms, p.Alarms...)
	}
	ids, tgs := instanceIDs(g.Instances), slices.Clone(g.TargetGroupARNs)
	s.mu.Unlock()
	s.terminate(c.Account, c.Region, ids)
	s.targets(c.Account, c.Region, tgs, nil, ids)
	DeleteAlarms(s.hub, c.Account, c.Region, alarms)
	return nil, nil
}

func instanceIDs(list []Instance) []string {
	var ids []string
	for _, i := range list {
		ids = append(ids, i.InstanceId)
	}
	return ids
}

func (s *Service) setDesiredCapacity(c *core.Ctx, in *struct {
	AutoScalingGroupName string
	DesiredCapacity      int
	HonorCooldown        bool
}) (any, error) {
	s.mu.Lock()
	g, err := s.group(c, in.AutoScalingGroupName)
	if err == nil && in.DesiredCapacity > g.MaxSize {
		err = invalid("New SetDesiredCapacity value %d is above max value %d for the AutoScalingGroup.", in.DesiredCapacity, g.MaxSize)
	} else if err == nil && in.DesiredCapacity < g.MinSize {
		err = invalid("New SetDesiredCapacity value %d is below min value %d for the AutoScalingGroup.", in.DesiredCapacity, g.MinSize)
	}
	var old int
	if err == nil {
		old, g.DesiredCapacity = g.DesiredCapacity, in.DesiredCapacity
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	s.reconcile(c.Account, c.Region, in.AutoScalingGroupName,
		fmt.Sprintf("a user request explicitly set group desired capacity changing the desired capacity from %d to %d", old, in.DesiredCapacity))
	return nil, nil
}

// ---- keeping capacity ----

// reconcile launches or terminates instances until a group has its desired
// capacity. EC2 and ELBv2 are called outside the lock; rec keeps two
// reconciliations from filling the same gap.
// ponytail: new instances are InService at once (no warm-up, hooks do not
// wait), which is what lets Terraform's capacity wait pass promptly.
func (s *Service) reconcile(account, region, name, cause string) {
	s.rec.Lock()
	defer s.rec.Unlock()
	s.mu.Lock()
	g := s.st.Groups[key(account, region, name)]
	if g == nil {
		s.mu.Unlock()
		return
	}
	gap, have := g.DesiredCapacity-len(g.Instances), len(g.Instances)
	var victims []Instance
	if gap < 0 && !g.suspended("Terminate") {
		victims = g.victims(-gap)
	}
	if g.suspended("Launch") {
		gap = 0
	}
	in, proto := g.launchInput(s.st.Configs[key(account, region, g.LaunchConfigurationName)])
	places, subnets := strings.Split(g.VPCZoneIdentifier, ","), true
	if g.VPCZoneIdentifier == "" {
		places, subnets = slices.Clone(g.AvailabilityZones), false
	}
	if len(places) == 0 {
		places = []string{""} // EC2 picks
	}
	tgs := slices.Clone(g.TargetGroupARNs)
	s.mu.Unlock()

	var launched []Instance
	var failed error
	for k := range max(gap, 0) {
		place := strings.TrimSpace(places[(have+k)%len(places)])
		in["SubnetId"], in["Placement"] = nil, nil
		switch {
		case place == "":
		case subnets:
			in["SubnetId"] = place
		default:
			in["Placement"] = map[string]string{"AvailabilityZone": place}
		}
		var out struct {
			Instances []struct {
				InstanceId, InstanceType string
				Placement                struct{ AvailabilityZone string }
			}
		}
		if failed = s.hub.Call(account, region, "AmazonEC2", "RunInstances", in, &out); failed != nil {
			break
		}
		i := proto
		i.InstanceId, i.InstanceType, i.AvailabilityZone = out.Instances[0].InstanceId, out.Instances[0].InstanceType, out.Instances[0].Placement.AvailabilityZone
		launched = append(launched, i)
	}
	gone := instanceIDs(victims)
	s.terminate(account, region, gone)
	s.targets(account, region, tgs, instanceIDs(launched), gone)

	s.mu.Lock()
	defer s.mu.Unlock()
	if g = s.st.Groups[key(account, region, name)]; g == nil {
		return
	}
	for _, i := range launched {
		g.Instances = append(g.Instances, i)
		g.record("Launching a new EC2 instance: "+i.InstanceId, cause, "Successful", "")
	}
	for _, id := range gone {
		g.Instances = slices.DeleteFunc(g.Instances, func(i Instance) bool { return i.InstanceId == id })
		g.record("Terminating EC2 instance: "+id, cause, "Successful", "")
	}
	if failed != nil {
		msg := core.AsError(failed).Msg
		g.record("Launching a new EC2 instance.  Status Reason: "+msg, cause, "Failed", msg)
	}
}

// victims are n unprotected instances to terminate: the newest, or the
// oldest under an Oldest* termination policy; lock held.
// ponytail: scale-in does not balance zones.
func (g *group) victims(n int) []Instance {
	list := slices.DeleteFunc(slices.Clone(g.Instances), func(i Instance) bool { return i.ProtectedFromScaleIn })
	if !slices.ContainsFunc(g.TerminationPolicies, func(p string) bool { return strings.HasPrefix(p, "Oldest") }) {
		slices.Reverse(list)
	}
	return list[:min(n, len(list))]
}

// launchInput is the RunInstances input for one of the group's instances
// (but where), and the group's record of it; lock held.
func (g *group) launchInput(lc *LaunchConfiguration) (map[string]any, Instance) {
	tags := []map[string]string{{"Key": "aws:autoscaling:groupName", "Value": g.AutoScalingGroupName}}
	for _, t := range g.Tags {
		if t.PropagateAtLaunch {
			tags = append(tags, map[string]string{"Key": t.Key, "Value": t.Value})
		}
	}
	in := map[string]any{"MinCount": 1, "MaxCount": 1, "TagSpecifications": []map[string]any{{"ResourceType": "instance", "Tags": tags}}}
	proto := Instance{LifecycleState: "InService", HealthStatus: "Healthy", ProtectedFromScaleIn: g.NewInstancesProtectedFromScaleIn}
	switch m := g.MixedInstancesPolicy; {
	case g.LaunchTemplate != nil:
		in["LaunchTemplate"], proto.LaunchTemplate = *g.LaunchTemplate, g.LaunchTemplate
	case m != nil && m.LaunchTemplate != nil && m.LaunchTemplate.LaunchTemplateSpecification != nil:
		in["LaunchTemplate"], proto.LaunchTemplate = *m.LaunchTemplate.LaunchTemplateSpecification, m.LaunchTemplate.LaunchTemplateSpecification
		if len(m.LaunchTemplate.Overrides) > 0 && m.LaunchTemplate.Overrides[0].InstanceType != "" {
			in["InstanceType"] = m.LaunchTemplate.Overrides[0].InstanceType
		}
	case lc != nil:
		proto.LaunchConfigurationName = lc.LaunchConfigurationName
		in["ImageId"], in["InstanceType"], in["KeyName"], in["UserData"], in["EbsOptimized"] = lc.ImageId, lc.InstanceType, lc.KeyName, lc.UserData, lc.EbsOptimized
		var ids, names []string
		for _, sg := range lc.SecurityGroups {
			if strings.HasPrefix(sg, "sg-") {
				ids = append(ids, sg)
			} else {
				names = append(names, sg)
			}
		}
		in["SecurityGroupIds"], in["SecurityGroups"] = ids, names
		if p := lc.IamInstanceProfile; strings.HasPrefix(p, "arn:") {
			in["IamInstanceProfile"] = map[string]string{"Arn": p}
		} else if p != "" {
			in["IamInstanceProfile"] = map[string]string{"Name": p}
		}
		if lc.InstanceMonitoring != nil {
			in["Monitoring"] = lc.InstanceMonitoring
		}
		if lc.MetadataOptions != nil {
			in["MetadataOptions"] = lc.MetadataOptions
		}
		var bdms []any
		for _, b := range lc.BlockDeviceMappings {
			if b.Ebs != nil {
				bdms = append(bdms, map[string]any{"DeviceName": b.DeviceName, "Ebs": b.Ebs})
			}
		}
		in["BlockDeviceMappings"] = bdms
	}
	return in, proto
}

// terminate terminates instances through EC2.
func (s *Service) terminate(account, region string, ids []string) {
	if len(ids) > 0 {
		if err := s.hub.Call(account, region, "AmazonEC2", "TerminateInstances", map[string]any{"InstanceIds": ids}, nil); err != nil {
			s.hub.Log.Warn("autoscaling: terminate instances", "err", err)
		}
	}
}

// targets registers instances with target groups and deregisters others;
// ELBv2 may not be enabled.
func (s *Service) targets(account, region string, tgs, add, remove []string) {
	for _, tg := range tgs {
		for _, call := range []struct {
			op  string
			ids []string
		}{{"RegisterTargets", add}, {"DeregisterTargets", remove}} {
			if len(call.ids) == 0 {
				continue
			}
			var list []map[string]string
			for _, id := range call.ids {
				list = append(list, map[string]string{"Id": id})
			}
			if err := s.hub.Call(account, region, "ElasticLoadBalancing_v10", call.op, map[string]any{"TargetGroupArn": tg, "Targets": list}, nil); err != nil {
				s.hub.Log.Debug("autoscaling: target group", "op", call.op, "targetGroup", tg, "err", err)
			}
		}
	}
}

// Tick runs due scheduled actions, replaces instances that stopped running
// and brings every group to its desired capacity; it runs every 10 seconds.
func (s *Service) Tick(now time.Time) {
	var refs [][3]string
	s.mu.Lock()
	for _, k := range core.SortedKeys(s.st.Groups) {
		g := s.st.Groups[k]
		refs = append(refs, [3]string{g.Account, g.Region, g.AutoScalingGroupName})
		if !g.suspended("ScheduledActions") {
			g.schedule(now)
		}
	}
	s.mu.Unlock()
	for _, r := range refs {
		s.check(r[0], r[1], r[2])
		s.reconcile(r[0], r[1], r[2], "an instance was started in response to a difference between desired and actual capacity")
	}
}

// check takes out of service the instances EC2 no longer runs and those
// marked unhealthy, terminating what still runs; reconcile replaces them.
func (s *Service) check(account, region, name string) {
	s.rec.Lock()
	defer s.rec.Unlock()
	s.mu.Lock()
	g := s.st.Groups[key(account, region, name)]
	idle := g == nil || len(g.Instances) == 0 || g.suspended("HealthCheck")
	s.mu.Unlock()
	if idle {
		return
	}
	var out struct {
		Reservations []struct {
			Instances []struct {
				InstanceId string
				State      struct{ Name string }
			}
		}
	}
	err := s.hub.Call(account, region, "AmazonEC2", "DescribeInstances",
		map[string]any{"Filters": []map[string]any{{"Name": "tag:aws:autoscaling:groupName", "Values": []string{name}}}}, &out)
	if err != nil {
		return
	}
	states := map[string]string{}
	for _, r := range out.Reservations {
		for _, i := range r.Instances {
			states[i.InstanceId] = i.State.Name
		}
	}
	var gone, kill []string
	s.mu.Lock()
	if g = s.st.Groups[key(account, region, name)]; g != nil && !g.suspended("ReplaceUnhealthy") {
		for _, i := range g.Instances {
			st := states[i.InstanceId]
			if (st == "pending" || st == "running") && i.HealthStatus != "Unhealthy" {
				continue
			}
			gone = append(gone, i.InstanceId)
			if st != "" && st != "shutting-down" && st != "terminated" {
				kill = append(kill, i.InstanceId)
			}
			g.record("Terminating EC2 instance: "+i.InstanceId, "an instance was taken out of service in response to a health check", "Successful", "")
		}
		g.Instances = slices.DeleteFunc(g.Instances, func(i Instance) bool { return slices.Contains(gone, i.InstanceId) })
	}
	var tgs []string
	if g != nil {
		tgs = slices.Clone(g.TargetGroupARNs)
	}
	s.mu.Unlock()
	s.terminate(account, region, kill)
	s.targets(account, region, tgs, nil, gone)
}

// ---- instances ----

func (s *Service) describeInstances(c *core.Ctx, in *struct {
	InstanceIds []string
	MaxRecords  int
	NextToken   string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	type details struct {
		Instance
		AutoScalingGroupName string
	}
	out := []details{}
	for _, k := range core.SortedKeys(s.st.Groups) {
		g := s.st.Groups[k]
		for _, i := range g.Instances {
			if g.Account == c.Account && g.Region == c.Region && (len(in.InstanceIds) == 0 || slices.Contains(in.InstanceIds, i.InstanceId)) {
				out = append(out, details{i, g.AutoScalingGroupName})
			}
		}
	}
	return map[string]any{"AutoScalingInstances": out}, nil
}

func (s *Service) attachInstances(c *core.Ctx, in *struct {
	AutoScalingGroupName string
	InstanceIds          []string
}) (any, error) {
	s.rec.Lock()
	defer s.rec.Unlock()
	s.mu.Lock()
	g, err := s.group(c, in.AutoScalingGroupName)
	if err == nil && len(g.Instances)+len(in.InstanceIds) > g.MaxSize {
		err = invalid("Attaching %d instance(s) to the AutoScalingGroup %s would exceed its max size %d.", len(in.InstanceIds), g.AutoScalingGroupName, g.MaxSize)
	}
	s.mu.Unlock()
	if err != nil || len(in.InstanceIds) == 0 {
		return nil, err
	}
	var out struct {
		Reservations []struct {
			Instances []struct {
				InstanceId, InstanceType string
				State                    struct{ Name string }
				Placement                struct{ AvailabilityZone string }
			}
		}
	}
	if err := s.hub.Call(c.Account, c.Region, "AmazonEC2", "DescribeInstances", map[string]any{"InstanceIds": in.InstanceIds}, &out); err != nil {
		return nil, invalid("%s", core.AsError(err).Msg)
	}
	var add []Instance
	for _, r := range out.Reservations {
		for _, i := range r.Instances {
			if i.State.Name != "running" && i.State.Name != "pending" {
				return nil, invalid("Instance %s is not in correct state. Instance is in %s state.", i.InstanceId, i.State.Name)
			}
			add = append(add, Instance{InstanceId: i.InstanceId, InstanceType: i.InstanceType, AvailabilityZone: i.Placement.AvailabilityZone,
				LifecycleState: "InService", HealthStatus: "Healthy"})
		}
	}
	s.hub.Call(c.Account, c.Region, "AmazonEC2", "CreateTags", map[string]any{"Resources": in.InstanceIds,
		"Tags": []map[string]string{{"Key": "aws:autoscaling:groupName", "Value": in.AutoScalingGroupName}}}, nil)
	s.mu.Lock()
	var tgs []string
	if g, err = s.group(c, in.AutoScalingGroupName); err == nil {
		for _, i := range add {
			g.Instances = append(g.Instances, i)
			g.record("Attaching an existing EC2 instance: "+i.InstanceId, "a user request attached the instance to the group", "Successful", "")
		}
		g.DesiredCapacity += len(add)
		tgs = slices.Clone(g.TargetGroupARNs)
	}
	s.mu.Unlock()
	s.targets(c.Account, c.Region, tgs, in.InstanceIds, nil)
	return nil, err
}

// remove takes instances out of a group, lowering its desired capacity when
// asked; lock held.
func (g *group) remove(ids []string, decrement bool, desc string) ([]Activity, error) {
	for _, id := range ids {
		if !slices.ContainsFunc(g.Instances, func(i Instance) bool { return i.InstanceId == id }) {
			return nil, invalid("The instance %s is not part of Auto Scaling group %s.", id, g.AutoScalingGroupName)
		}
	}
	if decrement && g.DesiredCapacity-len(ids) < g.MinSize {
		return nil, invalid("Currently, desiredCapacity is %d and minSize is %d. Removing %d instance(s) would make desiredCapacity less than minSize.",
			g.DesiredCapacity, g.MinSize, len(ids))
	}
	var acts []Activity
	for _, id := range ids {
		g.Instances = slices.DeleteFunc(g.Instances, func(i Instance) bool { return i.InstanceId == id })
		acts = append(acts, g.record(desc+id, "a user request removed the instance from the group", "Successful", ""))
	}
	if decrement {
		g.DesiredCapacity -= len(ids)
	}
	return acts, nil
}

func (s *Service) detachInstances(c *core.Ctx, in *struct {
	AutoScalingGroupName           string
	InstanceIds                    []string
	ShouldDecrementDesiredCapacity bool
}) (any, error) {
	s.mu.Lock()
	g, err := s.group(c, in.AutoScalingGroupName)
	var acts []Activity
	var tgs []string
	if err == nil {
		acts, err = g.remove(in.InstanceIds, in.ShouldDecrementDesiredCapacity, "Detaching EC2 instance: ")
		tgs = slices.Clone(g.TargetGroupARNs)
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	s.hub.Call(c.Account, c.Region, "AmazonEC2", "DeleteTags", map[string]any{"Resources": in.InstanceIds,
		"Tags": []map[string]string{{"Key": "aws:autoscaling:groupName"}}}, nil)
	s.targets(c.Account, c.Region, tgs, nil, in.InstanceIds)
	s.reconcile(c.Account, c.Region, in.AutoScalingGroupName, "an instance was detached from the group")
	return map[string]any{"Activities": acts}, nil
}

// owner finds the group of an instance; lock held.
func (s *Service) owner(c *core.Ctx, id string) (*group, error) {
	for _, g := range s.st.Groups {
		if g.Account == c.Account && g.Region == c.Region && slices.ContainsFunc(g.Instances, func(i Instance) bool { return i.InstanceId == id }) {
			return g, nil
		}
	}
	return nil, invalid("Instance Id not found - No managed instance found for instance ID: %s", id)
}

func (s *Service) terminateInstance(c *core.Ctx, in *struct {
	InstanceId                     string
	ShouldDecrementDesiredCapacity bool
}) (any, error) {
	s.mu.Lock()
	g, err := s.owner(c, in.InstanceId)
	var acts []Activity
	var name string
	var tgs []string
	if err == nil {
		name, tgs = g.AutoScalingGroupName, slices.Clone(g.TargetGroupARNs)
		acts, err = g.remove([]string{in.InstanceId}, in.ShouldDecrementDesiredCapacity, "Terminating EC2 instance: ")
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	s.terminate(c.Account, c.Region, []string{in.InstanceId})
	s.targets(c.Account, c.Region, tgs, nil, []string{in.InstanceId})
	s.reconcile(c.Account, c.Region, name, "an instance was terminated by a user request")
	return map[string]any{"Activity": acts[0]}, nil
}

func (s *Service) setInstanceProtection(c *core.Ctx, in *struct {
	AutoScalingGroupName string
	InstanceIds          []string
	ProtectedFromScaleIn bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.AutoScalingGroupName)
	if err != nil {
		return nil, err
	}
	for _, id := range in.InstanceIds {
		k := slices.IndexFunc(g.Instances, func(i Instance) bool { return i.InstanceId == id })
		if k < 0 {
			return nil, invalid("The instance %s is not part of Auto Scaling group %s.", id, g.AutoScalingGroupName)
		}
		g.Instances[k].ProtectedFromScaleIn = in.ProtectedFromScaleIn
	}
	return nil, nil
}

func (s *Service) setInstanceHealth(c *core.Ctx, in *struct {
	InstanceId, HealthStatus string
	ShouldRespectGracePeriod bool
}) (any, error) {
	s.mu.Lock()
	g, err := s.owner(c, in.InstanceId)
	var name string
	if err == nil {
		name = g.AutoScalingGroupName
		k := slices.IndexFunc(g.Instances, func(i Instance) bool { return i.InstanceId == in.InstanceId })
		g.Instances[k].HealthStatus = in.HealthStatus
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if in.HealthStatus == "Unhealthy" {
		s.check(c.Account, c.Region, name)
		s.reconcile(c.Account, c.Region, name, "an unhealthy instance was replaced")
	}
	return nil, nil
}

// ---- load balancing ----

func (s *Service) targetGroups(attach bool) func(*core.Ctx, *struct {
	AutoScalingGroupName string
	TargetGroupARNs      []string
}) (any, error) {
	return func(c *core.Ctx, in *struct {
		AutoScalingGroupName string
		TargetGroupARNs      []string
	}) (any, error) {
		s.mu.Lock()
		g, err := s.group(c, in.AutoScalingGroupName)
		var ids []string
		if err == nil {
			ids = instanceIDs(g.Instances)
			g.TargetGroupARNs = slices.DeleteFunc(slices.Clone(g.TargetGroupARNs), func(t string) bool { return slices.Contains(in.TargetGroupARNs, t) })
			if attach {
				g.TargetGroupARNs = append(g.TargetGroupARNs, in.TargetGroupARNs...)
			}
		}
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if attach {
			s.targets(c.Account, c.Region, in.TargetGroupARNs, ids, nil)
		} else {
			s.targets(c.Account, c.Region, in.TargetGroupARNs, nil, ids)
		}
		return nil, nil
	}
}

func (s *Service) describeTargetGroups(c *core.Ctx, in *struct{ AutoScalingGroupName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.AutoScalingGroupName)
	if err != nil {
		return nil, err
	}
	out := []map[string]string{}
	for _, t := range g.TargetGroupARNs {
		out = append(out, map[string]string{"LoadBalancerTargetGroupARN": t, "State": "InService"})
	}
	return map[string]any{"LoadBalancerTargetGroups": out}, nil
}

// loadBalancers attaches Classic Load Balancers: metadata, there is no ELB Classic.
func (s *Service) loadBalancers(attach bool) func(*core.Ctx, *struct {
	AutoScalingGroupName string
	LoadBalancerNames    []string
}) (any, error) {
	return func(c *core.Ctx, in *struct {
		AutoScalingGroupName string
		LoadBalancerNames    []string
	}) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		g, err := s.group(c, in.AutoScalingGroupName)
		if err != nil {
			return nil, err
		}
		g.LoadBalancerNames = slices.DeleteFunc(slices.Clone(g.LoadBalancerNames), func(n string) bool { return slices.Contains(in.LoadBalancerNames, n) })
		if attach {
			g.LoadBalancerNames = append(g.LoadBalancerNames, in.LoadBalancerNames...)
		}
		return nil, nil
	}
}

func (s *Service) describeLoadBalancers(c *core.Ctx, in *struct{ AutoScalingGroupName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.AutoScalingGroupName)
	if err != nil {
		return nil, err
	}
	out := []map[string]string{}
	for _, n := range g.LoadBalancerNames {
		out = append(out, map[string]string{"LoadBalancerName": n, "State": "InService"})
	}
	return map[string]any{"LoadBalancers": out}, nil
}

func (s *Service) describeActivities(c *core.Ctx, in *struct {
	AutoScalingGroupName string
	ActivityIds          []string
	MaxRecords           int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Activity{}
	for _, k := range core.SortedKeys(s.st.Groups) {
		g := s.st.Groups[k]
		if g.Account != c.Account || g.Region != c.Region || in.AutoScalingGroupName != "" && g.AutoScalingGroupName != in.AutoScalingGroupName {
			continue
		}
		for _, a := range g.Activities {
			if len(in.ActivityIds) == 0 || slices.Contains(in.ActivityIds, a.ActivityId) {
				out = append(out, a)
			}
		}
	}
	return map[string]any{"Activities": out[:min(len(out), cmp.Or(in.MaxRecords, 100))]}, nil
}

// ---- launch configurations ----

func (s *Service) createLaunchConfiguration(c *core.Ctx, in *LaunchConfiguration) (any, error) {
	switch {
	case in.LaunchConfigurationName == "":
		return nil, invalid("1 validation error detected: Value null at 'launchConfigurationName' failed to satisfy constraint: Member must not be null")
	case in.ImageId == "" || in.InstanceType == "":
		return nil, invalid("Valid requests must contain either the InstanceID parameter or both the ImageId and InstanceType parameters.")
	}
	lc := *in
	lc.LaunchConfigurationARN = c.ARN("autoscaling", "launchConfiguration:"+core.UUID()+":launchConfigurationName/"+lc.LaunchConfigurationName)
	lc.CreatedTime = core.Time(time.Now().UTC())
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, lc.LaunchConfigurationName)
	if s.st.Configs[k] != nil {
		return nil, core.Errorf(400, "AlreadyExists", "Launch Configuration by this name already exists - A launch configuration already exists with the name %s", lc.LaunchConfigurationName)
	}
	s.st.Configs[k] = &lc
	return nil, nil
}

func (s *Service) describeLaunchConfigurations(c *core.Ctx, in *struct{ LaunchConfigurationNames []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []LaunchConfiguration{}
	for _, k := range core.SortedKeys(s.st.Configs) {
		lc := s.st.Configs[k]
		if strings.HasPrefix(k, c.Account+"/"+c.Region+"/") && (len(in.LaunchConfigurationNames) == 0 || slices.Contains(in.LaunchConfigurationNames, lc.LaunchConfigurationName)) {
			out = append(out, *lc)
		}
	}
	return map[string]any{"LaunchConfigurations": out}, nil
}

func (s *Service) deleteLaunchConfiguration(c *core.Ctx, in *struct{ LaunchConfigurationName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, in.LaunchConfigurationName)
	if s.st.Configs[k] == nil {
		return nil, invalid("Launch configuration name not found - Launch configuration %s not found", in.LaunchConfigurationName)
	}
	for _, g := range s.st.Groups {
		if g.Account == c.Account && g.Region == c.Region && g.LaunchConfigurationName == in.LaunchConfigurationName {
			return nil, core.Errorf(400, "ResourceInUse", "Cannot delete launch configuration %s because it is attached to AutoScalingGroup %s",
				in.LaunchConfigurationName, g.AutoScalingGroupName)
		}
	}
	delete(s.st.Configs, k)
	return nil, nil
}

// ---- policies ----

func (s *Service) putPolicy(c *core.Ctx, in *struct {
	Policy
	Enabled *bool
}) (any, error) {
	p := in.Policy
	p.PolicyType, p.Enabled, p.Alarms = cmp.Or(p.PolicyType, "SimpleScaling"), in.Enabled == nil || *in.Enabled, nil
	switch p.PolicyType {
	case "SimpleScaling", "StepScaling":
		p.AdjustmentType = cmp.Or(p.AdjustmentType, "ChangeInCapacity")
		if p.PolicyType == "StepScaling" {
			p.MetricAggregationType = cmp.Or(p.MetricAggregationType, "Average")
		}
		if p.PolicyType == "SimpleScaling" && p.ScalingAdjustment == nil || p.PolicyType == "StepScaling" && len(p.StepAdjustments) == 0 {
			return nil, invalid("ScalingAdjustment is required for SimpleScaling and StepAdjustments for StepScaling policies")
		}
	case "TargetTrackingScaling":
		if p.TargetTrackingConfiguration == nil {
			return nil, invalid("TargetTrackingConfiguration is required for TargetTrackingScaling policies")
		}
	case "PredictiveScaling": // stored only
	default:
		return nil, invalid("Invalid policy type %s", p.PolicyType)
	}
	s.mu.Lock()
	g, err := s.group(c, p.AutoScalingGroupName)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	var stale []Alarm
	if old := g.Policies[p.PolicyName]; old != nil {
		p.PolicyARN, stale = old.PolicyARN, old.Alarms
	} else {
		p.PolicyARN = c.ARN("autoscaling", "scalingPolicy:"+core.UUID()+":autoScalingGroupName/"+g.AutoScalingGroupName+":policyName/"+p.PolicyName)
	}
	g.Policies[p.PolicyName] = &p
	s.mu.Unlock()
	DeleteAlarms(s.hub, c.Account, c.Region, stale)
	if t := p.TargetTrackingConfiguration; p.PolicyType == "TargetTrackingScaling" {
		alarms := TrackingAlarms(s.hub, c.Account, c.Region, p.AutoScalingGroupName, p.PolicyARN, t.metric(p.AutoScalingGroupName), t.TargetValue, !t.DisableScaleIn)
		s.mu.Lock()
		if cur := g.Policies[p.PolicyName]; cur != nil && cur.PolicyARN == p.PolicyARN {
			cur.Alarms = alarms
		}
		s.mu.Unlock()
		return map[string]any{"PolicyARN": p.PolicyARN, "Alarms": alarms}, nil
	}
	return map[string]any{"PolicyARN": p.PolicyARN}, nil
}

// metric is the metric a target tracking policy of a group follows.
func (t *Tracking) metric(group string) Metric {
	if c := t.CustomizedMetricSpecification; c != nil {
		return Metric{c.Namespace, c.MetricName, cmp.Or(c.Statistic, "Average"), c.Dimensions}
	}
	asg := []Dimension{{"AutoScalingGroupName", group}}
	switch p := t.PredefinedMetricSpecification; {
	case p == nil:
	case p.PredefinedMetricType == "ASGAverageNetworkIn":
		return Metric{"AWS/EC2", "NetworkIn", "Average", asg}
	case p.PredefinedMetricType == "ASGAverageNetworkOut":
		return Metric{"AWS/EC2", "NetworkOut", "Average", asg}
	case p.PredefinedMetricType == "ALBRequestCountPerTarget":
		return RequestsPerTarget(p.ResourceLabel)
	}
	return Metric{"AWS/EC2", "CPUUtilization", "Average", asg}
}

// policy finds a policy by name or ARN; lock held.
func (g *group) policy(name string) (*Policy, error) {
	for _, p := range g.Policies {
		if p.PolicyName == name || p.PolicyARN == name {
			return p, nil
		}
	}
	return nil, invalid("Policy %s not found for AutoScalingGroup %s", name, g.AutoScalingGroupName)
}

func (s *Service) describePolicies(c *core.Ctx, in *struct {
	AutoScalingGroupName     string
	PolicyNames, PolicyTypes []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Policy{}
	for _, k := range core.SortedKeys(s.st.Groups) {
		g := s.st.Groups[k]
		if g.Account != c.Account || g.Region != c.Region || in.AutoScalingGroupName != "" && g.AutoScalingGroupName != in.AutoScalingGroupName {
			continue
		}
		for _, n := range core.SortedKeys(g.Policies) {
			p := g.Policies[n]
			if (len(in.PolicyNames) == 0 || slices.Contains(in.PolicyNames, p.PolicyName) || slices.Contains(in.PolicyNames, p.PolicyARN)) &&
				(len(in.PolicyTypes) == 0 || slices.Contains(in.PolicyTypes, p.PolicyType)) {
				cp := *p
				cp.Alarms = slices.Clone(cp.Alarms)
				out = append(out, cp)
			}
		}
	}
	return map[string]any{"ScalingPolicies": out}, nil
}

func (s *Service) deletePolicy(c *core.Ctx, in *struct{ AutoScalingGroupName, PolicyName string }) (any, error) {
	s.mu.Lock()
	g, err := s.group(c, in.AutoScalingGroupName)
	var p *Policy
	if err == nil {
		p, err = g.policy(in.PolicyName)
	}
	if err == nil {
		delete(g.Policies, p.PolicyName)
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	DeleteAlarms(s.hub, c.Account, c.Region, p.Alarms)
	return nil, nil
}

func (s *Service) executePolicy(c *core.Ctx, in *struct {
	AutoScalingGroupName, PolicyName string
	HonorCooldown                    bool
	MetricValue, BreachThreshold     *float64
}) (any, error) {
	s.mu.Lock()
	var p *Policy
	g, err := s.group(c, in.AutoScalingGroupName)
	if err == nil {
		p, err = g.policy(in.PolicyName)
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	breach := 0.0
	switch p.PolicyType {
	case "TargetTrackingScaling", "PredictiveScaling":
		return nil, invalid("ExecutePolicy is not supported for policy type %s.", p.PolicyType)
	case "StepScaling":
		if in.MetricValue == nil || in.BreachThreshold == nil {
			return nil, invalid("MetricValue and BreachThreshold are required for executing a step scaling policy.")
		}
		breach = *in.MetricValue - *in.BreachThreshold
	}
	s.execute(c.Account, c.Region, in.AutoScalingGroupName, p.PolicyName, breach, 0, true, "a user request executed policy "+p.PolicyName)
	return nil, nil
}

// execute runs a policy on its group: step scaling takes the step holding
// breach (metric minus alarm threshold), target tracking scales by ratio
// (metric over target, 0 when unknown).
func (s *Service) execute(account, region, name, policy string, breach, ratio float64, high bool, cause string) {
	s.mu.Lock()
	g := s.st.Groups[key(account, region, name)]
	var p *Policy
	if g != nil {
		p = g.Policies[policy]
	}
	if p == nil || !p.Enabled {
		s.mu.Unlock()
		return
	}
	old, want := g.DesiredCapacity, g.DesiredCapacity
	switch p.PolicyType {
	case "SimpleScaling":
		want = Adjust(old, p.AdjustmentType, deref(p.ScalingAdjustment), deref(p.MinAdjustmentMagnitude))
	case "StepScaling":
		if adj, ok := Step(p.StepAdjustments, breach); ok {
			want = Adjust(old, p.AdjustmentType, adj, deref(p.MinAdjustmentMagnitude))
		}
	case "TargetTrackingScaling":
		want = Track(old, ratio, high, p.TargetTrackingConfiguration.DisableScaleIn)
	}
	g.DesiredCapacity = min(max(want, g.MinSize), g.MaxSize)
	want = g.DesiredCapacity
	s.mu.Unlock()
	if want != old {
		s.reconcile(account, region, name, fmt.Sprintf("%s changing the desired capacity from %d to %d", cause, old, want))
	}
}

func deref(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}

// deliver runs the policy a CloudWatch alarm action names. Application Auto
// Scaling's policies share the autoscaling namespace (their ARNs name a
// resource/): those go on to it.
func (s *Service) deliver(arn string, m *core.Message) error {
	_, region, account, res, _ := core.ParseARN(arn)
	if strings.Contains(res, ":resource/") {
		return s.hub.Deliver(strings.Replace(arn, ":autoscaling:", ":application-autoscaling:", 1), m)
	}
	s.mu.Lock()
	var name, policy string
	var target float64
	off := false
	for _, g := range s.st.Groups {
		for _, p := range g.Policies {
			if p.PolicyARN == arn {
				name, policy, off = g.AutoScalingGroupName, p.PolicyName, g.suspended("AlarmNotification")
				if t := p.TargetTrackingConfiguration; t != nil {
					target = t.TargetValue
				}
			}
		}
	}
	s.mu.Unlock()
	if policy == "" {
		return fmt.Errorf("no scaling policy %s", arn)
	}
	if off {
		return nil
	}
	a := ReadSignal(s.hub, account, region, m)
	s.execute(account, region, name, policy, a.Breach(), a.Ratio(target), a.High,
		"a monitor alarm "+a.AlarmName+" in state "+a.NewStateValue+" triggered policy "+policy)
	return nil
}

// ---- scheduled actions ----

func (s *Service) putAction(c *core.Ctx, in *Action) (any, error) {
	a := &action{Action: *in}
	a.StartTime = cmp.Or(a.StartTime, a.Time)
	a.Time = nil
	now := time.Now()
	if a.Recurrence != "" {
		if _, err := events.ParseSchedule(unixCron(a.Recurrence)); err != nil {
			return nil, invalid("Given recurrence is not a valid cron expression: %s", a.Recurrence)
		}
	} else if a.StartTime == nil || time.Time(*a.StartTime).Before(now) {
		return nil, invalid("Given start time is in the past")
	}
	a.Next = a.next(now)
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, a.AutoScalingGroupName)
	if err != nil {
		return nil, err
	}
	a.ScheduledActionARN = c.ARN("autoscaling", "scheduledUpdateGroupAction:"+core.UUID()+":autoScalingGroupName/"+g.AutoScalingGroupName+
		":scheduledActionName/"+a.ScheduledActionName)
	if old := g.Actions[a.ScheduledActionName]; old != nil {
		a.ScheduledActionARN = old.ScheduledActionARN
	}
	g.Actions[a.ScheduledActionName] = a
	return nil, nil
}

// next is when a scheduled action runs after t: its start time once, or its
// recurrence from then on until its end time.
func (a *action) next(t time.Time) time.Time {
	start := time.Time{}
	if a.StartTime != nil {
		start = time.Time(*a.StartTime)
	}
	if a.Recurrence == "" {
		if start.After(t) {
			return start
		}
		return time.Time{}
	}
	sched, err := events.ParseSchedule(unixCron(a.Recurrence))
	loc, lerr := time.LoadLocation(cmp.Or(a.TimeZone, "UTC"))
	if err != nil || lerr != nil {
		return time.Time{}
	}
	if start.After(t) {
		t = start.Add(-time.Second)
	}
	n := sched.Next(t.In(loc))
	if a.EndTime != nil && n.After(time.Time(*a.EndTime)) {
		return time.Time{}
	}
	return n
}

var digits = regexp.MustCompile(`\d+`)

// unixCron turns a Unix cron expression (minute hour day month weekday,
// Sunday 0) into the cron(…) form events parses.
func unixCron(expr string) string {
	f := strings.Fields(strings.ToUpper(expr))
	if len(f) != 5 {
		return expr
	}
	dom, dow := f[2], f[4]
	if dow == "*" {
		dow = "?"
	} else {
		dom = "?"
		dow = digits.ReplaceAllStringFunc(dow, func(d string) string { n, _ := strconv.Atoi(d); return strconv.Itoa(n%7 + 1) })
	}
	return "cron(" + strings.Join([]string{f[0], f[1], dom, f[3], dow, "*"}, " ") + ")"
}

// schedule applies the group's due scheduled actions; lock held.
func (g *group) schedule(now time.Time) {
	for _, n := range core.SortedKeys(g.Actions) {
		a := g.Actions[n]
		if a.Next.IsZero() || now.Before(a.Next) {
			continue
		}
		if a.MinSize != nil {
			g.MinSize = *a.MinSize
		}
		if a.MaxSize != nil {
			g.MaxSize = *a.MaxSize
		}
		if a.DesiredCapacity != nil {
			g.DesiredCapacity = *a.DesiredCapacity
		}
		g.DesiredCapacity = min(max(g.DesiredCapacity, g.MinSize), g.MaxSize)
		if a.Next = a.next(now); a.Next.IsZero() {
			delete(g.Actions, n)
		}
	}
}

func (s *Service) describeActions(c *core.Ctx, in *struct {
	AutoScalingGroupName string
	ScheduledActionNames []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Action{}
	for _, k := range core.SortedKeys(s.st.Groups) {
		g := s.st.Groups[k]
		if g.Account != c.Account || g.Region != c.Region || in.AutoScalingGroupName != "" && g.AutoScalingGroupName != in.AutoScalingGroupName {
			continue
		}
		for _, n := range core.SortedKeys(g.Actions) {
			if len(in.ScheduledActionNames) == 0 || slices.Contains(in.ScheduledActionNames, n) {
				a := g.Actions[n].Action
				a.Time = a.StartTime
				out = append(out, a)
			}
		}
	}
	return map[string]any{"ScheduledUpdateGroupActions": out}, nil
}

func (s *Service) deleteAction(c *core.Ctx, in *struct{ AutoScalingGroupName, ScheduledActionName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.AutoScalingGroupName)
	if err != nil {
		return nil, err
	}
	if g.Actions[in.ScheduledActionName] == nil {
		return nil, invalid("Scheduled Update Group Action name not found - Could not find scheduled update group action %s for group %s",
			in.ScheduledActionName, in.AutoScalingGroupName)
	}
	delete(g.Actions, in.ScheduledActionName)
	return nil, nil
}

// ---- lifecycle hooks ----

func hookDefaults(h Hook) *Hook {
	h.HeartbeatTimeout, h.DefaultResult = cmp.Or(h.HeartbeatTimeout, 3600), cmp.Or(h.DefaultResult, "ABANDON")
	h.GlobalTimeout = min(100*h.HeartbeatTimeout, 172800)
	return &h
}

func (s *Service) putHook(c *core.Ctx, in *Hook) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.AutoScalingGroupName)
	if err != nil {
		return nil, err
	}
	g.Hooks[in.LifecycleHookName] = hookDefaults(*in)
	return nil, nil
}

func (s *Service) describeHooks(c *core.Ctx, in *struct {
	AutoScalingGroupName string
	LifecycleHookNames   []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.AutoScalingGroupName)
	if err != nil {
		return nil, err
	}
	out := []Hook{}
	for _, n := range core.SortedKeys(g.Hooks) {
		if len(in.LifecycleHookNames) == 0 || slices.Contains(in.LifecycleHookNames, n) {
			out = append(out, *g.Hooks[n])
		}
	}
	return map[string]any{"LifecycleHooks": out}, nil
}

func (s *Service) deleteHook(c *core.Ctx, in *struct{ AutoScalingGroupName, LifecycleHookName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.AutoScalingGroupName)
	if err != nil {
		return nil, err
	}
	if g.Hooks[in.LifecycleHookName] == nil {
		return nil, invalid("No Lifecycle Hook found with name %s for group %s", in.LifecycleHookName, in.AutoScalingGroupName)
	}
	delete(g.Hooks, in.LifecycleHookName)
	return nil, nil
}

// lifecycleAction answers CompleteLifecycleAction and heartbeats: nothing waits.
func (s *Service) lifecycleAction(c *core.Ctx, in *struct{ AutoScalingGroupName, LifecycleHookName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if g, err := s.group(c, in.AutoScalingGroupName); err != nil || g.Hooks[in.LifecycleHookName] == nil {
		return nil, invalid("No active Lifecycle Action found with hook %s for group %s", in.LifecycleHookName, in.AutoScalingGroupName)
	}
	return nil, nil
}

// ---- processes, metrics, tags, warm pools, limits ----

var processNames = strings.Fields("Launch Terminate AddToLoadBalancer AlarmNotification AZRebalance HealthCheck InstanceRefresh ReplaceUnhealthy ScheduledActions")

func (s *Service) processes(suspend bool) func(*core.Ctx, *struct {
	AutoScalingGroupName string
	ScalingProcesses     []string
}) (any, error) {
	return func(c *core.Ctx, in *struct {
		AutoScalingGroupName string
		ScalingProcesses     []string
	}) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		g, err := s.group(c, in.AutoScalingGroupName)
		if err != nil {
			return nil, err
		}
		names := in.ScalingProcesses
		if len(names) == 0 {
			names = processNames
		}
		for _, n := range names {
			if !slices.Contains(processNames, n) {
				return nil, invalid("Invalid scaling process %s", n)
			}
			g.SuspendedProcesses = slices.DeleteFunc(slices.Clone(g.SuspendedProcesses), func(p Process) bool { return p.ProcessName == n })
			if suspend {
				g.SuspendedProcesses = append(g.SuspendedProcesses, Process{n, "User suspended at " + time.Now().UTC().Format(time.RFC3339)})
			}
		}
		return nil, nil
	}
}

var metricNames = strings.Fields("GroupMinSize GroupMaxSize GroupDesiredCapacity GroupInServiceInstances GroupPendingInstances " +
	"GroupStandbyInstances GroupTerminatingInstances GroupTotalInstances GroupInServiceCapacity GroupPendingCapacity GroupStandbyCapacity " +
	"GroupTerminatingCapacity GroupTotalCapacity WarmPoolDesiredCapacity WarmPoolWarmedCapacity WarmPoolPendingCapacity " +
	"WarmPoolTerminatingCapacity WarmPoolTotalCapacity GroupAndWarmPoolDesiredCapacity GroupAndWarmPoolTotalCapacity")

// metrics enables or disables group metrics (metadata: pogi publishes none).
func (s *Service) metrics(enable bool) func(*core.Ctx, *struct {
	AutoScalingGroupName, Granularity string
	Metrics                           []string
}) (any, error) {
	return func(c *core.Ctx, in *struct {
		AutoScalingGroupName, Granularity string
		Metrics                           []string
	}) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		g, err := s.group(c, in.AutoScalingGroupName)
		if err != nil {
			return nil, err
		}
		names := in.Metrics
		if len(names) == 0 {
			names = metricNames
		}
		g.EnabledMetrics = slices.DeleteFunc(slices.Clone(g.EnabledMetrics), func(m EnabledMetric) bool { return slices.Contains(names, m.Metric) })
		for _, n := range names {
			if enable {
				g.EnabledMetrics = append(g.EnabledMetrics, EnabledMetric{n, cmp.Or(in.Granularity, "1Minute")})
			}
		}
		return nil, nil
	}
}

func (s *Service) tags(set bool) func(*core.Ctx, *struct{ Tags []Tag }) (any, error) {
	return func(c *core.Ctx, in *struct{ Tags []Tag }) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, t := range in.Tags {
			g, err := s.group(c, t.ResourceId)
			if err != nil {
				return nil, err
			}
			g.Tags = slices.DeleteFunc(slices.Clone(g.Tags), func(x Tag) bool { return x.Key == t.Key })
			if set {
				t.ResourceType = "auto-scaling-group"
				g.Tags = append(g.Tags, t)
			}
		}
		return nil, nil
	}
}

func (s *Service) describeTags(c *core.Ctx, in *struct{ Filters []filter }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Tag{}
	for _, k := range core.SortedKeys(s.st.Groups) {
		g := s.st.Groups[k]
		for _, t := range g.Tags {
			ok := g.Account == c.Account && g.Region == c.Region
			for _, f := range in.Filters {
				v := map[string]string{"auto-scaling-group": t.ResourceId, "key": t.Key, "value": t.Value,
					"propagate-at-launch": strconv.FormatBool(t.PropagateAtLaunch)}[f.Name]
				ok = ok && slices.Contains(f.Values, v)
			}
			if ok {
				out = append(out, t)
			}
		}
	}
	return map[string]any{"Tags": out}, nil
}

func (s *Service) putWarmPool(c *core.Ctx, in *struct {
	AutoScalingGroupName string
	WarmPool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.AutoScalingGroupName)
	if err != nil {
		return nil, err
	}
	w := in.WarmPool
	w.PoolState, w.Status = cmp.Or(w.PoolState, "Stopped"), ""
	g.WarmPoolConfiguration = &w
	return nil, nil
}

func (s *Service) describeWarmPool(c *core.Ctx, in *struct{ AutoScalingGroupName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.AutoScalingGroupName)
	if err != nil {
		return nil, err
	}
	return map[string]any{"WarmPoolConfiguration": g.WarmPoolConfiguration, "Instances": []Instance{}}, nil
}

func (s *Service) deleteWarmPool(c *core.Ctx, in *struct {
	AutoScalingGroupName string
	ForceDelete          bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, err := s.group(c, in.AutoScalingGroupName)
	if err == nil && g.WarmPoolConfiguration == nil {
		err = invalid("No warm pool found for Auto Scaling group %s", in.AutoScalingGroupName)
	}
	if err != nil {
		return nil, err
	}
	g.WarmPoolConfiguration = nil
	return nil, nil
}

func (s *Service) describeAccountLimits(c *core.Ctx, _ *struct{}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	groups, configs := 0, 0
	for _, g := range s.st.Groups {
		if g.Account == c.Account && g.Region == c.Region {
			groups++
		}
	}
	for k := range s.st.Configs {
		if strings.HasPrefix(k, c.Account+"/"+c.Region+"/") {
			configs++
		}
	}
	return map[string]int{"MaxNumberOfAutoScalingGroups": 500, "MaxNumberOfLaunchConfigurations": 200,
		"NumberOfAutoScalingGroups": groups, "NumberOfLaunchConfigurations": configs}, nil
}

// ---- console ----

// Resources lists groups.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Auto Scaling groups", Columns: []string{"Group", "Min", "Desired", "Max", "Instances", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.Groups) {
		g := s.st.Groups[k]
		t.Rows = append(t.Rows, []string{g.AutoScalingGroupName, strconv.Itoa(g.MinSize), strconv.Itoa(g.DesiredCapacity), strconv.Itoa(g.MaxSize),
			strconv.Itoa(len(g.Instances)), g.Region, g.Account})
	}
	return []core.Table{t}
}

// Inspect returns a group with its policies.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range s.st.Groups {
		if g.AutoScalingGroupName == id {
			b, _ := json.Marshal(g)
			var v any
			json.Unmarshal(b, &v)
			return v
		}
	}
	return nil
}
