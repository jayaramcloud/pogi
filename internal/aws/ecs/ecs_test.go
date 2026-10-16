package ecs_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	ec2svc "github.com/nrndev/pogi/internal/aws/ec2"
	ecssvc "github.com/nrndev/pogi/internal/aws/ecs"
	eventssvc "github.com/nrndev/pogi/internal/aws/events"
	"github.com/nrndev/pogi/internal/aws/logs"
	smsvc "github.com/nrndev/pogi/internal/aws/secretsmanager"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	ssmsvc "github.com/nrndev/pogi/internal/aws/ssm"
	stssvc "github.com/nrndev/pogi/internal/aws/sts"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// wantErr fails unless err is an API error with this code.
func wantErr(t *testing.T, err error, code string) {
	t.Helper()
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func setup(t *testing.T, more ...func(*core.Hub) core.Service) (*core.Hub, string, *ecs.Client) {
	hub, url := core.ServeTest(t, append([]func(*core.Hub) core.Service{
		func(h *core.Hub) core.Service { return ecssvc.New(h) },
		func(h *core.Hub) core.Service { return logs.New(h) },
		func(h *core.Hub) core.Service { return ssmsvc.New(h) },
		func(h *core.Hub) core.Service { return smsvc.New(h) },
		func(h *core.Hub) core.Service { return stssvc.New(h) },
		func(h *core.Hub) core.Service { return ec2svc.New(h) }}, more...)...)
	return hub, url, ecs.NewFromConfig(awstest.Config(t, url))
}

// elb stands in for ELBv2 and records the target calls ECS makes in-process.
type elb struct {
	mu    sync.Mutex
	calls []string // "RegisterTargets <target group> <id>:<port>"
}

func (e *elb) Info() core.Info {
	return core.Info{Name: "elbv2", Cloud: core.AWS, Targets: []string{"ElasticLoadBalancing_v10"}}
}

func (e *elb) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TargetGroupArn string
		Targets        []struct {
			Id   string
			Port int
		}
	}
	json.NewDecoder(r.Body).Decode(&in)
	_, op, _ := strings.Cut(r.Header.Get("X-Amz-Target"), ".")
	e.mu.Lock()
	for _, tg := range in.Targets {
		e.calls = append(e.calls, op+" "+in.TargetGroupArn+" "+tg.Id+":"+strconv.Itoa(tg.Port))
	}
	e.mu.Unlock()
	w.Write([]byte("{}"))
}

// called waits until the fake load balancer saw call.
func (e *elb) called(t *testing.T, call string) {
	t.Helper()
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		e.mu.Lock()
		ok := slices.Contains(e.calls, call)
		e.mu.Unlock()
		if ok {
			return
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	t.Fatalf("no %q among %q", call, e.calls)
}

func TestLoadBalancer(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	lb := &elb{}
	_, _, c := setup(t, func(*core.Hub) core.Service { return lb })
	must(c.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String("lb")}))
	td := must(c.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{Family: aws.String("api"), NetworkMode: types.NetworkModeAwsvpc,
		Memory: aws.String("512"), ContainerDefinitions: []types.ContainerDefinition{{Name: aws.String("api"), Image: aws.String("nginx:alpine"),
			PortMappings: []types.PortMapping{{ContainerPort: aws.Int32(8080)}}}}})).TaskDefinition
	tg := "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/api/0123456789abcdef"
	in := &ecs.CreateServiceInput{Cluster: aws.String("lb"), ServiceName: aws.String("api"), TaskDefinition: td.TaskDefinitionArn, DesiredCount: aws.Int32(1),
		NetworkConfiguration: &types.NetworkConfiguration{AwsvpcConfiguration: &types.AwsVpcConfiguration{Subnets: []string{"subnet-1"}}},
		LoadBalancers:        []types.LoadBalancer{{TargetGroupArn: aws.String(tg), ContainerName: aws.String("api"), ContainerPort: aws.Int32(80)}}}
	_, err := c.CreateService(ctx, in)
	wantErr(t, err, "InvalidParameterException") // no container port 80
	in.LoadBalancers[0].ContainerPort = aws.Int32(8080)
	must(c.CreateService(ctx, in))
	waitStable(t, c, "lb", "api")
	task := describeTask(t, c, "lb", serviceTasks(t, c, "lb", "api")[0])
	target := tg + " " + detail(task.Attachments[0], "privateIPv4Address") + ":8080"
	lb.called(t, "RegisterTargets "+target)
	must(c.DeleteService(ctx, &ecs.DeleteServiceInput{Cluster: aws.String("lb"), Service: aws.String("api"), Force: aws.Bool(true)}))
	lb.called(t, "DeregisterTargets "+target)
}

// floci #1382: a negative desiredCount is refused, not stored (it used to panic the reconcile loop).
func TestNegativeDesiredCount(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, _, c := setup(t)
	must(c.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String("c")}))
	td := must(c.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{Family: aws.String("web"),
		ContainerDefinitions: []types.ContainerDefinition{{Name: aws.String("app"), Image: aws.String("alpine"), Memory: aws.Int32(64)}}})).TaskDefinition
	_, err := c.CreateService(ctx, &ecs.CreateServiceInput{Cluster: aws.String("c"), ServiceName: aws.String("neg"), TaskDefinition: td.TaskDefinitionArn, DesiredCount: aws.Int32(-5)})
	wantErr(t, err, "InvalidParameterException")
	must(c.CreateService(ctx, &ecs.CreateServiceInput{Cluster: aws.String("c"), ServiceName: aws.String("ok"), TaskDefinition: td.TaskDefinitionArn}))
	_, err = c.UpdateService(ctx, &ecs.UpdateServiceInput{Cluster: aws.String("c"), Service: aws.String("ok"), DesiredCount: aws.Int32(-1)})
	wantErr(t, err, "InvalidParameterException")
}

// floci #2478: DescribeServices returns availabilityZoneRebalancing (ENABLED by default for a replica service).
// floci #4746: DeleteService removes the service's deployment records.
func TestServiceDefaultsAndDelete(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, _, c := setup(t)
	must(c.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String("c")}))
	td := must(c.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{Family: aws.String("web"),
		ContainerDefinitions: []types.ContainerDefinition{{Name: aws.String("app"), Image: aws.String("alpine"), Memory: aws.Int32(64)}}})).TaskDefinition
	sv := must(c.CreateService(ctx, &ecs.CreateServiceInput{Cluster: aws.String("c"), ServiceName: aws.String("s"), TaskDefinition: td.TaskDefinitionArn, DesiredCount: aws.Int32(0)})).Service
	d := must(c.DescribeServices(ctx, &ecs.DescribeServicesInput{Cluster: aws.String("c"), Services: []string{"s"}})).Services[0]
	if d.AvailabilityZoneRebalancing != types.AvailabilityZoneRebalancingEnabled {
		t.Errorf("availabilityZoneRebalancing = %q", d.AvailabilityZoneRebalancing)
	}
	list := func() int {
		return len(must(c.ListServiceDeployments(ctx, &ecs.ListServiceDeploymentsInput{Cluster: aws.String("c"), Service: sv.ServiceArn})).ServiceDeployments)
	}
	if list() == 0 {
		t.Fatal("no service deployment before DeleteService")
	}
	must(c.DeleteService(ctx, &ecs.DeleteServiceInput{Cluster: aws.String("c"), Service: aws.String("s"), Force: aws.Bool(true)}))
	if n := list(); n != 0 {
		t.Errorf("%d service deployments after DeleteService", n)
	}
}

// floci #3661 and #2557: a deployment keeps its Service Connect configuration, and ECS puts aws.ecs events on the default bus.
func TestServiceConnectAndEvents(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, url, c := setup(t, func(h *core.Hub) core.Service { return eventssvc.New(h) }, func(h *core.Hub) core.Service { return sqssvc.New(h) })
	cfg := awstest.Config(t, url)
	q, eb := sqs.NewFromConfig(cfg), eventbridge.NewFromConfig(cfg)
	qurl := must(q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("ecs-events")})).QueueUrl
	qarn := must(q.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: qurl, AttributeNames: []sqstypes.QueueAttributeName{"QueueArn"}})).Attributes["QueueArn"]
	must(eb.PutRule(ctx, &eventbridge.PutRuleInput{Name: aws.String("ecs"), EventPattern: aws.String(`{"source":["aws.ecs"]}`)}))
	must(eb.PutTargets(ctx, &eventbridge.PutTargetsInput{Rule: aws.String("ecs"), Targets: []ebtypes.Target{{Id: aws.String("q"), Arn: aws.String(qarn)}}}))
	must(c.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String("c")}))
	td := must(c.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{Family: aws.String("web"),
		ContainerDefinitions: []types.ContainerDefinition{{Name: aws.String("app"), Image: aws.String("alpine"), Memory: aws.Int32(64)}}})).TaskDefinition
	must(c.CreateService(ctx, &ecs.CreateServiceInput{Cluster: aws.String("c"), ServiceName: aws.String("svc"), TaskDefinition: td.TaskDefinitionArn, DesiredCount: aws.Int32(0),
		ServiceConnectConfiguration: &types.ServiceConnectConfiguration{Enabled: true, Namespace: aws.String("ns")}}))
	dep := func() types.Deployment {
		return must(c.DescribeServices(ctx, &ecs.DescribeServicesInput{Cluster: aws.String("c"), Services: []string{"svc"}})).Services[0].Deployments[0]
	}
	first := dep()
	if sc := first.ServiceConnectConfiguration; sc == nil || !sc.Enabled || *sc.Namespace != "ns" {
		t.Fatalf("deployment = %+v", first)
	}
	must(c.UpdateService(ctx, &ecs.UpdateServiceInput{Cluster: aws.String("c"), Service: aws.String("svc"), ForceNewDeployment: true}))
	if next := dep(); *next.Id == *first.Id || next.ServiceConnectConfiguration == nil || *next.ServiceConnectConfiguration.Namespace != "ns" {
		t.Fatalf("new deployment = %+v", next)
	}
	var seen string
	for end := time.Now().Add(10 * time.Second); !strings.Contains(seen, "SERVICE_DEPLOYMENT_IN_PROGRESS") && time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
		for _, m := range must(q.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: qurl, MaxNumberOfMessages: 10})).Messages {
			seen += *m.Body
		}
	}
	if !strings.Contains(seen, `"source":"aws.ecs"`) || !strings.Contains(seen, "SERVICE_DEPLOYMENT_IN_PROGRESS") || !strings.Contains(seen, "ECS Deployment State Change") {
		t.Fatalf("events = %s", seen)
	}
}

const wait = 2 * time.Minute

func fast(min, max *time.Duration) { *min, *max = 50*time.Millisecond, 500*time.Millisecond }

func waitRunning(t *testing.T, c *ecs.Client, cluster string, tasks ...string) {
	t.Helper()
	err := ecs.NewTasksRunningWaiter(c, func(o *ecs.TasksRunningWaiterOptions) { fast(&o.MinDelay, &o.MaxDelay) }).
		Wait(ctx, &ecs.DescribeTasksInput{Cluster: aws.String(cluster), Tasks: tasks}, wait)
	if err != nil {
		t.Fatal(err)
	}
}

func waitStopped(t *testing.T, c *ecs.Client, cluster string, tasks ...string) []types.Task {
	t.Helper()
	out, err := ecs.NewTasksStoppedWaiter(c, func(o *ecs.TasksStoppedWaiterOptions) { fast(&o.MinDelay, &o.MaxDelay) }).
		WaitForOutput(ctx, &ecs.DescribeTasksInput{Cluster: aws.String(cluster), Tasks: tasks}, wait)
	if err != nil {
		t.Fatal(err)
	}
	return out.Tasks
}

func waitStable(t *testing.T, c *ecs.Client, cluster, service string) types.Service {
	t.Helper()
	out, err := ecs.NewServicesStableWaiter(c, func(o *ecs.ServicesStableWaiterOptions) { fast(&o.MinDelay, &o.MaxDelay) }).
		WaitForOutput(ctx, &ecs.DescribeServicesInput{Cluster: aws.String(cluster), Services: []string{service}}, wait)
	if err != nil {
		t.Fatal(err)
	}
	return out.Services[0]
}

func describeTask(t *testing.T, c *ecs.Client, cluster, task string) types.Task {
	t.Helper()
	out := must(c.DescribeTasks(ctx, &ecs.DescribeTasksInput{Cluster: aws.String(cluster), Tasks: []string{task}, Include: []types.TaskField{types.TaskFieldTags}}))
	if len(out.Tasks) != 1 {
		t.Fatalf("task %s: %+v", task, out.Failures)
	}
	return out.Tasks[0]
}

func detail(a types.Attachment, name string) string {
	for _, kv := range a.Details {
		if *kv.Name == name {
			return *kv.Value
		}
	}
	return ""
}

func serviceTasks(t *testing.T, c *ecs.Client, cluster, service string) []string {
	t.Helper()
	return must(c.ListTasks(ctx, &ecs.ListTasksInput{Cluster: aws.String(cluster), ServiceName: aws.String(service)})).TaskArns
}

func TestMetadataOnly(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent") // no container engine
	hub, url, c := setup(t)

	// clusters
	cl := must(c.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String("dev"),
		Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("a")}}})).Cluster
	if *cl.ClusterArn != "arn:aws:ecs:us-east-1:000000000000:cluster/dev" || *cl.Status != "ACTIVE" || len(cl.Settings) != 1 || *cl.Settings[0].Value != "disabled" {
		t.Fatalf("cluster = %+v", cl)
	}
	_, err := c.ListTasks(ctx, &ecs.ListTasksInput{Cluster: aws.String("nope")})
	wantErr(t, err, "ClusterNotFoundException")
	dc := must(c.DescribeClusters(ctx, &ecs.DescribeClustersInput{Clusters: []string{"dev", "nope"}, Include: []types.ClusterField{types.ClusterFieldTags}}))
	if len(dc.Clusters) != 1 || len(dc.Clusters[0].Tags) != 1 || len(dc.Clusters[0].Settings) != 0 || len(dc.Failures) != 1 || *dc.Failures[0].Reason != "MISSING" {
		t.Fatalf("describe = %+v", dc)
	}
	must(c.UpdateClusterSettings(ctx, &ecs.UpdateClusterSettingsInput{Cluster: aws.String("dev"),
		Settings: []types.ClusterSetting{{Name: types.ClusterSettingNameContainerInsights, Value: aws.String("enabled")}}}))
	if arns := must(c.ListClusters(ctx, &ecs.ListClustersInput{})).ClusterArns; !slices.Equal(arns, []string{*cl.ClusterArn}) {
		t.Fatalf("clusters = %v", arns)
	}

	// task definitions: validation, revisions, listings, deregistration
	_, err = c.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{Family: aws.String("web"),
		ContainerDefinitions: []types.ContainerDefinition{{Name: aws.String("web"), Memory: aws.Int32(64)}}})
	wantErr(t, err, "ClientException")
	_, err = c.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{Family: aws.String("web"), RequiresCompatibilities: []types.Compatibility{types.CompatibilityFargate},
		Cpu: aws.String("256"), Memory: aws.String("512"), ContainerDefinitions: []types.ContainerDefinition{{Name: aws.String("web"), Image: aws.String("nginx:alpine")}}})
	if wantErr(t, err, "ClientException"); !strings.Contains(err.Error(), "awsvpc") {
		t.Fatalf("fargate without awsvpc: %v", err)
	}
	web := types.ContainerDefinition{Name: aws.String("web"), Image: aws.String("nginx:alpine"), PortMappings: []types.PortMapping{{ContainerPort: aws.Int32(80)}},
		Environment: []types.KeyValuePair{{Name: aws.String("A"), Value: aws.String("1")}}}
	reg := func() *types.TaskDefinition {
		return must(c.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{Family: aws.String("web"), NetworkMode: types.NetworkModeAwsvpc,
			RequiresCompatibilities: []types.Compatibility{types.CompatibilityFargate}, Cpu: aws.String("256"), Memory: aws.String("512"),
			ContainerDefinitions: []types.ContainerDefinition{web}, Tags: []types.Tag{{Key: aws.String("app"), Value: aws.String("web")}}})).TaskDefinition
	}
	td1, td2 := reg(), reg()
	if td1.Revision != 1 || td2.Revision != 2 || *td2.TaskDefinitionArn != "arn:aws:ecs:us-east-1:000000000000:task-definition/web:2" ||
		!slices.Contains(td2.Compatibilities, types.CompatibilityFargate) || *td2.ContainerDefinitions[0].Environment[0].Value != "1" {
		t.Fatalf("task definition = %+v", td2)
	}
	got := must(c.DescribeTaskDefinition(ctx, &ecs.DescribeTaskDefinitionInput{TaskDefinition: aws.String("web"), Include: []types.TaskDefinitionField{types.TaskDefinitionFieldTags}}))
	if got.TaskDefinition.Revision != 2 || len(got.Tags) != 1 {
		t.Fatalf("latest = %d %v", got.TaskDefinition.Revision, got.Tags)
	}
	if arns := must(c.ListTaskDefinitions(ctx, &ecs.ListTaskDefinitionsInput{FamilyPrefix: aws.String("web"), Sort: types.SortOrderDesc})).TaskDefinitionArns; len(arns) != 2 || arns[0] != *td2.TaskDefinitionArn {
		t.Fatalf("list = %v", arns)
	}
	must(c.DeregisterTaskDefinition(ctx, &ecs.DeregisterTaskDefinitionInput{TaskDefinition: aws.String("web:1")}))
	if fams := must(c.ListTaskDefinitionFamilies(ctx, &ecs.ListTaskDefinitionFamiliesInput{FamilyPrefix: aws.String("w")})).Families; !slices.Equal(fams, []string{"web"}) {
		t.Fatalf("families = %v", fams)
	}
	del := must(c.DeleteTaskDefinitions(ctx, &ecs.DeleteTaskDefinitionsInput{TaskDefinitions: []string{"web:1", "web:2"}}))
	if len(del.TaskDefinitions) != 1 || del.TaskDefinitions[0].Status != types.TaskDefinitionStatusDeleteInProgress || len(del.Failures) != 1 {
		t.Fatalf("delete = %+v", del)
	}
	_, err = c.DescribeTaskDefinition(ctx, &ecs.DescribeTaskDefinitionInput{TaskDefinition: aws.String("web:1")})
	wantErr(t, err, "ClientException")

	// tasks: an awsvpc task gets an address of its subnet from EC2
	e2 := ec2.NewFromConfig(awstest.Config(t, url))
	vpc := must(e2.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.9.0.0/16")}))
	subnet := *must(e2.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: vpc.Vpc.VpcId, CidrBlock: aws.String("10.9.3.0/24")})).Subnet.SubnetId
	net := &types.NetworkConfiguration{AwsvpcConfiguration: &types.AwsVpcConfiguration{Subnets: []string{subnet}}}
	_, err = c.RunTask(ctx, &ecs.RunTaskInput{Cluster: aws.String("dev"), TaskDefinition: aws.String("web")})
	wantErr(t, err, "InvalidParameterException")
	_, err = c.RunTask(ctx, &ecs.RunTaskInput{Cluster: aws.String("dev"), TaskDefinition: aws.String("nope"), NetworkConfiguration: net})
	wantErr(t, err, "ClientException")
	run := must(c.RunTask(ctx, &ecs.RunTaskInput{Cluster: aws.String("dev"), TaskDefinition: aws.String("web"), Count: aws.Int32(2), LaunchType: types.LaunchTypeFargate,
		NetworkConfiguration: net, StartedBy: aws.String("me"), Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}}))
	if len(run.Tasks) != 2 || *run.Tasks[0].LastStatus != "PROVISIONING" || *run.Tasks[0].DesiredStatus != "RUNNING" || *run.Tasks[0].Group != "family:web" {
		t.Fatalf("run = %+v", run.Tasks)
	}
	a1, a2 := *run.Tasks[0].TaskArn, *run.Tasks[1].TaskArn
	waitRunning(t, c, "dev", a1, a2)
	task := describeTask(t, c, "dev", a1)
	ip := netip.MustParseAddr(detail(task.Attachments[0], "privateIPv4Address"))
	if !netip.MustParsePrefix("10.9.3.0/24").Contains(ip) || *task.Attachments[0].Status != "ATTACHED" || len(task.Tags) != 1 ||
		!strings.Contains(*task.StoppedReason, "pogi") || *task.Containers[0].LastStatus != "RUNNING" || *task.PlatformVersion == "" {
		t.Fatalf("task = %+v", task)
	}
	if ip2 := detail(describeTask(t, c, "dev", a2).Attachments[0], "privateIPv4Address"); ip2 == ip.String() {
		t.Fatalf("two tasks share %s", ip2)
	}
	if arns := must(c.ListTasks(ctx, &ecs.ListTasksInput{Cluster: aws.String("dev"), StartedBy: aws.String("me"), Family: aws.String("web")})).TaskArns; len(arns) != 2 {
		t.Fatalf("list = %v", arns)
	}
	stop := must(c.StopTask(ctx, &ecs.StopTaskInput{Cluster: aws.String("dev"), Task: aws.String(a1), Reason: aws.String("done")})).Task
	if *stop.DesiredStatus != "STOPPED" {
		t.Fatalf("stop = %+v", stop)
	}
	if st := waitStopped(t, c, "dev", a1)[0]; st.StopCode != types.TaskStopCodeUserInitiated || *st.StoppedReason != "done" || st.StoppedAt == nil {
		t.Fatalf("stopped = %+v", st)
	}
	_, err = c.StopTask(ctx, &ecs.StopTaskInput{Cluster: aws.String("dev"), Task: aws.String("0123")})
	wantErr(t, err, "InvalidParameterException")
	_, err = c.DeleteCluster(ctx, &ecs.DeleteClusterInput{Cluster: aws.String("dev")})
	wantErr(t, err, "ClusterContainsTasksException")
	must(c.StopTask(ctx, &ecs.StopTaskInput{Cluster: aws.String("dev"), Task: aws.String(a2)}))
	waitStopped(t, c, "dev", a2)

	// services keep their desired count, roll out new revisions and drain
	td3 := reg()
	_, err = c.CreateService(ctx, &ecs.CreateServiceInput{Cluster: aws.String("dev"), ServiceName: aws.String("site"), TaskDefinition: aws.String("web")})
	wantErr(t, err, "InvalidParameterException")
	svc := must(c.CreateService(ctx, &ecs.CreateServiceInput{Cluster: aws.String("dev"), ServiceName: aws.String("site"), TaskDefinition: td3.TaskDefinitionArn,
		DesiredCount: aws.Int32(2), LaunchType: types.LaunchTypeFargate, NetworkConfiguration: net})).Service
	if *svc.Status != "ACTIVE" || len(svc.Deployments) != 1 || *svc.Deployments[0].Status != "PRIMARY" || *svc.DeploymentConfiguration.MaximumPercent != 200 {
		t.Fatalf("service = %+v", svc)
	}
	_, err = c.CreateService(ctx, &ecs.CreateServiceInput{Cluster: aws.String("dev"), ServiceName: aws.String("site"), TaskDefinition: td3.TaskDefinitionArn, NetworkConfiguration: net})
	wantErr(t, err, "InvalidParameterException")
	stable := waitStable(t, c, "dev", "site")
	if stable.RunningCount != 2 || stable.Deployments[0].RolloutState != types.DeploymentRolloutStateCompleted || !strings.Contains(*stable.Events[0].Message, "steady state") {
		t.Fatalf("stable = %+v", stable)
	}
	before := serviceTasks(t, c, "dev", "site")
	must(c.StopTask(ctx, &ecs.StopTaskInput{Cluster: aws.String("dev"), Task: aws.String(before[0])}))
	waitStopped(t, c, "dev", before[0])
	waitStable(t, c, "dev", "site")
	if after := serviceTasks(t, c, "dev", "site"); len(after) != 2 || slices.Contains(after, before[0]) {
		t.Fatalf("not replaced: %v → %v", before, after)
	}
	td4 := reg()
	up := must(c.UpdateService(ctx, &ecs.UpdateServiceInput{Cluster: aws.String("dev"), Service: aws.String("site"), TaskDefinition: td4.TaskDefinitionArn})).Service
	if len(up.Deployments) < 2 || *up.Deployments[0].TaskDefinition != *td4.TaskDefinitionArn || *up.Deployments[1].Status != "ACTIVE" {
		t.Fatalf("update = %+v", up.Deployments)
	}
	stable = waitStable(t, c, "dev", "site")
	for _, arn := range serviceTasks(t, c, "dev", "site") {
		if d := describeTask(t, c, "dev", arn).TaskDefinitionArn; *d != *td4.TaskDefinitionArn {
			t.Fatalf("task of %s after the rollout", *d)
		}
	}
	_, err = c.DeleteService(ctx, &ecs.DeleteServiceInput{Cluster: aws.String("dev"), Service: aws.String("site")})
	wantErr(t, err, "InvalidParameterException")
	_, err = c.DeleteCluster(ctx, &ecs.DeleteClusterInput{Cluster: aws.String("dev")})
	wantErr(t, err, "ClusterContainsServicesException")

	// state survives a snapshot; running tasks come back STOPPED and the service replaces them
	prev := serviceTasks(t, c, "dev", "site")
	snap := must(hub.Snapshot())
	hub.Reset()
	if err := hub.Restore(snap); err != nil {
		t.Fatal(err)
	}
	old := describeTask(t, c, "dev", prev[0])
	if *old.LastStatus != "STOPPED" || *old.StoppedReason != "pogi restarted" {
		t.Fatalf("restored = %+v", old)
	}
	stable = waitStable(t, c, "dev", "site")

	must(c.UpdateService(ctx, &ecs.UpdateServiceInput{Cluster: aws.String("dev"), Service: aws.String("site"), DesiredCount: aws.Int32(0)}))
	gone := must(c.DeleteService(ctx, &ecs.DeleteServiceInput{Cluster: aws.String("dev"), Service: stable.ServiceArn})).Service
	if *gone.Status != "DRAINING" {
		t.Fatalf("delete = %+v", gone)
	}
	err = ecs.NewServicesInactiveWaiter(c, func(o *ecs.ServicesInactiveWaiterOptions) { fast(&o.MinDelay, &o.MaxDelay) }).
		Wait(ctx, &ecs.DescribeServicesInput{Cluster: aws.String("dev"), Services: []string{"site"}}, wait)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.UpdateService(ctx, &ecs.UpdateServiceInput{Cluster: aws.String("dev"), Service: aws.String("site"), DesiredCount: aws.Int32(1)})
	wantErr(t, err, "ServiceNotActiveException")
	_, err = c.UpdateService(ctx, &ecs.UpdateServiceInput{Cluster: aws.String("dev"), Service: aws.String("nope")})
	wantErr(t, err, "ServiceNotFoundException")

	// tags, settings, capacity providers, container instances, unsupported calls
	must(c.TagResource(ctx, &ecs.TagResourceInput{ResourceArn: cl.ClusterArn, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}))
	must(c.UntagResource(ctx, &ecs.UntagResourceInput{ResourceArn: cl.ClusterArn, TagKeys: []string{"team"}}))
	if tags := must(c.ListTagsForResource(ctx, &ecs.ListTagsForResourceInput{ResourceArn: cl.ClusterArn})).Tags; len(tags) != 1 || *tags[0].Key != "env" {
		t.Fatalf("tags = %+v", tags)
	}
	_, err = c.ListTagsForResource(ctx, &ecs.ListTagsForResourceInput{ResourceArn: aws.String("arn:aws:ecs:us-east-1:000000000000:cluster/nope")})
	wantErr(t, err, "ResourceNotFoundException")
	must(c.PutAccountSetting(ctx, &ecs.PutAccountSettingInput{Name: types.SettingNameContainerInsights, Value: aws.String("enabled")}))
	if set := must(c.ListAccountSettings(ctx, &ecs.ListAccountSettingsInput{Name: types.SettingNameContainerInsights, EffectiveSettings: true})).Settings; len(set) != 1 || *set[0].Value != "enabled" {
		t.Fatalf("settings = %+v", set)
	}
	if v := must(c.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String("insights")})).Cluster.Settings[0].Value; *v != "enabled" {
		t.Fatalf("containerInsights = %s", *v)
	}
	if cps := must(c.DescribeCapacityProviders(ctx, &ecs.DescribeCapacityProvidersInput{})).CapacityProviders; len(cps) != 2 || *cps[0].Name != "FARGATE" {
		t.Fatalf("capacity providers = %+v", cps)
	}
	_, err = c.CreateCapacityProvider(ctx, &ecs.CreateCapacityProviderInput{Name: aws.String("fargate-mine"),
		AutoScalingGroupProvider: &types.AutoScalingGroupProvider{AutoScalingGroupArn: aws.String("arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:x")}})
	wantErr(t, err, "InvalidParameterException")
	must(c.PutClusterCapacityProviders(ctx, &ecs.PutClusterCapacityProvidersInput{Cluster: aws.String("dev"), CapacityProviders: []string{"FARGATE"},
		DefaultCapacityProviderStrategy: []types.CapacityProviderStrategyItem{{CapacityProvider: aws.String("FARGATE"), Weight: 1}}}))
	ci := must(c.RegisterContainerInstance(ctx, &ecs.RegisterContainerInstanceInput{Cluster: aws.String("dev"),
		InstanceIdentityDocument: aws.String(`{"instanceId":"i-0123456789abcdef0"}`)})).ContainerInstance
	if *ci.Ec2InstanceId != "i-0123456789abcdef0" || *ci.Status != "ACTIVE" {
		t.Fatalf("instance = %+v", ci)
	}
	_, err = c.DeleteCluster(ctx, &ecs.DeleteClusterInput{Cluster: aws.String("dev")})
	wantErr(t, err, "ClusterContainsContainerInstancesException")
	must(c.DeregisterContainerInstance(ctx, &ecs.DeregisterContainerInstanceInput{Cluster: aws.String("dev"), ContainerInstance: ci.ContainerInstanceArn}))
	_, err = c.UpdateServicePrimaryTaskSet(ctx, &ecs.UpdateServicePrimaryTaskSetInput{Cluster: aws.String("dev"), Service: aws.String("site"), PrimaryTaskSet: aws.String("ts")})
	wantErr(t, err, "InvalidParameterException")
	_, err = c.ExecuteCommand(ctx, &ecs.ExecuteCommandInput{Cluster: aws.String("dev"), Task: aws.String(a1), Command: aws.String("sh"), Interactive: true})
	wantErr(t, err, "ClientException")
	if d := must(c.DeleteCluster(ctx, &ecs.DeleteClusterInput{Cluster: aws.String("dev")})).Cluster; *d.Status != "INACTIVE" {
		t.Fatalf("deleted = %+v", d)
	}
}

func TestDocker(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	var ids []string // tasks whose containers must be gone once the hub closes
	t.Cleanup(func() {
		list, _ := d.List(ctx, docker.LabelService+"=ecs")
		for _, ct := range list {
			if slices.Contains(ids, ct.Labels[docker.LabelResource]) {
				t.Errorf("container %v left behind", ct.Names)
				d.Remove(ctx, ct.ID)
			}
		}
	})
	hub, url, c := setup(t)
	cfg := awstest.Config(t, url)
	must(c.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String("web")}))
	web := must(c.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{Family: aws.String("nginx"), NetworkMode: types.NetworkModeAwsvpc,
		RequiresCompatibilities: []types.Compatibility{types.CompatibilityFargate}, Cpu: aws.String("256"), Memory: aws.String("512"),
		ContainerDefinitions: []types.ContainerDefinition{{Name: aws.String("web"), Image: aws.String("nginx:alpine"),
			PortMappings: []types.PortMapping{{ContainerPort: aws.Int32(80)}}}}})).TaskDefinition
	net := &types.NetworkConfiguration{AwsvpcConfiguration: &types.AwsVpcConfiguration{Subnets: []string{"subnet-12345678"}}}
	// RUNNING says the container started, not that nginx listens yet: until it does, the published port resets connections
	get := func(addr string) string {
		t.Helper()
		for end := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
			resp, err := http.Get("http://" + addr)
			if err == nil {
				defer resp.Body.Close()
				b, _ := io.ReadAll(resp.Body)
				return string(b)
			}
			if time.Now().After(end) {
				t.Fatal(err)
			}
		}
	}

	t.Run("nginx", func(t *testing.T) {
		arn := *must(c.RunTask(ctx, &ecs.RunTaskInput{Cluster: aws.String("web"), TaskDefinition: web.TaskDefinitionArn, LaunchType: types.LaunchTypeFargate,
			NetworkConfiguration: net})).Tasks[0].TaskArn
		ids = append(ids, arn[strings.LastIndex(arn, "/")+1:])
		waitRunning(t, c, "web", arn)
		task := describeTask(t, c, "web", arn)
		ip := detail(task.Attachments[0], "privateIPv4Address")
		addr, ok := hub.Endpoint("ip:" + ip + ":80")
		if !ok || task.Containers[0].NetworkBindings[0].HostPort == nil || !strings.HasSuffix(addr, ":"+strconv.Itoa(int(*task.Containers[0].NetworkBindings[0].HostPort))) {
			t.Fatalf("endpoint %q %v, task %+v", addr, ok, task.Containers[0])
		}
		if body := get(addr); !strings.Contains(body, "Welcome to nginx") {
			t.Fatalf("GET %s = %q", addr, body)
		}
		must(c.StopTask(ctx, &ecs.StopTaskInput{Cluster: aws.String("web"), Task: aws.String(arn)}))
		if st := waitStopped(t, c, "web", arn)[0]; st.Containers[0].ExitCode == nil {
			t.Fatalf("no exit code: %+v", st.Containers[0])
		}
		if _, ok := hub.Endpoint("ip:" + ip + ":80"); ok {
			t.Fatal("endpoint kept after the task stopped")
		}
	})

	t.Run("service", func(t *testing.T) {
		must(c.CreateService(ctx, &ecs.CreateServiceInput{Cluster: aws.String("web"), ServiceName: aws.String("site"), TaskDefinition: web.TaskDefinitionArn,
			DesiredCount: aws.Int32(2), LaunchType: types.LaunchTypeFargate, NetworkConfiguration: net}))
		waitStable(t, c, "web", "site")
		arns := serviceTasks(t, c, "web", "site")
		for _, arn := range arns {
			id := arn[strings.LastIndex(arn, "/")+1:]
			ids = append(ids, id)
			if info, err := d.Inspect(ctx, docker.Name("ecs", id+"-web")); err != nil || !info.State.Running {
				t.Fatalf("container of %s: %v %+v", id, err, info)
			}
		}
		if len(arns) != 2 {
			t.Fatalf("tasks = %v", arns)
		}
		must(c.StopTask(ctx, &ecs.StopTaskInput{Cluster: aws.String("web"), Task: aws.String(arns[0])}))
		waitStopped(t, c, "web", arns[0])
		if s := waitStable(t, c, "web", "site"); s.RunningCount != 2 {
			t.Fatalf("service = %+v", s)
		}
		after := serviceTasks(t, c, "web", "site")
		for _, arn := range after {
			ids = append(ids, arn[strings.LastIndex(arn, "/")+1:])
		}
		if slices.Contains(after, arns[0]) || len(after) != 2 {
			t.Fatalf("not replaced: %v → %v", arns, after)
		}
		must(c.DeleteService(ctx, &ecs.DeleteServiceInput{Cluster: aws.String("web"), Service: aws.String("site"), Force: aws.Bool(true)}))
	})

	t.Run("logs, secrets and credentials", func(t *testing.T) {
		must(ssm.NewFromConfig(cfg).PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String("/app/db-pass"), Value: aws.String("s3cret"), Type: ssmtypes.ParameterTypeSecureString}))
		sec := must(secretsmanager.NewFromConfig(cfg).CreateSecret(ctx, &secretsmanager.CreateSecretInput{Name: aws.String("app/creds"), SecretString: aws.String(`{"user":"admin"}`)}))
		script := `echo hello from ecs; echo "pass=$DB_PASS user=$DB_USER"
wget -qO- --header "Authorization: $AWS_CONTAINER_AUTHORIZATION_TOKEN" "$AWS_CONTAINER_CREDENTIALS_FULL_URI"; echo
wget -qO- "$ECS_CONTAINER_METADATA_URI_V4/task"; echo`
		job := must(c.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{Family: aws.String("job"), TaskRoleArn: aws.String("arn:aws:iam::000000000000:role/app"),
			ContainerDefinitions: []types.ContainerDefinition{{Name: aws.String("job"), Image: aws.String("alpine:3"), Memory: aws.Int32(64),
				Command: []string{"sh", "-c", script},
				Secrets: []types.Secret{{Name: aws.String("DB_PASS"), ValueFrom: aws.String("/app/db-pass")}, {Name: aws.String("DB_USER"), ValueFrom: aws.String(*sec.ARN + ":user::")}},
				LogConfiguration: &types.LogConfiguration{LogDriver: types.LogDriverAwslogs, Options: map[string]string{"awslogs-group": "/ecs/job",
					"awslogs-region": "us-east-1", "awslogs-stream-prefix": "ecs", "awslogs-create-group": "true"}}}}})).TaskDefinition
		arn := *must(c.RunTask(ctx, &ecs.RunTaskInput{Cluster: aws.String("web"), TaskDefinition: job.TaskDefinitionArn})).Tasks[0].TaskArn
		id := arn[strings.LastIndex(arn, "/")+1:]
		ids = append(ids, id)
		st := waitStopped(t, c, "web", arn)[0]
		if st.StopCode != types.TaskStopCodeEssentialContainerExited || st.Containers[0].ExitCode == nil || *st.Containers[0].ExitCode != 0 {
			t.Fatalf("job = %+v %+v", st, st.Containers[0])
		}
		events := must(cloudwatchlogs.NewFromConfig(cfg).GetLogEvents(ctx, &cloudwatchlogs.GetLogEventsInput{LogGroupName: aws.String("/ecs/job"),
			LogStreamName: aws.String("ecs/job/" + id), StartFromHead: aws.Bool(true)})).Events
		var lines []string
		for _, e := range events {
			lines = append(lines, *e.Message)
		}
		if len(lines) < 4 || lines[0] != "hello from ecs" || lines[1] != "pass=s3cret user=admin" || !strings.Contains(lines[3], arn) {
			t.Fatalf("log lines = %q", lines)
		}
		var cr struct{ AccessKeyId, SecretAccessKey, Token, RoleArn string }
		if err := json.Unmarshal([]byte(lines[2]), &cr); err != nil || cr.RoleArn != "arn:aws:iam::000000000000:role/app" {
			t.Fatalf("credentials %q: %v", lines[2], err)
		}
		who := must(sts.NewFromConfig(awstest.ConfigAs(t, url, cr.AccessKeyId, cr.SecretAccessKey, cr.Token)).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}))
		if *who.Arn != "arn:aws:sts::000000000000:assumed-role/app/"+id {
			t.Fatalf("caller = %s", *who.Arn)
		}
	})

	// floci #5479 and #3806: a task volume with only a name is shared by the containers of the task, and volumesFrom mounts the same volumes.
	t.Run("task volumes", func(t *testing.T) {
		sleep := func(name, script string) types.ContainerDefinition {
			return types.ContainerDefinition{Name: aws.String(name), Image: aws.String("alpine:3"), Memory: aws.Int32(64), Command: []string{"sh", "-c", script + "; sleep 600"}}
		}
		mount := []types.MountPoint{{SourceVolume: aws.String("shared"), ContainerPath: aws.String("/data")}}
		w, r := sleep("writer", "echo from-writer > /data/hello"), sleep("reader", "true")
		w.MountPoints = mount
		r.VolumesFrom = []types.VolumeFrom{{SourceContainer: aws.String("writer")}}
		td := must(c.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{Family: aws.String("vol"), Volumes: []types.Volume{{Name: aws.String("shared")}},
			ContainerDefinitions: []types.ContainerDefinition{r, w}})).TaskDefinition // the reader comes first: volumesFrom starts the writer before it
		arn := *must(c.RunTask(ctx, &ecs.RunTaskInput{Cluster: aws.String("web"), TaskDefinition: td.TaskDefinitionArn})).Tasks[0].TaskArn
		id := arn[strings.LastIndex(arn, "/")+1:]
		ids = append(ids, id)
		waitRunning(t, c, "web", arn)
		var got []byte
		for end := time.Now().Add(10 * time.Second); string(got) != "from-writer\n" && time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
			got, _, _ = d.Exec(ctx, docker.Name("ecs", id+"-reader"), "cat", "/data/hello")
		}
		if string(got) != "from-writer\n" {
			t.Fatalf("reader sees %q", got)
		}
		must(c.StopTask(ctx, &ecs.StopTaskInput{Cluster: aws.String("web"), Task: aws.String(arn)}))
		waitStopped(t, c, "web", arn)
	})

	// floci #4745: with the circuit breaker on, a service whose tasks keep failing to start FAILS its deployment
	// instead of staying IN_PROGRESS, and with rollback on it goes back to the last completed revision.
	t.Run("circuit breaker", func(t *testing.T) {
		def := func(family string, log map[string]string) *types.TaskDefinition {
			cd := types.ContainerDefinition{Name: aws.String("x"), Image: aws.String("alpine:3"), Memory: aws.Int32(64), Command: []string{"sleep", "600"}}
			if log != nil {
				cd.LogConfiguration = &types.LogConfiguration{LogDriver: types.LogDriverAwslogs, Options: log}
			}
			return must(c.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{Family: aws.String(family), ContainerDefinitions: []types.ContainerDefinition{cd}})).TaskDefinition
		}
		good, bad := def("cb-good", nil), def("cb-bad", map[string]string{"awslogs-group": "/nope"}) // the missing log group fails every task
		breaker := func(rollback bool) *types.DeploymentConfiguration {
			return &types.DeploymentConfiguration{DeploymentCircuitBreaker: &types.DeploymentCircuitBreaker{Enable: true, Rollback: rollback}}
		}
		primary := func(name string) types.Deployment {
			t.Helper()
			sv := must(c.DescribeServices(ctx, &ecs.DescribeServicesInput{Cluster: aws.String("web"), Services: []string{name}})).Services[0]
			for _, d := range sv.Deployments {
				if aws.ToString(d.Status) == "PRIMARY" {
					return d
				}
			}
			t.Fatalf("%s has no primary deployment: %+v", name, sv.Deployments)
			return types.Deployment{}
		}
		until := func(name string, ok func(types.Deployment) bool) types.Deployment {
			t.Helper()
			for end := time.Now().Add(90 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
				if d := primary(name); ok(d) {
					return d
				}
			}
			t.Fatalf("%s: deployment is %+v", name, primary(name))
			return types.Deployment{}
		}
		del := func(name string) {
			for _, arn := range serviceTasks(t, c, "web", name) {
				ids = append(ids, arn[strings.LastIndex(arn, "/")+1:])
			}
			must(c.DeleteService(ctx, &ecs.DeleteServiceInput{Cluster: aws.String("web"), Service: aws.String(name), Force: aws.Bool(true)}))
		}

		// no rollback: the deployment FAILS and stays failed, no more tasks are started
		created := must(c.CreateService(ctx, &ecs.CreateServiceInput{Cluster: aws.String("web"), ServiceName: aws.String("cb-fail"), TaskDefinition: bad.TaskDefinitionArn,
			DesiredCount: aws.Int32(1), DeploymentConfiguration: breaker(false)}))
		if cb := created.Service.DeploymentConfiguration.DeploymentCircuitBreaker; cb == nil || !cb.Enable || cb.Rollback {
			t.Fatalf("circuit breaker not kept: %+v", created.Service.DeploymentConfiguration)
		}
		d := until("cb-fail", func(d types.Deployment) bool { return d.RolloutState == types.DeploymentRolloutStateFailed })
		if !strings.Contains(aws.ToString(d.RolloutStateReason), "circuit breaker") || d.FailedTasks < 3 {
			t.Errorf("failed deployment = %+v", d)
		}
		time.Sleep(3 * time.Second)
		if again := primary("cb-fail"); again.FailedTasks != d.FailedTasks || again.RolloutState != types.DeploymentRolloutStateFailed {
			t.Errorf("tasks still started after the failure: %+v -> %+v", d, again)
		}
		del("cb-fail")

		// rollback: a bad update goes back to the revision that was running
		must(c.CreateService(ctx, &ecs.CreateServiceInput{Cluster: aws.String("web"), ServiceName: aws.String("cb-back"), TaskDefinition: good.TaskDefinitionArn,
			DesiredCount: aws.Int32(1), DeploymentConfiguration: breaker(true)}))
		waitStable(t, c, "web", "cb-back")
		must(c.UpdateService(ctx, &ecs.UpdateServiceInput{Cluster: aws.String("web"), Service: aws.String("cb-back"), TaskDefinition: bad.TaskDefinitionArn}))
		d = until("cb-back", func(d types.Deployment) bool {
			return aws.ToString(d.TaskDefinition) == aws.ToString(good.TaskDefinitionArn) && d.RolloutState == types.DeploymentRolloutStateCompleted
		})
		if sv := waitStable(t, c, "web", "cb-back"); sv.RunningCount != 1 || len(sv.Deployments) != 1 {
			t.Errorf("after rollback: %+v", sv)
		}
		del("cb-back")
	})

	t.Run("missing log group fails the task", func(t *testing.T) {
		bad := must(c.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{Family: aws.String("bad"),
			ContainerDefinitions: []types.ContainerDefinition{{Name: aws.String("x"), Image: aws.String("alpine:3"), Memory: aws.Int32(64),
				LogConfiguration: &types.LogConfiguration{LogDriver: types.LogDriverAwslogs, Options: map[string]string{"awslogs-group": "/nope"}}}}})).TaskDefinition
		arn := *must(c.RunTask(ctx, &ecs.RunTaskInput{Cluster: aws.String("web"), TaskDefinition: bad.TaskDefinitionArn})).Tasks[0].TaskArn
		ids = append(ids, arn[strings.LastIndex(arn, "/")+1:])
		if st := waitStopped(t, c, "web", arn)[0]; st.StopCode != types.TaskStopCodeTaskFailedToStart || !strings.HasPrefix(*st.StoppedReason, "ResourceInitializationError") {
			t.Fatalf("task = %+v", st)
		}
	})
}
