package codedeploy_test

import (
	"errors"
	"io"
	"net"
	"net/http"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/codedeploy"
	"github.com/aws/aws-sdk-go-v2/service/codedeploy/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	elb "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbtypes "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/smithy-go"
	codedeploysvc "github.com/nrndev/pogi/internal/aws/codedeploy"
	ecssvc "github.com/nrndev/pogi/internal/aws/ecs"
	elbsvc "github.com/nrndev/pogi/internal/aws/elbv2"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

type ecsEnv struct {
	env
	ecs          *ecs.Client
	elb          *elb.Client
	blue, green  string // target group ARNs
	prod, test   string // listener ARNs
	oldTD, newTD string
}

func freePort(t *testing.T) int32 {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return int32(ln.Addr().(*net.TCPAddr).Port)
}

// setupECS serves CodeDeploy, ECS, ELB v2 and Lambda with a service on the
// CODE_DEPLOY controller behind a blue target group, two task definitions
// (old and new), and an ECS application with a deployment group.
// terminateWait is the group's wait, in minutes, before the original task set ends.
// With docker the tasks are real: revision n serves "v<n>" on port 80.
func setupECS(t *testing.T, config string, terminateWait int32, test, docker bool) ecsEnv {
	if !docker {
		t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	}
	var svc *codedeploysvc.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return lambdasvc.New(h) }, func(h *core.Hub) core.Service { return ecssvc.New(h) },
		func(h *core.Hub) core.Service { return elbsvc.New(h) },
		func(h *core.Hub) core.Service { svc = codedeploysvc.New(h); return svc })
	cfg := awstest.Config(t, url)
	e := ecsEnv{env: env{svc, codedeploy.NewFromConfig(cfg), lambda.NewFromConfig(cfg)}, ecs: ecs.NewFromConfig(cfg), elb: elb.NewFromConfig(cfg)}

	lb := must(e.elb.CreateLoadBalancer(ctx, &elb.CreateLoadBalancerInput{Name: aws.String("web"), Subnets: []string{"subnet-1", "subnet-2"}})).LoadBalancers[0]
	tg := func(name string) string {
		return *must(e.elb.CreateTargetGroup(ctx, &elb.CreateTargetGroupInput{Name: aws.String(name), Protocol: elbtypes.ProtocolEnumHttp, Port: aws.Int32(80),
			VpcId: aws.String("vpc-1"), TargetType: elbtypes.TargetTypeEnumIp})).TargetGroups[0].TargetGroupArn
	}
	e.blue, e.green = tg("blue"), tg("green")
	listener := func() string {
		return *must(e.elb.CreateListener(ctx, &elb.CreateListenerInput{LoadBalancerArn: lb.LoadBalancerArn, Protocol: elbtypes.ProtocolEnumHttp, Port: aws.Int32(freePort(t)),
			DefaultActions: []elbtypes.Action{{Type: elbtypes.ActionTypeEnumForward, TargetGroupArn: aws.String(e.blue)}}})).Listeners[0].ListenerArn
	}
	e.prod = listener()
	pair := types.TargetGroupPairInfo{TargetGroups: []types.TargetGroupInfo{{Name: aws.String("blue")}, {Name: aws.String("green")}},
		ProdTrafficRoute: &types.TrafficRoute{ListenerArns: []string{e.prod}}}
	if test {
		e.test = listener()
		pair.TestTrafficRoute = &types.TrafficRoute{ListenerArns: []string{e.test}}
	}

	must(e.ecs.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String("c")}))
	revision := 0
	register := func() string {
		revision++
		mode := ecstypes.NetworkModeBridge
		def := ecstypes.ContainerDefinition{Name: aws.String("app"), Image: aws.String("nginx:alpine"), Memory: aws.Int32(64),
			PortMappings: []ecstypes.PortMapping{{ContainerPort: aws.Int32(80)}}}
		if docker {
			mode = ecstypes.NetworkModeAwsvpc // load balancers reach awsvpc tasks by private address
			def.Command = []string{"sh", "-c", "echo v" + strconv.Itoa(revision) + " > /usr/share/nginx/html/index.html && exec nginx -g 'daemon off;'"}
		}
		return *must(e.ecs.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{Family: aws.String("web"), NetworkMode: mode,
			ContainerDefinitions: []ecstypes.ContainerDefinition{def}})).TaskDefinition.TaskDefinitionArn
	}
	e.oldTD, e.newTD = register(), register()
	var net *ecstypes.NetworkConfiguration
	if docker {
		net = &ecstypes.NetworkConfiguration{AwsvpcConfiguration: &ecstypes.AwsVpcConfiguration{Subnets: []string{"subnet-1"}}}
	}
	must(e.ecs.CreateService(ctx, &ecs.CreateServiceInput{Cluster: aws.String("c"), ServiceName: aws.String("s"), TaskDefinition: aws.String(e.oldTD), DesiredCount: aws.Int32(2), NetworkConfiguration: net,
		DeploymentController: &ecstypes.DeploymentController{Type: ecstypes.DeploymentControllerTypeCodeDeploy},
		LoadBalancers:        []ecstypes.LoadBalancer{{TargetGroupArn: aws.String(e.blue), ContainerName: aws.String("app"), ContainerPort: aws.Int32(80)}}}))

	role := aws.String("arn:aws:iam::000000000000:role/r")
	must(e.cd.CreateApplication(ctx, &codedeploy.CreateApplicationInput{ApplicationName: aws.String("app"), ComputePlatform: types.ComputePlatformEcs}))
	must(e.cd.CreateDeploymentGroup(ctx, &codedeploy.CreateDeploymentGroupInput{ApplicationName: aws.String("app"), DeploymentGroupName: aws.String("group"),
		ServiceRoleArn: role, DeploymentConfigName: aws.String(config),
		EcsServices:      []types.ECSService{{ClusterName: aws.String("c"), ServiceName: aws.String("s")}},
		LoadBalancerInfo: &types.LoadBalancerInfo{TargetGroupPairInfoList: []types.TargetGroupPairInfo{pair}},
		DeploymentStyle:  &types.DeploymentStyle{DeploymentType: types.DeploymentTypeBlueGreen, DeploymentOption: types.DeploymentOptionWithTrafficControl},
		BlueGreenDeploymentConfiguration: &types.BlueGreenDeploymentConfiguration{
			DeploymentReadyOption:                     &types.DeploymentReadyOption{ActionOnTimeout: types.DeploymentReadyActionContinueDeployment},
			TerminateBlueInstancesOnDeploymentSuccess: &types.BlueInstanceTerminationOption{Action: types.InstanceActionTerminate, TerminationWaitTimeInMinutes: terminateWait}}}))
	return e
}

// deployECS starts a deployment of the new task definition.
func deployECS(t *testing.T, e ecsEnv, hooks string) string {
	t.Helper()
	spec := `version: 0.0
Resources:
  - TargetService:
      Type: AWS::ECS::Service
      Properties:
        TaskDefinition: "` + e.newTD + `"
        LoadBalancerInfo:
          ContainerName: "app"
          ContainerPort: 80
` + hooks
	return *must(e.cd.CreateDeployment(ctx, &codedeploy.CreateDeploymentInput{ApplicationName: aws.String("app"), DeploymentGroupName: aws.String("group"),
		Revision: &types.RevisionLocation{RevisionType: types.RevisionLocationTypeAppSpecContent, AppSpecContent: &types.AppSpecContent{Content: aws.String(spec)}}})).DeploymentId
}

// weights are the percent of a listener's traffic on the blue and green groups.
func (e ecsEnv) weights(arn string) (blue, green int) {
	l := must(e.elb.DescribeListeners(ctx, &elb.DescribeListenersInput{ListenerArns: []string{arn}})).Listeners[0]
	f := l.DefaultActions[0].ForwardConfig
	if f == nil {
		return 100, 0
	}
	for _, g := range f.TargetGroups {
		switch *g.TargetGroupArn {
		case e.blue:
			blue = int(*g.Weight)
		case e.green:
			green = int(*g.Weight)
		}
	}
	return
}

// sets are the service's task sets by status and task definition.
func (e ecsEnv) sets() map[string]string {
	out := map[string]string{}
	for _, ts := range must(e.ecs.DescribeServices(ctx, &ecs.DescribeServicesInput{Cluster: aws.String("c"), Services: []string{"s"}})).Services[0].TaskSets {
		out[*ts.Status] = *ts.TaskDefinition
	}
	return out
}

func (e ecsEnv) events(t *testing.T, id string) []string {
	t.Helper()
	tgt := must(e.cd.GetDeploymentTarget(ctx, &codedeploy.GetDeploymentTargetInput{DeploymentId: aws.String(id), TargetId: aws.String("c:s")})).DeploymentTarget
	if tgt.DeploymentTargetType != types.DeploymentTargetTypeEcsTarget {
		t.Fatalf("target type %s", tgt.DeploymentTargetType)
	}
	var out []string
	for _, ev := range tgt.EcsTarget.LifecycleEvents {
		out = append(out, *ev.LifecycleEventName+":"+string(ev.Status))
	}
	return out
}

func (e ecsEnv) wantWeights(t *testing.T, what, listener string, blue, green int) {
	t.Helper()
	if b, g := e.weights(listener); b != blue || g != green {
		t.Fatalf("%s: weights blue %d, green %d; want %d, %d", what, b, g, blue, green)
	}
}

func (e ecsEnv) wait(t *testing.T, id string, status types.DeploymentStatus) {
	t.Helper()
	waitFor(t, "deployment "+string(status), func() bool { return info(e.env, id).Status == status })
}

func TestECSAllAtOnce(t *testing.T) {
	e := setupECS(t, "CodeDeployDefault.ECSAllAtOnce", 0, true, false)
	id := deployECS(t, e, "")
	e.wait(t, id, types.DeploymentStatusSucceeded)
	d := info(e.env, id)
	if d.ComputePlatform != types.ComputePlatformEcs || d.CompleteTime == nil || d.DeploymentOverview.Succeeded != 1 {
		t.Fatalf("deployment %+v", d)
	}
	if got, want := e.events(t, id), []string{"BeforeInstall:Skipped", "Install:Succeeded", "AfterInstall:Skipped", "AllowTestTraffic:Succeeded",
		"AfterAllowTestTraffic:Skipped", "BeforeAllowTraffic:Skipped", "AllowTraffic:Succeeded", "AfterAllowTraffic:Skipped"}; !slices.Equal(got, want) {
		t.Fatalf("events %v", got)
	}
	e.wantWeights(t, "production", e.prod, 0, 100)
	e.wantWeights(t, "test", e.test, 0, 100)
	waitFor(t, "the original task set to go", func() bool { return len(e.sets()) == 1 })
	if s := e.sets(); s["PRIMARY"] != e.newTD {
		t.Fatalf("task sets %v", s)
	}
	if ids := must(e.cd.ListDeploymentTargets(ctx, &codedeploy.ListDeploymentTargetsInput{DeploymentId: aws.String(id)})).TargetIds; len(ids) != 1 || ids[0] != "c:s" {
		t.Fatalf("targets %v", ids)
	}
	tgt := must(e.cd.GetDeploymentTarget(ctx, &codedeploy.GetDeploymentTargetInput{DeploymentId: aws.String(id), TargetId: aws.String("c:s")})).DeploymentTarget.EcsTarget
	if tgt.Status != types.TargetStatusSucceeded || len(tgt.TaskSetsInfo) != 1 {
		t.Fatalf("target %+v", tgt)
	}

	// the next deployment goes back the other way: green is the original now
	e.oldTD, e.newTD = e.newTD, e.oldTD
	id = deployECS(t, e, "")
	e.wait(t, id, types.DeploymentStatusSucceeded)
	e.wantWeights(t, "production", e.prod, 100, 0)
}

func TestECSCanaryAndTerminationWait(t *testing.T) {
	e := setupECS(t, "CodeDeployDefault.ECSCanary10Percent5Minutes", 5, false, false)
	id := deployECS(t, e, "")
	waitFor(t, "the 10% step", func() bool { b, g := e.weights(e.prod); return b == 90 && g == 10 })
	if s := e.sets(); len(s) != 2 {
		t.Fatalf("during the canary: task sets %v", s)
	}
	tgt := must(e.cd.GetDeploymentTarget(ctx, &codedeploy.GetDeploymentTargetInput{DeploymentId: aws.String(id), TargetId: aws.String("c:s")})).DeploymentTarget.EcsTarget
	if len(tgt.TaskSetsInfo) != 2 || tgt.TaskSetsInfo[0].TrafficWeight != 90 || tgt.TaskSetsInfo[1].TrafficWeight != 10 || *tgt.TaskSetsInfo[1].TargetGroup.Name != "green" {
		t.Fatalf("task sets info %+v", tgt.TaskSetsInfo)
	}
	base := time.Now()
	e.svc.Tick(base.Add(time.Minute)) // before the interval: nothing moves
	e.wantWeights(t, "before the interval", e.prod, 90, 10)
	e.svc.Tick(base.Add(6 * time.Minute))
	e.wantWeights(t, "after the interval", e.prod, 0, 100)
	if d := info(e.env, id); d.Status != types.DeploymentStatusInProgress || len(e.sets()) != 2 {
		t.Fatalf("while the original task set waits: %s, task sets %v", d.Status, e.sets())
	}
	e.svc.Tick(base.Add(8 * time.Minute)) // the 5 minute wait is not over
	if d := info(e.env, id); d.Status != types.DeploymentStatusInProgress {
		t.Fatalf("early: %s", d.Status)
	}
	e.svc.Tick(base.Add(12 * time.Minute))
	e.wait(t, id, types.DeploymentStatusSucceeded)
	waitFor(t, "the original task set to go", func() bool { return len(e.sets()) == 1 })
}

func TestECSStopRollsBack(t *testing.T) {
	e := setupECS(t, "CodeDeployDefault.ECSLinear10PercentEvery1Minutes", 0, true, false)
	id := deployECS(t, e, "")
	waitFor(t, "the 10% step", func() bool { b, g := e.weights(e.prod); return b == 90 && g == 10 })
	must(e.cd.StopDeployment(ctx, &codedeploy.StopDeploymentInput{DeploymentId: aws.String(id), AutoRollbackEnabled: aws.Bool(true)}))
	d := info(e.env, id)
	if d.Status != types.DeploymentStatusStopped || d.RollbackInfo == nil || d.ErrorInformation.Code != types.ErrorCodeManualStop {
		t.Fatalf("stopped %+v", d)
	}
	e.wantWeights(t, "production", e.prod, 100, 0)
	e.wantWeights(t, "test", e.test, 100, 0)
	waitFor(t, "the replacement task set to go", func() bool { return len(e.sets()) == 1 })
	if s := e.sets(); s["PRIMARY"] != e.oldTD {
		t.Fatalf("task sets %v", s)
	}
	if got := e.events(t, id); got[1] != "Install:Succeeded" || got[6] != "AllowTraffic:Failed" || got[7] != "AfterAllowTraffic:Skipped" {
		t.Fatalf("events %v", got)
	}
}

func TestECSHookFailureRollsBack(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	e := setupECS(t, "CodeDeployDefault.ECSAllAtOnce", 0, true, false)
	must(e.fn.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("pre-traffic"), Runtime: lambdatypes.RuntimePython312,
		Handler: aws.String("index.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"), Code: &lambdatypes.FunctionCode{ZipFile: pyZip(reportFailed)}}))
	id := deployECS(t, e, "Hooks:\n  - BeforeAllowTraffic: pre-traffic\n")
	e.wait(t, id, types.DeploymentStatusFailed)
	d := info(e.env, id)
	if d.ErrorInformation.Code != types.ErrorCodeHookExecutionFailure || d.RollbackInfo == nil {
		t.Fatalf("deployment %+v %+v", d, d.ErrorInformation)
	}
	e.wantWeights(t, "production", e.prod, 100, 0)
	e.wantWeights(t, "test", e.test, 100, 0)
	waitFor(t, "the replacement task set to go", func() bool { return len(e.sets()) == 1 })
	if s := e.sets(); s["PRIMARY"] != e.oldTD {
		t.Fatalf("task sets %v", s)
	}
	if got := e.events(t, id); got[3] != "AllowTestTraffic:Succeeded" || got[5] != "BeforeAllowTraffic:Failed" || got[6] != "AllowTraffic:Skipped" {
		t.Fatalf("events %v", got)
	}
}

func TestECSRefusals(t *testing.T) {
	e := setupECS(t, "CodeDeployDefault.ECSLinear10PercentEvery3Minutes", 0, false, false)
	code := func(err error) string {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			return ae.ErrorCode()
		}
		return "no error"
	}
	pair := func(blue, green string, listeners ...string) types.TargetGroupPairInfo {
		return types.TargetGroupPairInfo{TargetGroups: []types.TargetGroupInfo{{Name: aws.String(blue)}, {Name: aws.String(green)}}, ProdTrafficRoute: &types.TrafficRoute{ListenerArns: listeners}}
	}
	group := func(name string, mod func(*codedeploy.CreateDeploymentGroupInput)) error {
		in := &codedeploy.CreateDeploymentGroupInput{ApplicationName: aws.String("app"), DeploymentGroupName: aws.String(name), ServiceRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
			EcsServices:      []types.ECSService{{ClusterName: aws.String("c"), ServiceName: aws.String("s")}},
			LoadBalancerInfo: &types.LoadBalancerInfo{TargetGroupPairInfoList: []types.TargetGroupPairInfo{pair("blue", "green", e.prod)}}}
		mod(in)
		_, err := e.cd.CreateDeploymentGroup(ctx, in)
		return err
	}
	for want, mod := range map[string]func(*codedeploy.CreateDeploymentGroupInput){
		"InvalidECSServiceException": func(in *codedeploy.CreateDeploymentGroupInput) { in.EcsServices = nil },
		"ECSServiceMappingLimitExceededException": func(in *codedeploy.CreateDeploymentGroupInput) {
			in.EcsServices = append(in.EcsServices, in.EcsServices[0])
		},
		"InvalidTargetGroupPairException": func(in *codedeploy.CreateDeploymentGroupInput) {
			in.LoadBalancerInfo.TargetGroupPairInfoList[0] = pair("blue", "blue", e.prod)
		},
		"InvalidDeploymentStyleException": func(in *codedeploy.CreateDeploymentGroupInput) {
			in.DeploymentStyle = &types.DeploymentStyle{DeploymentType: types.DeploymentTypeInPlace}
		},
		"InvalidBlueGreenDeploymentConfigurationException": func(in *codedeploy.CreateDeploymentGroupInput) {
			in.BlueGreenDeploymentConfiguration = &types.BlueGreenDeploymentConfiguration{TerminateBlueInstancesOnDeploymentSuccess: &types.BlueInstanceTerminationOption{
				Action: types.InstanceActionTerminate, TerminationWaitTimeInMinutes: -1}}
		},
	} {
		if got := code(group("bad", mod)); got != want {
			t.Errorf("want %s, got %s", want, got)
		}
	}
	if got := code(group("missing-service", func(in *codedeploy.CreateDeploymentGroupInput) { in.EcsServices[0].ServiceName = aws.String("nope") })); got != "InvalidECSServiceException" {
		t.Errorf("missing service: %s", got)
	}
	if got := code(group("missing-tg", func(in *codedeploy.CreateDeploymentGroupInput) {
		in.LoadBalancerInfo.TargetGroupPairInfoList[0] = pair("blue", "gone", e.prod)
	})); got != "InvalidTargetGroupPairException" {
		t.Errorf("missing target group: %s", got)
	}
	if got := code(group("ok", func(*codedeploy.CreateDeploymentGroupInput) {})); got != "no error" {
		t.Errorf("a plain group: %s", got)
	}

	// deployments: a bad AppSpec, an unknown hook, and a second one while the first runs
	for what, spec := range map[string]string{
		"no task definition": "version: 0.0\nResources:\n  - S:\n      Type: AWS::ECS::Service\n      Properties:\n        LoadBalancerInfo:\n          ContainerName: app\n          ContainerPort: 80\n",
		"a lambda resource":  "version: 0.0\nResources:\n  - S:\n      Type: AWS::Lambda::Function\n      Properties:\n        Name: f\n",
		"a hook on Install":  "version: 0.0\nResources:\n  - S:\n      Type: AWS::ECS::Service\n      Properties:\n        TaskDefinition: x\n        LoadBalancerInfo:\n          ContainerName: app\n          ContainerPort: 80\nHooks:\n  - Install: f\n",
	} {
		_, err := e.cd.CreateDeployment(ctx, &codedeploy.CreateDeploymentInput{ApplicationName: aws.String("app"), DeploymentGroupName: aws.String("ok"),
			Revision: &types.RevisionLocation{RevisionType: types.RevisionLocationTypeAppSpecContent, AppSpecContent: &types.AppSpecContent{Content: aws.String(spec)}}})
		if got := code(err); got != "InvalidRevisionException" {
			t.Errorf("%s: %s", what, got)
		}
	}
	spec := "version: 0.0\nResources:\n  - S:\n      Type: AWS::ECS::Service\n      Properties:\n        TaskDefinition: " + e.newTD + "\n        LoadBalancerInfo:\n          ContainerName: app\n          ContainerPort: " + strconv.Itoa(80) + "\n"
	create := func() error {
		_, err := e.cd.CreateDeployment(ctx, &codedeploy.CreateDeploymentInput{ApplicationName: aws.String("app"), DeploymentGroupName: aws.String("ok"),
			Revision: &types.RevisionLocation{RevisionType: types.RevisionLocationTypeAppSpecContent, AppSpecContent: &types.AppSpecContent{Content: aws.String(spec)}}})
		return err
	}
	if err := create(); err != nil {
		t.Fatal(err)
	}
	if got := code(create()); got != "DeploymentLimitExceededException" {
		t.Errorf("second deployment: %s", got)
	}
}

// without a container engine the tasks are metadata only, and the deployment still runs every state and weight
func TestECSNoDocker(t *testing.T) {
	e := setupECS(t, "CodeDeployDefault.ECSAllAtOnce", 0, false, false)
	if got := must(e.ecs.DescribeServices(ctx, &ecs.DescribeServicesInput{Cluster: aws.String("c"), Services: []string{"s"}})).Services[0]; len(got.TaskSets) != 1 {
		t.Fatalf("service %+v", got.TaskSets)
	}
	id := deployECS(t, e, "")
	e.wait(t, id, types.DeploymentStatusSucceeded)
	e.wantWeights(t, "production", e.prod, 0, 100)
	if got := e.events(t, id); got[3] != "AllowTestTraffic:Skipped" {
		t.Fatalf("events %v", got)
	}
}

// with a container engine, requests through the production listener reach the replacement task once traffic has moved
func TestECSDocker(t *testing.T) {
	if testing.Short() || !docker.New(docker.Options{}).Available() {
		t.Skip("needs a running container engine")
	}
	e := setupECS(t, "CodeDeployDefault.ECSAllAtOnce", 0, false, true)
	port := must(e.elb.DescribeListeners(ctx, &elb.DescribeListenersInput{ListenerArns: []string{e.prod}})).Listeners[0].Port
	get := func() string {
		resp, err := http.Get("http://127.0.0.1:" + strconv.Itoa(int(*port)) + "/")
		if err != nil {
			return err.Error()
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return strings.TrimSpace(string(b))
	}
	waitFor(t, "the original task to answer", func() bool { return get() == "v1" })
	id := deployECS(t, e, "")
	e.wait(t, id, types.DeploymentStatusSucceeded)
	waitFor(t, "the replacement task to answer", func() bool { return get() == "v2" })
}
