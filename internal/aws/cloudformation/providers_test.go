package cloudformation_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	cfn "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	elb "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/pipes"
	pipestypes "github.com/aws/aws-sdk-go-v2/service/pipes/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

const netHead = `"Vpc":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"10.9.0.0/16"}},
"S1":{"Type":"AWS::EC2::Subnet","Properties":{"VpcId":{"Ref":"Vpc"},"CidrBlock":"10.9.1.0/24","AvailabilityZone":"us-east-1a"}},
"S2":{"Type":"AWS::EC2::Subnet","Properties":{"VpcId":{"Ref":"Vpc"},"CidrBlock":"10.9.2.0/24","AvailabilityZone":"us-east-1b"}}`

// floci #4879, #1634: ECS cluster, task definition and service are real, and DeleteStack removes them.
func TestECSStack(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent") // no container engine: tasks run as metadata only
	cfg := start(t)
	c, e := cfn.NewFromConfig(cfg), ecs.NewFromConfig(cfg)
	tpl := `{"Resources":{"Cl":{"Type":"AWS::ECS::Cluster","Properties":{"ClusterName":"apps","Tags":[{"Key":"env","Value":"dev"}]}},
"TD":{"Type":"AWS::ECS::TaskDefinition","Properties":{"Family":"web","ContainerDefinitions":[{"Name":"c","Image":"busybox","Memory":"64","Command":["sleep","300"],
 "PortMappings":[{"ContainerPort":80}],"Environment":[{"Name":"A","Value":"1"}],"LogConfiguration":{"LogDriver":"awslogs","Options":{"awslogs-group":"g","awslogs-region":"us-east-1"}}}]}},
"Svc":{"Type":"AWS::ECS::Service","Properties":{"Cluster":{"Ref":"Cl"},"TaskDefinition":{"Ref":"TD"},"DesiredCount":%s}}},
"Outputs":{"Arn":{"Value":{"Fn::GetAtt":["Cl","Arn"]}},"Td":{"Value":{"Ref":"TD"}},"SvcName":{"Value":{"Fn::GetAtt":["Svc","Name"]}}}}`
	st := deploy(t, c, "ecs", fmt.Sprintf(tpl, "1"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)
	cl := must(e.DescribeClusters(ctx, &ecs.DescribeClustersInput{Clusters: []string{"apps"}, Include: []ecstypes.ClusterField{"TAGS"}})).Clusters
	if len(cl) != 1 || aws.ToString(cl[0].ClusterArn) != out["Arn"] || len(cl[0].Tags) != 1 {
		t.Fatalf("cluster %+v", cl)
	}
	td := must(e.DescribeTaskDefinition(ctx, &ecs.DescribeTaskDefinitionInput{TaskDefinition: aws.String(out["Td"])})).TaskDefinition
	if aws.ToString(td.Family) != "web" || len(td.ContainerDefinitions) != 1 || aws.ToInt32(td.ContainerDefinitions[0].Memory) != 64 ||
		td.ContainerDefinitions[0].LogConfiguration.Options["awslogs-group"] != "g" {
		t.Fatalf("task definition %+v", td)
	}
	sv := must(e.DescribeServices(ctx, &ecs.DescribeServicesInput{Cluster: aws.String("apps"), Services: []string{out["SvcName"]}})).Services
	if len(sv) != 1 || sv[0].DesiredCount != 1 || sv[0].RunningCount != 1 {
		t.Fatalf("service %+v", sv)
	}

	// A new desired count is an in-place update; a changed container is a new task definition revision, so the service moves to it.
	wantStatus(t, c, redeploy(t, c, "ecs", fmt.Sprintf(tpl, "2")), types.StackStatusUpdateComplete)
	if sv := must(e.DescribeServices(ctx, &ecs.DescribeServicesInput{Cluster: aws.String("apps"), Services: []string{out["SvcName"]}})).Services; sv[0].DesiredCount != 2 {
		t.Fatalf("service after update %+v", sv)
	}
	st = redeploy(t, c, "ecs", strings.Replace(fmt.Sprintf(tpl, "2"), `"Value":"1"`, `"Value":"2"`, 1))
	wantStatus(t, c, st, types.StackStatusUpdateComplete)
	if outputs(st)["Td"] == out["Td"] {
		t.Fatal("task definition was not replaced")
	}
	if sv := must(e.DescribeServices(ctx, &ecs.DescribeServicesInput{Cluster: aws.String("apps"), Services: []string{out["SvcName"]}})).Services; aws.ToString(sv[0].TaskDefinition) != outputs(st)["Td"] {
		t.Fatalf("service uses %s, want %s", aws.ToString(sv[0].TaskDefinition), outputs(st)["Td"])
	}

	remove(t, c, "ecs")
	if cl := must(e.ListClusters(ctx, &ecs.ListClustersInput{})).ClusterArns; len(cl) != 0 {
		t.Fatalf("clusters after DeleteStack: %v", cl)
	}
	if l := must(e.ListTaskDefinitions(ctx, &ecs.ListTaskDefinitionsInput{Status: "ACTIVE"})).TaskDefinitionArns; len(l) != 0 {
		t.Fatalf("active task definitions after DeleteStack: %v", l)
	}
}

// floci #1913: a VPC, subnets, a load balancer, target group, listener and rule are real, update in place and delete.
func TestLoadBalancerStack(t *testing.T) {
	cfg := start(t)
	c, e := cfn.NewFromConfig(cfg), elb.NewFromConfig(cfg)
	tpl := `{"Resources":{` + netHead + `,
"Lb":{"Type":"AWS::ElasticLoadBalancingV2::LoadBalancer","Properties":{"Subnets":[{"Ref":"S1"},{"Ref":"S2"}],"Tags":[{"Key":"env","Value":"%s"}],"LoadBalancerAttributes":[{"Key":"idle_timeout.timeout_seconds","Value":"%s"}]}},
"Tg":{"Type":"AWS::ElasticLoadBalancingV2::TargetGroup","Properties":{"VpcId":{"Ref":"Vpc"},"Port":80,"Protocol":"HTTP","TargetType":"ip","HealthCheckPath":"%s","Targets":[{"Id":"10.9.1.10","Port":80}]}},
"L":{"Type":"AWS::ElasticLoadBalancingV2::Listener","Properties":{"LoadBalancerArn":{"Ref":"Lb"},"Port":80,"Protocol":"HTTP","DefaultActions":[{"Type":"fixed-response","FixedResponseConfig":{"StatusCode":"404"}}]}},
"R":{"Type":"AWS::ElasticLoadBalancingV2::ListenerRule","Properties":{"ListenerArn":{"Ref":"L"},"Priority":%s,"Conditions":[{"Field":"path-pattern","Values":["/api/*"]}],"Actions":[{"Type":"forward","TargetGroupArn":{"Ref":"Tg"}}]}}},
"Outputs":{"Dns":{"Value":{"Fn::GetAtt":["Lb","DNSName"]}},"Full":{"Value":{"Fn::GetAtt":["Lb","LoadBalancerFullName"]}},"TgFull":{"Value":{"Fn::GetAtt":["Tg","TargetGroupFullName"]}}}}`
	st := deploy(t, c, "elb", fmt.Sprintf(tpl, "a", "60", "/", "10"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)
	lbs := must(e.DescribeLoadBalancers(ctx, &elb.DescribeLoadBalancersInput{})).LoadBalancers
	if len(lbs) != 1 || aws.ToString(lbs[0].DNSName) != out["Dns"] || !strings.HasPrefix(out["Full"], "app/") || len(lbs[0].AvailabilityZones) != 2 {
		t.Fatalf("load balancers %+v outputs %v", lbs, out)
	}
	if !strings.HasPrefix(out["TgFull"], "targetgroup/") {
		t.Fatalf("target group full name %s", out["TgFull"])
	}
	rules := must(e.DescribeRules(ctx, &elb.DescribeRulesInput{ListenerArn: must(e.DescribeListeners(ctx, &elb.DescribeListenersInput{LoadBalancerArn: lbs[0].LoadBalancerArn})).Listeners[0].ListenerArn})).Rules
	if len(rules) != 2 {
		t.Fatalf("rules %+v", rules)
	}
	tgs := must(e.DescribeTargetGroups(ctx, &elb.DescribeTargetGroupsInput{})).TargetGroups
	if len(tgs) != 1 || aws.ToString(tgs[0].HealthCheckPath) != "/" {
		t.Fatalf("target groups %+v", tgs)
	}
	if h := must(e.DescribeTargetHealth(ctx, &elb.DescribeTargetHealthInput{TargetGroupArn: tgs[0].TargetGroupArn})).TargetHealthDescriptions; len(h) != 1 {
		t.Fatalf("targets %+v", h)
	}

	// health check, tags, attributes and rule priority change in place
	wantStatus(t, c, redeploy(t, c, "elb", fmt.Sprintf(tpl, "b", "90", "/health", "20")), types.StackStatusUpdateComplete)
	tgs2 := must(e.DescribeTargetGroups(ctx, &elb.DescribeTargetGroupsInput{})).TargetGroups
	if len(tgs2) != 1 || aws.ToString(tgs2[0].TargetGroupArn) != aws.ToString(tgs[0].TargetGroupArn) || aws.ToString(tgs2[0].HealthCheckPath) != "/health" {
		t.Fatalf("target group after update %+v", tgs2)
	}
	if lbs2 := must(e.DescribeLoadBalancers(ctx, &elb.DescribeLoadBalancersInput{})).LoadBalancers; len(lbs2) != 1 || aws.ToString(lbs2[0].LoadBalancerArn) != aws.ToString(lbs[0].LoadBalancerArn) {
		t.Fatalf("load balancer replaced: %+v", lbs2)
	}
	at := must(e.DescribeLoadBalancerAttributes(ctx, &elb.DescribeLoadBalancerAttributesInput{LoadBalancerArn: lbs[0].LoadBalancerArn})).Attributes
	for _, a := range at {
		if aws.ToString(a.Key) == "idle_timeout.timeout_seconds" && aws.ToString(a.Value) != "90" {
			t.Fatalf("idle timeout %s", aws.ToString(a.Value))
		}
	}
	remove(t, c, "elb")
	if lbs := must(e.DescribeLoadBalancers(ctx, &elb.DescribeLoadBalancersInput{})).LoadBalancers; len(lbs) != 0 {
		t.Fatalf("load balancers after DeleteStack: %+v", lbs)
	}
}

// floci #2005: an Auto Scaling group with a mixed instances policy finds its launch template and version.
func TestAutoScalingMixedInstances(t *testing.T) {
	cfg := start(t)
	c, a := cfn.NewFromConfig(cfg), autoscaling.NewFromConfig(cfg)
	tpl := `{"Resources":{"LT":{"Type":"AWS::EC2::LaunchTemplate","Properties":{"LaunchTemplateData":{"ImageId":"ami-12345678","InstanceType":"t3.micro"}}},
"ASG":{"Type":"AWS::AutoScaling::AutoScalingGroup","Properties":{"MinSize":"0","MaxSize":"%s","AvailabilityZones":["us-east-1a"],"Tags":[{"Key":"env","Value":"dev","PropagateAtLaunch":true}],
"MixedInstancesPolicy":{"LaunchTemplate":{"LaunchTemplateSpecification":{"LaunchTemplateId":{"Ref":"LT"},"Version":{"Fn::GetAtt":["LT","LatestVersionNumber"]}},"Overrides":[{"InstanceType":"t3.micro"},{"InstanceType":"t3.small"}]}}}}}}`
	wantStatus(t, c, deploy(t, c, "asg", fmt.Sprintf(tpl, "1")), types.StackStatusCreateComplete)
	g := must(a.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{})).AutoScalingGroups
	if len(g) != 1 || g[0].MixedInstancesPolicy == nil || len(g[0].MixedInstancesPolicy.LaunchTemplate.Overrides) != 2 || len(g[0].Tags) != 1 {
		t.Fatalf("groups %+v", g)
	}
	wantStatus(t, c, redeploy(t, c, "asg", fmt.Sprintf(tpl, "3")), types.StackStatusUpdateComplete)
	if g := must(a.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{})).AutoScalingGroups; len(g) != 1 || aws.ToInt32(g[0].MaxSize) != 3 {
		t.Fatalf("group after update %+v", g)
	}
	remove(t, c, "asg")
	if g := must(a.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{})).AutoScalingGroups; len(g) != 0 {
		t.Fatalf("groups after DeleteStack: %+v", g)
	}
}

// floci #3500: a DB cluster is real; fixed settings replace it, others change in place.
func TestDBCluster(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent") // no container engine: the cluster is metadata only
	cfg := start(t)
	c, r := cfn.NewFromConfig(cfg), rds.NewFromConfig(cfg)
	tpl := `{"Resources":{"Cl":{"Type":"AWS::RDS::DBCluster","Properties":{"Engine":"aurora-postgresql","MasterUsername":"admin","MasterUserPassword":"password123",
"StorageEncrypted":%s,"BackupRetentionPeriod":%s,"Tags":[{"Key":"env","Value":"%s"}]}}},"Outputs":{"Host":{"Value":{"Fn::GetAtt":["Cl","Endpoint.Address"]}},"Id":{"Value":{"Ref":"Cl"}}}}`
	st := deploy(t, c, "rds", fmt.Sprintf(tpl, "false", "1", "a"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)
	cl := must(r.DescribeDBClusters(ctx, &rds.DescribeDBClustersInput{})).DBClusters
	if len(cl) != 1 || aws.ToString(cl[0].DBClusterIdentifier) != out["Id"] || aws.ToString(cl[0].Endpoint) != out["Host"] || out["Host"] == "" {
		t.Fatalf("clusters %+v outputs %v", cl, out)
	}
	wantStatus(t, c, redeploy(t, c, "rds", fmt.Sprintf(tpl, "false", "7", "b")), types.StackStatusUpdateComplete)
	cl = must(r.DescribeDBClusters(ctx, &rds.DescribeDBClustersInput{})).DBClusters
	if len(cl) != 1 || aws.ToString(cl[0].DBClusterIdentifier) != out["Id"] || aws.ToInt32(cl[0].BackupRetentionPeriod) != 7 || len(cl[0].TagList) != 1 || aws.ToString(cl[0].TagList[0].Value) != "b" {
		t.Fatalf("cluster after in place update %+v", cl)
	}
	st = redeploy(t, c, "rds", fmt.Sprintf(tpl, "true", "7", "b"))
	wantStatus(t, c, st, types.StackStatusUpdateComplete)
	cl = must(r.DescribeDBClusters(ctx, &rds.DescribeDBClustersInput{})).DBClusters
	if len(cl) != 1 || aws.ToString(cl[0].DBClusterIdentifier) == out["Id"] || !aws.ToBool(cl[0].StorageEncrypted) {
		t.Fatalf("cluster after replacement %+v", cl)
	}
	remove(t, c, "rds")
	if cl := must(r.DescribeDBClusters(ctx, &rds.DescribeDBClustersInput{})).DBClusters; len(cl) != 0 {
		t.Fatalf("clusters after DeleteStack: %+v", cl)
	}
}

// floci #2966, #3209: a pipe updates in place, and a failed update rolls it back.
func TestPipe(t *testing.T) {
	cfg := start(t)
	c, p := cfn.NewFromConfig(cfg), pipes.NewFromConfig(cfg)
	tpl := func(desc string, bad bool) string {
		extra := ""
		if bad {
			extra = `,"Bad":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"not-fifo-name","FifoQueue":true}}`
		}
		return `{"Resources":{"R":{"Type":"AWS::IAM::Role","Properties":{"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"pipes.amazonaws.com"},"Action":"sts:AssumeRole"}]}}},
"Src":{"Type":"AWS::SQS::Queue"},"Dst":{"Type":"AWS::SQS::Queue"},
"Pipe":{"Type":"AWS::Pipes::Pipe","Properties":{"Name":"cfnpipe","RoleArn":{"Fn::GetAtt":["R","Arn"]},"Source":{"Fn::GetAtt":["Src","Arn"]},"Target":{"Fn::GetAtt":["Dst","Arn"]},"Description":"` + desc + `"}}` + extra + `},
"Outputs":{"Arn":{"Value":{"Fn::GetAtt":["Pipe","Arn"]}}}}`
	}
	wantStatus(t, c, deploy(t, c, "pipe", tpl("v1", false)), types.StackStatusCreateComplete)
	d := must(p.DescribePipe(ctx, &pipes.DescribePipeInput{Name: aws.String("cfnpipe")}))
	if aws.ToString(d.Description) != "v1" || d.CurrentState != pipestypes.PipeStateRunning {
		t.Fatalf("pipe %+v", d)
	}
	wantStatus(t, c, redeploy(t, c, "pipe", tpl("v2", false)), types.StackStatusUpdateComplete)
	if d := must(p.DescribePipe(ctx, &pipes.DescribePipeInput{Name: aws.String("cfnpipe")})); aws.ToString(d.Description) != "v2" {
		t.Fatalf("pipe after update %+v", d)
	}
	wantStatus(t, c, redeploy(t, c, "pipe", tpl("v3", true)), types.StackStatusUpdateRollbackComplete)
	if d := must(p.DescribePipe(ctx, &pipes.DescribePipeInput{Name: aws.String("cfnpipe")})); aws.ToString(d.Description) != "v2" {
		t.Fatalf("pipe after rollback %+v", d)
	}
	remove(t, c, "pipe")
	if l := must(p.ListPipes(ctx, &pipes.ListPipesInput{})).Pipes; len(l) != 0 {
		t.Fatalf("pipes after DeleteStack: %+v", l)
	}
}

// cognitoCall sends a signed Cognito JSON request (the Cognito SDK is not a dependency of this module).
func cognitoCall(t *testing.T, cfg aws.Config, op string, in map[string]any) map[string]any {
	t.Helper()
	b, _ := json.Marshal(in)
	req, _ := http.NewRequest("POST", aws.ToString(cfg.BaseEndpoint)+"/", bytes.NewReader(b))
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	awstest.Sign(t, req, "cognito-idp")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode >= 300 {
		t.Fatalf("%s: %d %v", op, resp.StatusCode, out)
	}
	return out
}

// floci #4836: a user pool group is real, updates in place and a rename replaces it.
func TestUserPoolGroup(t *testing.T) {
	cfg := start(t)
	c := cfn.NewFromConfig(cfg)
	tpl := `{"Resources":{"UserPool":{"Type":"AWS::Cognito::UserPool","Properties":{"UserPoolName":"p"}},
"G":{"Type":"AWS::Cognito::UserPoolGroup","Properties":{"UserPoolId":{"Ref":"UserPool"},"GroupName":"%s","Description":"%s","Precedence":%s}}},"Outputs":{"Pool":{"Value":{"Ref":"UserPool"}}}}`
	groups := func(pool string) []any {
		return cognitoCall(t, cfg, "ListGroups", map[string]any{"UserPoolId": pool})["Groups"].([]any)
	}
	st := deploy(t, c, "grp", fmt.Sprintf(tpl, "one", "first", "3"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	pool := outputs(st)["Pool"]
	if g := groups(pool); len(g) != 1 || g[0].(map[string]any)["GroupName"] != "one" || g[0].(map[string]any)["Precedence"] != float64(3) {
		t.Fatalf("groups %v", g)
	}
	wantStatus(t, c, redeploy(t, c, "grp", fmt.Sprintf(tpl, "one", "changed", "5")), types.StackStatusUpdateComplete)
	if g := groups(pool); len(g) != 1 || g[0].(map[string]any)["Description"] != "changed" {
		t.Fatalf("groups after update %v", g)
	}
	wantStatus(t, c, redeploy(t, c, "grp", fmt.Sprintf(tpl, "two", "changed", "5")), types.StackStatusUpdateComplete)
	if g := groups(pool); len(g) != 1 || g[0].(map[string]any)["GroupName"] != "two" {
		t.Fatalf("groups after rename %v", g)
	}
	remove(t, c, "grp")
}

// floci #4700, #788, #1758: REST API TOKEN and COGNITO_USER_POOLS authorizers, methods that use them, and HTTP API JWT authorizers.
func TestAuthorizers(t *testing.T) {
	cfg := start(t)
	c, a, v2 := cfn.NewFromConfig(cfg), apigateway.NewFromConfig(cfg), apigatewayv2.NewFromConfig(cfg)
	tpl := `{"Resources":{"Api":{"Type":"AWS::ApiGateway::RestApi","Properties":{"Name":"auth"}},
"Res":{"Type":"AWS::ApiGateway::Resource","Properties":{"RestApiId":{"Ref":"Api"},"ParentId":{"Fn::GetAtt":["Api","RootResourceId"]},"PathPart":"x"}},
"Tok":{"Type":"AWS::ApiGateway::Authorizer","Properties":{"RestApiId":{"Ref":"Api"},"Name":"tok","Type":"TOKEN","IdentitySource":"method.request.header.Authorization","AuthorizerResultTtlInSeconds":%s,
 "AuthorizerUri":"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:auth/invocations"}},
"Cog":{"Type":"AWS::ApiGateway::Authorizer","Properties":{"RestApiId":{"Ref":"Api"},"Name":"cog","Type":"COGNITO_USER_POOLS","IdentitySource":"method.request.header.Authorization",
 "ProviderARNs":%s}},
"M":{"Type":"AWS::ApiGateway::Method","Properties":{"RestApiId":{"Ref":"Api"},"ResourceId":{"Ref":"Res"},"HttpMethod":"GET","AuthorizationType":"CUSTOM","AuthorizerId":{"Ref":"Tok"},"Integration":{"Type":"MOCK"}}},
"M2":{"Type":"AWS::ApiGateway::Method","Properties":{"RestApiId":{"Ref":"Api"},"ResourceId":{"Ref":"Res"},"HttpMethod":"POST","AuthorizationType":"COGNITO_USER_POOLS","AuthorizerId":{"Ref":"Cog"},"Integration":{"Type":"MOCK"}}},
"H":{"Type":"AWS::ApiGatewayV2::Api","Properties":{"Name":"http","ProtocolType":"HTTP"}},
"JwtAuth":{"Type":"AWS::ApiGatewayV2::Authorizer","Properties":{"ApiId":{"Ref":"H"},"Name":"jwt","AuthorizerType":"JWT","IdentitySource":["$request.header.Authorization"],"JwtConfiguration":{"Audience":["a"],"Issuer":"https://issuer.example"}}},
"Int":{"Type":"AWS::ApiGatewayV2::Integration","Properties":{"ApiId":{"Ref":"H"},"IntegrationType":"HTTP_PROXY","IntegrationUri":"https://example.com","IntegrationMethod":"GET","PayloadFormatVersion":"1.0"}},
"Route":{"Type":"AWS::ApiGatewayV2::Route","Properties":{"ApiId":{"Ref":"H"},"RouteKey":"GET /x","AuthorizationType":"JWT","AuthorizerId":{"Ref":"JwtAuth"},"Target":{"Fn::Sub":"integrations/${Int}"}}}},
"Outputs":{"Api":{"Value":{"Ref":"Api"}},"H":{"Value":{"Ref":"H"}},"Tok":{"Value":{"Ref":"Tok"}}}}`
	pool := `["arn:aws:cognito-idp:us-east-1:000000000000:userpool/us-east-1_abc"]`
	st := deploy(t, c, "auth", fmt.Sprintf(tpl, "300", pool))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)
	l := must(a.GetAuthorizers(ctx, &apigateway.GetAuthorizersInput{RestApiId: aws.String(out["Api"])})).Items
	if len(l) != 2 {
		t.Fatalf("rest authorizers %+v", l)
	}
	for _, x := range l {
		switch aws.ToString(x.Name) {
		case "cog":
			if len(x.ProviderARNs) != 1 || x.Type != apigwtypes.AuthorizerTypeCognitoUserPools {
				t.Fatalf("cognito authorizer %+v", x)
			}
		case "tok":
			if x.Type != apigwtypes.AuthorizerTypeToken || aws.ToInt32(x.AuthorizerResultTtlInSeconds) != 300 || aws.ToString(x.Id) != out["Tok"] {
				t.Fatalf("token authorizer %+v", x)
			}
		}
	}
	res := must(a.GetResources(ctx, &apigateway.GetResourcesInput{RestApiId: aws.String(out["Api"])})).Items
	var m *apigateway.GetMethodOutput
	for _, r := range res {
		if aws.ToString(r.Path) == "/x" {
			m = must(a.GetMethod(ctx, &apigateway.GetMethodInput{RestApiId: aws.String(out["Api"]), ResourceId: r.Id, HttpMethod: aws.String("GET")}))
		}
	}
	if m == nil || aws.ToString(m.AuthorizerId) != out["Tok"] || aws.ToString(m.AuthorizationType) != "CUSTOM" {
		t.Fatalf("method %+v", m)
	}
	if h := must(v2.GetAuthorizers(ctx, &apigatewayv2.GetAuthorizersInput{ApiId: aws.String(out["H"])})).Items; len(h) != 1 || h[0].JwtConfiguration == nil {
		t.Fatalf("http authorizers %+v", h)
	}

	// a changed TTL and a second pool are in place changes
	two := `["arn:aws:cognito-idp:us-east-1:000000000000:userpool/us-east-1_abc","arn:aws:cognito-idp:us-east-1:000000000000:userpool/us-east-1_def"]`
	wantStatus(t, c, redeploy(t, c, "auth", fmt.Sprintf(tpl, "60", two)), types.StackStatusUpdateComplete)
	for _, x := range must(a.GetAuthorizers(ctx, &apigateway.GetAuthorizersInput{RestApiId: aws.String(out["Api"])})).Items {
		if aws.ToString(x.Name) == "cog" && len(x.ProviderARNs) != 2 || aws.ToString(x.Name) == "tok" && aws.ToInt32(x.AuthorizerResultTtlInSeconds) != 60 {
			t.Fatalf("authorizer after update %+v", x)
		}
	}
	remove(t, c, "auth")
}

// floci #2967: a resource type pogi cannot create is refused with a clear error, never turned into a placeholder.
func TestUnknownTypeFails(t *testing.T) {
	cfg := start(t)
	c := cfn.NewFromConfig(cfg)
	tpl := `{"Resources":{"Q":{"Type":"AWS::SQS::Queue"},"P":{"Type":"AWS::NotAReal::Thing"},"B":{"Type":"AWS::Fake::Other"}}}`
	want := "pogi does not support AWS::Fake::Other, AWS::NotAReal::Thing yet"
	_, err := c.ValidateTemplate(ctx, &cfn.ValidateTemplateInput{TemplateBody: aws.String(tpl)})
	if err == nil || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "Unrecognized resource types") {
		t.Errorf("ValidateTemplate: %v", err)
	}
	if _, err = c.CreateStack(ctx, &cfn.CreateStackInput{StackName: aws.String("nr"), TemplateBody: aws.String(tpl)}); err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("CreateStack: %v", err)
	}
	if _, err = c.CreateChangeSet(ctx, &cfn.CreateChangeSetInput{StackName: aws.String("nr2"), ChangeSetName: aws.String("x"), ChangeSetType: types.ChangeSetTypeCreate, TemplateBody: aws.String(tpl)}); err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("CreateChangeSet: %v", err)
	}
	if l := must(c.ListStacks(ctx, &cfn.ListStacksInput{})).StackSummaries; len(l) != 0 {
		t.Errorf("a refused template left stacks: %+v", l)
	}
}

// floci #1913, #1634: DeleteStack still works on ECS and load balancer resources after a restart with saved state.
func TestDeleteAfterRestart(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	conf := pogi.Config{DataDir: t.TempDir(), Storage: "persistent"}
	first := pogi.Start(t, conf)
	c := cfn.NewFromConfig(awstest.Config(t, first.URL))
	tpl := `{"Resources":{` + netHead + `,
"Lb":{"Type":"AWS::ElasticLoadBalancingV2::LoadBalancer","Properties":{"Subnets":[{"Ref":"S1"},{"Ref":"S2"}]}},
"Tg":{"Type":"AWS::ElasticLoadBalancingV2::TargetGroup","Properties":{"VpcId":{"Ref":"Vpc"},"Port":80,"Protocol":"HTTP"}},
"L":{"Type":"AWS::ElasticLoadBalancingV2::Listener","Properties":{"LoadBalancerArn":{"Ref":"Lb"},"Port":80,"Protocol":"HTTP","DefaultActions":[{"Type":"forward","TargetGroupArn":{"Ref":"Tg"}}]}},
"Cl":{"Type":"AWS::ECS::Cluster"},
"TD":{"Type":"AWS::ECS::TaskDefinition","Properties":{"Family":"f","ContainerDefinitions":[{"Name":"c","Image":"busybox","Memory":64}]}},
"Svc":{"Type":"AWS::ECS::Service","Properties":{"Cluster":{"Ref":"Cl"},"TaskDefinition":{"Ref":"TD"},"DesiredCount":1}}}}`
	wantStatus(t, c, deploy(t, c, "keep", tpl), types.StackStatusCreateComplete)
	// persistent mode saves every write: a second server on the same data dir starts with this state

	second := pogi.Start(t, conf)
	c = cfn.NewFromConfig(awstest.Config(t, second.URL))
	remove(t, c, "keep")
	if lbs := must(elb.NewFromConfig(awstest.Config(t, second.URL)).DescribeLoadBalancers(ctx, &elb.DescribeLoadBalancersInput{})).LoadBalancers; len(lbs) != 0 {
		t.Fatalf("load balancers after DeleteStack: %+v", lbs)
	}
	if cl := must(ecs.NewFromConfig(awstest.Config(t, second.URL)).ListClusters(ctx, &ecs.ListClustersInput{})).ClusterArns; len(cl) != 0 {
		t.Fatalf("clusters after DeleteStack: %v", cl)
	}
}
