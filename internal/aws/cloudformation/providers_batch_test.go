package cloudformation_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/aws/aws-sdk-go-v2/service/appsync"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/backup"
	cfn "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/codebuild"
	"github.com/aws/aws-sdk-go-v2/service/codedeploy"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2"
	"github.com/aws/aws-sdk-go-v2/service/memorydb"
	"github.com/aws/aws-sdk-go-v2/service/mq"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"

	"github.com/nrndev/pogi/internal/awstest"
)

const statesTrust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"states.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

// Logs, Secrets Manager, IAM, EventBridge, Step Functions, CloudWatch and Kinesis resources beyond the first set.
func TestMoreTypesA(t *testing.T) {
	cfg := start(t)
	c := cfn.NewFromConfig(cfg)
	tpl := `{"Resources":{
"Lg":{"Type":"AWS::Logs::LogGroup"},
"Ls":{"Type":"AWS::Logs::LogStream","Properties":{"LogGroupName":{"Ref":"Lg"},"LogStreamName":"s1"}},
"Mf":{"Type":"AWS::Logs::MetricFilter","Properties":{"LogGroupName":{"Ref":"Lg"},"FilterPattern":"ERROR","MetricTransformations":[{"MetricName":"errors","MetricNamespace":"app","MetricValue":"1"}]}},
"Secret":{"Type":"AWS::SecretsManager::Secret","Properties":{"Name":"s"}},
"SecretPolicy":{"Type":"AWS::SecretsManager::ResourcePolicy","Properties":{"SecretId":{"Ref":"Secret"},"ResourcePolicy":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::000000000000:root"},"Action":"secretsmanager:GetSecretValue","Resource":"*"}]}}},
"User":{"Type":"AWS::IAM::User"},"Group":{"Type":"AWS::IAM::Group"},
"Member":{"Type":"AWS::IAM::UserToGroupAddition","Properties":{"GroupName":{"Ref":"Group"},"Users":[{"Ref":"User"}]}},
"Oidc":{"Type":"AWS::IAM::OIDCProvider","Properties":{"Url":"https://issuer.example.com","ClientIdList":["a"],"ThumbprintList":["9e99a48a9960b14926bb7f3b02e22da2b0ab7280"]}},
"Bus":{"Type":"AWS::Events::EventBus","Properties":{"Name":"bus"}},
"Arch":{"Type":"AWS::Events::Archive","Properties":{"ArchiveName":"arch","SourceArn":{"Fn::GetAtt":["Bus","Arn"]},"RetentionDays":3,"EventPattern":{"source":["app"]}}},
"BusPolicy":{"Type":"AWS::Events::EventBusPolicy","Properties":{"EventBusName":{"Ref":"Bus"},"StatementId":"allow","Action":"events:PutEvents","Principal":"111111111111"}},
"Role":{"Type":"AWS::IAM::Role","Properties":{"AssumeRolePolicyDocument":` + statesTrust + `}},
"Sm":{"Type":"AWS::StepFunctions::StateMachine","Properties":{"RoleArn":{"Fn::GetAtt":["Role","Arn"]},"DefinitionString":"{\"StartAt\":\"P\",\"States\":{\"P\":{\"Type\":\"Pass\",\"End\":true}}}"}},
"Ver":{"Type":"AWS::StepFunctions::StateMachineVersion","Properties":{"StateMachineArn":{"Ref":"Sm"},"Description":"v1"}},
"Alias":{"Type":"AWS::StepFunctions::StateMachineAlias","Properties":{"Name":"live","DeploymentPreference":{"StateMachineVersionArn":{"Ref":"Ver"},"Type":"ALL_AT_ONCE"}}},
"Act":{"Type":"AWS::StepFunctions::Activity","Properties":{"Name":"act"}},
"Dash":{"Type":"AWS::CloudWatch::Dashboard","Properties":{"DashboardName":"dash","DashboardBody":"{\"widgets\":[]}"}},
"Al1":{"Type":"AWS::CloudWatch::Alarm","Properties":{"AlarmName":"a1","MetricName":"m","Namespace":"n","Statistic":"Sum","Period":60,"EvaluationPeriods":1,"Threshold":1,"ComparisonOperator":"GreaterThanThreshold"}},
"Comp":{"Type":"AWS::CloudWatch::CompositeAlarm","Properties":{"AlarmName":"comp","AlarmRule":"ALARM(a1)"}},
"Ks":{"Type":"AWS::Kinesis::Stream","Properties":{"ShardCount":1}},
"Consumer":{"Type":"AWS::Kinesis::StreamConsumer","Properties":{"ConsumerName":"fan","StreamARN":{"Fn::GetAtt":["Ks","Arn"]}}}},
"Outputs":{"Alias":{"Value":{"Ref":"Alias"}},"Consumer":{"Value":{"Fn::GetAtt":["Consumer","ConsumerARN"]}}}}`
	st := deploy(t, c, "a", tpl)
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)

	lg := cloudwatchlogs.NewFromConfig(cfg)
	grp := aws.ToString(must(c.DescribeStackResource(ctx, &cfn.DescribeStackResourceInput{StackName: aws.String("a"), LogicalResourceId: aws.String("Lg")})).StackResourceDetail.PhysicalResourceId)
	if s := must(lg.DescribeLogStreams(ctx, &cloudwatchlogs.DescribeLogStreamsInput{LogGroupName: aws.String(grp)})).LogStreams; len(s) != 1 {
		t.Fatalf("log streams %+v", s)
	}
	if f := must(lg.DescribeMetricFilters(ctx, &cloudwatchlogs.DescribeMetricFiltersInput{LogGroupName: aws.String(grp)})).MetricFilters; len(f) != 1 {
		t.Fatalf("metric filters %+v", f)
	}
	if p := must(secretsmanager.NewFromConfig(cfg).GetResourcePolicy(ctx, &secretsmanager.GetResourcePolicyInput{SecretId: aws.String("s")})).ResourcePolicy; aws.ToString(p) == "" {
		t.Fatal("secret has no resource policy")
	}
	im := iam.NewFromConfig(cfg)
	if g := must(im.GetGroup(ctx, &iam.GetGroupInput{GroupName: aws.String(physical(t, c, "a", "Group"))})).Users; len(g) != 1 {
		t.Fatalf("group members %+v", g)
	}
	if o := must(im.ListOpenIDConnectProviders(ctx, &iam.ListOpenIDConnectProvidersInput{})).OpenIDConnectProviderList; len(o) != 1 {
		t.Fatalf("oidc providers %+v", o)
	}
	eb := eventbridge.NewFromConfig(cfg)
	if a := must(eb.ListArchives(ctx, &eventbridge.ListArchivesInput{})).Archives; len(a) != 1 || aws.ToInt32(a[0].RetentionDays) != 3 {
		t.Fatalf("archives %+v", a)
	}
	if b := must(eb.DescribeEventBus(ctx, &eventbridge.DescribeEventBusInput{Name: aws.String("bus")})).Policy; aws.ToString(b) == "" {
		t.Fatal("event bus has no policy")
	}
	sf := sfn.NewFromConfig(cfg)
	if a := must(sf.DescribeStateMachineAlias(ctx, &sfn.DescribeStateMachineAliasInput{StateMachineAliasArn: aws.String(out["Alias"])})); len(a.RoutingConfiguration) != 1 {
		t.Fatalf("alias %+v", a)
	}
	if a := must(sf.ListActivities(ctx, &sfn.ListActivitiesInput{})).Activities; len(a) != 1 {
		t.Fatalf("activities %+v", a)
	}
	if d := must(cloudwatch.NewFromConfig(cfg).DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{AlarmTypes: []cwtypes.AlarmType{"CompositeAlarm", "MetricAlarm"}})); len(d.CompositeAlarms) != 1 || len(d.MetricAlarms) != 1 {
		t.Fatalf("alarms %+v", d)
	}
	if d := must(cloudwatch.NewFromConfig(cfg).ListDashboards(ctx, &cloudwatch.ListDashboardsInput{})).DashboardEntries; len(d) != 1 {
		t.Fatalf("dashboards %+v", d)
	}
	if k := must(kinesis.NewFromConfig(cfg).ListStreamConsumers(ctx, &kinesis.ListStreamConsumersInput{StreamARN: aws.String(physicalArn(t, c, "a", "Ks"))})).Consumers; len(k) != 1 || aws.ToString(k[0].ConsumerARN) != out["Consumer"] {
		t.Fatalf("consumers %+v", k)
	}
	remove(t, c, "a")
}

func physical(t *testing.T, c *cfn.Client, stack, logical string) string {
	t.Helper()
	return aws.ToString(must(c.DescribeStackResource(ctx, &cfn.DescribeStackResourceInput{StackName: aws.String(stack), LogicalResourceId: aws.String(logical)})).StackResourceDetail.PhysicalResourceId)
}

// physicalArn is the stream ARN of a Kinesis stack resource named by its stream name.
func physicalArn(t *testing.T, c *cfn.Client, stack, logical string) string {
	t.Helper()
	return "arn:aws:kinesis:us-east-1:000000000000:stream/" + physical(t, c, stack, logical)
}

// Auto Scaling policies, Application Auto Scaling, ECS capacity providers, API Gateway keys, usage plans and domains.
func TestMoreTypesB(t *testing.T) {
	cfg := start(t)
	c := cfn.NewFromConfig(cfg)
	tpl := `{"Resources":{` + netHead + `,
"Lc":{"Type":"AWS::AutoScaling::LaunchConfiguration","Properties":{"ImageId":"ami-12345678","InstanceType":"t3.micro"}},
"Asg":{"Type":"AWS::AutoScaling::AutoScalingGroup","Properties":{"MinSize":"0","MaxSize":"3","LaunchConfigurationName":{"Ref":"Lc"},"VPCZoneIdentifier":[{"Ref":"S1"}]}},
"Up":{"Type":"AWS::AutoScaling::ScalingPolicy","Properties":{"AutoScalingGroupName":{"Ref":"Asg"},"PolicyType":"SimpleScaling","AdjustmentType":"ChangeInCapacity","ScalingAdjustment":1}},
"Cpu":{"Type":"AWS::AutoScaling::ScalingPolicy","Properties":{"AutoScalingGroupName":{"Ref":"Asg"},"PolicyType":"TargetTrackingScaling","TargetTrackingConfiguration":{"PredefinedMetricSpecification":{"PredefinedMetricType":"ASGAverageCPUUtilization"},"TargetValue":50}}},
"Sched":{"Type":"AWS::AutoScaling::ScheduledAction","Properties":{"AutoScalingGroupName":{"Ref":"Asg"},"MinSize":1,"Recurrence":"0 8 * * *"}},
"Hook":{"Type":"AWS::AutoScaling::LifecycleHook","Properties":{"AutoScalingGroupName":{"Ref":"Asg"},"LifecycleTransition":"autoscaling:EC2_INSTANCE_TERMINATING","HeartbeatTimeout":60}},
"Table":{"Type":"AWS::DynamoDB::Table","Properties":{"BillingMode":"PROVISIONED","ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5},"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}},
"Target":{"Type":"AWS::ApplicationAutoScaling::ScalableTarget","Properties":{"ServiceNamespace":"dynamodb","ResourceId":{"Fn::Sub":"table/${Table}"},"ScalableDimension":"dynamodb:table:ReadCapacityUnits","MinCapacity":5,"MaxCapacity":50}},
"Track":{"Type":"AWS::ApplicationAutoScaling::ScalingPolicy","Properties":{"PolicyName":"track","PolicyType":"TargetTrackingScaling","ScalingTargetId":{"Ref":"Target"},"TargetTrackingScalingPolicyConfiguration":{"TargetValue":70,"PredefinedMetricSpecification":{"PredefinedMetricType":"DynamoDBReadCapacityUtilization"}}}},
"Cl":{"Type":"AWS::ECS::Cluster"},
"Cp":{"Type":"AWS::ECS::CapacityProvider","Properties":{"AutoScalingGroupProvider":{"AutoScalingGroupArn":{"Fn::Sub":"arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:x:autoScalingGroupName/${Asg}"},"ManagedScaling":{"Status":"DISABLED"}}}},
"Assoc":{"Type":"AWS::ECS::ClusterCapacityProviderAssociations","Properties":{"Cluster":{"Ref":"Cl"},"CapacityProviders":["FARGATE",{"Ref":"Cp"}],"DefaultCapacityProviderStrategy":[{"CapacityProvider":"FARGATE","Weight":1}]}},
"Cert":{"Type":"AWS::CertificateManager::Certificate","Properties":{"DomainName":"app.example.com"}},
"Lb":{"Type":"AWS::ElasticLoadBalancingV2::LoadBalancer","Properties":{"Subnets":[{"Ref":"S1"},{"Ref":"S2"}]}},
"L":{"Type":"AWS::ElasticLoadBalancingV2::Listener","Properties":{"LoadBalancerArn":{"Ref":"Lb"},"Port":443,"Protocol":"HTTPS","Certificates":[{"CertificateArn":{"Ref":"Cert"}}],"DefaultActions":[{"Type":"fixed-response","FixedResponseConfig":{"StatusCode":"200"}}]}},
"Cert2":{"Type":"AWS::CertificateManager::Certificate","Properties":{"DomainName":"two.example.com"}},
"LCert":{"Type":"AWS::ElasticLoadBalancingV2::ListenerCertificate","Properties":{"ListenerArn":{"Ref":"L"},"Certificates":[{"CertificateArn":{"Ref":"Cert2"}}]}},
"Fn":{"Type":"AWS::Lambda::Function","Properties":{"Runtime":"python3.12","Handler":"i.h","Role":"arn:aws:iam::000000000000:role/r","Code":{"ZipFile":"def h(e,c): return 1"}}},
"Inv":{"Type":"AWS::Lambda::EventInvokeConfig","Properties":{"FunctionName":{"Ref":"Fn"},"Qualifier":"$LATEST","MaximumRetryAttempts":1}},
"Api":{"Type":"AWS::ApiGateway::RestApi","Properties":{"Name":"keys"}},
"M":{"Type":"AWS::ApiGateway::Method","Properties":{"RestApiId":{"Ref":"Api"},"ResourceId":{"Fn::GetAtt":["Api","RootResourceId"]},"HttpMethod":"GET","AuthorizationType":"NONE","ApiKeyRequired":true,"Integration":{"Type":"MOCK"}}},
"Dep":{"Type":"AWS::ApiGateway::Deployment","DependsOn":"M","Properties":{"RestApiId":{"Ref":"Api"}}},
"Stage":{"Type":"AWS::ApiGateway::Stage","Properties":{"RestApiId":{"Ref":"Api"},"DeploymentId":{"Ref":"Dep"},"StageName":"prod"}},
"Key":{"Type":"AWS::ApiGateway::ApiKey","Properties":{"Name":"k","Enabled":true}},
"Plan":{"Type":"AWS::ApiGateway::UsagePlan","Properties":{"UsagePlanName":"plan","ApiStages":[{"ApiId":{"Ref":"Api"},"Stage":{"Ref":"Stage"}}],"Throttle":{"BurstLimit":10,"RateLimit":5},"Quota":{"Limit":100,"Period":"DAY"}}},
"PlanKey":{"Type":"AWS::ApiGateway::UsagePlanKey","Properties":{"KeyId":{"Ref":"Key"},"KeyType":"API_KEY","UsagePlanId":{"Ref":"Plan"}}},
"Dom":{"Type":"AWS::ApiGateway::DomainName","Properties":{"DomainName":"api.example.com","RegionalCertificateArn":{"Ref":"Cert"},"EndpointConfiguration":{"Types":["REGIONAL"]}}},
"Map":{"Type":"AWS::ApiGateway::BasePathMapping","DependsOn":"Stage","Properties":{"DomainName":{"Ref":"Dom"},"RestApiId":{"Ref":"Api"},"Stage":"prod","BasePath":"v1"}},
"H":{"Type":"AWS::ApiGatewayV2::Api","Properties":{"Name":"http","ProtocolType":"HTTP"}},
"HStage":{"Type":"AWS::ApiGatewayV2::Stage","Properties":{"ApiId":{"Ref":"H"},"StageName":"live","AutoDeploy":true}},
"HDom":{"Type":"AWS::ApiGatewayV2::DomainName","Properties":{"DomainName":"http.example.com","DomainNameConfigurations":[{"CertificateArn":{"Ref":"Cert"},"EndpointType":"REGIONAL"}]}},
"HMap":{"Type":"AWS::ApiGatewayV2::ApiMapping","Properties":{"ApiId":{"Ref":"H"},"DomainName":{"Ref":"HDom"},"Stage":{"Ref":"HStage"}}}},
"Outputs":{"Domain":{"Value":{"Fn::GetAtt":["Dom","RegionalDomainName"]}},"Plan":{"Value":{"Ref":"Plan"}},"Policy":{"Value":{"Ref":"Cpu"}}}}`
	st := deploy(t, c, "b", tpl)
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)
	a := autoscaling.NewFromConfig(cfg)
	if p := must(a.DescribePolicies(ctx, &autoscaling.DescribePoliciesInput{})).ScalingPolicies; len(p) != 2 {
		t.Fatalf("scaling policies %+v", p)
	}
	if s := must(a.DescribeScheduledActions(ctx, &autoscaling.DescribeScheduledActionsInput{})).ScheduledUpdateGroupActions; len(s) != 1 {
		t.Fatalf("scheduled actions %+v", s)
	}
	if h := must(a.DescribeLifecycleHooks(ctx, &autoscaling.DescribeLifecycleHooksInput{AutoScalingGroupName: aws.String(physical(t, c, "b", "Asg"))})).LifecycleHooks; len(h) != 1 {
		t.Fatalf("lifecycle hooks %+v", h)
	}
	if out["Plan"] == "" || out["Domain"] == "" || out["Policy"] == "" {
		t.Fatalf("outputs %v", out)
	}
	remove(t, c, "b")
	if p := must(a.DescribePolicies(ctx, &autoscaling.DescribePoliciesInput{})).ScalingPolicies; len(p) != 0 {
		t.Fatalf("scaling policies after DeleteStack %+v", p)
	}
}

// Cognito domains, resource servers, identity providers, users, identity pools and their roles.
func TestCognitoTypes(t *testing.T) {
	cfg := start(t)
	c := cfn.NewFromConfig(cfg)
	tpl := `{"Resources":{"Pool":{"Type":"AWS::Cognito::UserPool","Properties":{"UserPoolName":"p"}},
"Client":{"Type":"AWS::Cognito::UserPoolClient","Properties":{"UserPoolId":{"Ref":"Pool"},"ClientName":"web"}},
"Dom":{"Type":"AWS::Cognito::UserPoolDomain","Properties":{"UserPoolId":{"Ref":"Pool"},"Domain":"my-app-domain"}},
"Rs":{"Type":"AWS::Cognito::UserPoolResourceServer","Properties":{"UserPoolId":{"Ref":"Pool"},"Identifier":"https://api.example.com","Name":"api","Scopes":[{"ScopeName":"read","ScopeDescription":"read"}]}},
"Idp":{"Type":"AWS::Cognito::UserPoolIdentityProvider","Properties":{"UserPoolId":{"Ref":"Pool"},"ProviderName":"corp","ProviderType":"OIDC","ProviderDetails":{"client_id":"x","client_secret":"y","attributes_request_method":"GET","oidc_issuer":"https://issuer.example.com","authorize_scopes":"openid"}}},
"Group":{"Type":"AWS::Cognito::UserPoolGroup","Properties":{"UserPoolId":{"Ref":"Pool"},"GroupName":"admins"}},
"User":{"Type":"AWS::Cognito::UserPoolUser","Properties":{"UserPoolId":{"Ref":"Pool"},"Username":"ann","UserAttributes":[{"Name":"email","Value":"ann@example.com"}]}},
"Attach":{"Type":"AWS::Cognito::UserPoolUserToGroupAttachment","Properties":{"UserPoolId":{"Ref":"Pool"},"Username":{"Ref":"User"},"GroupName":{"Ref":"Group"}}},
"Ident":{"Type":"AWS::Cognito::IdentityPool","Properties":{"IdentityPoolName":"ip","AllowUnauthenticatedIdentities":false,"CognitoIdentityProviders":[{"ClientId":{"Ref":"Client"},"ProviderName":{"Fn::GetAtt":["Pool","ProviderName"]}}]}},
"Role":{"Type":"AWS::IAM::Role","Properties":{"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"cognito-identity.amazonaws.com"},"Action":"sts:AssumeRoleWithWebIdentity"}]}}},
"Roles":{"Type":"AWS::Cognito::IdentityPoolRoleAttachment","Properties":{"IdentityPoolId":{"Ref":"Ident"},"Roles":{"authenticated":{"Fn::GetAtt":["Role","Arn"]}}}}},
"Outputs":{"Pool":{"Value":{"Ref":"Pool"}},"Ident":{"Value":{"Ref":"Ident"}}}}`
	st := deploy(t, c, "cog", tpl)
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	pool := outputs(st)["Pool"]
	call := func(op string, in map[string]any) map[string]any { return cognitoCall(t, cfg, op, in) }
	if d := call("DescribeUserPool", map[string]any{"UserPoolId": pool})["UserPool"].(map[string]any); d["Domain"] != "my-app-domain" {
		t.Fatalf("pool %v", d)
	}
	if l := call("ListResourceServers", map[string]any{"UserPoolId": pool, "MaxResults": 10})["ResourceServers"].([]any); len(l) != 1 {
		t.Fatalf("resource servers %v", l)
	}
	if l := call("ListIdentityProviders", map[string]any{"UserPoolId": pool, "MaxResults": 10})["Providers"].([]any); len(l) != 1 {
		t.Fatalf("identity providers %v", l)
	}
	if l := call("ListUsersInGroup", map[string]any{"UserPoolId": pool, "GroupName": "admins"})["Users"].([]any); len(l) != 1 {
		t.Fatalf("users in group %v", l)
	}
	remove(t, c, "cog")
}

// Route 53 zones, records, record set groups and health checks.
func TestRoute53Types(t *testing.T) {
	cfg := start(t)
	c, r := cfn.NewFromConfig(cfg), route53.NewFromConfig(cfg)
	tpl := `{"Resources":{"Zone":{"Type":"AWS::Route53::HostedZone","Properties":{"Name":"example.com","HostedZoneTags":[{"Key":"env","Value":"dev"}]}},
"A":{"Type":"AWS::Route53::RecordSet","Properties":{"HostedZoneId":{"Ref":"Zone"},"Name":"www.example.com.","Type":"A","TTL":"%s","ResourceRecords":["10.0.0.1"]}},
"Group":{"Type":"AWS::Route53::RecordSetGroup","Properties":{"HostedZoneName":"example.com.","RecordSets":[{"Name":"txt.example.com.","Type":"TXT","TTL":"60","ResourceRecords":["\"hello\""]},{"Name":"cn.example.com.","Type":"CNAME","TTL":"60","ResourceRecords":["www.example.com"]}]}},
"Hc":{"Type":"AWS::Route53::HealthCheck","Properties":{"HealthCheckConfig":{"Type":"HTTP","FullyQualifiedDomainName":"www.example.com","Port":80,"ResourcePath":"/"}}}},
"Outputs":{"Zone":{"Value":{"Ref":"Zone"}},"Ns":{"Value":{"Fn::Join":[",",{"Fn::GetAtt":["Zone","NameServers"]}]}}}}`
	st := deploy(t, c, "dns", fmt.Sprintf(tpl, "300"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	zone := outputs(st)["Zone"]
	records := func() []r53types.ResourceRecordSet {
		return must(r.ListResourceRecordSets(ctx, &route53.ListResourceRecordSetsInput{HostedZoneId: aws.String(zone)})).ResourceRecordSets
	}
	if l := records(); len(l) != 5 { // NS, SOA and the three records
		t.Fatalf("records %+v", l)
	}
	if outputs(st)["Ns"] == "" {
		t.Fatal("no name servers")
	}
	if h := must(r.ListHealthChecks(ctx, &route53.ListHealthChecksInput{})).HealthChecks; len(h) != 1 {
		t.Fatalf("health checks %+v", h)
	}
	wantStatus(t, c, redeploy(t, c, "dns", fmt.Sprintf(tpl, "120")), types.StackStatusUpdateComplete)
	for _, rs := range records() {
		if aws.ToString(rs.Name) == "www.example.com." && aws.ToInt64(rs.TTL) != 120 {
			t.Fatalf("ttl after update %+v", rs)
		}
	}
	remove(t, c, "dns")
	if z := must(r.ListHostedZones(ctx, &route53.ListHostedZonesInput{})).HostedZones; len(z) != 0 {
		t.Fatalf("zones after DeleteStack %+v", z)
	}
}

// CloudFront distributions with their policies, origin access control and function.
func TestCloudFrontTypes(t *testing.T) {
	cfg := start(t)
	c, f := cfn.NewFromConfig(cfg), cloudfront.NewFromConfig(cfg)
	tpl := `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket"},
"Oac":{"Type":"AWS::CloudFront::OriginAccessControl","Properties":{"OriginAccessControlConfig":{"Name":"oac","OriginAccessControlOriginType":"s3","SigningBehavior":"always","SigningProtocol":"sigv4"}}},
"Cache":{"Type":"AWS::CloudFront::CachePolicy","Properties":{"CachePolicyConfig":{"Name":"cache","MinTTL":0,"DefaultTTL":60,"MaxTTL":600,"ParametersInCacheKeyAndForwardedToOrigin":{"EnableAcceptEncodingGzip":true,
 "HeadersConfig":{"HeaderBehavior":"whitelist","Headers":["Accept"]},"CookiesConfig":{"CookieBehavior":"none"},"QueryStringsConfig":{"QueryStringBehavior":"none"}}}}},
"Fn":{"Type":"AWS::CloudFront::Function","Properties":{"Name":"viewer","AutoPublish":true,"FunctionCode":"function handler(event) { return event.request; }","FunctionConfig":{"Comment":"c","Runtime":"cloudfront-js-2.0"}}},
"Dist":{"Type":"AWS::CloudFront::Distribution","Properties":{"Tags":[{"Key":"env","Value":"dev"}],"DistributionConfig":{"Enabled":true,"Comment":"%s","DefaultRootObject":"index.html","IPV6Enabled":true,"Aliases":["www.example.com"],
 "Origins":[{"Id":"s3","DomainName":{"Fn::GetAtt":["Bucket","RegionalDomainName"]},"S3OriginConfig":{"OriginAccessIdentity":""},"OriginAccessControlId":{"Fn::GetAtt":["Oac","Id"]}}],
 "DefaultCacheBehavior":{"TargetOriginId":"s3","ViewerProtocolPolicy":"redirect-to-https","CachePolicyId":{"Ref":"Cache"},"AllowedMethods":["GET","HEAD"],"CachedMethods":["GET","HEAD"],
  "FunctionAssociations":[{"EventType":"viewer-request","FunctionARN":{"Fn::GetAtt":["Fn","FunctionARN"]}}]},
 "CustomErrorResponses":[{"ErrorCode":404,"ResponseCode":200,"ResponsePagePath":"/index.html"}]}}}},
"Outputs":{"Domain":{"Value":{"Fn::GetAtt":["Dist","DomainName"]}},"Id":{"Value":{"Ref":"Dist"}}}}`
	st := deploy(t, c, "cdn", fmt.Sprintf(tpl, "v1"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)
	d := must(f.GetDistribution(ctx, &cloudfront.GetDistributionInput{Id: aws.String(out["Id"])})).Distribution
	if aws.ToString(d.DomainName) != out["Domain"] || aws.ToString(d.DistributionConfig.Comment) != "v1" || len(d.DistributionConfig.Origins.Items) != 1 ||
		d.DistributionConfig.Aliases.Items[0] != "www.example.com" || aws.ToString(d.DistributionConfig.DefaultRootObject) != "index.html" {
		t.Fatalf("distribution %+v", d)
	}
	wantStatus(t, c, redeploy(t, c, "cdn", fmt.Sprintf(tpl, "v2")), types.StackStatusUpdateComplete)
	if d := must(f.GetDistribution(ctx, &cloudfront.GetDistributionInput{Id: aws.String(out["Id"])})).Distribution; aws.ToString(d.DistributionConfig.Comment) != "v2" {
		t.Fatalf("distribution after update %+v", d)
	}
	remove(t, c, "cdn")
	if l := must(f.ListDistributions(ctx, &cloudfront.ListDistributionsInput{})).DistributionList; l != nil && len(l.Items) != 0 {
		t.Fatalf("distributions after DeleteStack %+v", l.Items)
	}
	if l := must(f.ListFunctions(ctx, &cloudfront.ListFunctionsInput{})).FunctionList; l != nil && len(l.Items) != 0 {
		t.Fatalf("functions after DeleteStack %+v", l.Items)
	}
}

// ElastiCache clusters, replication groups, groups and users.
func TestElastiCacheTypes(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	cfg := start(t)
	c := cfn.NewFromConfig(cfg)
	describe := func(op, k, v string) string { return queryCall(t, cfg, "elasticache", op, k, v) }
	tpl := `{"Resources":{` + netHead + `,
"Sg":{"Type":"AWS::ElastiCache::SubnetGroup","Properties":{"Description":"cache","SubnetIds":[{"Ref":"S1"},{"Ref":"S2"}]}},
"Pg":{"Type":"AWS::ElastiCache::ParameterGroup","Properties":{"CacheParameterGroupFamily":"redis7","Description":"pg","Properties":{"maxmemory-policy":"allkeys-lru"}}},
"Cl":{"Type":"AWS::ElastiCache::CacheCluster","Properties":{"Engine":"redis","CacheNodeType":"cache.t3.micro","NumCacheNodes":1,"CacheSubnetGroupName":{"Ref":"Sg"},"CacheParameterGroupName":{"Ref":"Pg"},"SnapshotRetentionLimit":%s}},
"Rg":{"Type":"AWS::ElastiCache::ReplicationGroup","Properties":{"ReplicationGroupDescription":"rg","Engine":"redis","CacheNodeType":"cache.t3.micro","NumCacheClusters":2,"AutomaticFailoverEnabled":true}},
"Usr":{"Type":"AWS::ElastiCache::User","Properties":{"UserId":"ann","UserName":"ann","Engine":"redis","AccessString":"on ~* +@all","Passwords":["password12345678"]}},
"Def":{"Type":"AWS::ElastiCache::User","Properties":{"UserId":"dflt","UserName":"default","Engine":"redis","AccessString":"off ~* -@all","NoPasswordRequired":true}},
"Ug":{"Type":"AWS::ElastiCache::UserGroup","Properties":{"UserGroupId":"grp","Engine":"redis","UserIds":[{"Ref":"Usr"},{"Ref":"Def"}]}}},
"Outputs":{"Host":{"Value":{"Fn::GetAtt":["Rg","PrimaryEndPoint.Address"]}},"Cl":{"Value":{"Ref":"Cl"}}}}`
	st := deploy(t, c, "cache", fmt.Sprintf(tpl, "1"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)
	cl := describe("DescribeCacheClusters", "CacheClusterId", out["Cl"])
	if !strings.Contains(cl, "<SnapshotRetentionLimit>1</SnapshotRetentionLimit>") {
		t.Fatalf("cluster %s", cl)
	}
	if g := describe("DescribeReplicationGroups", "", ""); !strings.Contains(g, "<ReplicationGroupId>") || out["Host"] == "" {
		t.Fatalf("replication groups %s host %q", g, out["Host"])
	}
	wantStatus(t, c, redeploy(t, c, "cache", fmt.Sprintf(tpl, "5")), types.StackStatusUpdateComplete)
	if cl := describe("DescribeCacheClusters", "CacheClusterId", out["Cl"]); !strings.Contains(cl, "<SnapshotRetentionLimit>5</SnapshotRetentionLimit>") {
		t.Fatalf("cluster after update %s", cl)
	}
	remove(t, c, "cache")
	if g := describe("DescribeReplicationGroups", "", ""); strings.Contains(g, "<ReplicationGroupId>") {
		t.Fatalf("replication groups after DeleteStack %s", g)
	}
}

// queryCall sends a signed Query-protocol request (some SDKs are not dependencies of this module) and returns the answer.
func queryCall(t *testing.T, cfg aws.Config, service, op, key, value string) string {
	t.Helper()
	form := url.Values{"Action": {op}}
	if key != "" {
		form.Set(key, value)
	}
	req, _ := http.NewRequest("POST", aws.ToString(cfg.BaseEndpoint)+"/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	awstest.Sign(t, req, service)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("%s: %d %s", op, resp.StatusCode, b)
	}
	return string(b)
}

// An OpenSearch domain, and SES configuration sets, event destinations, identities and templates.
func TestSearchAndMailTypes(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	cfg := start(t)
	c, os, ses := cfn.NewFromConfig(cfg), opensearch.NewFromConfig(cfg), sesv2.NewFromConfig(cfg)
	tpl := `{"Resources":{"Domain":{"Type":"AWS::OpenSearchService::Domain","Properties":{"DomainName":"logs-search","EngineVersion":"OpenSearch_2.11","ClusterConfig":{"InstanceType":"t3.small.search","InstanceCount":%s},"EBSOptions":{"EBSEnabled":true,"VolumeSize":10},
 "Tags":[{"Key":"env","Value":"dev"}]}},
"Topic":{"Type":"AWS::SNS::Topic"},
"Cs":{"Type":"AWS::SES::ConfigurationSet","Properties":{"Name":"cs","SendingOptions":{"SendingEnabled":%s}}},
"Dest":{"Type":"AWS::SES::ConfigurationSetEventDestination","Properties":{"ConfigurationSetName":{"Ref":"Cs"},"EventDestination":{"Name":"to-sns","Enabled":true,"MatchingEventTypes":["bounce","complaint"],"SnsDestination":{"TopicARN":{"Ref":"Topic"}}}}},
"Id":{"Type":"AWS::SES::EmailIdentity","Properties":{"EmailIdentity":"example.com"}},
"Tpl":{"Type":"AWS::SES::Template","Properties":{"Template":{"TemplateName":"welcome","SubjectPart":"Hi {{name}}","TextPart":"Welcome {{name}}"}}}},
"Outputs":{"Endpoint":{"Value":{"Fn::GetAtt":["Domain","DomainEndpoint"]}},"Arn":{"Value":{"Fn::GetAtt":["Domain","Arn"]}}}}`
	st := deploy(t, c, "srch", fmt.Sprintf(tpl, "1", "true"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)
	d := must(os.DescribeDomain(ctx, &opensearch.DescribeDomainInput{DomainName: aws.String("logs-search")})).DomainStatus
	if !strings.HasPrefix(out["Endpoint"], "logs-search-") || aws.ToString(d.Endpoint) == "" || aws.ToString(d.ARN) != out["Arn"] || aws.ToInt32(d.ClusterConfig.InstanceCount) != 1 {
		t.Fatalf("domain %+v outputs %v", d, out)
	}
	if e := must(ses.GetConfigurationSetEventDestinations(ctx, &sesv2.GetConfigurationSetEventDestinationsInput{ConfigurationSetName: aws.String("cs")})).EventDestinations; len(e) != 1 || e[0].SnsDestination == nil {
		t.Fatalf("event destinations %+v", e)
	}
	if i := must(ses.ListEmailIdentities(ctx, &sesv2.ListEmailIdentitiesInput{})).EmailIdentities; len(i) != 1 {
		t.Fatalf("identities %+v", i)
	}
	if tp := must(ses.GetEmailTemplate(ctx, &sesv2.GetEmailTemplateInput{TemplateName: aws.String("welcome")})); aws.ToString(tp.TemplateContent.Subject) != "Hi {{name}}" {
		t.Fatalf("template %+v", tp)
	}
	wantStatus(t, c, redeploy(t, c, "srch", fmt.Sprintf(tpl, "2", "false")), types.StackStatusUpdateComplete)
	if d := must(os.DescribeDomain(ctx, &opensearch.DescribeDomainInput{DomainName: aws.String("logs-search")})).DomainStatus; aws.ToInt32(d.ClusterConfig.InstanceCount) != 2 {
		t.Fatalf("domain after update %+v", d)
	}
	if cs := must(ses.GetConfigurationSet(ctx, &sesv2.GetConfigurationSetInput{ConfigurationSetName: aws.String("cs")})); cs.SendingOptions == nil || cs.SendingOptions.SendingEnabled {
		t.Fatalf("configuration set after update %+v", cs)
	}
	remove(t, c, "srch")
	if l := must(os.ListDomainNames(ctx, &opensearch.ListDomainNamesInput{})).DomainNames; len(l) != 0 {
		t.Fatalf("domains after DeleteStack %+v", l)
	}
}

// An AppSync API with its schema, data source, resolver, key and function, and a schedule group.
func TestAppSyncTypes(t *testing.T) {
	cfg := start(t)
	c, a, sc := cfn.NewFromConfig(cfg), appsync.NewFromConfig(cfg), scheduler.NewFromConfig(cfg)
	tpl := `{"Resources":{"Api":{"Type":"AWS::AppSync::GraphQLApi","Properties":{"Name":"%s","AuthenticationType":"API_KEY"}},
"Schema":{"Type":"AWS::AppSync::GraphQLSchema","Properties":{"ApiId":{"Fn::GetAtt":["Api","ApiId"]},"Definition":"type Query { hello: String }\nschema { query: Query }"}},
"Ds":{"Type":"AWS::AppSync::DataSource","Properties":{"ApiId":{"Fn::GetAtt":["Api","ApiId"]},"Name":"none","Type":"NONE"}},
"Fnc":{"Type":"AWS::AppSync::FunctionConfiguration","Properties":{"ApiId":{"Fn::GetAtt":["Api","ApiId"]},"Name":"fn","DataSourceName":{"Fn::GetAtt":["Ds","Name"]},"FunctionVersion":"2018-05-29","RequestMappingTemplate":"{}","ResponseMappingTemplate":"$util.toJson(\"hi\")"}},
"Res":{"Type":"AWS::AppSync::Resolver","DependsOn":"Schema","Properties":{"ApiId":{"Fn::GetAtt":["Api","ApiId"]},"TypeName":"Query","FieldName":"hello","DataSourceName":{"Fn::GetAtt":["Ds","Name"]},"RequestMappingTemplate":"{}","ResponseMappingTemplate":"$util.toJson(\"hi\")"}},
"Key":{"Type":"AWS::AppSync::ApiKey","Properties":{"ApiId":{"Fn::GetAtt":["Api","ApiId"]}}},
"Grp":{"Type":"AWS::Scheduler::ScheduleGroup","Properties":{"Name":"nightly"}}},
"Outputs":{"Id":{"Value":{"Fn::GetAtt":["Api","ApiId"]}},"Url":{"Value":{"Fn::GetAtt":["Api","GraphQLUrl"]}}}}`
	st := deploy(t, c, "gql", fmt.Sprintf(tpl, "one"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)
	if g := must(a.GetGraphqlApi(ctx, &appsync.GetGraphqlApiInput{ApiId: aws.String(out["Id"])})).GraphqlApi; aws.ToString(g.Name) != "one" || out["Url"] == "" {
		t.Fatalf("api %+v outputs %v", g, out)
	}
	if r := must(a.ListResolvers(ctx, &appsync.ListResolversInput{ApiId: aws.String(out["Id"]), TypeName: aws.String("Query")})).Resolvers; len(r) != 1 {
		t.Fatalf("resolvers %+v", r)
	}
	if k := must(a.ListApiKeys(ctx, &appsync.ListApiKeysInput{ApiId: aws.String(out["Id"])})).ApiKeys; len(k) != 1 {
		t.Fatalf("keys %+v", k)
	}
	if f := must(a.ListFunctions(ctx, &appsync.ListFunctionsInput{ApiId: aws.String(out["Id"])})).Functions; len(f) != 1 {
		t.Fatalf("functions %+v", f)
	}
	if g := must(sc.ListScheduleGroups(ctx, &scheduler.ListScheduleGroupsInput{})).ScheduleGroups; len(g) != 2 { // default and nightly
		t.Fatalf("schedule groups %+v", g)
	}
	wantStatus(t, c, redeploy(t, c, "gql", fmt.Sprintf(tpl, "two")), types.StackStatusUpdateComplete)
	if g := must(a.GetGraphqlApi(ctx, &appsync.GetGraphqlApiInput{ApiId: aws.String(out["Id"])})).GraphqlApi; aws.ToString(g.Name) != "two" {
		t.Fatalf("api after update %+v", g)
	}
	remove(t, c, "gql")
	if l := must(a.ListGraphqlApis(ctx, &appsync.ListGraphqlApisInput{})).GraphqlApis; len(l) != 0 {
		t.Fatalf("apis after DeleteStack %+v", l)
	}
}

// Glue, Athena, Systems Manager, Backup and Flink resources.
func TestDataTypes(t *testing.T) {
	cfg := start(t)
	c := cfn.NewFromConfig(cfg)
	tpl := `{"Resources":{"Role":{"Type":"AWS::IAM::Role","Properties":{"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"glue.amazonaws.com"},"Action":"sts:AssumeRole"}]}}},
"Db":{"Type":"AWS::Glue::Database","Properties":{"CatalogId":"000000000000","DatabaseInput":{"Name":"lake","Description":"%s"}}},
"Tbl":{"Type":"AWS::Glue::Table","Properties":{"CatalogId":"000000000000","DatabaseName":{"Ref":"Db"},"TableInput":{"Name":"events","TableType":"EXTERNAL_TABLE","StorageDescriptor":{"Columns":[{"Name":"id","Type":"string"}],"Location":"s3://lake/events/"}}}},
"Crawl":{"Type":"AWS::Glue::Crawler","Properties":{"Role":{"Fn::GetAtt":["Role","Arn"]},"DatabaseName":{"Ref":"Db"},"Targets":{"S3Targets":[{"Path":"s3://lake/raw/"}]},"Schedule":{"ScheduleExpression":"cron(0 1 * * ? *)"}}},
"Job":{"Type":"AWS::Glue::Job","Properties":{"Name":"etl","Role":{"Fn::GetAtt":["Role","Arn"]},"Command":{"Name":"glueetl","ScriptLocation":"s3://lake/etl.py"},"GlueVersion":"4.0"}},
"Trig":{"Type":"AWS::Glue::Trigger","Properties":{"Name":"nightly","Type":"SCHEDULED","Schedule":"cron(0 2 * * ? *)","Actions":[{"JobName":{"Ref":"Job"}}]}},
"Reg":{"Type":"AWS::Glue::Registry","Properties":{"Name":"schemas"}},
"Sch":{"Type":"AWS::Glue::Schema","Properties":{"Name":"user","DataFormat":"JSON","Compatibility":"NONE","SchemaDefinition":"{\"type\":\"object\"}","Registry":{"Arn":{"Fn::GetAtt":["Reg","Arn"]}}}},
"Wg":{"Type":"AWS::Athena::WorkGroup","Properties":{"Name":"analytics","Description":"%s","WorkGroupConfiguration":{"ResultConfiguration":{"OutputLocation":"s3://lake/results/"},"EnforceWorkGroupConfiguration":true}}},
"Nq":{"Type":"AWS::Athena::NamedQuery","Properties":{"Database":"lake","Name":"all","QueryString":"select 1","WorkGroup":{"Ref":"Wg"}}},
"Cat":{"Type":"AWS::Athena::DataCatalog","Properties":{"Name":"hive","Type":"HIVE","Parameters":{"metadata-function":"arn:aws:lambda:us-east-1:000000000000:function:f"}}},
"Doc":{"Type":"AWS::SSM::Document","Properties":{"Name":"greet","DocumentType":"Command","Content":{"schemaVersion":"2.2","description":"hi","mainSteps":[{"action":"aws:runShellScript","name":"s","inputs":{"runCommand":["echo hi"]}}]}}},
"Assoc":{"Type":"AWS::SSM::Association","Properties":{"Name":{"Ref":"Doc"},"AssociationName":"nightly","Targets":[{"Key":"InstanceIds","Values":["i-0123456789abcdef0"]}],"ScheduleExpression":"rate(1 day)"}},
"Vault":{"Type":"AWS::Backup::BackupVault","Properties":{"BackupVaultName":"vault"}},
"Plan":{"Type":"AWS::Backup::BackupPlan","Properties":{"BackupPlan":{"BackupPlanName":"daily","BackupPlanRule":[{"RuleName":"d","TargetBackupVault":{"Ref":"Vault"},"ScheduleExpression":"cron(0 5 * * ? *)","Lifecycle":{"DeleteAfterDays":30}}]}}},
"Sel":{"Type":"AWS::Backup::BackupSelection","Properties":{"BackupPlanId":{"Ref":"Plan"},"BackupSelection":{"SelectionName":"all","IamRoleArn":{"Fn::GetAtt":["Role","Arn"]},"Resources":["arn:aws:dynamodb:us-east-1:000000000000:table/t"]}}},
"App":{"Type":"AWS::KinesisAnalyticsV2::Application","Properties":{"ApplicationName":"flink","RuntimeEnvironment":"FLINK-1_18","ServiceExecutionRole":{"Fn::GetAtt":["Role","Arn"]}}}},
"Outputs":{"Plan":{"Value":{"Ref":"Plan"}}}}`
	st := deploy(t, c, "data", fmt.Sprintf(tpl, "a", "one"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)

	g := glue.NewFromConfig(cfg)
	if d := must(g.GetDatabase(ctx, &glue.GetDatabaseInput{Name: aws.String("lake")})).Database; aws.ToString(d.Description) != "a" {
		t.Fatalf("database %+v", d)
	}
	if tb := must(g.GetTable(ctx, &glue.GetTableInput{DatabaseName: aws.String("lake"), Name: aws.String("events")})).Table; len(tb.StorageDescriptor.Columns) != 1 {
		t.Fatalf("table %+v", tb)
	}
	if cr := must(g.GetCrawler(ctx, &glue.GetCrawlerInput{Name: aws.String(physical(t, c, "data", "Crawl"))})).Crawler; cr.Schedule == nil || aws.ToString(cr.Schedule.ScheduleExpression) != "cron(0 1 * * ? *)" {
		t.Fatalf("crawler %+v", cr)
	}
	if j := must(g.GetJob(ctx, &glue.GetJobInput{JobName: aws.String("etl")})).Job; aws.ToString(j.GlueVersion) != "4.0" {
		t.Fatalf("job %+v", j)
	}
	if tr := must(g.GetTrigger(ctx, &glue.GetTriggerInput{Name: aws.String("nightly")})).Trigger; len(tr.Actions) != 1 {
		t.Fatalf("trigger %+v", tr)
	}
	if s := must(g.GetSchema(ctx, &glue.GetSchemaInput{SchemaId: &gluetypes.SchemaId{RegistryName: aws.String("schemas"), SchemaName: aws.String("user")}})); aws.ToString(s.SchemaName) != "user" {
		t.Fatalf("schema %+v", s)
	}
	at := athena.NewFromConfig(cfg)
	if w := must(at.GetWorkGroup(ctx, &athena.GetWorkGroupInput{WorkGroup: aws.String("analytics")})).WorkGroup; aws.ToString(w.Description) != "one" {
		t.Fatalf("workgroup %+v", w)
	}
	if n := must(at.ListNamedQueries(ctx, &athena.ListNamedQueriesInput{WorkGroup: aws.String("analytics")})).NamedQueryIds; len(n) != 1 {
		t.Fatalf("named queries %+v", n)
	}
	if d := must(at.GetDataCatalog(ctx, &athena.GetDataCatalogInput{Name: aws.String("hive")})).DataCatalog; d.Type != athenatypes.DataCatalogTypeHive {
		t.Fatalf("data catalog %+v", d)
	}
	sm := ssm.NewFromConfig(cfg)
	if d := must(sm.DescribeDocument(ctx, &ssm.DescribeDocumentInput{Name: aws.String("greet")})).Document; aws.ToString(d.Name) != "greet" {
		t.Fatalf("document %+v", d)
	}
	if a := must(sm.ListAssociations(ctx, &ssm.ListAssociationsInput{})).Associations; len(a) != 1 {
		t.Fatalf("associations %+v", a)
	}
	bk := backup.NewFromConfig(cfg)
	if p := must(bk.ListBackupPlans(ctx, &backup.ListBackupPlansInput{})).BackupPlansList; len(p) != 1 {
		t.Fatalf("backup plans %+v", p)
	}
	if s := must(bk.ListBackupSelections(ctx, &backup.ListBackupSelectionsInput{BackupPlanId: aws.String(outputs(st)["Plan"])})).BackupSelectionsList; len(s) != 1 {
		t.Fatalf("backup selections %+v", s)
	}
	if v := must(bk.ListBackupVaults(ctx, &backup.ListBackupVaultsInput{})).BackupVaultList; len(v) < 1 {
		t.Fatalf("backup vaults %+v", v)
	}
	if a := must(kinesisanalyticsv2.NewFromConfig(cfg).ListApplications(ctx, &kinesisanalyticsv2.ListApplicationsInput{})).ApplicationSummaries; len(a) != 1 {
		t.Fatalf("flink applications %+v", a)
	}

	wantStatus(t, c, redeploy(t, c, "data", fmt.Sprintf(tpl, "b", "two")), types.StackStatusUpdateComplete)
	if d := must(g.GetDatabase(ctx, &glue.GetDatabaseInput{Name: aws.String("lake")})).Database; aws.ToString(d.Description) != "b" {
		t.Fatalf("database after update %+v", d)
	}
	if w := must(at.GetWorkGroup(ctx, &athena.GetWorkGroupInput{WorkGroup: aws.String("analytics")})).WorkGroup; aws.ToString(w.Description) != "two" {
		t.Fatalf("workgroup after update %+v", w)
	}
	remove(t, c, "data")
	if l := must(g.GetDatabases(ctx, &glue.GetDatabasesInput{})).DatabaseList; len(l) != 0 {
		t.Fatalf("databases after DeleteStack %+v", l)
	}
}

// A type with no provider of its own that an embedded service manages (EFS, WAF) is deployed through Cloud Control.
func TestEngineTypes(t *testing.T) {
	cfg := start(t)
	c := cfn.NewFromConfig(cfg)
	tpl := `{"Resources":{"Fs":{"Type":"AWS::EFS::FileSystem","Properties":{"PerformanceMode":"generalPurpose","Encrypted":true}},
"Acl":{"Type":"AWS::WAFv2::WebACL","Properties":{"Name":"acl","Scope":"REGIONAL","DefaultAction":{"Allow":{}},"VisibilityConfig":{"SampledRequestsEnabled":true,"CloudWatchMetricsEnabled":true,"MetricName":"acl"}}},
"Ips":{"Type":"AWS::WAFv2::IPSet","Properties":{"Name":"ips","Scope":"REGIONAL","IPAddressVersion":"IPV4","Addresses":["10.0.0.0/8"]}}},
"Outputs":{"Fs":{"Value":{"Ref":"Fs"}},"Acl":{"Value":{"Fn::GetAtt":["Acl","Arn"]}}}}`
	st := deploy(t, c, "eng", tpl)
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)
	if !strings.HasPrefix(out["Fs"], "fs-") || out["Acl"] == "" {
		t.Fatalf("outputs %v", out)
	}
	remove(t, c, "eng")
}

// REST API models, request validators and gateway responses, and a method that uses them.
func TestApiGatewayModels(t *testing.T) {
	cfg := start(t)
	c, a := cfn.NewFromConfig(cfg), apigateway.NewFromConfig(cfg)
	tpl := `{"Resources":{"Api":{"Type":"AWS::ApiGateway::RestApi","Properties":{"Name":"models"}},
"Model":{"Type":"AWS::ApiGateway::Model","Properties":{"RestApiId":{"Ref":"Api"},"Name":"Pet","ContentType":"application/json","Schema":{"type":"object","properties":{"name":{"type":"string"}}}}},
"Val":{"Type":"AWS::ApiGateway::RequestValidator","Properties":{"RestApiId":{"Ref":"Api"},"Name":"body","ValidateRequestBody":true,"ValidateRequestParameters":false}},
"Gw":{"Type":"AWS::ApiGateway::GatewayResponse","Properties":{"RestApiId":{"Ref":"Api"},"ResponseType":"DEFAULT_4XX","ResponseParameters":{"gatewayresponse.header.Access-Control-Allow-Origin":"'*'"}}},
"M":{"Type":"AWS::ApiGateway::Method","Properties":{"RestApiId":{"Ref":"Api"},"ResourceId":{"Fn::GetAtt":["Api","RootResourceId"]},"HttpMethod":"POST","AuthorizationType":"NONE","RequestValidatorId":{"Ref":"Val"},"RequestModels":{"application/json":{"Ref":"Model"}},"Integration":{"Type":"MOCK"}}}},
"Outputs":{"Api":{"Value":{"Ref":"Api"}}}}`
	st := deploy(t, c, "models", tpl)
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	api := aws.String(outputs(st)["Api"])
	if m := must(a.GetModels(ctx, &apigateway.GetModelsInput{RestApiId: api})).Items; len(m) != 1 || aws.ToString(m[0].Name) != "Pet" {
		t.Fatalf("models %+v", m)
	}
	if v := must(a.GetRequestValidators(ctx, &apigateway.GetRequestValidatorsInput{RestApiId: api})).Items; len(v) != 1 || !v[0].ValidateRequestBody {
		t.Fatalf("validators %+v", v)
	}
	if g := must(a.GetGatewayResponses(ctx, &apigateway.GetGatewayResponsesInput{RestApiId: api})).Items; len(g) != 1 {
		t.Fatalf("gateway responses %+v", g)
	}
	root := must(a.GetResources(ctx, &apigateway.GetResourcesInput{RestApiId: api})).Items[0]
	if m := must(a.GetMethod(ctx, &apigateway.GetMethodInput{RestApiId: api, ResourceId: root.Id, HttpMethod: aws.String("POST")})); aws.ToString(m.RequestValidatorId) == "" || m.RequestModels["application/json"] != "Pet" {
		t.Fatalf("method %+v", m)
	}
	remove(t, c, "models")
}

// Point-in-time recovery on a table, global tables, topic inline policies and secret rotation schedules.
func TestTableAndSecretTypes(t *testing.T) {
	cfg := start(t)
	c, d := cfn.NewFromConfig(cfg), dynamodb.NewFromConfig(cfg)
	tpl := `{"Resources":{"Plain":{"Type":"AWS::DynamoDB::Table","Properties":{"BillingMode":"PAY_PER_REQUEST","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
 "PointInTimeRecoverySpecification":{"PointInTimeRecoveryEnabled":true}}},
"Global":{"Type":"AWS::DynamoDB::GlobalTable","Properties":{"TableName":"gtable","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
 "StreamSpecification":{"StreamViewType":"NEW_AND_OLD_IMAGES"},"Replicas":[{"Region":"us-east-1"},{"Region":"us-west-2"}%s]}},
"Topic":{"Type":"AWS::SNS::Topic"},
"Inline":{"Type":"AWS::SNS::TopicInlinePolicy","Properties":{"TopicArn":{"Ref":"Topic"},"PolicyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sns:Publish","Resource":{"Ref":"Topic"}}]}}},
"Secret":{"Type":"AWS::SecretsManager::Secret","Properties":{"Name":"rotating"}},
"Fn":{"Type":"AWS::Lambda::Function","Properties":{"Runtime":"python3.12","Handler":"i.h","Role":"arn:aws:iam::000000000000:role/r","Code":{"ZipFile":"def h(e,c): return 1"}}},
"Rot":{"Type":"AWS::SecretsManager::RotationSchedule","Properties":{"SecretId":{"Ref":"Secret"},"RotationLambdaArn":{"Fn::GetAtt":["Fn","Arn"]},"RotateImmediatelyOnUpdate":false,"RotationRules":{"AutomaticallyAfterDays":30}}}},
"Outputs":{"Plain":{"Value":{"Ref":"Plain"}}}}`
	st := deploy(t, c, "tbl", fmt.Sprintf(tpl, ""))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	b := must(d.DescribeContinuousBackups(ctx, &dynamodb.DescribeContinuousBackupsInput{TableName: aws.String(outputs(st)["Plain"])})).ContinuousBackupsDescription
	if b.PointInTimeRecoveryDescription == nil || b.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus != dynamodbtypes.PointInTimeRecoveryStatusEnabled {
		t.Fatalf("point in time recovery %+v", b)
	}
	if g := must(d.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("gtable")})).Table; len(g.Replicas) != 2 {
		t.Fatalf("global table replicas %+v", g.Replicas)
	}
	wantStatus(t, c, redeploy(t, c, "tbl", fmt.Sprintf(tpl, `,{"Region":"eu-west-1"}`)), types.StackStatusUpdateComplete)
	if g := must(d.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("gtable")})).Table; len(g.Replicas) != 3 {
		t.Fatalf("global table replicas after update %+v", g.Replicas)
	}
	remove(t, c, "tbl")
}

// Cloud Map, CloudTrail, CodeBuild, CodePipeline, CodeDeploy, MemoryDB and Verified Permissions resources.
func TestInfraTypes(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	cfg := start(t)
	c := cfn.NewFromConfig(cfg)
	tpl := `{"Resources":{` + netHead + `,
"Ns":{"Type":"AWS::ServiceDiscovery::PrivateDnsNamespace","Properties":{"Name":"corp.local","Vpc":{"Ref":"Vpc"}}},
"Svc":{"Type":"AWS::ServiceDiscovery::Service","Properties":{"Name":"api","NamespaceId":{"Fn::GetAtt":["Ns","Id"]},"DnsConfig":{"RoutingPolicy":"MULTIVALUE","DnsRecords":[{"Type":"A","TTL":60}]}}},
"Inst":{"Type":"AWS::ServiceDiscovery::Instance","Properties":{"ServiceId":{"Ref":"Svc"},"InstanceId":"one","InstanceAttributes":{"AWS_INSTANCE_IPV4":"10.9.1.5"}}},
"Logs":{"Type":"AWS::S3::Bucket"},
"Trail":{"Type":"AWS::CloudTrail::Trail","Properties":{"S3BucketName":{"Ref":"Logs"},"IsLogging":true,"IsMultiRegionTrail":%s,"EventSelectors":[{"ReadWriteType":"All","IncludeManagementEvents":true}]}},
"Role":{"Type":"AWS::IAM::Role","Properties":{"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"codebuild.amazonaws.com"},"Action":"sts:AssumeRole"}]}}},
"Build":{"Type":"AWS::CodeBuild::Project","Properties":{"Name":"build","ServiceRole":{"Fn::GetAtt":["Role","Arn"]},"Source":{"Type":"NO_SOURCE","BuildSpec":"version: 0.2\nphases: {build: {commands: [echo hi]}}"},"Artifacts":{"Type":"NO_ARTIFACTS"},
 "Environment":{"Type":"LINUX_CONTAINER","Image":"aws/codebuild/standard:7.0","ComputeType":"BUILD_GENERAL1_SMALL"},"TimeoutInMinutes":%s}},
"Pipe":{"Type":"AWS::CodePipeline::Pipeline","Properties":{"Name":"pipe","RoleArn":{"Fn::GetAtt":["Role","Arn"]},"ArtifactStore":{"Type":"S3","Location":{"Ref":"Logs"}},
 "Stages":[{"Name":"Source","Actions":[{"Name":"src","ActionTypeId":{"Category":"Source","Owner":"AWS","Provider":"S3","Version":"1"},"Configuration":{"S3Bucket":{"Ref":"Logs"},"S3ObjectKey":"src.zip","PollForSourceChanges":false},"OutputArtifacts":[{"Name":"src"}]}]},
  {"Name":"Build","Actions":[{"Name":"b","ActionTypeId":{"Category":"Build","Owner":"AWS","Provider":"CodeBuild","Version":"1"},"Configuration":{"ProjectName":{"Ref":"Build"}},"InputArtifacts":[{"Name":"src"}],"OutputArtifacts":[{"Name":"out"}]}]}]}},
"App":{"Type":"AWS::CodeDeploy::Application","Properties":{"ApplicationName":"deploy-app","ComputePlatform":"Lambda"}},
"Cfg":{"Type":"AWS::CodeDeploy::DeploymentConfig","Properties":{"DeploymentConfigName":"canary","ComputePlatform":"Lambda","TrafficRoutingConfig":{"Type":"TimeBasedCanary","TimeBasedCanary":{"CanaryPercentage":10,"CanaryInterval":5}}}},
"Dg":{"Type":"AWS::CodeDeploy::DeploymentGroup","Properties":{"ApplicationName":{"Ref":"App"},"ServiceRoleArn":{"Fn::GetAtt":["Role","Arn"]},"DeploymentConfigName":{"Ref":"Cfg"},"DeploymentStyle":{"DeploymentType":"BLUE_GREEN","DeploymentOption":"WITH_TRAFFIC_CONTROL"}}},
"MemSg":{"Type":"AWS::MemoryDB::SubnetGroup","Properties":{"SubnetGroupName":"mem","SubnetIds":[{"Ref":"S1"},{"Ref":"S2"}]}},
"MemAcl":{"Type":"AWS::MemoryDB::ACL","Properties":{"ACLName":"my-acl"}},
"Mem":{"Type":"AWS::MemoryDB::Cluster","Properties":{"ClusterName":"mem-cluster","NodeType":"db.t4g.small","ACLName":{"Ref":"MemAcl"},"SubnetGroupName":{"Ref":"MemSg"},"NumShards":1,"NumReplicasPerShard":0}},
"Store":{"Type":"AWS::VerifiedPermissions::PolicyStore","Properties":{"ValidationSettings":{"Mode":"OFF"}}},
"Pol":{"Type":"AWS::VerifiedPermissions::Policy","Properties":{"PolicyStoreId":{"Ref":"Store"},"Definition":{"Static":{"Statement":"permit(principal, action, resource);","Description":"all"}}}}},
"Outputs":{"Ns":{"Value":{"Ref":"Ns"}},"Store":{"Value":{"Ref":"Store"}}}}`
	st := deploy(t, c, "infra", fmt.Sprintf(tpl, "false", "30"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	if l := must(servicediscovery.NewFromConfig(cfg).ListServices(ctx, &servicediscovery.ListServicesInput{})).Services; len(l) != 1 {
		t.Fatalf("cloud map services %+v", l)
	}
	if tr := must(cloudtrail.NewFromConfig(cfg).DescribeTrails(ctx, &cloudtrail.DescribeTrailsInput{})).TrailList; len(tr) != 1 {
		t.Fatalf("trails %+v", tr)
	}
	if b := must(codebuild.NewFromConfig(cfg).BatchGetProjects(ctx, &codebuild.BatchGetProjectsInput{Names: []string{"build"}})).Projects; len(b) != 1 ||
		aws.ToString(b[0].Source.Buildspec) == "" || aws.ToInt32(b[0].TimeoutInMinutes) != 30 {
		t.Fatalf("build projects %+v", b)
	}
	pl := must(codepipeline.NewFromConfig(cfg).GetPipeline(ctx, &codepipeline.GetPipelineInput{Name: aws.String("pipe")})).Pipeline
	if len(pl.Stages) != 2 || pl.Stages[0].Actions[0].Configuration["S3ObjectKey"] != "src.zip" || pl.Stages[0].Actions[0].Configuration["PollForSourceChanges"] != "false" {
		t.Fatalf("pipeline %+v", pl)
	}
	if g := must(codedeploy.NewFromConfig(cfg).ListDeploymentGroups(ctx, &codedeploy.ListDeploymentGroupsInput{ApplicationName: aws.String("deploy-app")})).DeploymentGroups; len(g) != 1 {
		t.Fatalf("deployment groups %v", g)
	}
	if m := must(memorydb.NewFromConfig(cfg).DescribeClusters(ctx, &memorydb.DescribeClustersInput{})).Clusters; len(m) != 1 || aws.ToString(m[0].Status) != "available" {
		t.Fatalf("memorydb clusters %+v", m)
	}
	if s := must(verifiedpermissions.NewFromConfig(cfg).ListPolicies(ctx, &verifiedpermissions.ListPoliciesInput{PolicyStoreId: aws.String(outputs(st)["Store"])})).Policies; len(s) != 1 {
		t.Fatalf("policies %+v", s)
	}
	wantStatus(t, c, redeploy(t, c, "infra", fmt.Sprintf(tpl, "true", "45")), types.StackStatusUpdateComplete)
	if b := must(codebuild.NewFromConfig(cfg).BatchGetProjects(ctx, &codebuild.BatchGetProjectsInput{Names: []string{"build"}})).Projects; aws.ToInt32(b[0].TimeoutInMinutes) != 45 {
		t.Fatalf("build project after update %+v", b)
	}
	remove(t, c, "infra")
}

// AppConfig, Amazon MQ, EKS, MSK and IoT resources.
func TestMessagingAndConfigTypes(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	cfg := start(t)
	c := cfn.NewFromConfig(cfg)
	tpl := `{"Resources":{` + netHead + `,
"App":{"Type":"AWS::AppConfig::Application","Properties":{"Name":"app","Description":"%s"}},
"Env":{"Type":"AWS::AppConfig::Environment","Properties":{"ApplicationId":{"Ref":"App"},"Name":"prod"}},
"Prof":{"Type":"AWS::AppConfig::ConfigurationProfile","Properties":{"ApplicationId":{"Ref":"App"},"Name":"flags","LocationUri":"hosted"}},
"Ver":{"Type":"AWS::AppConfig::HostedConfigurationVersion","Properties":{"ApplicationId":{"Ref":"App"},"ConfigurationProfileId":{"Ref":"Prof"},"Content":"{\"a\":1}","ContentType":"application/json"}},
"Strat":{"Type":"AWS::AppConfig::DeploymentStrategy","Properties":{"Name":"fast","DeploymentDurationInMinutes":0,"GrowthFactor":100,"ReplicateTo":"NONE"}},
"Deploy":{"Type":"AWS::AppConfig::Deployment","Properties":{"ApplicationId":{"Ref":"App"},"EnvironmentId":{"Ref":"Env"},"ConfigurationProfileId":{"Ref":"Prof"},"ConfigurationVersion":{"Fn::GetAtt":["Ver","VersionNumber"]},"DeploymentStrategyId":{"Ref":"Strat"}}},
"Broker":{"Type":"AWS::AmazonMQ::Broker","Properties":{"BrokerName":"mq","EngineType":"ACTIVEMQ","EngineVersion":"5.17.6","HostInstanceType":"mq.t3.micro","DeploymentMode":"SINGLE_INSTANCE","PubliclyAccessible":true,
 "Users":[{"Username":"admin","Password":"password12345"}],"AutoMinorVersionUpgrade":%s}},
"Eks":{"Type":"AWS::EKS::Cluster","Properties":{"Name":"k","RoleArn":"arn:aws:iam::000000000000:role/eks","ResourcesVpcConfig":{"SubnetIds":[{"Ref":"S1"},{"Ref":"S2"}]}}},
"MskCfg":{"Type":"AWS::MSK::Configuration","Properties":{"Name":"kcfg","KafkaVersionsList":["3.6.0"],"ServerProperties":"auto.create.topics.enable=true"}},
"Msk":{"Type":"AWS::MSK::Cluster","Properties":{"ClusterName":"msk","KafkaVersion":"3.6.0","NumberOfBrokerNodes":2,"BrokerNodeGroupInfo":{"InstanceType":"kafka.t3.small","ClientSubnets":[{"Ref":"S1"},{"Ref":"S2"}]}}},
"Thing":{"Type":"AWS::IoT::Thing","Properties":{"ThingName":"sensor","AttributePayload":{"Attributes":{"site":"a"}}}},
"IotPol":{"Type":"AWS::IoT::Policy","Properties":{"PolicyName":"allow","PolicyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}}},
"Rule":{"Type":"AWS::IoT::TopicRule","Properties":{"RuleName":"toSqs","TopicRulePayload":{"Sql":"SELECT * FROM 'a/b'","Actions":[{"Sqs":{"QueueUrl":"http://q","RoleArn":"arn:aws:iam::000000000000:role/r"}}]}}}},
"Outputs":{"Broker":{"Value":{"Ref":"Broker"}},"Eks":{"Value":{"Fn::GetAtt":["Eks","Endpoint"]}}}}`
	st := deploy(t, c, "msg", fmt.Sprintf(tpl, "one", "false"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)
	if out["Broker"] == "" || out["Eks"] == "" {
		t.Fatalf("outputs %v", out)
	}
	if a := must(appconfig.NewFromConfig(cfg).ListApplications(ctx, &appconfig.ListApplicationsInput{})).Items; len(a) != 1 {
		t.Fatalf("appconfig applications %+v", a)
	}
	if b := must(mq.NewFromConfig(cfg).ListBrokers(ctx, &mq.ListBrokersInput{})).BrokerSummaries; len(b) != 1 {
		t.Fatalf("brokers %+v", b)
	}
	if k := must(kafka.NewFromConfig(cfg).ListClustersV2(ctx, &kafka.ListClustersV2Input{})).ClusterInfoList; len(k) != 1 {
		t.Fatalf("msk clusters %+v", k)
	}
	if e := must(eks.NewFromConfig(cfg).ListClusters(ctx, &eks.ListClustersInput{})).Clusters; len(e) != 1 {
		t.Fatalf("eks clusters %+v", e)
	}
	wantStatus(t, c, redeploy(t, c, "msg", fmt.Sprintf(tpl, "two", "true")), types.StackStatusUpdateComplete)
	remove(t, c, "msg")
}

// Redshift, Redshift Serverless and Timestream for InfluxDB resources.
func TestWarehouseTypes(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	cfg := start(t)
	c := cfn.NewFromConfig(cfg)
	tpl := `{"Resources":{` + netHead + `,
"Sg":{"Type":"AWS::Redshift::ClusterSubnetGroup","Properties":{"Description":"rs","SubnetIds":[{"Ref":"S1"},{"Ref":"S2"}]}},
"Pg":{"Type":"AWS::Redshift::ClusterParameterGroup","Properties":{"Description":"pg","ParameterGroupFamily":"redshift-1.0","Parameters":[{"ParameterName":"enable_user_activity_logging","ParameterValue":"true"}]}},
"Cl":{"Type":"AWS::Redshift::Cluster","Properties":{"ClusterType":"single-node","NodeType":"dc2.large","MasterUsername":"admin","MasterUserPassword":"Passw0rdPassw0rd","DBName":"dev","ClusterSubnetGroupName":{"Ref":"Sg"},"ClusterParameterGroupName":{"Ref":"Pg"},"AutomatedSnapshotRetentionPeriod":%s}},
"Ns":{"Type":"AWS::RedshiftServerless::Namespace","Properties":{"NamespaceName":"nspace","DbName":"dev","AdminUsername":"admin","AdminUserPassword":"Passw0rdPassw0rd"}},
"Wg":{"Type":"AWS::RedshiftServerless::Workgroup","Properties":{"WorkgroupName":"wgroup","NamespaceName":{"Ref":"Ns"},"BaseCapacity":%s}},
"Flux":{"Type":"AWS::Timestream::InfluxDBInstance","Properties":{"Name":"flux","Username":"admin","Password":"Passw0rd1234","Organization":"org","Bucket":"b","DbInstanceType":"db.influx.medium","AllocatedStorage":20,"VpcSubnetIds":[{"Ref":"S1"}],"VpcSecurityGroupIds":["sg-12345678"]}}},
"Outputs":{"Host":{"Value":{"Fn::GetAtt":["Cl","Endpoint.Address"]}},"Cl":{"Value":{"Ref":"Cl"}}}}`
	st := deploy(t, c, "wh", fmt.Sprintf(tpl, "1", "32"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	out := outputs(st)
	if out["Host"] == "" || out["Cl"] == "" {
		t.Fatalf("outputs %v", out)
	}
	if d := queryCall(t, cfg, "redshift", "DescribeClusters", "ClusterIdentifier", out["Cl"]); !strings.Contains(d, "<AutomatedSnapshotRetentionPeriod>1</AutomatedSnapshotRetentionPeriod>") {
		t.Fatalf("cluster %s", d)
	}
	wantStatus(t, c, redeploy(t, c, "wh", fmt.Sprintf(tpl, "3", "64")), types.StackStatusUpdateComplete)
	if d := queryCall(t, cfg, "redshift", "DescribeClusters", "ClusterIdentifier", out["Cl"]); !strings.Contains(d, "<AutomatedSnapshotRetentionPeriod>3</AutomatedSnapshotRetentionPeriod>") {
		t.Fatalf("cluster after update %s", d)
	}
	remove(t, c, "wh")
}

// Batch, Organizations, and DocumentDB and Neptune resources (RDS runs them).
func TestBatchOrganizationsAndGraphTypes(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	cfg := start(t)
	c := cfn.NewFromConfig(cfg)
	orgCall := func(op string, in map[string]any) map[string]any {
		b, _ := json.Marshal(in)
		req, _ := http.NewRequest("POST", aws.ToString(cfg.BaseEndpoint)+"/", bytes.NewReader(b))
		req.Header.Set("X-Amz-Target", "AWSOrganizationsV20161128."+op)
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		awstest.Sign(t, req, "organizations")
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
	orgCall("CreateOrganization", map[string]any{"FeatureSet": "ALL"})
	root := orgCall("ListRoots", map[string]any{})["Roots"].([]any)[0].(map[string]any)["Id"]
	orgCall("EnablePolicyType", map[string]any{"RootId": root, "PolicyType": "SERVICE_CONTROL_POLICY"})
	tpl := fmt.Sprintf(`{"Resources":{`+netHead+`,
"Env":{"Type":"AWS::Batch::ComputeEnvironment","Properties":{"Type":"UNMANAGED","State":"ENABLED"}},
"Queue":{"Type":"AWS::Batch::JobQueue","Properties":{"Priority":%%s,"ComputeEnvironmentOrder":[{"Order":1,"ComputeEnvironment":{"Ref":"Env"}}]}},
"Def":{"Type":"AWS::Batch::JobDefinition","Properties":{"Type":"container","ContainerProperties":{"Image":"busybox","Vcpus":1,"Memory":128,"Command":["echo","hi"]}}},
"Ou":{"Type":"AWS::Organizations::OrganizationalUnit","Properties":{"ParentId":%q,"Name":"dev"}},
"Scp":{"Type":"AWS::Organizations::Policy","Properties":{"Name":"deny","Type":"SERVICE_CONTROL_POLICY","TargetIds":[{"Ref":"Ou"}],"Content":{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:Delete*","Resource":"*"}]}}},
"DocSg":{"Type":"AWS::DocDB::DBSubnetGroup","Properties":{"DBSubnetGroupDescription":"doc","SubnetIds":[{"Ref":"S1"},{"Ref":"S2"}]}},
"Doc":{"Type":"AWS::DocDB::DBCluster","Properties":{"MasterUsername":"admin","MasterUserPassword":"password123","DBSubnetGroupName":{"Ref":"DocSg"}}},
"DocI":{"Type":"AWS::DocDB::DBInstance","Properties":{"DBClusterIdentifier":{"Ref":"Doc"},"DBInstanceClass":"db.r6g.large"}},
"Nep":{"Type":"AWS::Neptune::DBCluster","Properties":{}}}}`, root)
	st := deploy(t, c, "misc", fmt.Sprintf(tpl, "1"))
	wantStatus(t, c, st, types.StackStatusCreateComplete)
	wantStatus(t, c, redeploy(t, c, "misc", fmt.Sprintf(tpl, "5")), types.StackStatusUpdateComplete)
	remove(t, c, "misc")
}
