package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appflow"
	"github.com/aws/aws-sdk-go-v2/service/appflow/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestAppFlow(t *testing.T) {
	_, url := start(t, "appflow")
	ctx := context.Background()
	c := appflow.NewFromConfig(awstest.Config(t, url))

	// an S3 to S3 flow
	flowIn := &appflow.CreateFlowInput{
		FlowName: aws.String("copy"),
		SourceFlowConfig: &types.SourceFlowConfig{ConnectorType: types.ConnectorTypeS3,
			SourceConnectorProperties: &types.SourceConnectorProperties{S3: &types.S3SourceProperties{BucketName: aws.String("src"), BucketPrefix: aws.String("in")}}},
		DestinationFlowConfigList: []types.DestinationFlowConfig{{ConnectorType: types.ConnectorTypeS3,
			DestinationConnectorProperties: &types.DestinationConnectorProperties{S3: &types.S3DestinationProperties{BucketName: aws.String("dst")}}}},
		Tasks:         []types.Task{{TaskType: types.TaskTypeMapAll, SourceFields: []string{}, ConnectorOperator: &types.ConnectorOperator{S3: types.S3ConnectorOperatorNoOp}}},
		TriggerConfig: &types.TriggerConfig{TriggerType: types.TriggerTypeOndemand},
		Tags:          map[string]string{"env": "dev"},
	}
	f, err := c.CreateFlow(ctx, flowIn)
	if err != nil || aws.ToString(f.FlowArn) == "" {
		t.Fatalf("create flow: %v %+v", err, f)
	}
	if _, err := c.CreateFlow(ctx, flowIn); err == nil {
		t.Fatal("duplicate flow succeeded")
	}
	if _, err := c.CreateFlow(ctx, &appflow.CreateFlowInput{FlowName: aws.String("bare")}); err == nil {
		t.Fatal("flow without a source succeeded")
	}
	d, err := c.DescribeFlow(ctx, &appflow.DescribeFlowInput{FlowName: aws.String("copy")})
	if err != nil || aws.ToString(d.FlowName) != "copy" || d.FlowStatus != types.FlowStatusActive ||
		aws.ToString(d.DestinationFlowConfigList[0].DestinationConnectorProperties.S3.BucketName) != "dst" {
		t.Fatalf("describe: %v %+v", err, d)
	}
	l, err := c.ListFlows(ctx, &appflow.ListFlowsInput{})
	if err != nil || len(l.Flows) != 1 || aws.ToString(l.Flows[0].FlowName) != "copy" {
		t.Fatalf("list: %v %+v", err, l)
	}
	tags, err := c.ListTagsForResource(ctx, &appflow.ListTagsForResourceInput{ResourceArn: f.FlowArn})
	if err != nil || tags.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, tags)
	}

	// a connector profile: the properties read back, the credentials do not
	pIn := &appflow.CreateConnectorProfileInput{
		ConnectorProfileName: aws.String("slack"), ConnectorType: types.ConnectorTypeSlack, ConnectionMode: types.ConnectionModePublic,
		ConnectorProfileConfig: &types.ConnectorProfileConfig{
			ConnectorProfileProperties: &types.ConnectorProfileProperties{Slack: &types.SlackConnectorProfileProperties{InstanceUrl: aws.String("https://example.slack.com")}},
			ConnectorProfileCredentials: &types.ConnectorProfileCredentials{Slack: &types.SlackConnectorProfileCredentials{
				ClientId: aws.String("id"), ClientSecret: aws.String("secret"), AccessToken: aws.String("token")}}},
	}
	if _, err := c.CreateConnectorProfile(ctx, pIn); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateConnectorProfile(ctx, pIn); err == nil {
		t.Fatal("duplicate profile succeeded")
	}
	ps, err := c.DescribeConnectorProfiles(ctx, &appflow.DescribeConnectorProfilesInput{ConnectorProfileNames: []string{"slack"}})
	if err != nil || len(ps.ConnectorProfileDetails) != 1 {
		t.Fatalf("describe profiles: %v %+v", err, ps)
	}
	if p := ps.ConnectorProfileDetails[0]; p.ConnectorProfileProperties == nil || aws.ToString(p.ConnectorProfileProperties.Slack.InstanceUrl) != "https://example.slack.com" {
		t.Fatalf("properties: %+v", p)
	}

	// a connector you register is a custom one
	if _, err := c.DescribeConnector(ctx, &appflow.DescribeConnectorInput{ConnectorType: types.ConnectorTypeCustomconnector}); err == nil {
		t.Fatal("describe of an unregistered connector succeeded")
	}
	if _, err := c.RegisterConnector(ctx, &appflow.RegisterConnectorInput{ConnectorLabel: aws.String("mine")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UnregisterConnector(ctx, &appflow.UnregisterConnectorInput{ConnectorLabel: aws.String("other")}); err == nil {
		t.Fatal("unregister of another label succeeded")
	}
	if _, err := c.UnregisterConnector(ctx, &appflow.UnregisterConnectorInput{ConnectorLabel: aws.String("mine")}); err != nil {
		t.Fatal(err)
	}

	// deletes, then not found
	if _, err := c.DeleteFlow(ctx, &appflow.DeleteFlowInput{FlowName: aws.String("copy")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeFlow(ctx, &appflow.DescribeFlowInput{FlowName: aws.String("copy")}); err == nil {
		t.Fatal("deleted flow still described")
	}
	if _, err := c.DeleteConnectorProfile(ctx, &appflow.DeleteConnectorProfileInput{ConnectorProfileName: aws.String("slack")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteConnectorProfile(ctx, &appflow.DeleteConnectorProfileInput{ConnectorProfileName: aws.String("slack")}); err == nil {
		t.Fatal("second profile delete succeeded")
	}
}
