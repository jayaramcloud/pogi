package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/grafana"
	"github.com/aws/aws-sdk-go-v2/service/grafana/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestGrafana(t *testing.T) {
	_, url := start(t, "grafana")
	ctx := context.Background()
	c := grafana.NewFromConfig(awstest.Config(t, url))

	in := &grafana.CreateWorkspaceInput{WorkspaceName: aws.String("ops"), AccountAccessType: types.AccountAccessTypeCurrentAccount,
		AuthenticationProviders: []types.AuthenticationProviderTypes{types.AuthenticationProviderTypesAwsSso},
		PermissionType:          types.PermissionTypeServiceManaged, Tags: map[string]string{"env": "dev"}}
	w, err := c.CreateWorkspace(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	id := aws.ToString(w.Workspace.Id)
	if id == "" || aws.ToString(w.Workspace.Name) != "ops" || w.Workspace.Status != types.WorkspaceStatusActive {
		t.Fatalf("create: %+v", w.Workspace)
	}
	d, err := c.DescribeWorkspace(ctx, &grafana.DescribeWorkspaceInput{WorkspaceId: aws.String(id)})
	if err != nil || aws.ToString(d.Workspace.Name) != "ops" || len(d.Workspace.Authentication.Providers) != 1 ||
		d.Workspace.Authentication.Providers[0] != types.AuthenticationProviderTypesAwsSso || aws.ToString(d.Workspace.Endpoint) == "" {
		t.Fatalf("describe: %v %+v", err, d)
	}
	u, err := c.UpdateWorkspace(ctx, &grafana.UpdateWorkspaceInput{WorkspaceId: aws.String(id), WorkspaceName: aws.String("ops2")})
	if err != nil || aws.ToString(u.Workspace.Name) != "ops2" {
		t.Fatalf("update: %v %+v", err, u)
	}
	l, err := c.ListWorkspaces(ctx, &grafana.ListWorkspacesInput{})
	if err != nil || len(l.Workspaces) != 1 || aws.ToString(l.Workspaces[0].Name) != "ops2" {
		t.Fatalf("list: %v %+v", err, l)
	}
	tags, err := c.ListTagsForResource(ctx, &grafana.ListTagsForResourceInput{ResourceArn: aws.String("arn:aws:grafana:us-east-1:000000000000:/workspaces/" + id)})
	if err != nil || tags.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	// failures
	if _, err := c.CreateWorkspace(ctx, &grafana.CreateWorkspaceInput{WorkspaceName: aws.String("bare")}); err == nil {
		t.Fatal("workspace without an access type succeeded")
	}
	bad := *in
	bad.AccountAccessType = "BOGUS"
	if _, err := c.CreateWorkspace(ctx, &bad); err == nil {
		t.Fatal("bad access type succeeded")
	}
	if _, err := c.DescribeWorkspace(ctx, &grafana.DescribeWorkspaceInput{WorkspaceId: aws.String("g-0000000000")}); err == nil {
		t.Fatal("unknown workspace described")
	}
	if _, err := c.DeleteWorkspace(ctx, &grafana.DeleteWorkspaceInput{WorkspaceId: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeWorkspace(ctx, &grafana.DescribeWorkspaceInput{WorkspaceId: aws.String(id)}); err == nil {
		t.Fatal("deleted workspace described")
	}
	if _, err := c.DeleteWorkspace(ctx, &grafana.DeleteWorkspaceInput{WorkspaceId: aws.String(id)}); err == nil {
		t.Fatal("second delete succeeded")
	}
}
