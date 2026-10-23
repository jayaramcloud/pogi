package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/workspacesweb"
	"github.com/aws/aws-sdk-go-v2/service/workspacesweb/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestWorkSpacesWeb(t *testing.T) {
	_, url := start(t, "workspaces-web")
	ctx := context.Background()
	c := workspacesweb.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreatePortal(ctx, &workspacesweb.CreatePortalInput{DisplayName: aws.String("browser"), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil || aws.ToString(o.PortalArn) == "" {
		t.Fatalf("create: %v %+v", err, o)
	}
	g, err := c.GetPortal(ctx, &workspacesweb.GetPortalInput{PortalArn: o.PortalArn})
	if err != nil || g.Portal == nil || aws.ToString(g.Portal.DisplayName) != "browser" {
		t.Fatalf("get: %v %+v", err, g)
	}
	if _, err := c.UpdatePortal(ctx, &workspacesweb.UpdatePortalInput{PortalArn: o.PortalArn, DisplayName: aws.String("browser2")}); err != nil {
		t.Fatal(err)
	}
	l, err := c.ListPortals(ctx, &workspacesweb.ListPortalsInput{})
	if err != nil || len(l.Portals) != 1 || aws.ToString(l.Portals[0].DisplayName) != "browser2" {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.GetPortal(ctx, &workspacesweb.GetPortalInput{PortalArn: aws.String("arn:aws:workspaces-web:us-east-1:000000000000:portal/nope")}); err == nil {
		t.Fatal("unknown portal found")
	}
	if _, err := c.DeletePortal(ctx, &workspacesweb.DeletePortalInput{PortalArn: o.PortalArn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetPortal(ctx, &workspacesweb.GetPortalInput{PortalArn: o.PortalArn}); err == nil {
		t.Fatal("deleted portal found")
	}
}
