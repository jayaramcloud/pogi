package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/amplify"
	"github.com/aws/aws-sdk-go-v2/service/amplify/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestAmplify(t *testing.T) {
	_, url := start(t, "amplify")
	ctx := context.Background()
	c := amplify.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateApp(ctx, &amplify.CreateAppInput{Name: aws.String("site"), Tags: map[string]string{"env": "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	a := o.App
	id := aws.ToString(a.AppId)
	if id == "" || aws.ToString(a.DefaultDomain) != id+".amplifyapp.com" || !strings.HasSuffix(aws.ToString(a.AppArn), ":apps/"+id) {
		t.Fatalf("app: id %q domain %q arn %q", id, aws.ToString(a.DefaultDomain), aws.ToString(a.AppArn))
	}
	if a.Tags["env"] != "dev" {
		t.Fatalf("tags: %v", a.Tags)
	}
	if _, err := c.UpdateApp(ctx, &amplify.UpdateAppInput{AppId: aws.String(id), Description: aws.String("new")}); err != nil {
		t.Fatal(err)
	}
	g, err := c.GetApp(ctx, &amplify.GetAppInput{AppId: aws.String(id)})
	if err != nil || aws.ToString(g.App.Description) != "new" {
		t.Fatalf("get: %v %+v", err, g)
	}

	b, err := c.CreateBranch(ctx, &amplify.CreateBranchInput{AppId: aws.String(id), BranchName: aws.String("main")})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(aws.ToString(b.Branch.BranchArn), ":apps/"+id+"/branches/main") {
		t.Fatalf("branch arn: %q", aws.ToString(b.Branch.BranchArn))
	}
	if _, err := c.CreateBranch(ctx, &amplify.CreateBranchInput{AppId: aws.String(id), BranchName: aws.String("main")}); err == nil {
		t.Fatal("duplicate branch succeeded")
	}
	if _, err := c.CreateBranch(ctx, &amplify.CreateBranchInput{AppId: aws.String("dnoapp"), BranchName: aws.String("x")}); err == nil {
		t.Fatal("branch of a missing app succeeded")
	}
	bl, err := c.ListBranches(ctx, &amplify.ListBranchesInput{AppId: aws.String(id)})
	if err != nil || len(bl.Branches) != 1 {
		t.Fatalf("list branches: %v %+v", err, bl)
	}
	al, err := c.ListApps(ctx, &amplify.ListAppsInput{})
	if err != nil || len(al.Apps) != 1 {
		t.Fatalf("list apps: %v %+v", err, al)
	}
	if _, err := c.DeleteBranch(ctx, &amplify.DeleteBranchInput{AppId: aws.String(id), BranchName: aws.String("main")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteApp(ctx, &amplify.DeleteAppInput{AppId: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	var nf *types.NotFoundException
	if _, err := c.GetApp(ctx, &amplify.GetAppInput{AppId: aws.String(id)}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.DeleteApp(ctx, &amplify.DeleteAppInput{AppId: aws.String(id)}); !errors.As(err, &nf) {
		t.Fatalf("second delete: %T %v", err, err)
	}
	if _, err := c.CreateApp(ctx, &amplify.CreateAppInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}
