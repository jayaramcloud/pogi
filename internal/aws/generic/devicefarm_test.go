package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/devicefarm"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestDeviceFarm(t *testing.T) {
	_, url := start(t, "devicefarm")
	ctx := context.Background()
	c := devicefarm.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateProject(ctx, &devicefarm.CreateProjectInput{Name: aws.String("app"), DefaultJobTimeoutMinutes: aws.Int32(30)})
	if err != nil || o.Project == nil || aws.ToString(o.Project.Arn) == "" || aws.ToString(o.Project.Name) != "app" {
		t.Fatalf("create: %v %+v", err, o)
	}
	arn := o.Project.Arn
	g, err := c.GetProject(ctx, &devicefarm.GetProjectInput{Arn: arn})
	if err != nil || aws.ToInt32(g.Project.DefaultJobTimeoutMinutes) != 30 {
		t.Fatalf("get: %v %+v", err, g)
	}
	if _, err := c.UpdateProject(ctx, &devicefarm.UpdateProjectInput{Arn: arn, Name: aws.String("app2")}); err != nil {
		t.Fatal(err)
	}
	l, err := c.ListProjects(ctx, &devicefarm.ListProjectsInput{})
	if err != nil || len(l.Projects) != 1 || aws.ToString(l.Projects[0].Name) != "app2" {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.CreateProject(ctx, &devicefarm.CreateProjectInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
	if _, err := c.GetProject(ctx, &devicefarm.GetProjectInput{Arn: aws.String("arn:aws:devicefarm:us-west-2:000000000000:project:nope")}); err == nil {
		t.Fatal("unknown project found")
	}
	if _, err := c.DeleteProject(ctx, &devicefarm.DeleteProjectInput{Arn: arn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetProject(ctx, &devicefarm.GetProjectInput{Arn: arn}); err == nil {
		t.Fatal("deleted project found")
	}
}
