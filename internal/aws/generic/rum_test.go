package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rum"
	"github.com/aws/aws-sdk-go-v2/service/rum/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestRUM(t *testing.T) {
	_, url := start(t, "rum")
	ctx := context.Background()
	c := rum.NewFromConfig(awstest.Config(t, url))

	cr, err := c.CreateAppMonitor(ctx, &rum.CreateAppMonitorInput{Name: aws.String("web"), Domain: aws.String("example.com"), Tags: map[string]string{"env": "dev"}})
	if err != nil || aws.ToString(cr.Id) == "" {
		t.Fatalf("create: %v %+v", err, cr)
	}
	if _, err := c.CreateAppMonitor(ctx, &rum.CreateAppMonitorInput{Name: aws.String("web"), Domain: aws.String("example.com")}); err == nil {
		t.Fatal("a second monitor with the same name")
	}
	g, err := c.GetAppMonitor(ctx, &rum.GetAppMonitorInput{Name: aws.String("web")})
	if err != nil {
		t.Fatal(err)
	}
	m := g.AppMonitor
	if aws.ToString(m.Name) != "web" || aws.ToString(m.Domain) != "example.com" || aws.ToString(m.Id) != aws.ToString(cr.Id) || m.State != types.StateEnum("ACTIVE") ||
		m.Tags["env"] != "dev" || aws.ToString(m.Created) == "" {
		t.Fatalf("monitor: %+v", m)
	}
	if _, err := c.UpdateAppMonitor(ctx, &rum.UpdateAppMonitorInput{Name: aws.String("web"), Domain: aws.String("example.org"), AppMonitorConfiguration: &types.AppMonitorConfiguration{SessionSampleRate: 0.5}}); err != nil {
		t.Fatal(err)
	}
	g, _ = c.GetAppMonitor(ctx, &rum.GetAppMonitorInput{Name: aws.String("web")})
	if aws.ToString(g.AppMonitor.Domain) != "example.org" || g.AppMonitor.AppMonitorConfiguration == nil || g.AppMonitor.AppMonitorConfiguration.SessionSampleRate != 0.5 {
		t.Fatalf("updated: %+v", g.AppMonitor)
	}
	l, err := c.ListAppMonitors(ctx, &rum.ListAppMonitorsInput{})
	if err != nil || len(l.AppMonitorSummaries) != 1 || aws.ToString(l.AppMonitorSummaries[0].Name) != "web" {
		t.Fatalf("list: %v %+v", err, l)
	}
	arn := "arn:aws:rum:us-east-1:000000000000:appmonitor/web"
	if _, err := c.TagResource(ctx, &rum.TagResourceInput{ResourceArn: aws.String(arn), Tags: map[string]string{"a": "b"}}); err != nil {
		t.Fatal(err)
	}
	tg, err := c.ListTagsForResource(ctx, &rum.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	if err != nil || tg.Tags["a"] != "b" || tg.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.UntagResource(ctx, &rum.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tg, _ = c.ListTagsForResource(ctx, &rum.ListTagsForResourceInput{ResourceArn: aws.String(arn)}); len(tg.Tags) != 1 {
		t.Fatalf("tags after untag: %+v", tg.Tags)
	}
	if _, err := c.DeleteAppMonitor(ctx, &rum.DeleteAppMonitorInput{Name: aws.String("web")}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err = c.GetAppMonitor(ctx, &rum.GetAppMonitorInput{Name: aws.String("web")}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
}
