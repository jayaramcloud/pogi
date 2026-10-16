package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appintegrations"
	"github.com/aws/aws-sdk-go-v2/service/appintegrations/types"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestAppIntegrations(t *testing.T) {
	s := pogi.Start(t)
	ctx := context.Background()
	c := appintegrations.NewFromConfig(awstest.Config(t, s.URL))
	var nf *types.ResourceNotFoundException

	// event integration: create, get, list, update, tag, delete
	ei, err := c.CreateEventIntegration(ctx, &appintegrations.CreateEventIntegrationInput{Name: aws.String("ei1"), EventBridgeBus: aws.String("default"),
		EventFilter: &types.EventFilter{Source: aws.String("aws.partner/example")}, Description: aws.String("first"), Tags: map[string]string{"a": "b"}})
	if err != nil || !strings.HasSuffix(aws.ToString(ei.EventIntegrationArn), ":event-integration/ei1") {
		t.Fatalf("create: %v %+v", err, ei)
	}
	if _, err := c.CreateEventIntegration(ctx, &appintegrations.CreateEventIntegrationInput{Name: aws.String("ei1"), EventBridgeBus: aws.String("default"), EventFilter: &types.EventFilter{Source: aws.String("x")}}); err == nil {
		t.Fatal("duplicate event integration")
	}
	g, err := c.GetEventIntegration(ctx, &appintegrations.GetEventIntegrationInput{Name: aws.String("ei1")})
	if err != nil || aws.ToString(g.EventBridgeBus) != "default" || aws.ToString(g.EventFilter.Source) != "aws.partner/example" || g.Tags["a"] != "b" || aws.ToString(g.Description) != "first" {
		t.Fatalf("get: %v %+v", err, g)
	}
	if l, err := c.ListEventIntegrations(ctx, &appintegrations.ListEventIntegrationsInput{}); err != nil || len(l.EventIntegrations) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.UpdateEventIntegration(ctx, &appintegrations.UpdateEventIntegrationInput{Name: aws.String("ei1"), Description: aws.String("second")}); err != nil {
		t.Fatal(err)
	}
	if g, _ := c.GetEventIntegration(ctx, &appintegrations.GetEventIntegrationInput{Name: aws.String("ei1")}); aws.ToString(g.Description) != "second" {
		t.Fatalf("updated: %+v", g)
	}
	arn := aws.ToString(ei.EventIntegrationArn)
	if _, err := c.TagResource(ctx, &appintegrations.TagResourceInput{ResourceArn: aws.String(arn), Tags: map[string]string{"c": "d"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &appintegrations.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	if l, err := c.ListTagsForResource(ctx, &appintegrations.ListTagsForResourceInput{ResourceArn: aws.String(arn)}); err != nil || len(l.Tags) != 1 || l.Tags["c"] != "d" {
		t.Fatalf("tags: %v %+v", err, l)
	}

	// data integration
	di, err := c.CreateDataIntegration(ctx, &appintegrations.CreateDataIntegrationInput{Name: aws.String("di1"), KmsKey: aws.String("alias/x"), SourceURI: aws.String("Salesforce://AppFlow/x"),
		ScheduleConfig: &types.ScheduleConfiguration{ScheduleExpression: aws.String("rate(1 hour)"), FirstExecutionFrom: aws.String("1/1/2026")}})
	if err != nil || aws.ToString(di.Name) != "di1" || aws.ToString(di.Id) == "" {
		t.Fatalf("data integration: %v %+v", err, di)
	}
	if g, err := c.GetDataIntegration(ctx, &appintegrations.GetDataIntegrationInput{Identifier: di.Id}); err != nil || aws.ToString(g.SourceURI) != "Salesforce://AppFlow/x" {
		t.Fatalf("get data integration: %v %+v", err, g)
	}
	if _, err := c.UpdateDataIntegration(ctx, &appintegrations.UpdateDataIntegrationInput{Identifier: di.Id, Description: aws.String("synced")}); err != nil {
		t.Fatal(err)
	}
	if g, _ := c.GetDataIntegration(ctx, &appintegrations.GetDataIntegrationInput{Identifier: di.Id}); aws.ToString(g.Description) != "synced" {
		t.Fatalf("updated data integration: %+v", g)
	}
	if l, err := c.ListDataIntegrations(ctx, &appintegrations.ListDataIntegrationsInput{}); err != nil || len(l.DataIntegrations) != 1 {
		t.Fatalf("list data integrations: %v %+v", err, l)
	}
	if _, err := c.DeleteDataIntegration(ctx, &appintegrations.DeleteDataIntegrationInput{DataIntegrationIdentifier: di.Id}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetDataIntegration(ctx, &appintegrations.GetDataIntegrationInput{Identifier: di.Id}); !errors.As(err, &nf) {
		t.Fatalf("get deleted data integration: %v", err)
	}

	// application
	app, err := c.CreateApplication(ctx, &appintegrations.CreateApplicationInput{Name: aws.String("app1"), Namespace: aws.String("ns1"),
		ApplicationSourceConfig: &types.ApplicationSourceConfig{ExternalUrlConfig: &types.ExternalUrlConfig{AccessUrl: aws.String("https://app.example.com")}}})
	if err != nil || aws.ToString(app.Arn) == "" {
		t.Fatalf("application: %v %+v", err, app)
	}
	if g, err := c.GetApplication(ctx, &appintegrations.GetApplicationInput{Arn: app.Arn}); err != nil || aws.ToString(g.Name) != "app1" || aws.ToString(g.Namespace) != "ns1" {
		t.Fatalf("get application: %v %+v", err, g)
	}
	if _, err := c.DeleteApplication(ctx, &appintegrations.DeleteApplicationInput{Arn: app.Arn}); err != nil {
		t.Fatal(err)
	}

	// delete, then the typed not-found error
	if _, err := c.DeleteEventIntegration(ctx, &appintegrations.DeleteEventIntegrationInput{Name: aws.String("ei1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetEventIntegration(ctx, &appintegrations.GetEventIntegrationInput{Name: aws.String("ei1")}); !errors.As(err, &nf) {
		t.Fatalf("get deleted: %v", err)
	}
}
