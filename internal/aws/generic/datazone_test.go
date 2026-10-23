package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/datazone"
	"github.com/aws/aws-sdk-go-v2/service/datazone/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestDataZone(t *testing.T) {
	_, url := start(t, "datazone")
	ctx := context.Background()
	c := datazone.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateDomain(ctx, &datazone.CreateDomainInput{Name: aws.String("dom"), DomainExecutionRole: aws.String("arn:aws:iam::000000000000:role/dz"), Tags: map[string]string{"env": "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(aws.ToString(o.Id), "dzd_") || o.Status != types.DomainStatusAvailable {
		t.Fatalf("domain: %+v", o)
	}
	g, err := c.GetDomain(ctx, &datazone.GetDomainInput{Identifier: o.Id})
	if err != nil || aws.ToString(g.Name) != "dom" || g.Status != types.DomainStatusAvailable {
		t.Fatalf("get: %v %+v", err, g)
	}
	p, err := c.CreateProject(ctx, &datazone.CreateProjectInput{DomainIdentifier: o.Id, Name: aws.String("proj")})
	if err != nil || aws.ToString(p.Name) != "proj" || aws.ToString(p.Id) == "" {
		t.Fatalf("project: %v %+v", err, p)
	}
	pl, err := c.ListProjects(ctx, &datazone.ListProjectsInput{DomainIdentifier: o.Id})
	if err != nil || len(pl.Items) != 1 {
		t.Fatalf("projects: %v %+v", err, pl)
	}
	l, err := c.ListDomains(ctx, &datazone.ListDomainsInput{})
	if err != nil || len(l.Items) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.DeleteDomain(ctx, &datazone.DeleteDomainInput{Identifier: o.Id, SkipDeletionCheck: aws.Bool(true)}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetDomain(ctx, &datazone.GetDomainInput{Identifier: o.Id}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.CreateDomain(ctx, &datazone.CreateDomainInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}
