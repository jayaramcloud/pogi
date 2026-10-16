package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact/types"
	"github.com/nrndev/pogi/internal/aws/generic"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

func start(t *testing.T, names ...string) (*core.Hub, string) {
	t.Helper()
	var builds []func(*core.Hub) core.Service
	for _, n := range names {
		builds = append(builds, func(h *core.Hub) core.Service { return generic.New(h, n) })
	}
	return core.ServeTest(t, builds...)
}

func TestCodeArtifact(t *testing.T) {
	_, url := start(t, "codeartifact")
	ctx := context.Background()
	c := codeartifact.NewFromConfig(awstest.Config(t, url))

	d, err := c.CreateDomain(ctx, &codeartifact.CreateDomainInput{Domain: aws.String("acme"), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	arn := aws.ToString(d.Domain.Arn)
	if aws.ToString(d.Domain.Name) != "acme" || arn == "" || d.Domain.Status != types.DomainStatusActive {
		t.Fatalf("domain: %+v", d.Domain)
	}
	if _, err := c.CreateDomain(ctx, &codeartifact.CreateDomainInput{Domain: aws.String("acme")}); err == nil {
		t.Fatal("duplicate create succeeded")
	}
	g, err := c.DescribeDomain(ctx, &codeartifact.DescribeDomainInput{Domain: aws.String("acme")})
	if err != nil || aws.ToString(g.Domain.Arn) != arn {
		t.Fatalf("describe: %v %+v", err, g)
	}
	for i := range 3 {
		if _, err := c.CreateRepository(ctx, &codeartifact.CreateRepositoryInput{Domain: aws.String("acme"), Repository: aws.String("r" + string(rune('a'+i))), Description: aws.String("repo")}); err != nil {
			t.Fatal(err)
		}
	}
	var names []string
	p := codeartifact.NewListRepositoriesInDomainPaginator(c, &codeartifact.ListRepositoriesInDomainInput{Domain: aws.String("acme"), MaxResults: aws.Int32(2)})
	pages := 0
	for p.HasMorePages() {
		pages++
		out, err := p.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range out.Repositories {
			names = append(names, aws.ToString(r.Name))
		}
	}
	if pages != 2 || len(names) != 3 {
		t.Fatalf("pages %d names %v", pages, names)
	}
	tags, err := c.ListTagsForResource(ctx, &codeartifact.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	if err != nil || len(tags.Tags) != 1 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.TagResource(ctx, &codeartifact.TagResourceInput{ResourceArn: aws.String(arn), Tags: []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &codeartifact.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	tags, _ = c.ListTagsForResource(ctx, &codeartifact.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	if len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "a" {
		t.Fatalf("tags after: %+v", tags.Tags)
	}
	if _, err := c.DeleteRepository(ctx, &codeartifact.DeleteRepositoryInput{Domain: aws.String("acme"), Repository: aws.String("ra")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeRepository(ctx, &codeartifact.DescribeRepositoryInput{Domain: aws.String("acme"), Repository: aws.String("ra")})
	var nf *types.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.DeleteDomain(ctx, &codeartifact.DeleteDomainInput{Domain: aws.String("acme")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeDomain(ctx, &codeartifact.DescribeDomainInput{Domain: aws.String("acme")})
	if !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	_, err = c.CreateDomain(ctx, &codeartifact.CreateDomainInput{})
	if err == nil {
		t.Fatal("empty create succeeded")
	}
	t.Log(err)
}
