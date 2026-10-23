package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/aws/aws-sdk-go-v2/service/codecommit/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestCodeCommit(t *testing.T) {
	_, url := start(t, "codecommit")
	ctx := context.Background()
	c := codecommit.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateRepository(ctx, &codecommit.CreateRepositoryInput{RepositoryName: aws.String("app"), RepositoryDescription: aws.String("d"), Tags: map[string]string{"env": "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	m := o.RepositoryMetadata
	if aws.ToString(m.CloneUrlHttp) != "https://git-codecommit.us-east-1.amazonaws.com/v1/repos/app" ||
		aws.ToString(m.CloneUrlSsh) != "ssh://git-codecommit.us-east-1.amazonaws.com/v1/repos/app" ||
		aws.ToString(m.RepositoryId) == "" || aws.ToString(m.Arn) != "arn:aws:codecommit:us-east-1:000000000000:app" || m.CreationDate == nil {
		t.Fatalf("metadata: %+v", m)
	}
	_, err = c.CreateRepository(ctx, &codecommit.CreateRepositoryInput{RepositoryName: aws.String("app")})
	var ex *types.RepositoryNameExistsException
	if !errors.As(err, &ex) {
		t.Fatalf("duplicate: %T %v", err, err)
	}
	g, err := c.GetRepository(ctx, &codecommit.GetRepositoryInput{RepositoryName: aws.String("app")})
	if err != nil || aws.ToString(g.RepositoryMetadata.RepositoryDescription) != "d" {
		t.Fatalf("get: %v %+v", err, g)
	}
	tg, err := c.ListTagsForResource(ctx, &codecommit.ListTagsForResourceInput{ResourceArn: m.Arn})
	if err != nil || tg.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.UpdateRepositoryDescription(ctx, &codecommit.UpdateRepositoryDescriptionInput{RepositoryName: aws.String("app"), RepositoryDescription: aws.String("e")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateRepository(ctx, &codecommit.CreateRepositoryInput{RepositoryName: aws.String("lib")}); err != nil {
		t.Fatal(err)
	}
	l, err := c.ListRepositories(ctx, &codecommit.ListRepositoriesInput{})
	if err != nil || len(l.Repositories) != 2 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.UpdateRepositoryName(ctx, &codecommit.UpdateRepositoryNameInput{OldName: aws.String("lib"), NewName: aws.String("lib2")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetRepository(ctx, &codecommit.GetRepositoryInput{RepositoryName: aws.String("lib")}); err == nil {
		t.Fatal("old name still found")
	}

	d, err := c.DeleteRepository(ctx, &codecommit.DeleteRepositoryInput{RepositoryName: aws.String("app")})
	if err != nil || aws.ToString(d.RepositoryId) != aws.ToString(m.RepositoryId) {
		t.Fatalf("delete: %v %+v", err, d)
	}
	var nf *types.RepositoryDoesNotExistException
	if _, err := c.GetRepository(ctx, &codecommit.GetRepositoryInput{RepositoryName: aws.String("app")}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	// real CodeCommit answers a repeated delete with no repository id, not an error
	d, err = c.DeleteRepository(ctx, &codecommit.DeleteRepositoryInput{RepositoryName: aws.String("app")})
	if err != nil || d.RepositoryId != nil {
		t.Fatalf("second delete: %v %+v", err, d)
	}
	if _, err := c.CreateRepository(ctx, &codecommit.CreateRepositoryInput{RepositoryName: aws.String("bad name!")}); err == nil {
		t.Fatal("bad name accepted")
	}
	if _, err := c.CreateRepository(ctx, &codecommit.CreateRepositoryInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}
