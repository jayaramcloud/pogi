package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/codegurureviewer"
	"github.com/aws/aws-sdk-go-v2/service/codegurureviewer/types"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestCodeGuruReviewer(t *testing.T) {
	s := pogi.Start(t)
	ctx := context.Background()
	c := codegurureviewer.NewFromConfig(awstest.Config(t, s.URL))
	lifecycle(t, time.Hour)

	a, err := c.AssociateRepository(ctx, &codegurureviewer.AssociateRepositoryInput{Repository: &types.Repository{CodeCommit: &types.CodeCommitRepository{Name: aws.String("shop")}},
		Tags: map[string]string{"team": "web"}})
	if err != nil {
		t.Fatal(err)
	}
	ra := a.RepositoryAssociation
	if aws.ToString(ra.Name) != "shop" || ra.ProviderType != types.ProviderTypeCodeCommit || ra.State != types.RepositoryAssociationStateAssociating || aws.ToString(ra.Owner) != "000000000000" ||
		!strings.Contains(aws.ToString(ra.AssociationArn), ":association:"+aws.ToString(ra.AssociationId)) || a.Tags["team"] != "web" {
		t.Fatalf("associate: %+v", a)
	}
	arn := ra.AssociationArn
	if _, err := c.AssociateRepository(ctx, &codegurureviewer.AssociateRepositoryInput{Repository: &types.Repository{CodeCommit: &types.CodeCommitRepository{Name: aws.String("shop")}}}); err == nil {
		t.Fatal("associated twice")
	}
	lifecycle(t, 0)
	d, err := c.DescribeRepositoryAssociation(ctx, &codegurureviewer.DescribeRepositoryAssociationInput{AssociationArn: arn})
	if err != nil || d.RepositoryAssociation.State != types.RepositoryAssociationStateAssociated || d.Tags["team"] != "web" {
		t.Fatalf("describe: %v %+v", err, d)
	}
	if _, err := c.AssociateRepository(ctx, &codegurureviewer.AssociateRepositoryInput{Repository: &types.Repository{S3Bucket: &types.S3Repository{Name: aws.String("src-bucket"), BucketName: aws.String("src-bucket")}}}); err != nil {
		t.Fatal(err)
	}
	l, err := c.ListRepositoryAssociations(ctx, &codegurureviewer.ListRepositoryAssociationsInput{ProviderTypes: []types.ProviderType{types.ProviderTypeCodeCommit}})
	if err != nil || len(l.RepositoryAssociationSummaries) != 1 || aws.ToString(l.RepositoryAssociationSummaries[0].Name) != "shop" {
		t.Fatalf("list: %v %+v", err, l)
	}
	if l, _ := c.ListRepositoryAssociations(ctx, &codegurureviewer.ListRepositoryAssociationsInput{}); len(l.RepositoryAssociationSummaries) != 2 {
		t.Fatalf("list all: %+v", l)
	}
	if _, err := c.TagResource(ctx, &codegurureviewer.TagResourceInput{ResourceArn: arn, Tags: map[string]string{"env": "dev"}}); err != nil {
		t.Fatal(err)
	}
	if tg, err := c.ListTagsForResource(ctx, &codegurureviewer.ListTagsForResourceInput{ResourceArn: arn}); err != nil || len(tg.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, tg)
	}

	// a code review needs the association
	cr, err := c.CreateCodeReview(ctx, &codegurureviewer.CreateCodeReviewInput{Name: aws.String("review1"), RepositoryAssociationArn: arn,
		Type: &types.CodeReviewType{RepositoryAnalysis: &types.RepositoryAnalysis{RepositoryHead: &types.RepositoryHeadSourceCodeType{BranchName: aws.String("main")}}}})
	if err != nil || aws.ToString(cr.CodeReview.Name) != "review1" || aws.ToString(cr.CodeReview.RepositoryName) != "shop" || aws.ToString(cr.CodeReview.CodeReviewArn) == "" {
		t.Fatalf("code review: %v %+v", err, cr)
	}
	dr, err := c.DescribeCodeReview(ctx, &codegurureviewer.DescribeCodeReviewInput{CodeReviewArn: cr.CodeReview.CodeReviewArn})
	if err != nil || dr.CodeReview.State != types.JobStateCompleted {
		t.Fatalf("describe review: %v %+v", err, dr)
	}
	if _, err := c.CreateCodeReview(ctx, &codegurureviewer.CreateCodeReviewInput{Name: aws.String("bad"), RepositoryAssociationArn: aws.String("arn:aws:codeguru-reviewer:us-east-1:000000000000:association:nope"),
		Type: &types.CodeReviewType{RepositoryAnalysis: &types.RepositoryAnalysis{RepositoryHead: &types.RepositoryHeadSourceCodeType{BranchName: aws.String("main")}}}}); err == nil {
		t.Fatal("review of an unknown association")
	}
	if rl, err := c.ListCodeReviews(ctx, &codegurureviewer.ListCodeReviewsInput{Type: types.TypeRepositoryAnalysis}); err != nil || len(rl.CodeReviewSummaries) != 1 {
		t.Fatalf("list reviews: %v %+v", err, rl)
	}
	if rec, err := c.ListRecommendations(ctx, &codegurureviewer.ListRecommendationsInput{CodeReviewArn: cr.CodeReview.CodeReviewArn}); err != nil || len(rec.RecommendationSummaries) != 0 {
		t.Fatalf("recommendations: %v %+v", err, rec)
	}

	// disassociate, then the typed not-found error
	di, err := c.DisassociateRepository(ctx, &codegurureviewer.DisassociateRepositoryInput{AssociationArn: arn})
	if err != nil || di.RepositoryAssociation.State != types.RepositoryAssociationStateDisassociating {
		t.Fatalf("disassociate: %v %+v", err, di)
	}
	var nf *types.NotFoundException
	if _, err := c.DescribeRepositoryAssociation(ctx, &codegurureviewer.DescribeRepositoryAssociationInput{AssociationArn: arn}); !errors.As(err, &nf) {
		t.Fatalf("describe disassociated: %v", err)
	}
	if _, err := c.DisassociateRepository(ctx, &codegurureviewer.DisassociateRepositoryInput{AssociationArn: arn}); !errors.As(err, &nf) {
		t.Fatalf("disassociate twice: %v", err)
	}
}
