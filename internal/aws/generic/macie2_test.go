package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/aws/aws-sdk-go-v2/service/macie2/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestMacie2(t *testing.T) {
	_, base := start(t, "macie2")
	ctx := context.Background()
	c := macie2.NewFromConfig(awstest.Config(t, base))

	if _, err := c.GetMacieSession(ctx, &macie2.GetMacieSessionInput{}); !errors.As(err, new(*types.AccessDeniedException)) {
		t.Fatalf("session before enable: %T %v", err, err)
	}
	if _, err := c.EnableMacie(ctx, &macie2.EnableMacieInput{FindingPublishingFrequency: types.FindingPublishingFrequencyOneHour}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.EnableMacie(ctx, &macie2.EnableMacieInput{}); !errors.As(err, new(*types.ConflictException)) {
		t.Fatalf("enable twice: %T %v", err, err)
	}
	s, err := c.GetMacieSession(ctx, &macie2.GetMacieSessionInput{})
	if err != nil || s.Status != types.MacieStatusEnabled || s.FindingPublishingFrequency != types.FindingPublishingFrequencyOneHour || s.CreatedAt == nil ||
		aws.ToString(s.ServiceRole) != "arn:aws:iam::000000000000:role/aws-service-role/macie.amazonaws.com/AWSServiceRoleForAmazonMacie" {
		t.Fatalf("session: %v %+v", err, s)
	}
	if _, err := c.UpdateMacieSession(ctx, &macie2.UpdateMacieSessionInput{Status: types.MacieStatusPaused}); err != nil {
		t.Fatal(err)
	}
	if s, _ = c.GetMacieSession(ctx, &macie2.GetMacieSessionInput{}); s.Status != types.MacieStatusPaused || s.FindingPublishingFrequency != types.FindingPublishingFrequencyOneHour {
		t.Fatalf("update session: %+v", s)
	}

	// Custom data identifiers: create, get, list, tag, delete.
	cd, err := c.CreateCustomDataIdentifier(ctx, &macie2.CreateCustomDataIdentifierInput{Name: aws.String("badge"), Regex: aws.String(`B-\d{6}`), Description: aws.String("badge ids"),
		Keywords: []string{"badge"}, MaximumMatchDistance: aws.Int32(30), Tags: map[string]string{"env": "dev"}})
	if err != nil || aws.ToString(cd.CustomDataIdentifierId) == "" {
		t.Fatalf("create identifier: %v %+v", err, cd)
	}
	g, err := c.GetCustomDataIdentifier(ctx, &macie2.GetCustomDataIdentifierInput{Id: cd.CustomDataIdentifierId})
	if err != nil || aws.ToString(g.Name) != "badge" || aws.ToString(g.Regex) != `B-\d{6}` || aws.ToString(g.Arn) == "" || g.Tags["env"] != "dev" || aws.ToInt32(g.MaximumMatchDistance) != 30 {
		t.Fatalf("get identifier: %v %+v", err, g)
	}
	l, err := c.ListCustomDataIdentifiers(ctx, &macie2.ListCustomDataIdentifiersInput{})
	if err != nil || len(l.Items) != 1 || aws.ToString(l.Items[0].Id) != aws.ToString(cd.CustomDataIdentifierId) || aws.ToString(l.Items[0].Name) != "badge" {
		t.Fatalf("list identifiers: %v %+v", err, l)
	}
	if _, err := c.TagResource(ctx, &macie2.TagResourceInput{ResourceArn: g.Arn, Tags: map[string]string{"team": "sec"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &macie2.UntagResourceInput{ResourceArn: g.Arn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListTagsForResource(ctx, &macie2.ListTagsForResourceInput{ResourceArn: g.Arn})
	if err != nil || len(tags.Tags) != 1 || tags.Tags["team"] != "sec" {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.DeleteCustomDataIdentifier(ctx, &macie2.DeleteCustomDataIdentifierInput{Id: cd.CustomDataIdentifierId}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetCustomDataIdentifier(ctx, &macie2.GetCustomDataIdentifierInput{Id: cd.CustomDataIdentifierId}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("get after delete: %T %v", err, err)
	}

	// Findings filters and allow lists, with update.
	ff, err := c.CreateFindingsFilter(ctx, &macie2.CreateFindingsFilterInput{Name: aws.String("noise"), Action: types.FindingsFilterActionArchive, Position: aws.Int32(1),
		FindingCriteria: &types.FindingCriteria{Criterion: map[string]types.CriterionAdditionalProperties{"severity.description": {Eq: []string{"Low"}}}}})
	if err != nil || aws.ToString(ff.Id) == "" || aws.ToString(ff.Arn) == "" {
		t.Fatalf("create filter: %v %+v", err, ff)
	}
	if _, err := c.UpdateFindingsFilter(ctx, &macie2.UpdateFindingsFilterInput{Id: ff.Id, Description: aws.String("low severity")}); err != nil {
		t.Fatal(err)
	}
	gf, err := c.GetFindingsFilter(ctx, &macie2.GetFindingsFilterInput{Id: ff.Id})
	if err != nil || aws.ToString(gf.Description) != "low severity" || aws.ToString(gf.Name) != "noise" || len(gf.FindingCriteria.Criterion) != 1 {
		t.Fatalf("get filter: %v %+v", err, gf)
	}
	lf, err := c.ListFindingsFilters(ctx, &macie2.ListFindingsFiltersInput{})
	if err != nil || len(lf.FindingsFilterListItems) != 1 {
		t.Fatalf("list filters: %v %+v", err, lf)
	}
	if _, err := c.DeleteFindingsFilter(ctx, &macie2.DeleteFindingsFilterInput{Id: ff.Id}); err != nil {
		t.Fatal(err)
	}
	al, err := c.CreateAllowList(ctx, &macie2.CreateAllowListInput{Name: aws.String("known"), ClientToken: aws.String("t1"), Criteria: &types.AllowListCriteria{Regex: aws.String("x+")}})
	if err != nil || aws.ToString(al.Id) == "" {
		t.Fatalf("create allow list: %v %+v", err, al)
	}
	if ls, err := c.ListAllowLists(ctx, &macie2.ListAllowListsInput{}); err != nil || len(ls.AllowLists) != 1 || aws.ToString(ls.AllowLists[0].Name) != "known" {
		t.Fatalf("list allow lists: %v %+v", err, ls)
	}
	if _, err := c.DeleteAllowList(ctx, &macie2.DeleteAllowListInput{Id: al.Id}); err != nil {
		t.Fatal(err)
	}

	// Disabling ends the session.
	if _, err := c.DisableMacie(ctx, &macie2.DisableMacieInput{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetMacieSession(ctx, &macie2.GetMacieSessionInput{}); !errors.As(err, new(*types.AccessDeniedException)) {
		t.Fatalf("session after disable: %T %v", err, err)
	}
	if _, err := c.DisableMacie(ctx, &macie2.DisableMacieInput{}); !errors.As(err, new(*types.AccessDeniedException)) {
		t.Fatalf("disable twice: %T %v", err, err)
	}
}
