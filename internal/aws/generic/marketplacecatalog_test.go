package generic_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/marketplacecatalog"
	"github.com/aws/aws-sdk-go-v2/service/marketplacecatalog/document"
	"github.com/aws/aws-sdk-go-v2/service/marketplacecatalog/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestMarketplaceCatalog(t *testing.T) {
	_, url := start(t, "marketplace-catalog")
	ctx := context.Background()
	mc := marketplacecatalog.NewFromConfig(awstest.Config(t, url))
	const catalog = "AWSMarketplace"

	// a change set that creates a product
	cs, err := mc.StartChangeSet(ctx, &marketplacecatalog.StartChangeSetInput{Catalog: aws.String(catalog), ChangeSetName: aws.String("create"), ClientRequestToken: aws.String("tok-1"),
		ChangeSet:     []types.Change{{ChangeType: aws.String("CreateProduct"), ChangeName: aws.String("make"), Entity: &types.Entity{Type: aws.String("SaaSProduct@1.0")}, Details: aws.String(`{"ProductTitle":"My SaaS"}`)}},
		ChangeSetTags: []types.Tag{{Key: aws.String("team"), Value: aws.String("a")}}})
	if err != nil || aws.ToString(cs.ChangeSetId) == "" || !strings.Contains(aws.ToString(cs.ChangeSetArn), "AWSMarketplace/ChangeSet/") {
		t.Fatalf("start: %v %+v", err, cs)
	}
	again, err := mc.StartChangeSet(ctx, &marketplacecatalog.StartChangeSetInput{Catalog: aws.String(catalog), ClientRequestToken: aws.String("tok-1"),
		ChangeSet: []types.Change{{ChangeType: aws.String("CreateProduct"), Entity: &types.Entity{Type: aws.String("SaaSProduct@1.0")}}}})
	if err != nil || aws.ToString(again.ChangeSetId) != aws.ToString(cs.ChangeSetId) {
		t.Fatalf("same token: %v %+v", err, again)
	}
	d, err := mc.DescribeChangeSet(ctx, &marketplacecatalog.DescribeChangeSetInput{Catalog: aws.String(catalog), ChangeSetId: cs.ChangeSetId})
	if err != nil || d.Status != types.ChangeStatusSucceeded || aws.ToString(d.ChangeSetName) != "create" || len(d.ChangeSet) != 1 || d.ChangeSet[0].Entity == nil ||
		!strings.HasPrefix(aws.ToString(d.ChangeSet[0].Entity.Identifier), "prod-") || aws.ToString(d.ChangeSet[0].ChangeName) != "make" {
		t.Fatalf("describe: %v %+v", err, d)
	}
	product := aws.ToString(d.ChangeSet[0].Entity.Identifier)

	// the entity it made
	e, err := mc.DescribeEntity(ctx, &marketplacecatalog.DescribeEntityInput{Catalog: aws.String(catalog), EntityId: aws.String(product)})
	if err != nil || aws.ToString(e.EntityType) != "SaaSProduct@1.0" || aws.ToString(e.EntityIdentifier) != product || !strings.HasSuffix(aws.ToString(e.EntityArn), "/SaaSProduct/"+product) {
		t.Fatalf("entity: %v %+v", err, e)
	}
	var got map[string]any
	if err := e.DetailsDocument.UnmarshalSmithyDocument(&got); err != nil || got["ProductTitle"] != "My SaaS" || !strings.Contains(aws.ToString(e.Details), "My SaaS") {
		t.Fatalf("details: %v %v %s", err, got, aws.ToString(e.Details))
	}
	l, err := mc.ListEntities(ctx, &marketplacecatalog.ListEntitiesInput{Catalog: aws.String(catalog), EntityType: aws.String("SaaSProduct")})
	if err != nil || len(l.EntitySummaryList) != 1 || aws.ToString(l.EntitySummaryList[0].Name) != "My SaaS" || aws.ToString(l.EntitySummaryList[0].EntityId) != product ||
		l.EntitySummaryList[0].SaaSProductSummary == nil || aws.ToString(l.EntitySummaryList[0].SaaSProductSummary.ProductTitle) != "My SaaS" {
		t.Fatalf("entities: %v %+v", err, l)
	}
	if l, err := mc.ListEntities(ctx, &marketplacecatalog.ListEntitiesInput{Catalog: aws.String(catalog), EntityType: aws.String("Offer")}); err != nil || len(l.EntitySummaryList) != 0 {
		t.Fatalf("offers: %v %+v", err, l)
	}

	// an update merges its details (here as a document); a missing entity fails the change set
	up, err := mc.StartChangeSet(ctx, &marketplacecatalog.StartChangeSetInput{Catalog: aws.String(catalog), ChangeSetName: aws.String("rename"),
		ChangeSet: []types.Change{{ChangeType: aws.String("UpdateInformation"), Entity: &types.Entity{Type: aws.String("SaaSProduct@1.0"), Identifier: aws.String(product)},
			DetailsDocument: document.NewLazyDocument(map[string]any{"ProductTitle": "Renamed", "Description": "d"})}}})
	if err != nil {
		t.Fatal(err)
	}
	if d, _ := mc.DescribeChangeSet(ctx, &marketplacecatalog.DescribeChangeSetInput{Catalog: aws.String(catalog), ChangeSetId: up.ChangeSetId}); d.Status != types.ChangeStatusSucceeded {
		t.Fatalf("rename: %+v", d)
	}
	e, _ = mc.DescribeEntity(ctx, &marketplacecatalog.DescribeEntityInput{Catalog: aws.String(catalog), EntityId: aws.String(product)})
	var det map[string]any
	if json.Unmarshal([]byte(aws.ToString(e.Details)), &det); det["ProductTitle"] != "Renamed" || det["Description"] != "d" {
		t.Fatalf("merged details: %v", det)
	}
	bad, err := mc.StartChangeSet(ctx, &marketplacecatalog.StartChangeSetInput{Catalog: aws.String(catalog), ChangeSetName: aws.String("bad"), ChangeSet: []types.Change{
		{ChangeType: aws.String("CreateProduct"), Entity: &types.Entity{Type: aws.String("AmiProduct@1.0")}},
		{ChangeType: aws.String("UpdateInformation"), Entity: &types.Entity{Type: aws.String("SaaSProduct@1.0"), Identifier: aws.String("prod-missing")}}}})
	if err != nil {
		t.Fatal(err)
	}
	d, _ = mc.DescribeChangeSet(ctx, &marketplacecatalog.DescribeChangeSetInput{Catalog: aws.String(catalog), ChangeSetId: bad.ChangeSetId})
	if d.Status != types.ChangeStatusFailed || d.FailureCode != types.FailureCodeClientError || len(d.ChangeSet[1].ErrorDetailList) != 1 || aws.ToString(d.ChangeSet[1].ErrorDetailList[0].ErrorCode) != "ENTITY_NOT_FOUND" {
		t.Fatalf("failed change set: %+v", d)
	}
	if l, _ := mc.ListEntities(ctx, &marketplacecatalog.ListEntitiesInput{Catalog: aws.String(catalog), EntityType: aws.String("AmiProduct")}); len(l.EntitySummaryList) != 0 {
		t.Fatalf("a failed change set applied: %+v", l)
	}
	if _, err := mc.StartChangeSet(ctx, &marketplacecatalog.StartChangeSetInput{Catalog: aws.String(catalog), Intent: types.IntentValidate,
		ChangeSet: []types.Change{{ChangeType: aws.String("CreateProduct"), Entity: &types.Entity{Type: aws.String("ContainerProduct@1.0")}}}}); err != nil {
		t.Fatal(err)
	}
	if l, _ := mc.ListEntities(ctx, &marketplacecatalog.ListEntitiesInput{Catalog: aws.String(catalog), EntityType: aws.String("ContainerProduct")}); len(l.EntitySummaryList) != 0 {
		t.Fatalf("a validation applied: %+v", l)
	}

	// listing change sets
	all, err := mc.ListChangeSets(ctx, &marketplacecatalog.ListChangeSetsInput{Catalog: aws.String(catalog)})
	if err != nil || len(all.ChangeSetSummaryList) != 4 {
		t.Fatalf("change sets: %v %+v", err, all)
	}
	failed, err := mc.ListChangeSets(ctx, &marketplacecatalog.ListChangeSetsInput{Catalog: aws.String(catalog), FilterList: []types.Filter{{Name: aws.String("Status"), ValueList: []string{"FAILED"}}}})
	if err != nil || len(failed.ChangeSetSummaryList) != 1 || aws.ToString(failed.ChangeSetSummaryList[0].ChangeSetName) != "bad" {
		t.Fatalf("failed change sets: %v %+v", err, failed)
	}
	if all.ChangeSetSummaryList[0].StartTime == nil || len(all.ChangeSetSummaryList[0].EntityIdList) != 1 {
		t.Fatalf("summary: %+v", all.ChangeSetSummaryList[0])
	}
	batch, err := mc.BatchDescribeEntities(ctx, &marketplacecatalog.BatchDescribeEntitiesInput{EntityRequestList: []types.EntityRequest{
		{Catalog: aws.String(catalog), EntityId: aws.String(product)}, {Catalog: aws.String(catalog), EntityId: aws.String("prod-missing")}}})
	if err != nil || len(batch.EntityDetails) != 1 || aws.ToString(batch.EntityDetails[product].EntityIdentifier) != product || aws.ToString(batch.Errors["prod-missing"].ErrorCode) != "ENTITY_NOT_FOUND" {
		t.Fatalf("batch: %v %+v", err, batch)
	}

	// cancelling a change set that is done, and typed not-found errors
	var inUse *types.ResourceInUseException
	if _, err := mc.CancelChangeSet(ctx, &marketplacecatalog.CancelChangeSetInput{Catalog: aws.String(catalog), ChangeSetId: cs.ChangeSetId}); !errors.As(err, &inUse) {
		t.Fatalf("cancel: %T %v", err, err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := mc.CancelChangeSet(ctx, &marketplacecatalog.CancelChangeSetInput{Catalog: aws.String(catalog), ChangeSetId: aws.String("nope")}); !errors.As(err, &nf) {
		t.Fatalf("cancel a missing change set: %T %v", err, err)
	}
	if _, err := mc.DescribeEntity(ctx, &marketplacecatalog.DescribeEntityInput{Catalog: aws.String(catalog), EntityId: aws.String("prod-missing")}); !errors.As(err, &nf) {
		t.Fatalf("describe a missing entity: %T %v", err, err)
	}
	var invalid *types.ValidationException
	if _, err := mc.DescribeEntity(ctx, &marketplacecatalog.DescribeEntityInput{Catalog: aws.String("Other"), EntityId: aws.String(product)}); !errors.As(err, &invalid) {
		t.Fatalf("another catalog: %T %v", err, err)
	}

	// tags, and a policy on the entity
	arn := cs.ChangeSetArn
	tags, err := mc.ListTagsForResource(ctx, &marketplacecatalog.ListTagsForResourceInput{ResourceArn: arn})
	if err != nil || len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "team" {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := mc.TagResource(ctx, &marketplacecatalog.TagResourceInput{ResourceArn: arn, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := mc.UntagResource(ctx, &marketplacecatalog.UntagResourceInput{ResourceArn: arn, TagKeys: []string{"team"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ = mc.ListTagsForResource(ctx, &marketplacecatalog.ListTagsForResourceInput{ResourceArn: arn}); len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "env" {
		t.Fatalf("tags after: %+v", tags.Tags)
	}
	policy := `{"Version":"2012-10-17","Statement":[]}`
	if _, err := mc.PutResourcePolicy(ctx, &marketplacecatalog.PutResourcePolicyInput{ResourceArn: e.EntityArn, Policy: aws.String(policy)}); err != nil {
		t.Fatal(err)
	}
	if p, err := mc.GetResourcePolicy(ctx, &marketplacecatalog.GetResourcePolicyInput{ResourceArn: e.EntityArn}); err != nil || aws.ToString(p.Policy) != policy {
		t.Fatalf("policy: %v %+v", err, p)
	}
	if _, err := mc.DeleteResourcePolicy(ctx, &marketplacecatalog.DeleteResourcePolicyInput{ResourceArn: e.EntityArn}); err != nil {
		t.Fatal(err)
	}
	if _, err := mc.GetResourcePolicy(ctx, &marketplacecatalog.GetResourcePolicyInput{ResourceArn: e.EntityArn}); !errors.As(err, &nf) {
		t.Fatalf("deleted policy: %T %v", err, err)
	}
	if _, err := mc.PutResourcePolicy(ctx, &marketplacecatalog.PutResourcePolicyInput{ResourceArn: aws.String("arn:aws:aws-marketplace:us-east-1:000000000000:AWSMarketplace/SaaSProduct/prod-nope"), Policy: aws.String(policy)}); !errors.As(err, &nf) {
		t.Fatalf("policy of a missing entity: %T %v", err, err)
	}
}
