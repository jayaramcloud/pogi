package resourceexplorer_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	re "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2"
	"github.com/aws/aws-sdk-go-v2/service/resourceexplorer2/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	ddbsvc "github.com/nrndev/pogi/internal/aws/dynamodb"
	"github.com/nrndev/pogi/internal/aws/resourceexplorer"
	s3svc "github.com/nrndev/pogi/internal/aws/s3"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

const (
	bucket = "arn:aws:s3:::logs"
	queue  = "arn:aws:sqs:us-east-1:000000000000:jobs"
	table  = "arn:aws:dynamodb:us-east-1:000000000000:table/orders"
	west   = "arn:aws:sqs:us-west-2:000000000000:west"
)

func TestResourceExplorer(t *testing.T) {
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return s3svc.New(h) },
		func(h *core.Hub) core.Service { return sqssvc.New(h) },
		func(h *core.Hub) core.Service { return ddbsvc.New(h) },
		func(h *core.Hub) core.Service { return resourceexplorer.New(h) })
	cfg := awstest.Config(t, url)
	c := re.NewFromConfig(cfg)

	// An index and a default view that includes tags.
	ix := must(c.CreateIndex(ctx, &re.CreateIndexInput{Tags: map[string]string{"env": "dev"}}))
	if got := must(c.GetIndex(ctx, &re.GetIndexInput{})); *got.Arn != *ix.Arn || got.State != types.IndexStateActive ||
		got.Type != types.IndexTypeLocal || got.CreatedAt == nil || got.Tags["env"] != "dev" {
		t.Fatalf("index = %+v", got)
	}
	var conflict *types.ConflictException
	if _, err := c.CreateIndex(ctx, &re.CreateIndexInput{}); !errors.As(err, &conflict) {
		t.Fatalf("second index: %v", err)
	}
	all := must(c.CreateView(ctx, &re.CreateViewInput{ViewName: aws.String("all"),
		IncludedProperties: []types.IncludedProperty{{Name: aws.String("tags")}}})).View
	if *all.Owner != "000000000000" || *all.Scope != "arn:aws:iam::000000000000:root" || all.Filters == nil || all.LastUpdatedAt == nil {
		t.Fatalf("view = %+v", all)
	}
	must(c.AssociateDefaultView(ctx, &re.AssociateDefaultViewInput{ViewArn: all.ViewArn}))

	must(s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }).CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("logs")}))
	must(sqs.NewFromConfig(cfg).CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("jobs")}))
	must(dynamodb.NewFromConfig(cfg).CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String("orders"), BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
		Tags:                 []ddbtypes.Tag{{Key: aws.String("team"), Value: aws.String("data")}}}))

	search := func(q string, view *string) (arns []string) {
		for _, r := range must(c.Search(ctx, &re.SearchInput{QueryString: aws.String(q), ViewArn: view})).Resources {
			arns = append(arns, *r.Arn)
		}
		return arns
	}
	for q, want := range map[string][]string{
		"service:s3":             {bucket},
		"resourcetype:sqs:queue": {queue},
		"tag:team=data":          {table},
		"tag.key:team":           {table},
		"ORDER":                  {table},
		"-service:s3":            {table, queue},
		"region:us-east-1":       {table, bucket, queue},
		"service:s3 service:sqs": {bucket, queue},
		"service:sq* tag:none":   {queue},
		"id:log* -tag:all":       {bucket},
		`"jobs" orders`:          {table, queue},
		"region:eu-west-1":       nil,
	} {
		if got := search(q, nil); !slices.Equal(got, want) {
			t.Errorf("Search(%q) = %v, want %v", q, got, want)
		}
	}

	res := must(c.Search(ctx, &re.SearchInput{QueryString: aws.String("tag:team=data")}))
	r, data := res.Resources[0], []map[string]string{}
	if *res.ViewArn != *all.ViewArn || *res.Count.TotalResources != 1 || !*res.Count.Complete || *r.ResourceType != "dynamodb:table" ||
		*r.Service != "dynamodb" || *r.Region != "us-east-1" || *r.OwningAccountId != "000000000000" || r.LastReportedAt == nil ||
		len(r.Properties) != 1 || *r.Properties[0].Name != "tags" || r.Properties[0].Data.UnmarshalSmithyDocument(&data) != nil ||
		len(data) != 1 || data[0]["Key"] != "team" || data[0]["Value"] != "data" {
		t.Fatalf("search = %+v %+v", res, data)
	}
	if b := must(c.Search(ctx, &re.SearchInput{QueryString: aws.String("service:s3")})).Resources[0]; *b.ResourceType != "s3:bucket" ||
		*b.Region != "us-east-1" || *b.OwningAccountId != "000000000000" || len(b.Properties) != 0 {
		t.Fatalf("bucket = %+v", b)
	}
	p1 := must(c.Search(ctx, &re.SearchInput{QueryString: aws.String(""), MaxResults: aws.Int32(2)}))
	p2 := must(c.Search(ctx, &re.SearchInput{QueryString: aws.String(""), MaxResults: aws.Int32(2), NextToken: p1.NextToken}))
	if len(p1.Resources) != 2 || p1.NextToken == nil || *p1.Count.TotalResources != 3 || len(p2.Resources) != 1 || p2.NextToken != nil {
		t.Fatalf("pages = %+v, %+v", p1, p2)
	}
	lr := must(c.ListResources(ctx, &re.ListResourcesInput{Filters: &types.SearchFilter{FilterString: aws.String("service:sqs")}}))
	if len(lr.Resources) != 1 || *lr.Resources[0].Arn != queue || *lr.ViewArn != *all.ViewArn {
		t.Fatalf("list = %+v", lr)
	}

	// A view's filter restricts what searches through it see.
	tables := must(c.CreateView(ctx, &re.CreateViewInput{ViewName: aws.String("tables"),
		Filters: &types.SearchFilter{FilterString: aws.String("service:dynamodb")}})).View
	if got := search("region:us-east-1", tables.ViewArn); !slices.Equal(got, []string{table}) {
		t.Fatalf("through tables = %v", got)
	}
	must(c.UpdateView(ctx, &re.UpdateViewInput{ViewArn: tables.ViewArn, Filters: &types.SearchFilter{FilterString: aws.String("service:sqs")}}))
	if got := search("", tables.ViewArn); !slices.Equal(got, []string{queue}) {
		t.Fatalf("after update = %v", got)
	}
	if v := must(c.ListViews(ctx, &re.ListViewsInput{})).Views; len(v) != 2 {
		t.Fatalf("views = %v", v)
	}
	if b := must(c.BatchGetView(ctx, &re.BatchGetViewInput{ViewArns: []string{*tables.ViewArn, *tables.ViewArn + "x"}})); len(b.Views) != 1 ||
		*b.Views[0].Filters.FilterString != "service:sqs" || len(b.Errors) != 1 {
		t.Fatalf("batch = %+v", b)
	}

	// An aggregator index searches every indexed Region of the account.
	westRE := re.NewFromConfig(cfg, func(o *re.Options) { o.Region = "us-west-2" })
	must(westRE.CreateIndex(ctx, &re.CreateIndexInput{}))
	must(sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.Region = "us-west-2" }).CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("west")}))
	if got := search("service:sqs", nil); !slices.Equal(got, []string{queue}) {
		t.Fatalf("local index sees %v", got)
	}
	if up := must(c.UpdateIndexType(ctx, &re.UpdateIndexTypeInput{Arn: ix.Arn, Type: types.IndexTypeAggregator})); up.Type != types.IndexTypeAggregator ||
		must(c.GetIndex(ctx, &re.GetIndexInput{})).Type != types.IndexTypeAggregator {
		t.Fatalf("update = %+v", up)
	}
	if got := search("service:sqs", nil); !slices.Equal(got, []string{queue, west}) {
		t.Fatalf("aggregator sees %v", got)
	}
	var quota *types.ServiceQuotaExceededException
	if _, err := westRE.UpdateIndexType(ctx, &re.UpdateIndexTypeInput{Arn: must(westRE.GetIndex(ctx, &re.GetIndexInput{})).Arn,
		Type: types.IndexTypeAggregator}); !errors.As(err, &quota) {
		t.Fatalf("second aggregator: %v", err)
	}
	if l := must(c.ListIndexes(ctx, &re.ListIndexesInput{Type: types.IndexTypeLocal})).Indexes; len(l) != 1 || *l[0].Region != "us-west-2" {
		t.Fatalf("local indexes = %+v", l)
	}

	// Tags on views.
	must(c.TagResource(ctx, &re.TagResourceInput{ResourceArn: all.ViewArn, Tags: map[string]string{"env": "dev", "owner": "me"}}))
	must(c.UntagResource(ctx, &re.UntagResourceInput{ResourceArn: all.ViewArn, TagKeys: []string{"env"}}))
	if tags := must(c.ListTagsForResource(ctx, &re.ListTagsForResourceInput{ResourceArn: all.ViewArn})).Tags; !maps.Equal(tags, map[string]string{"owner": "me"}) {
		t.Fatalf("tags = %v", tags)
	}
	if v := must(c.GetView(ctx, &re.GetViewInput{ViewArn: all.ViewArn})); v.Tags["owner"] != "me" || *v.View.ViewName != "all" {
		t.Fatalf("view = %+v", v)
	}

	// Errors.
	var notFound *types.ResourceNotFoundException
	var invalid *types.ValidationException
	if _, err := c.GetView(ctx, &re.GetViewInput{ViewArn: aws.String(*all.ViewArn + "x")}); !errors.As(err, &notFound) {
		t.Fatalf("unknown view: %v", err)
	}
	if _, err := c.CreateView(ctx, &re.CreateViewInput{ViewName: aws.String("bad name")}); !errors.As(err, &invalid) {
		t.Fatalf("bad name: %v", err)
	}
	if _, err := c.CreateView(ctx, &re.CreateViewInput{ViewName: aws.String("x"), Filters: &types.SearchFilter{FilterString: aws.String(`"open`)}}); !errors.As(err, &invalid) {
		t.Fatalf("bad filter: %v", err)
	}
	if _, err := c.CreateView(ctx, &re.CreateViewInput{ViewName: aws.String("all")}); !errors.As(err, &conflict) {
		t.Fatalf("duplicate view: %v", err)
	}

	// Deleting the default view, then the index.
	must(c.DeleteView(ctx, &re.DeleteViewInput{ViewArn: all.ViewArn}))
	if d := must(c.GetDefaultView(ctx, &re.GetDefaultViewInput{})); d.ViewArn != nil {
		t.Fatalf("default = %v", *d.ViewArn)
	}
	if _, err := c.Search(ctx, &re.SearchInput{QueryString: aws.String("")}); !errors.As(err, &notFound) {
		t.Fatalf("search without a view: %v", err)
	}
	if d := must(c.DeleteIndex(ctx, &re.DeleteIndexInput{Arn: ix.Arn})); d.State != types.IndexStateDeleting {
		t.Fatalf("delete = %+v", d)
	}
	if _, err := c.GetIndex(ctx, &re.GetIndexInput{}); !errors.As(err, &notFound) {
		t.Fatalf("deleted index: %v", err)
	}
	if v := must(c.ListViews(ctx, &re.ListViewsInput{})).Views; len(v) != 0 {
		t.Fatalf("views outlived the index: %v", v)
	}
	if o := must(c.GetAccountLevelServiceConfiguration(ctx, &re.GetAccountLevelServiceConfigurationInput{})).OrgConfiguration; o.AWSServiceAccessStatus != types.AWSServiceAccessStatusDisabled {
		t.Fatalf("org = %+v", o)
	}
	if rt := must(c.ListSupportedResourceTypes(ctx, &re.ListSupportedResourceTypesInput{})).ResourceTypes; !slices.ContainsFunc(rt, func(x types.SupportedResourceType) bool {
		return *x.Service == "s3" && *x.ResourceType == "s3:bucket"
	}) {
		t.Fatalf("types = %+v", rt)
	}
}
