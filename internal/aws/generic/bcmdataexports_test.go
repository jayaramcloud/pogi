package generic_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bcmdataexports"
	"github.com/aws/aws-sdk-go-v2/service/bcmdataexports/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nrndev/pogi/internal/aws/generic"
)

func TestBCMDataExports(t *testing.T) {
	srv, cfg := startCost(t, "bcm-data-exports", "s3")
	ctx := context.Background()
	bx := bcmdataexports.NewFromConfig(cfg)
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	if _, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("exports")}); err != nil {
		t.Fatal(err)
	}
	export := func(name, bucket string) *types.Export {
		return &types.Export{Name: aws.String(name), Description: aws.String("monthly"),
			DataQuery: &types.DataQuery{QueryStatement: aws.String("SELECT line_item_usage_account_id, line_item_unblended_cost FROM COST_AND_USAGE_REPORT"),
				TableConfigurations: map[string]map[string]string{"COST_AND_USAGE_REPORT": {"TIME_GRANULARITY": "DAILY"}}},
			DestinationConfigurations: &types.DestinationConfigurations{S3Destination: &types.S3Destination{S3Bucket: aws.String(bucket), S3Prefix: aws.String("bcm"), S3Region: aws.String("us-east-1"),
				S3OutputConfigurations: &types.S3OutputConfigurations{OutputType: types.S3OutputTypeCustom, Format: types.FormatOptionTextOrCsv, Compression: types.CompressionOptionGzip, Overwrite: types.OverwriteOptionOverwriteReport}}},
			RefreshCadence: &types.RefreshCadence{Frequency: types.FrequencyOptionSynchronous}}
	}
	created, err := bx.CreateExport(ctx, &bcmdataexports.CreateExportInput{Export: export("costs", "exports"), ResourceTags: []types.ResourceTag{{Key: aws.String("team"), Value: aws.String("a")}}})
	if err != nil || !strings.Contains(aws.ToString(created.ExportArn), ":export/costs-") {
		t.Fatalf("create: %v %+v", err, created)
	}
	arn := created.ExportArn
	var ae interface{ ErrorCode() string }
	if _, err := bx.CreateExport(ctx, &bcmdataexports.CreateExportInput{Export: export("costs", "exports")}); !errors.As(err, &ae) || ae.ErrorCode() != "ValidationException" {
		t.Fatalf("duplicate name: %v", err)
	}
	if _, err := bx.CreateExport(ctx, &bcmdataexports.CreateExportInput{Export: export("other", "missing")}); err == nil {
		t.Fatal("export to a missing bucket")
	}

	g, err := bx.GetExport(ctx, &bcmdataexports.GetExportInput{ExportArn: arn})
	if err != nil || aws.ToString(g.Export.Name) != "costs" || aws.ToString(g.Export.ExportArn) != aws.ToString(arn) || g.ExportStatus.StatusCode != types.ExportStatusCodeHealthy ||
		aws.ToString(g.Export.DataQuery.QueryStatement) == "" || g.Export.DataQuery.TableConfigurations["COST_AND_USAGE_REPORT"]["TIME_GRANULARITY"] != "DAILY" ||
		aws.ToString(g.Export.DestinationConfigurations.S3Destination.S3Prefix) != "bcm" || g.ExportStatus.CreatedAt == nil {
		t.Fatalf("get: %v %+v", err, g)
	}
	l, err := bx.ListExports(ctx, &bcmdataexports.ListExportsInput{})
	if err != nil || len(l.Exports) != 1 || aws.ToString(l.Exports[0].ExportName) != "costs" || aws.ToString(l.Exports[0].ExportArn) != aws.ToString(arn) {
		t.Fatalf("list: %v %+v", err, l)
	}
	upd := export("costs", "exports")
	upd.Description = aws.String("changed")
	if _, err := bx.UpdateExport(ctx, &bcmdataexports.UpdateExportInput{ExportArn: arn, Export: upd}); err != nil {
		t.Fatal(err)
	}
	if g, _ = bx.GetExport(ctx, &bcmdataexports.GetExportInput{ExportArn: arn}); aws.ToString(g.Export.Description) != "changed" || aws.ToString(g.Export.ExportArn) != aws.ToString(arn) {
		t.Fatalf("after update: %+v", g.Export)
	}
	if _, err := bx.TagResource(ctx, &bcmdataexports.TagResourceInput{ResourceArn: arn, ResourceTags: []types.ResourceTag{{Key: aws.String("env"), Value: aws.String("dev")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := bx.UntagResource(ctx, &bcmdataexports.UntagResourceInput{ResourceArn: arn, ResourceTagKeys: []string{"team"}}); err != nil {
		t.Fatal(err)
	}
	tags, err := bx.ListTagsForResource(ctx, &bcmdataexports.ListTagsForResourceInput{ResourceArn: arn})
	if err != nil || len(tags.ResourceTags) != 1 || aws.ToString(tags.ResourceTags[0].Key) != "env" {
		t.Fatalf("tags: %v %+v", err, tags)
	}

	// the run writes the month's bill as CSV into the bucket, once a day
	if _, err := s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("exports"), Key: aws.String("seed"), Body: bytes.NewReader([]byte("x"))}); err != nil {
		t.Fatal(err)
	}
	svc := srv.Hub.Service("bcm-data-exports").(*generic.Service)
	svc.Tick(time.Now())
	svc.Tick(time.Now())
	month := time.Now().UTC().Format("2006-01")
	o, err := s3c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("exports"), Key: aws.String("bcm/costs/data/BILLING_PERIOD=" + month + "/costs-00001.csv.gz")})
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(o.Body)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(zr).ReadAll()
	if err != nil || len(rows) < 2 || rows[0][6] != "lineItem/ProductCode" {
		t.Fatalf("csv: %v %v", err, rows)
	}
	if _, err := s3c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("exports"), Key: aws.String("bcm/costs/metadata/BILLING_PERIOD=" + month + "/costs-Manifest.json")}); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	ex, err := bx.ListExecutions(ctx, &bcmdataexports.ListExecutionsInput{ExportArn: arn})
	if err != nil || len(ex.Executions) != 1 || ex.Executions[0].ExecutionStatus.StatusCode != types.ExecutionStatusCodeDeliverySuccess {
		t.Fatalf("executions: %v %+v", err, ex)
	}
	one, err := bx.GetExecution(ctx, &bcmdataexports.GetExecutionInput{ExportArn: arn, ExecutionId: ex.Executions[0].ExecutionId})
	if err != nil || aws.ToString(one.Export.Name) != "costs" || one.ExecutionStatus.CompletedAt == nil {
		t.Fatalf("execution: %v %+v", err, one)
	}
	var nf *types.ResourceNotFoundException
	if _, err := bx.GetExecution(ctx, &bcmdataexports.GetExecutionInput{ExportArn: arn, ExecutionId: aws.String("nope")}); !errors.As(err, &nf) {
		t.Fatalf("missing execution: %T %v", err, err)
	}

	// tables
	tb, err := bx.ListTables(ctx, &bcmdataexports.ListTablesInput{})
	if err != nil || len(tb.Tables) != 2 || aws.ToString(tb.Tables[0].TableName) != "COST_AND_USAGE_REPORT" || len(tb.Tables[0].TableProperties) == 0 {
		t.Fatalf("tables: %v %+v", err, tb)
	}
	gt, err := bx.GetTable(ctx, &bcmdataexports.GetTableInput{TableName: aws.String("COST_AND_USAGE_REPORT")})
	if err != nil || len(gt.Schema) != 13 || aws.ToString(gt.Schema[0].Name) != "identity/LineItemId" {
		t.Fatalf("table: %v %+v", err, gt)
	}
	if _, err := bx.GetTable(ctx, &bcmdataexports.GetTableInput{TableName: aws.String("NOPE")}); !errors.As(err, &nf) {
		t.Fatalf("missing table: %T %v", err, err)
	}

	if _, err := bx.DeleteExport(ctx, &bcmdataexports.DeleteExportInput{ExportArn: arn}); err != nil {
		t.Fatal(err)
	}
	if _, err := bx.GetExport(ctx, &bcmdataexports.GetExportInput{ExportArn: arn}); !errors.As(err, &nf) {
		t.Fatalf("deleted export: %T %v", err, err)
	}
	if _, err := bx.DeleteExport(ctx, &bcmdataexports.DeleteExportInput{ExportArn: arn}); !errors.As(err, &nf) {
		t.Fatalf("deleting again: %T %v", err, err)
	}
}
