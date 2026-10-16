package generic_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/costandusagereportservice"
	"github.com/aws/aws-sdk-go-v2/service/costandusagereportservice/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/nrndev/pogi/internal/aws/generic"
)

func TestCostAndUsageReport(t *testing.T) {
	srv, cfg := startCost(t, "cur", "s3")
	ctx := context.Background()
	cur := costandusagereportservice.NewFromConfig(cfg)
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	if _, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("reports")}); err != nil {
		t.Fatal(err)
	}
	def := types.ReportDefinition{ReportName: aws.String("daily"), TimeUnit: types.TimeUnitDaily, Format: types.ReportFormatCsv, Compression: types.CompressionFormatGzip,
		AdditionalSchemaElements: []types.SchemaElement{types.SchemaElementResources}, S3Bucket: aws.String("reports"), S3Prefix: aws.String("cur"), S3Region: types.AWSRegionUsStandard}
	if _, err := cur.PutReportDefinition(ctx, &costandusagereportservice.PutReportDefinitionInput{ReportDefinition: &def,
		Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("a")}}}); err != nil {
		t.Fatal(err)
	}
	var dup *types.DuplicateReportNameException
	if _, err := cur.PutReportDefinition(ctx, &costandusagereportservice.PutReportDefinitionInput{ReportDefinition: &def}); !errors.As(err, &dup) {
		t.Fatalf("duplicate: %T %v", err, err)
	}
	nobucket := def
	nobucket.ReportName, nobucket.S3Bucket = aws.String("other"), aws.String("missing")
	var ae smithy.APIError
	if _, err := cur.PutReportDefinition(ctx, &costandusagereportservice.PutReportDefinitionInput{ReportDefinition: &nobucket}); !errors.As(err, &ae) || ae.ErrorCode() != "ValidationException" {
		t.Fatalf("missing bucket: %v", err)
	}

	got, err := cur.DescribeReportDefinitions(ctx, &costandusagereportservice.DescribeReportDefinitionsInput{})
	if err != nil || len(got.ReportDefinitions) != 1 {
		t.Fatalf("describe: %v %+v", err, got)
	}
	d := got.ReportDefinitions[0]
	if aws.ToString(d.ReportName) != "daily" || d.TimeUnit != types.TimeUnitDaily || d.Compression != types.CompressionFormatGzip || aws.ToString(d.S3Prefix) != "cur" ||
		d.ReportVersioning != types.ReportVersioningCreateNewReport || len(d.AdditionalSchemaElements) != 1 || d.S3Region != types.AWSRegionUsStandard {
		t.Fatalf("definition: %+v", d)
	}

	def.TimeUnit = types.TimeUnitMonthly
	def.ReportVersioning = types.ReportVersioningOverwriteReport
	if _, err := cur.ModifyReportDefinition(ctx, &costandusagereportservice.ModifyReportDefinitionInput{ReportName: aws.String("daily"), ReportDefinition: &def}); err != nil {
		t.Fatal(err)
	}
	got, _ = cur.DescribeReportDefinitions(ctx, &costandusagereportservice.DescribeReportDefinitionsInput{})
	if got.ReportDefinitions[0].TimeUnit != types.TimeUnitMonthly || got.ReportDefinitions[0].ReportVersioning != types.ReportVersioningOverwriteReport {
		t.Fatalf("modified: %+v", got.ReportDefinitions[0])
	}

	tags, err := cur.ListTagsForResource(ctx, &costandusagereportservice.ListTagsForResourceInput{ReportName: aws.String("daily")})
	if err != nil || len(tags.Tags) != 1 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := cur.TagResource(ctx, &costandusagereportservice.TagResourceInput{ReportName: aws.String("daily"), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := cur.UntagResource(ctx, &costandusagereportservice.UntagResourceInput{ReportName: aws.String("daily"), TagKeys: []string{"team"}}); err != nil {
		t.Fatal(err)
	}
	tags, _ = cur.ListTagsForResource(ctx, &costandusagereportservice.ListTagsForResourceInput{ReportName: aws.String("daily")})
	if len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "env" {
		t.Fatalf("tags after: %+v", tags.Tags)
	}

	// the report is delivered once as the period comes round: some usage, then a CSV in the bucket
	if _, err := s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("reports"), Key: aws.String("seed"), Body: bytes.NewReader([]byte("x"))}); err != nil {
		t.Fatal(err)
	}
	svc := srv.Hub.Service("cur").(*generic.Service)
	svc.Tick(time.Now())
	keys := func() (out []string) {
		l, err := s3c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("reports"), Prefix: aws.String("cur/daily/")})
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range l.Contents {
			out = append(out, aws.ToString(o.Key))
		}
		return
	}
	period := time.Now().UTC()
	month := time.Date(period.Year(), period.Month(), 1, 0, 0, 0, 0, time.UTC)
	dir := "cur/daily/" + month.Format("20060102") + "-" + month.AddDate(0, 1, 0).Format("20060102") + "/"
	want := []string{dir + "daily-00001.csv.gz", dir + "daily-Manifest.json"}
	if k := keys(); len(k) != 2 || k[0] != want[0] || k[1] != want[1] {
		t.Fatalf("keys %v, want %v", k, want)
	}
	o, err := s3c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("reports"), Key: aws.String(want[0])})
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
	var s3Cost bool
	for _, r := range rows[1:] {
		s3Cost = s3Cost || r[6] == "AmazonS3" && strings.HasPrefix(r[7], "Requests-Tier")
	}
	if !s3Cost {
		t.Fatalf("no S3 requests in %v", rows)
	}
	m, err := s3c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("reports"), Key: aws.String(want[1])})
	if err != nil {
		t.Fatal(err)
	}
	var man struct{ ReportName, Bucket string }
	if b, _ := io.ReadAll(m.Body); json.Unmarshal(b, &man) != nil || man.ReportName != "daily" || man.Bucket != "reports" {
		t.Fatalf("manifest: %+v", man)
	}
	got, _ = cur.DescribeReportDefinitions(ctx, &costandusagereportservice.DescribeReportDefinitionsInput{})
	if st := got.ReportDefinitions[0].ReportStatus; st == nil || st.LastStatus != types.LastStatusSuccess || aws.ToString(st.LastDelivery) == "" {
		t.Fatalf("status: %+v", st)
	}

	// not again within the period (a monthly report)
	if _, err := s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("reports"), Key: aws.String("cur/daily/stray"), Body: bytes.NewReader([]byte("x"))}); err != nil {
		t.Fatal(err)
	}
	svc.Tick(time.Now())
	if k := keys(); len(k) != 3 {
		t.Fatalf("redelivered: %v", k)
	}

	// delete, then the typed not-found error
	if _, err := cur.DeleteReportDefinition(ctx, &costandusagereportservice.DeleteReportDefinitionInput{ReportName: aws.String("daily")}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := cur.ListTagsForResource(ctx, &costandusagereportservice.ListTagsForResourceInput{ReportName: aws.String("daily")}); !errors.As(err, &nf) {
		t.Fatalf("tags of a deleted report: %T %v", err, err)
	}
	if _, err := cur.DeleteReportDefinition(ctx, &costandusagereportservice.DeleteReportDefinitionInput{ReportName: aws.String("daily")}); !errors.As(err, &nf) {
		t.Fatalf("deleting again: %T %v", err, err)
	}
}
