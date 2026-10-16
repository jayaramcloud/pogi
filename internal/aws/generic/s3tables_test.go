package generic_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestS3Tables(t *testing.T) {
	srv := pogi.Start(t)
	cfg := awstest.Config(t, srv.URL)
	ctx := context.Background()
	c := s3tables.NewFromConfig(cfg)
	data := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })

	b, err := c.CreateTableBucket(ctx, &s3tables.CreateTableBucketInput{Name: aws.String("lake"), Tags: map[string]string{"env": "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(aws.ToString(b.Arn), ":bucket/lake") {
		t.Fatalf("arn %s", aws.ToString(b.Arn))
	}
	if _, err := c.CreateTableBucket(ctx, &s3tables.CreateTableBucketInput{Name: aws.String("lake")}); err == nil {
		t.Fatal("duplicate bucket")
	} else if ce := new(types.ConflictException); !errors.As(err, &ce) {
		t.Fatalf("duplicate: %T %v", err, err)
	}
	if _, err := c.CreateTableBucket(ctx, &s3tables.CreateTableBucketInput{Name: aws.String("Bad_Name")}); err == nil {
		t.Fatal("bad name accepted")
	}
	g, err := c.GetTableBucket(ctx, &s3tables.GetTableBucketInput{TableBucketARN: b.Arn})
	if err != nil || aws.ToString(g.Name) != "lake" || g.CreatedAt == nil || aws.ToString(g.OwnerAccountId) != "000000000000" {
		t.Fatalf("get bucket: %v %+v", err, g)
	}
	lb, err := c.ListTableBuckets(ctx, &s3tables.ListTableBucketsInput{Prefix: aws.String("la")})
	if err != nil || len(lb.TableBuckets) != 1 {
		t.Fatalf("list buckets: %v %+v", err, lb)
	}
	if _, err := c.PutTableBucketPolicy(ctx, &s3tables.PutTableBucketPolicyInput{TableBucketARN: b.Arn, ResourcePolicy: aws.String(`{"Version":"2012-10-17"}`)}); err != nil {
		t.Fatal(err)
	}
	pol, err := c.GetTableBucketPolicy(ctx, &s3tables.GetTableBucketPolicyInput{TableBucketARN: b.Arn})
	if err != nil || aws.ToString(pol.ResourcePolicy) != `{"Version":"2012-10-17"}` {
		t.Fatalf("policy: %v %+v", err, pol)
	}

	ns, err := c.CreateNamespace(ctx, &s3tables.CreateNamespaceInput{TableBucketARN: b.Arn, Namespace: []string{"sales"}})
	if err != nil || len(ns.Namespace) != 1 || ns.Namespace[0] != "sales" {
		t.Fatalf("namespace: %v %+v", err, ns)
	}
	gn, err := c.GetNamespace(ctx, &s3tables.GetNamespaceInput{TableBucketARN: b.Arn, Namespace: aws.String("sales")})
	if err != nil || gn.Namespace[0] != "sales" || gn.CreatedAt == nil {
		t.Fatalf("get namespace: %v %+v", err, gn)
	}
	if _, err := c.DeleteTableBucket(ctx, &s3tables.DeleteTableBucketInput{TableBucketARN: b.Arn}); err == nil {
		t.Fatal("deleted a bucket that has a namespace")
	}

	tb, err := c.CreateTable(ctx, &s3tables.CreateTableInput{TableBucketARN: b.Arn, Namespace: aws.String("sales"), Name: aws.String("orders"), Format: types.OpenTableFormatIceberg,
		Metadata: &types.TableMetadataMemberIceberg{Value: types.IcebergMetadata{Schema: &types.IcebergSchema{Fields: []types.SchemaField{
			{Name: aws.String("id"), Type: aws.String("long"), Required: true}, {Name: aws.String("total"), Type: aws.String("double")}}}}},
		Tags: map[string]string{"team": "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(aws.ToString(tb.TableARN), ":bucket/lake/table/") || aws.ToString(tb.VersionToken) == "" {
		t.Fatalf("table: %+v", tb)
	}
	if _, err := c.CreateTable(ctx, &s3tables.CreateTableInput{TableBucketARN: b.Arn, Namespace: aws.String("sales"), Name: aws.String("orders"), Format: types.OpenTableFormatIceberg}); err == nil {
		t.Fatal("duplicate table")
	}
	gt, err := c.GetTable(ctx, &s3tables.GetTableInput{TableBucketARN: b.Arn, Namespace: aws.String("sales"), Name: aws.String("orders")})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(gt.TableARN) != aws.ToString(tb.TableARN) || gt.Format != types.OpenTableFormatIceberg || !strings.HasSuffix(aws.ToString(gt.WarehouseLocation), "--table-s3") ||
		!strings.HasPrefix(aws.ToString(gt.MetadataLocation), aws.ToString(gt.WarehouseLocation)+"/metadata/") {
		t.Fatalf("get table: %+v", gt)
	}
	if g2, err := c.GetTable(ctx, &s3tables.GetTableInput{TableArn: tb.TableARN}); err != nil || aws.ToString(g2.Name) != "orders" {
		t.Fatalf("get table by arn: %v %+v", err, g2)
	}
	// the metadata location is a real object in pogi's S3
	bucket, key, _ := strings.Cut(strings.TrimPrefix(aws.ToString(gt.MetadataLocation), "s3://"), "/")
	obj, err := data.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("metadata object: %v", err)
	}
	md, _ := io.ReadAll(obj.Body)
	if !strings.Contains(string(md), `"format-version":2`) || !strings.Contains(string(md), `"name":"total"`) {
		t.Fatalf("metadata: %s", md)
	}

	// the version token guards the metadata location
	ml, err := c.GetTableMetadataLocation(ctx, &s3tables.GetTableMetadataLocationInput{TableBucketARN: b.Arn, Namespace: aws.String("sales"), Name: aws.String("orders")})
	if err != nil || aws.ToString(ml.VersionToken) != aws.ToString(tb.VersionToken) {
		t.Fatalf("metadata location: %v %+v", err, ml)
	}
	up, err := c.UpdateTableMetadataLocation(ctx, &s3tables.UpdateTableMetadataLocationInput{TableBucketARN: b.Arn, Namespace: aws.String("sales"), Name: aws.String("orders"),
		VersionToken: ml.VersionToken, MetadataLocation: aws.String(aws.ToString(gt.WarehouseLocation) + "/metadata/00001-x.metadata.json")})
	if err != nil || aws.ToString(up.VersionToken) == aws.ToString(ml.VersionToken) {
		t.Fatalf("update location: %v %+v", err, up)
	}
	_, err = c.UpdateTableMetadataLocation(ctx, &s3tables.UpdateTableMetadataLocationInput{TableBucketARN: b.Arn, Namespace: aws.String("sales"), Name: aws.String("orders"),
		VersionToken: ml.VersionToken, MetadataLocation: aws.String("s3://x/y")}) // the old token
	if ce := new(types.ConflictException); !errors.As(err, &ce) {
		t.Fatalf("stale token: %T %v", err, err)
	}
	ml, _ = c.GetTableMetadataLocation(ctx, &s3tables.GetTableMetadataLocationInput{TableBucketARN: b.Arn, Namespace: aws.String("sales"), Name: aws.String("orders")})
	if !strings.HasSuffix(aws.ToString(ml.MetadataLocation), "00001-x.metadata.json") || aws.ToString(ml.VersionToken) != aws.ToString(up.VersionToken) {
		t.Fatalf("location after update: %+v", ml)
	}

	if _, err := c.RenameTable(ctx, &s3tables.RenameTableInput{TableBucketARN: b.Arn, Namespace: aws.String("sales"), Name: aws.String("orders"), NewName: aws.String("orders_v2")}); err != nil {
		t.Fatal(err)
	}
	lt, err := c.ListTables(ctx, &s3tables.ListTablesInput{TableBucketARN: b.Arn, Namespace: aws.String("sales")})
	if err != nil || len(lt.Tables) != 1 || aws.ToString(lt.Tables[0].Name) != "orders_v2" {
		t.Fatalf("list tables: %v %+v", err, lt)
	}
	ln, err := c.ListNamespaces(ctx, &s3tables.ListNamespacesInput{TableBucketARN: b.Arn})
	if err != nil || len(ln.Namespaces) != 1 {
		t.Fatalf("list namespaces: %v %+v", err, ln)
	}

	// tags
	if _, err := c.TagResource(ctx, &s3tables.TagResourceInput{ResourceArn: tb.TableARN, Tags: map[string]string{"a": "b"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &s3tables.UntagResourceInput{ResourceArn: tb.TableARN, TagKeys: []string{"team"}}); err != nil {
		t.Fatal(err)
	}
	tg, err := c.ListTagsForResource(ctx, &s3tables.ListTagsForResourceInput{ResourceArn: tb.TableARN})
	if err != nil || len(tg.Tags) != 1 || tg.Tags["a"] != "b" {
		t.Fatalf("tags: %v %+v", err, tg)
	}

	// delete in order; the warehouse goes with the table
	if _, err := c.DeleteNamespace(ctx, &s3tables.DeleteNamespaceInput{TableBucketARN: b.Arn, Namespace: aws.String("sales")}); err == nil {
		t.Fatal("deleted a namespace that has a table")
	}
	if _, err := c.DeleteTable(ctx, &s3tables.DeleteTableInput{TableBucketARN: b.Arn, Namespace: aws.String("sales"), Name: aws.String("orders_v2")}); err != nil {
		t.Fatal(err)
	}
	if _, err := data.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err == nil {
		t.Fatal("the warehouse bucket outlived its table")
	}
	_, err = c.GetTable(ctx, &s3tables.GetTableInput{TableBucketARN: b.Arn, Namespace: aws.String("sales"), Name: aws.String("orders_v2")})
	if nf := new(types.NotFoundException); !errors.As(err, &nf) {
		t.Fatalf("table not found: %T %v", err, err)
	}
	if _, err := c.DeleteNamespace(ctx, &s3tables.DeleteNamespaceInput{TableBucketARN: b.Arn, Namespace: aws.String("sales")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteTableBucket(ctx, &s3tables.DeleteTableBucketInput{TableBucketARN: b.Arn}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetTableBucket(ctx, &s3tables.GetTableBucketInput{TableBucketARN: b.Arn})
	if nf := new(types.NotFoundException); !errors.As(err, &nf) {
		t.Fatalf("bucket not found: %T %v", err, err)
	}
}
