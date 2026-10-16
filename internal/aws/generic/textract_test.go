package generic_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/textract"
	"github.com/aws/aws-sdk-go-v2/service/textract/types"
	"github.com/aws/smithy-go"
	"github.com/nrndev/pogi"
)

// aiLines are the LINE blocks of an aiAnswer, with the WORD blocks counted.
func aiLines(blocks []types.Block) (texts []string, words int) {
	for _, b := range blocks {
		switch b.BlockType {
		case types.BlockTypeLine:
			texts = append(texts, aws.ToString(b.Text))
		case types.BlockTypeWord:
			words++
		}
	}
	return
}

func TestTextractDetectText(t *testing.T) {
	ctx := context.Background()
	c := textract.NewFromConfig(aiStart(t, pogi.Config{}))

	doc := &types.Document{Bytes: []byte("Invoice 42\n\n  Total due: 100 USD  \r\nThank you\n")}
	out, err := c.DetectDocumentText(ctx, &textract.DetectDocumentTextInput{Document: doc})
	if err != nil {
		t.Fatal(err)
	}
	got, words := aiLines(out.Blocks)
	if strings.Join(got, "|") != "Invoice 42|Total due: 100 USD|Thank you" || words != 8 || aws.ToInt32(out.DocumentMetadata.Pages) != 1 || out.Blocks[0].BlockType != types.BlockTypePage {
		t.Fatalf("lines %q words %d: %+v", got, words, out.DocumentMetadata)
	}
	// the page lists its aiLines, a line its words, and every id is unique
	ids := map[string]bool{}
	for _, b := range out.Blocks {
		if ids[aws.ToString(b.Id)] {
			t.Fatalf("duplicate id %s", aws.ToString(b.Id))
		}
		ids[aws.ToString(b.Id)] = true
		if b.BlockType == types.BlockTypeLine && (len(b.Relationships) != 1 || b.Geometry.BoundingBox.Width == 0 || b.Confidence == nil || *b.Confidence < 90) {
			t.Fatalf("line: %+v", b)
		}
	}
	if len(out.Blocks[0].Relationships[0].Ids) != 3 {
		t.Fatalf("page children: %v", out.Blocks[0].Relationships)
	}

	// bytes that are not text: one canned line, the same for the same bytes
	bin := &types.Document{Bytes: []byte{0x89, 'P', 'N', 'G', 0, 1, 2, 3}}
	a, _ := c.DetectDocumentText(ctx, &textract.DetectDocumentTextInput{Document: bin})
	b, _ := c.DetectDocumentText(ctx, &textract.DetectDocumentTextInput{Document: bin})
	la, _ := aiLines(a.Blocks)
	lb, _ := aiLines(b.Blocks)
	other, _ := c.DetectDocumentText(ctx, &textract.DetectDocumentTextInput{Document: &types.Document{Bytes: []byte{0x89, 'J', 'P', 'G', 0}}})
	lo, _ := aiLines(other.Blocks)
	if len(la) != 1 || la[0] != lb[0] || la[0] == lo[0] || !strings.Contains(la[0], "CANNED") {
		t.Fatalf("canned: %q %q %q", la, lb, lo)
	}
	an, err := c.AnalyzeDocument(ctx, &textract.AnalyzeDocumentInput{Document: doc, FeatureTypes: []types.FeatureType{types.FeatureTypeForms}})
	if l, _ := aiLines(an.Blocks); err != nil || len(l) != 3 || aws.ToString(an.AnalyzeDocumentModelVersion) == "" {
		t.Fatalf("analyze: %v %+v", err, an)
	}
	if _, err := c.DetectDocumentText(ctx, &textract.DetectDocumentTextInput{}); err == nil {
		t.Fatal("no document accepted")
	}
}

// A document in S3 is read through pogi's own S3, for the sync and the async calls.
func TestTextractFromS3(t *testing.T) {
	ctx := context.Background()
	cfg := aiStart(t, pogi.Config{})
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	if _, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("docs")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("docs"), Key: aws.String("in/memo.txt"), Body: bytes.NewReader([]byte("first line\nsecond line\n"))}); err != nil {
		t.Fatal(err)
	}
	c := textract.NewFromConfig(cfg)
	loc := &types.S3Object{Bucket: aws.String("docs"), Name: aws.String("in/memo.txt")}

	out, err := c.DetectDocumentText(ctx, &textract.DetectDocumentTextInput{Document: &types.Document{S3Object: loc}})
	if got, _ := aiLines(out.Blocks); err != nil || strings.Join(got, "|") != "first line|second line" {
		t.Fatalf("sync: %v %q", err, got)
	}
	job, err := c.StartDocumentTextDetection(ctx, &textract.StartDocumentTextDetectionInput{DocumentLocation: &types.DocumentLocation{S3Object: loc}})
	if err != nil || aws.ToString(job.JobId) == "" {
		t.Fatalf("start: %v %+v", err, job)
	}
	res, err := c.GetDocumentTextDetection(ctx, &textract.GetDocumentTextDetectionInput{JobId: job.JobId})
	if got, _ := aiLines(res.Blocks); err != nil || res.JobStatus != types.JobStatusSucceeded || strings.Join(got, "|") != "first line|second line" {
		t.Fatalf("get: %v %v %q", err, res.JobStatus, got)
	}
	aj, err := c.StartDocumentAnalysis(ctx, &textract.StartDocumentAnalysisInput{DocumentLocation: &types.DocumentLocation{S3Object: loc}, FeatureTypes: []types.FeatureType{types.FeatureTypeTables}})
	if err != nil {
		t.Fatal(err)
	}
	if ra, err := c.GetDocumentAnalysis(ctx, &textract.GetDocumentAnalysisInput{JobId: aj.JobId}); err != nil || ra.JobStatus != types.JobStatusSucceeded || len(ra.Blocks) == 0 {
		t.Fatalf("get analysis: %v %+v", err, ra)
	}
	_, err = c.GetDocumentTextDetection(ctx, &textract.GetDocumentTextDetectionInput{JobId: aws.String("nope")})
	var bad *types.InvalidJobIdException
	if !errors.As(err, &bad) {
		t.Fatalf("unknown job: %T %v", err, err)
	}
}

func TestTextractAdapter(t *testing.T) {
	ctx := context.Background()
	c := textract.NewFromConfig(aiStart(t, pogi.Config{}))

	a, err := c.CreateAdapter(ctx, &textract.CreateAdapterInput{AdapterName: aws.String("invoices"), FeatureTypes: []types.FeatureType{types.FeatureTypeForms}, Tags: map[string]string{"env": "dev"}})
	if err != nil || aws.ToString(a.AdapterId) == "" {
		t.Fatalf("create: %v %+v", err, a)
	}
	g, err := c.GetAdapter(ctx, &textract.GetAdapterInput{AdapterId: a.AdapterId})
	if err != nil || aws.ToString(g.AdapterName) != "invoices" || len(g.FeatureTypes) != 1 || g.Tags["env"] != "dev" || g.CreationTime == nil {
		t.Fatalf("get: %v %+v", err, g)
	}
	if l, err := c.ListAdapters(ctx, &textract.ListAdaptersInput{}); err != nil || len(l.Adapters) != 1 || aws.ToString(l.Adapters[0].AdapterName) != "invoices" {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.UpdateAdapter(ctx, &textract.UpdateAdapterInput{AdapterId: a.AdapterId, Description: aws.String("changed")}); err != nil {
		t.Fatal(err)
	}
	if g, _ = c.GetAdapter(ctx, &textract.GetAdapterInput{AdapterId: a.AdapterId}); aws.ToString(g.Description) != "changed" || aws.ToString(g.AdapterName) != "invoices" {
		t.Fatalf("after update: %+v", g)
	}
	arn := "arn:aws:textract:us-east-1:000000000000:/adapters/" + aws.ToString(a.AdapterId)
	if _, err := c.TagResource(ctx, &textract.TagResourceInput{ResourceARN: aws.String(arn), Tags: map[string]string{"team": "ai"}}); err != nil {
		t.Fatal(err)
	}
	if tags, err := c.ListTagsForResource(ctx, &textract.ListTagsForResourceInput{ResourceARN: aws.String(arn)}); err != nil || len(tags.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.UntagResource(ctx, &textract.UntagResourceInput{ResourceARN: aws.String(arn), TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ := c.ListTagsForResource(ctx, &textract.ListTagsForResourceInput{ResourceARN: aws.String(arn)}); len(tags.Tags) != 1 {
		t.Fatalf("tags after untag: %+v", tags)
	}
	if _, err := c.DeleteAdapter(ctx, &textract.DeleteAdapterInput{AdapterId: a.AdapterId}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetAdapter(ctx, &textract.GetAdapterInput{AdapterId: a.AdapterId})
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "ResourceNotFoundException" {
		t.Fatalf("get deleted: %T %v", err, err)
	}
}
