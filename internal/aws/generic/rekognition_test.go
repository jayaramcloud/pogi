package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rekognition"
	"github.com/aws/aws-sdk-go-v2/service/rekognition/types"
	"github.com/nrndev/pogi"
)

func TestRekognitionDetect(t *testing.T) {
	ctx := context.Background()
	c := rekognition.NewFromConfig(aiStart(t, pogi.Config{}))
	photo := &types.Image{Bytes: []byte{0xff, 0xd8, 0xff, 1, 2, 3, 4, 5, 6, 7, 8}}
	other := &types.Image{Bytes: []byte{0xff, 0xd8, 0xff, 9, 9, 9, 9, 9, 9, 9, 9}}

	labels := func(img *types.Image, in *rekognition.DetectLabelsInput) []string {
		in.Image = img
		out, err := c.DetectLabels(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, l := range out.Labels {
			if aws.ToFloat32(l.Confidence) < 50 || aws.ToString(l.Name) == "" {
				t.Fatalf("label: %+v", l)
			}
			names = append(names, aws.ToString(l.Name))
		}
		return names
	}
	a, b, o := labels(photo, &rekognition.DetectLabelsInput{}), labels(photo, &rekognition.DetectLabelsInput{}), labels(other, &rekognition.DetectLabelsInput{})
	if len(a) != 4 || strings.Join(a, ",") != strings.Join(b, ",") || strings.Join(a, ",") == strings.Join(o, ",") {
		t.Fatalf("labels: %v %v %v", a, b, o)
	}
	if two := labels(photo, &rekognition.DetectLabelsInput{MaxLabels: aws.Int32(2)}); len(two) != 2 || two[0] != a[0] {
		t.Fatalf("max labels: %v", two)
	}

	f, err := c.DetectFaces(ctx, &rekognition.DetectFacesInput{Image: photo})
	if err != nil || len(f.FaceDetails) != 1 || f.FaceDetails[0].BoundingBox == nil || f.FaceDetails[0].AgeRange == nil || aws.ToFloat32(f.FaceDetails[0].Confidence) < 90 {
		t.Fatalf("faces: %v %+v", err, f)
	}
	tx, err := c.DetectText(ctx, &rekognition.DetectTextInput{Image: &types.Image{Bytes: []byte("STOP\nMain Street\n")}})
	if err != nil {
		t.Fatal(err)
	}
	var aiLines []string
	words := 0
	for _, d := range tx.TextDetections {
		if d.Type == types.TextTypesLine {
			aiLines = append(aiLines, aws.ToString(d.DetectedText))
		} else {
			words++
		}
	}
	if strings.Join(aiLines, "|") != "STOP|Main Street" || words != 3 {
		t.Fatalf("text: %q words %d", aiLines, words)
	}
	if tx, _ = c.DetectText(ctx, &rekognition.DetectTextInput{Image: photo}); len(tx.TextDetections) < 2 || !strings.Contains(aws.ToString(tx.TextDetections[0].DetectedText), "CANNED") {
		t.Fatalf("canned text: %+v", tx.TextDetections)
	}
	if m, err := c.DetectModerationLabels(ctx, &rekognition.DetectModerationLabelsInput{Image: photo}); err != nil || len(m.ModerationLabels) != 0 || aws.ToString(m.ModerationModelVersion) == "" {
		t.Fatalf("moderation: %v %+v", err, m)
	}
	same, err := c.CompareFaces(ctx, &rekognition.CompareFacesInput{SourceImage: photo, TargetImage: photo})
	if err != nil || len(same.FaceMatches) != 1 || len(same.UnmatchedFaces) != 0 || aws.ToFloat32(same.FaceMatches[0].Similarity) < 90 {
		t.Fatalf("compare same: %v %+v", err, same)
	}
	diff, err := c.CompareFaces(ctx, &rekognition.CompareFacesInput{SourceImage: photo, TargetImage: other})
	if err != nil || len(diff.FaceMatches) != 0 || len(diff.UnmatchedFaces) != 1 {
		t.Fatalf("compare different: %v %+v", err, diff)
	}
	var bad *types.InvalidS3ObjectException
	if _, err := c.DetectLabels(ctx, &rekognition.DetectLabelsInput{Image: &types.Image{S3Object: &types.S3Object{Bucket: aws.String("nobucket"), Name: aws.String("x.jpg")}}}); !errors.As(err, &bad) {
		t.Fatalf("missing s3 image: %T %v", err, err)
	}
	if _, err := c.DetectLabels(ctx, &rekognition.DetectLabelsInput{}); err == nil {
		t.Fatal("no image accepted")
	}
}

func TestRekognitionCollection(t *testing.T) {
	ctx := context.Background()
	c := rekognition.NewFromConfig(aiStart(t, pogi.Config{}))
	photo := &types.Image{Bytes: []byte{0xff, 0xd8, 1, 2, 3, 4, 5, 6}}

	cc, err := c.CreateCollection(ctx, &rekognition.CreateCollectionInput{CollectionId: aws.String("people"), Tags: map[string]string{"env": "dev"}})
	arn := "arn:aws:rekognition:us-east-1:000000000000:collection/people"
	if err != nil || aws.ToString(cc.CollectionArn) != arn || aws.ToInt32(cc.StatusCode) != 200 || aws.ToString(cc.FaceModelVersion) == "" {
		t.Fatalf("create: %v %+v", err, cc)
	}
	d, err := c.DescribeCollection(ctx, &rekognition.DescribeCollectionInput{CollectionId: aws.String("people")})
	if err != nil || aws.ToString(d.CollectionARN) != arn || aws.ToInt64(d.FaceCount) != 0 || d.CreationTimestamp == nil {
		t.Fatalf("describe: %v %+v", err, d)
	}
	l, err := c.ListCollections(ctx, &rekognition.ListCollectionsInput{})
	if err != nil || len(l.CollectionIds) != 1 || l.CollectionIds[0] != "people" {
		t.Fatalf("list: %v %+v", err, l)
	}
	var exists *types.ResourceAlreadyExistsException
	if _, err := c.CreateCollection(ctx, &rekognition.CreateCollectionInput{CollectionId: aws.String("people")}); !errors.As(err, &exists) {
		t.Fatalf("duplicate: %T %v", err, err)
	}
	if _, err := c.TagResource(ctx, &rekognition.TagResourceInput{ResourceArn: aws.String(arn), Tags: map[string]string{"team": "ai"}}); err != nil {
		t.Fatal(err)
	}
	if tags, err := c.ListTagsForResource(ctx, &rekognition.ListTagsForResourceInput{ResourceArn: aws.String(arn)}); err != nil || len(tags.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.UntagResource(ctx, &rekognition.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ := c.ListTagsForResource(ctx, &rekognition.ListTagsForResourceInput{ResourceArn: aws.String(arn)}); len(tags.Tags) != 1 {
		t.Fatalf("tags after untag: %+v", tags)
	}

	// faces indexed from an image are found again by that image only
	ix, err := c.IndexFaces(ctx, &rekognition.IndexFacesInput{CollectionId: aws.String("people"), Image: photo, ExternalImageId: aws.String("alice")})
	if err != nil || len(ix.FaceRecords) != 1 || aws.ToString(ix.FaceRecords[0].Face.ExternalImageId) != "alice" || aws.ToString(ix.FaceRecords[0].Face.FaceId) == "" {
		t.Fatalf("index: %v %+v", err, ix)
	}
	faceID := aws.ToString(ix.FaceRecords[0].Face.FaceId)
	if d, _ = c.DescribeCollection(ctx, &rekognition.DescribeCollectionInput{CollectionId: aws.String("people")}); aws.ToInt64(d.FaceCount) != 1 {
		t.Fatalf("face count: %+v", d)
	}
	if lf, err := c.ListFaces(ctx, &rekognition.ListFacesInput{CollectionId: aws.String("people")}); err != nil || len(lf.Faces) != 1 || aws.ToString(lf.Faces[0].FaceId) != faceID {
		t.Fatalf("list faces: %v %+v", err, lf)
	}
	s, err := c.SearchFacesByImage(ctx, &rekognition.SearchFacesByImageInput{CollectionId: aws.String("people"), Image: photo})
	if err != nil || len(s.FaceMatches) != 1 || aws.ToString(s.FaceMatches[0].Face.FaceId) != faceID {
		t.Fatalf("search: %v %+v", err, s)
	}
	if s, _ = c.SearchFacesByImage(ctx, &rekognition.SearchFacesByImageInput{CollectionId: aws.String("people"), Image: &types.Image{Bytes: []byte("someone else")}}); len(s.FaceMatches) != 0 {
		t.Fatalf("search for another image: %+v", s)
	}
	if df, err := c.DeleteFaces(ctx, &rekognition.DeleteFacesInput{CollectionId: aws.String("people"), FaceIds: []string{faceID}}); err != nil || len(df.DeletedFaces) != 1 {
		t.Fatalf("delete faces: %v %+v", err, df)
	}

	dc, err := c.DeleteCollection(ctx, &rekognition.DeleteCollectionInput{CollectionId: aws.String("people")})
	if err != nil || aws.ToInt32(dc.StatusCode) != 200 {
		t.Fatalf("delete: %v %+v", err, dc)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.DescribeCollection(ctx, &rekognition.DescribeCollectionInput{CollectionId: aws.String("people")}); !errors.As(err, &nf) {
		t.Fatalf("describe deleted: %T %v", err, err)
	}
	if _, err := c.IndexFaces(ctx, &rekognition.IndexFacesInput{CollectionId: aws.String("people"), Image: photo}); !errors.As(err, &nf) {
		t.Fatalf("index into deleted: %T %v", err, err)
	}
}
