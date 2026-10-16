package generic_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/transcribe"
	"github.com/aws/aws-sdk-go-v2/service/transcribe/types"
	"github.com/nrndev/pogi"
)

func TestTranscribeJob(t *testing.T) {
	ctx := context.Background()
	cfg := aiStart(t, pogi.Config{})
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	c := transcribe.NewFromConfig(cfg)
	for _, b := range []string{"audio", "out"} {
		if _, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("audio"), Key: aws.String("call.mp3"), Body: bytes.NewReader([]byte("not really audio"))}); err != nil {
		t.Fatal(err)
	}
	start := func(name, key string) *transcribe.StartTranscriptionJobOutput {
		in := &transcribe.StartTranscriptionJobInput{TranscriptionJobName: aws.String(name), LanguageCode: types.LanguageCodeEnUs,
			Media: &types.Media{MediaFileUri: aws.String("s3://audio/call.mp3")}, MediaFormat: types.MediaFormatMp3, OutputBucketName: aws.String("out"),
			Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}
		if key != "" { // an empty key breaks the model's minimum length
			in.OutputKey = aws.String(key)
		}
		out, err := c.StartTranscriptionJob(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	s := start("job1", "")
	if j := s.TranscriptionJob; aws.ToString(j.TranscriptionJobName) != "job1" || j.TranscriptionJobStatus != types.TranscriptionJobStatusInProgress || j.LanguageCode != types.LanguageCodeEnUs ||
		aws.ToString(j.Media.MediaFileUri) != "s3://audio/call.mp3" || j.CreationTime == nil {
		t.Fatalf("start: %+v", j)
	}
	g, err := c.GetTranscriptionJob(ctx, &transcribe.GetTranscriptionJobInput{TranscriptionJobName: aws.String("job1")})
	if err != nil || g.TranscriptionJob.TranscriptionJobStatus != types.TranscriptionJobStatusInProgress {
		t.Fatalf("get right away: %v %+v", err, g)
	}
	for i := 0; g.TranscriptionJob.TranscriptionJobStatus != types.TranscriptionJobStatusCompleted; i++ {
		if i > 50 {
			t.Fatalf("never completed: %v", g.TranscriptionJob.TranscriptionJobStatus)
		}
		time.Sleep(50 * time.Millisecond)
		g, _ = c.GetTranscriptionJob(ctx, &transcribe.GetTranscriptionJobInput{TranscriptionJobName: aws.String("job1")})
	}
	j := g.TranscriptionJob
	if aws.ToString(j.Transcript.TranscriptFileUri) != "https://s3.us-east-1.amazonaws.com/out/job1.json" || j.CompletionTime == nil || len(j.Tags) != 1 {
		t.Fatalf("completed job: %+v", j)
	}

	// the transcript is in the output bucket, and is the same for the same media
	read := func(key string) map[string]any {
		o, err := s3c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("out"), Key: aws.String(key)})
		if err != nil {
			t.Fatal(err)
		}
		defer o.Body.Close()
		b, _ := io.ReadAll(o.Body)
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("%v: %s", err, b)
		}
		return m
	}
	tr := read("job1.json")
	text := tr["results"].(map[string]any)["transcripts"].([]any)[0].(map[string]any)["transcript"].(string)
	if tr["jobName"] != "job1" || tr["status"] != "COMPLETED" || !strings.Contains(text, "canned") || !strings.Contains(text, "call.mp3") || len(tr["results"].(map[string]any)["items"].([]any)) < 5 {
		t.Fatalf("transcript: %v", tr)
	}
	start("job2", "transcripts/two.json")
	if read("transcripts/two.json")["results"].(map[string]any)["transcripts"].([]any)[0].(map[string]any)["transcript"] != text {
		t.Fatal("the same media gave another transcript")
	}

	l, err := c.ListTranscriptionJobs(ctx, &transcribe.ListTranscriptionJobsInput{MaxResults: aws.Int32(1)})
	if err != nil || len(l.TranscriptionJobSummaries) != 1 || aws.ToString(l.TranscriptionJobSummaries[0].TranscriptionJobName) != "job2" || aws.ToString(l.NextToken) == "" ||
		l.TranscriptionJobSummaries[0].OutputLocationType != types.OutputLocationTypeCustomerBucket {
		t.Fatalf("list: %v %+v", err, l)
	}
	l, _ = c.ListTranscriptionJobs(ctx, &transcribe.ListTranscriptionJobsInput{MaxResults: aws.Int32(1), NextToken: l.NextToken})
	if len(l.TranscriptionJobSummaries) != 1 || aws.ToString(l.TranscriptionJobSummaries[0].TranscriptionJobName) != "job1" || l.NextToken != nil {
		t.Fatalf("second page: %+v", l)
	}
	if l, _ = c.ListTranscriptionJobs(ctx, &transcribe.ListTranscriptionJobsInput{JobNameContains: aws.String("job2")}); len(l.TranscriptionJobSummaries) != 1 {
		t.Fatalf("name filter: %+v", l)
	}

	// errors: a name used twice, an output bucket that does not exist, a job that does not exist
	var conflict *types.ConflictException
	if _, err := c.StartTranscriptionJob(ctx, &transcribe.StartTranscriptionJobInput{TranscriptionJobName: aws.String("job1"), Media: &types.Media{MediaFileUri: aws.String("s3://audio/call.mp3")}, LanguageCode: types.LanguageCodeEnUs}); !errors.As(err, &conflict) {
		t.Fatalf("duplicate: %T %v", err, err)
	}
	var bad *types.BadRequestException
	if _, err := c.StartTranscriptionJob(ctx, &transcribe.StartTranscriptionJobInput{TranscriptionJobName: aws.String("job3"), Media: &types.Media{MediaFileUri: aws.String("s3://audio/call.mp3")},
		LanguageCode: types.LanguageCodeEnUs, OutputBucketName: aws.String("no-such-bucket")}); !errors.As(err, &bad) {
		t.Fatalf("no bucket: %T %v", err, err)
	}

	if _, err := c.DeleteTranscriptionJob(ctx, &transcribe.DeleteTranscriptionJobInput{TranscriptionJobName: aws.String("job1")}); err != nil {
		t.Fatal(err)
	}
	var nf *types.NotFoundException
	if _, err := c.GetTranscriptionJob(ctx, &transcribe.GetTranscriptionJobInput{TranscriptionJobName: aws.String("job1")}); !errors.As(err, &nf) {
		t.Fatalf("get deleted: %T %v", err, err)
	}
	if _, err := c.DeleteTranscriptionJob(ctx, &transcribe.DeleteTranscriptionJobInput{TranscriptionJobName: aws.String("job1")}); !errors.As(err, &nf) {
		t.Fatalf("delete deleted: %T %v", err, err)
	}
}

func TestTranscribeVocabulary(t *testing.T) {
	ctx := context.Background()
	c := transcribe.NewFromConfig(aiStart(t, pogi.Config{}))

	v, err := c.CreateVocabulary(ctx, &transcribe.CreateVocabularyInput{VocabularyName: aws.String("terms"), LanguageCode: types.LanguageCodeEnUs, Phrases: []string{"pogi", "bedrock"},
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil || aws.ToString(v.VocabularyName) != "terms" {
		t.Fatalf("create: %v %+v", err, v)
	}
	g, err := c.GetVocabulary(ctx, &transcribe.GetVocabularyInput{VocabularyName: aws.String("terms")})
	if err != nil || g.VocabularyState != types.VocabularyStateReady || g.LanguageCode != types.LanguageCodeEnUs {
		t.Fatalf("get: %v %+v", err, g)
	}
	if l, err := c.ListVocabularies(ctx, &transcribe.ListVocabulariesInput{}); err != nil || len(l.Vocabularies) != 1 || aws.ToString(l.Vocabularies[0].VocabularyName) != "terms" {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.UpdateVocabulary(ctx, &transcribe.UpdateVocabularyInput{VocabularyName: aws.String("terms"), LanguageCode: types.LanguageCodeEnUs, Phrases: []string{"pogi"}}); err != nil {
		t.Fatal(err)
	}
	arn := "arn:aws:transcribe:us-east-1:000000000000:vocabulary/terms"
	if _, err := c.TagResource(ctx, &transcribe.TagResourceInput{ResourceArn: aws.String(arn), Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("ai")}}}); err != nil {
		t.Fatal(err)
	}
	if tags, err := c.ListTagsForResource(ctx, &transcribe.ListTagsForResourceInput{ResourceArn: aws.String(arn)}); err != nil || len(tags.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.UntagResource(ctx, &transcribe.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ := c.ListTagsForResource(ctx, &transcribe.ListTagsForResourceInput{ResourceArn: aws.String(arn)}); len(tags.Tags) != 1 {
		t.Fatalf("tags after untag: %+v", tags)
	}
	if _, err := c.DeleteVocabulary(ctx, &transcribe.DeleteVocabularyInput{VocabularyName: aws.String("terms")}); err != nil {
		t.Fatal(err)
	}
	var nf *types.NotFoundException
	if _, err := c.GetVocabulary(ctx, &transcribe.GetVocabularyInput{VocabularyName: aws.String("terms")}); !errors.As(err, &nf) {
		t.Fatalf("get deleted: %T %v", err, err)
	}
}
