package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/comprehend"
	"github.com/aws/aws-sdk-go-v2/service/comprehend/types"
	"github.com/nrndev/pogi"
)

func TestComprehendCannedAnalysis(t *testing.T) {
	ctx := context.Background()
	c := comprehend.NewFromConfig(aiStart(t, pogi.Config{}))

	s, err := c.DetectSentiment(ctx, &comprehend.DetectSentimentInput{Text: aws.String("I love this great product"), LanguageCode: types.LanguageCodeEn})
	if err != nil || s.Sentiment != types.SentimentTypePositive || aws.ToFloat32(s.SentimentScore.Positive) < 0.5 {
		t.Fatalf("positive: %v %+v", err, s)
	}
	for text, want := range map[string]types.SentimentType{"This is terrible and I hate it": types.SentimentTypeNegative, "The table has four legs": types.SentimentTypeNeutral,
		"The food was great but the service was awful": types.SentimentTypeMixed} {
		if s, err := c.DetectSentiment(ctx, &comprehend.DetectSentimentInput{Text: aws.String(text), LanguageCode: types.LanguageCodeEn}); err != nil || s.Sentiment != want {
			t.Fatalf("%q: %v %v", text, err, s)
		}
	}

	const text = "Yesterday Jeff Bezos flew to Seattle with 3 colleagues in March 2024."
	e, err := c.DetectEntities(ctx, &comprehend.DetectEntitiesInput{Text: aws.String(text), LanguageCode: types.LanguageCodeEn})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]types.EntityType{}
	for _, en := range e.Entities {
		if text[aws.ToInt32(en.BeginOffset):aws.ToInt32(en.EndOffset)] != aws.ToString(en.Text) {
			t.Errorf("offsets of %q are wrong: %d-%d", aws.ToString(en.Text), aws.ToInt32(en.BeginOffset), aws.ToInt32(en.EndOffset))
		}
		got[aws.ToString(en.Text)] = en.Type
	}
	if got["Jeff Bezos"] == "" || got["Seattle"] == "" || got["3"] != types.EntityTypeQuantity || got["March"] != types.EntityTypeDate {
		t.Fatalf("entities: %v", got)
	}
	e2, _ := c.DetectEntities(ctx, &comprehend.DetectEntitiesInput{Text: aws.String(text), LanguageCode: types.LanguageCodeEn})
	if len(e2.Entities) != len(e.Entities) || e2.Entities[0].Type != e.Entities[0].Type {
		t.Fatal("entities are not stable")
	}

	kp, err := c.DetectKeyPhrases(ctx, &comprehend.DetectKeyPhrasesInput{Text: aws.String("The quick brown fox jumps over the lazy dog"), LanguageCode: types.LanguageCodeEn})
	if err != nil || len(kp.KeyPhrases) < 2 || aws.ToString(kp.KeyPhrases[0].Text) != "quick brown fox jumps over" && aws.ToString(kp.KeyPhrases[0].Text) != "quick brown fox jumps" {
		t.Fatalf("key phrases: %v %+v", err, kp)
	}
	lang, err := c.DetectDominantLanguage(ctx, &comprehend.DetectDominantLanguageInput{Text: aws.String("El perro de la casa es un animal que come en el jardín")})
	if err != nil || len(lang.Languages) != 1 || aws.ToString(lang.Languages[0].LanguageCode) != "es" {
		t.Fatalf("language: %v %+v", err, lang)
	}
	sy, err := c.DetectSyntax(ctx, &comprehend.DetectSyntaxInput{Text: aws.String("The cat sat on 3 mats"), LanguageCode: types.SyntaxLanguageCodeEn})
	if err != nil || len(sy.SyntaxTokens) != 6 || sy.SyntaxTokens[0].PartOfSpeech.Tag != types.PartOfSpeechTagTypeDet || sy.SyntaxTokens[4].PartOfSpeech.Tag != types.PartOfSpeechTagTypeNum {
		t.Fatalf("syntax: %v %+v", err, sy)
	}
	pii, err := c.DetectPiiEntities(ctx, &comprehend.DetectPiiEntitiesInput{Text: aws.String("Mail bob@example.com or call 555-123-4567"), LanguageCode: types.LanguageCodeEn})
	if err != nil || len(pii.Entities) != 2 || pii.Entities[0].Type != types.PiiEntityTypeEmail {
		t.Fatalf("pii: %v %+v", err, pii)
	}
	if cp, err := c.ContainsPiiEntities(ctx, &comprehend.ContainsPiiEntitiesInput{Text: aws.String("ssn 123-45-6789"), LanguageCode: types.LanguageCodeEn}); err != nil || len(cp.Labels) != 1 || cp.Labels[0].Name != types.PiiEntityTypeSsn {
		t.Fatalf("contains pii: %v %+v", err, cp)
	}
	b, err := c.BatchDetectSentiment(ctx, &comprehend.BatchDetectSentimentInput{TextList: []string{"great", "awful", "plain"}, LanguageCode: types.LanguageCodeEn})
	if err != nil || len(b.ResultList) != 3 || b.ResultList[1].Sentiment != types.SentimentTypeNegative || aws.ToInt32(b.ResultList[2].Index) != 2 {
		t.Fatalf("batch: %v %+v", err, b)
	}
}

// With a model configured the answers come from it, through prompts that ask for JSON.
func TestComprehendLocalModel(t *testing.T) {
	ctx := context.Background()
	fm := aiNewFakeModel(t)
	fm.setReply(func(last string) string {
		switch {
		case strings.Contains(last, "sentiment"):
			return "Sure! ```json\n{\"sentiment\": \"negative\"}\n```" // the model's wrapping is tolerated
		case strings.Contains(last, "named entities"):
			return `[{"text":"Acme Corp","type":"ORGANIZATION"},{"text":"not in the text","type":"PERSON"}]`
		case strings.Contains(last, "key noun phrases"):
			return `["the new office"]`
		case strings.Contains(last, "language"):
			return `{"language":"nl"}`
		}
		return "no json here"
	})
	c := comprehend.NewFromConfig(aiStart(t, pogi.Config{AIEndpoint: fm.URL, AIModels: map[string]string{"*": "m"}}))

	s, err := c.DetectSentiment(ctx, &comprehend.DetectSentimentInput{Text: aws.String("I love this"), LanguageCode: types.LanguageCodeEn}) // the rules would say positive
	if err != nil || s.Sentiment != types.SentimentTypeNegative {
		t.Fatalf("model sentiment: %v %+v", err, s)
	}
	e, err := c.DetectEntities(ctx, &comprehend.DetectEntitiesInput{Text: aws.String("We work at Acme Corp now"), LanguageCode: types.LanguageCodeEn})
	if err != nil || len(e.Entities) != 1 || e.Entities[0].Type != types.EntityTypeOrganization || aws.ToInt32(e.Entities[0].BeginOffset) != 11 || aws.ToInt32(e.Entities[0].EndOffset) != 20 {
		t.Fatalf("model entities: %v %+v", err, e)
	}
	kp, err := c.DetectKeyPhrases(ctx, &comprehend.DetectKeyPhrasesInput{Text: aws.String("Visit the new office today"), LanguageCode: types.LanguageCodeEn})
	if err != nil || len(kp.KeyPhrases) != 1 || aws.ToString(kp.KeyPhrases[0].Text) != "the new office" {
		t.Fatalf("model key phrases: %v %+v", err, kp)
	}
	l, err := c.DetectDominantLanguage(ctx, &comprehend.DetectDominantLanguageInput{Text: aws.String("hello")})
	if err != nil || aws.ToString(l.Languages[0].LanguageCode) != "nl" {
		t.Fatalf("model language: %v %+v", err, l)
	}
	// an aiAnswer that is not JSON falls back to the rules
	fm.setReply(func(string) string { return "I cannot do that" })
	if s, err = c.DetectSentiment(ctx, &comprehend.DetectSentimentInput{Text: aws.String("I love this"), LanguageCode: types.LanguageCodeEn}); err != nil || s.Sentiment != types.SentimentTypePositive {
		t.Fatalf("fallback: %v %+v", err, s)
	}
}

func TestComprehendDocumentClassifier(t *testing.T) {
	ctx := context.Background()
	c := comprehend.NewFromConfig(aiStart(t, pogi.Config{}))

	d, err := c.CreateDocumentClassifier(ctx, &comprehend.CreateDocumentClassifierInput{DocumentClassifierName: aws.String("tickets"), DataAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/c"),
		LanguageCode: types.LanguageCodeEn, InputDataConfig: &types.DocumentClassifierInputDataConfig{S3Uri: aws.String("s3://b/train.csv")}, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil || !strings.HasSuffix(aws.ToString(d.DocumentClassifierArn), ":document-classifier/tickets") {
		t.Fatalf("create: %v %s", err, aws.ToString(d.DocumentClassifierArn))
	}
	g, err := c.DescribeDocumentClassifier(ctx, &comprehend.DescribeDocumentClassifierInput{DocumentClassifierArn: d.DocumentClassifierArn})
	if err != nil || g.DocumentClassifierProperties.Status != types.ModelStatusTrained || g.DocumentClassifierProperties.LanguageCode != types.LanguageCodeEn {
		t.Fatalf("describe: %v %+v", err, g)
	}
	l, err := c.ListDocumentClassifiers(ctx, &comprehend.ListDocumentClassifiersInput{})
	if err != nil || len(l.DocumentClassifierPropertiesList) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.TagResource(ctx, &comprehend.TagResourceInput{ResourceArn: d.DocumentClassifierArn, Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("ai")}}}); err != nil {
		t.Fatal(err)
	}
	if tags, err := c.ListTagsForResource(ctx, &comprehend.ListTagsForResourceInput{ResourceArn: d.DocumentClassifierArn}); err != nil || len(tags.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.UntagResource(ctx, &comprehend.UntagResourceInput{ResourceArn: d.DocumentClassifierArn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ := c.ListTagsForResource(ctx, &comprehend.ListTagsForResourceInput{ResourceArn: d.DocumentClassifierArn}); len(tags.Tags) != 1 {
		t.Fatalf("tags after untag: %+v", tags)
	}
	if _, err := c.DeleteDocumentClassifier(ctx, &comprehend.DeleteDocumentClassifierInput{DocumentClassifierArn: d.DocumentClassifierArn}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.DescribeDocumentClassifier(ctx, &comprehend.DescribeDocumentClassifierInput{DocumentClassifierArn: d.DocumentClassifierArn}); !errors.As(err, &nf) {
		t.Fatalf("describe deleted: %T %v", err, err)
	}
}
