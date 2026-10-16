package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/translate"
	"github.com/aws/aws-sdk-go-v2/service/translate/types"
	"github.com/nrndev/pogi"
)

func TestTranslateText(t *testing.T) {
	ctx := context.Background()
	fm := aiNewFakeModel(t)
	fm.setReply(func(last string) string { return "  Hola mundo \n" })
	local := translate.NewFromConfig(aiStart(t, pogi.Config{AIEndpoint: fm.URL, AIModels: map[string]string{"*": "m"}}))
	out, err := local.TranslateText(ctx, &translate.TranslateTextInput{Text: aws.String("Hello world"), SourceLanguageCode: aws.String("en"), TargetLanguageCode: aws.String("es")})
	if err != nil || aws.ToString(out.TranslatedText) != "Hola mundo" || aws.ToString(out.SourceLanguageCode) != "en" || aws.ToString(out.TargetLanguageCode) != "es" {
		t.Fatalf("local: %v %+v", err, out)
	}
	if sent := fm.reqs[0]["messages"].([]any)[1].(map[string]any)["content"].(string); !strings.Contains(sent, "from en to es") || !strings.Contains(sent, "Hello world") {
		t.Fatalf("prompt: %q", sent)
	}
	// "auto" finds the source language first
	out, err = local.TranslateText(ctx, &translate.TranslateTextInput{Text: aws.String("El perro de la casa es un animal que come en el jardín"), SourceLanguageCode: aws.String("auto"), TargetLanguageCode: aws.String("en")})
	if err != nil || aws.ToString(out.SourceLanguageCode) != "es" {
		t.Fatalf("auto: %v %+v", err, out)
	}

	canned := translate.NewFromConfig(aiStart(t, pogi.Config{}))
	a, err := canned.TranslateText(ctx, &translate.TranslateTextInput{Text: aws.String("Hello world"), SourceLanguageCode: aws.String("en"), TargetLanguageCode: aws.String("fr")})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := canned.TranslateText(ctx, &translate.TranslateTextInput{Text: aws.String("Hello world"), SourceLanguageCode: aws.String("en"), TargetLanguageCode: aws.String("fr")})
	if aws.ToString(a.TranslatedText) != aws.ToString(b.TranslatedText) || !strings.Contains(aws.ToString(a.TranslatedText), "canned") || !strings.Contains(aws.ToString(a.TranslatedText), "Hello world") {
		t.Fatalf("canned: %q %q", aws.ToString(a.TranslatedText), aws.ToString(b.TranslatedText))
	}
	l, err := canned.ListLanguages(ctx, &translate.ListLanguagesInput{})
	if err != nil || len(l.Languages) < 10 || aws.ToString(l.Languages[0].LanguageCode) == "" {
		t.Fatalf("languages: %v %+v", err, l)
	}
}

func TestTranslateTerminology(t *testing.T) {
	ctx := context.Background()
	c := translate.NewFromConfig(aiStart(t, pogi.Config{}))
	csv := func(rows string) *translate.ImportTerminologyInput {
		return &translate.ImportTerminologyInput{Name: aws.String("brand"), MergeStrategy: types.MergeStrategyOverwrite, Description: aws.String("names"),
			TerminologyData: &types.TerminologyData{Format: types.TerminologyDataFormatCsv, File: []byte(rows)}, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}
	}
	imp, err := c.ImportTerminology(ctx, csv("en,fr,de\nlaptop,ordinateur,Laptop\nmouse,souris,Maus\n"))
	if err != nil {
		t.Fatal(err)
	}
	p := imp.TerminologyProperties
	if aws.ToString(p.Name) != "brand" || aws.ToInt32(p.TermCount) != 2 || aws.ToString(p.SourceLanguageCode) != "en" || len(p.TargetLanguageCodes) != 2 ||
		aws.ToString(p.Arn) != "arn:aws:translate:us-east-1:000000000000:terminology/brand/LATEST" {
		t.Fatalf("import: %+v", p)
	}
	g, err := c.GetTerminology(ctx, &translate.GetTerminologyInput{Name: aws.String("brand")})
	if err != nil || aws.ToString(g.TerminologyProperties.Description) != "names" || aws.ToString(g.TerminologyDataLocation.Location) == "" {
		t.Fatalf("get: %v %+v", err, g)
	}
	l, err := c.ListTerminologies(ctx, &translate.ListTerminologiesInput{})
	if err != nil || len(l.TerminologyPropertiesList) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if again, err := c.ImportTerminology(ctx, csv("en,fr\nlaptop,ordinateur\n")); err != nil || aws.ToInt32(again.TerminologyProperties.TermCount) != 1 {
		t.Fatalf("overwrite: %v %+v", err, again)
	}
	if _, err := c.TagResource(ctx, &translate.TagResourceInput{ResourceArn: p.Arn, Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("ai")}}}); err != nil {
		t.Fatal(err)
	}
	if tags, err := c.ListTagsForResource(ctx, &translate.ListTagsForResourceInput{ResourceArn: p.Arn}); err != nil || len(tags.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.DeleteTerminology(ctx, &translate.DeleteTerminologyInput{Name: aws.String("brand")}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetTerminology(ctx, &translate.GetTerminologyInput{Name: aws.String("brand")}); !errors.As(err, &nf) {
		t.Fatalf("get deleted: %T %v", err, err)
	}
}
