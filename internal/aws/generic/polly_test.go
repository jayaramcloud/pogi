package generic_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/polly"
	"github.com/aws/aws-sdk-go-v2/service/polly/types"
	"github.com/nrndev/pogi/internal/awstest"
)

const pls = `<?xml version="1.0" encoding="UTF-8"?>
<lexicon version="1.0" xmlns="http://www.w3.org/2005/01/pronunciation-lexicon" alphabet="ipa" xml:lang="en-US">
<lexeme><grapheme>W3C</grapheme><alias>World Wide Web Consortium</alias></lexeme>
<lexeme><grapheme>pogi</grapheme><phoneme>poʊɡi</phoneme></lexeme>
</lexicon>`

func TestPolly(t *testing.T) {
	_, url := start(t, "polly")
	ctx := context.Background()
	c := polly.NewFromConfig(awstest.Config(t, url))

	if _, err := c.PutLexicon(ctx, &polly.PutLexiconInput{Name: aws.String("acme"), Content: aws.String(pls)}); err != nil {
		t.Fatal(err)
	}
	g, err := c.GetLexicon(ctx, &polly.GetLexiconInput{Name: aws.String("acme")})
	if err != nil || aws.ToString(g.Lexicon.Content) != pls {
		t.Fatalf("get: %v %+v", err, g)
	}
	a := g.LexiconAttributes
	if a == nil || aws.ToString(a.Alphabet) != "ipa" || string(a.LanguageCode) != "en-US" || a.LexemesCount != 2 ||
		aws.ToString(a.LexiconArn) != "arn:aws:polly:us-east-1:000000000000:lexicon/acme" || a.LastModified == nil {
		t.Fatalf("attributes: %+v", a)
	}
	// putting a lexicon again replaces it
	if _, err := c.PutLexicon(ctx, &polly.PutLexiconInput{Name: aws.String("acme"), Content: aws.String(pls)}); err != nil {
		t.Fatal(err)
	}
	l, err := c.ListLexicons(ctx, &polly.ListLexiconsInput{})
	if err != nil || len(l.Lexicons) != 1 || aws.ToString(l.Lexicons[0].Name) != "acme" {
		t.Fatalf("list: %v %+v", err, l)
	}
	var bad *types.InvalidLexiconException
	if _, err := c.PutLexicon(ctx, &polly.PutLexiconInput{Name: aws.String("junk"), Content: aws.String("not xml")}); !errors.As(err, &bad) {
		t.Fatalf("bad content: %T %v", err, err)
	}

	s, err := c.SynthesizeSpeech(ctx, &polly.SynthesizeSpeechInput{OutputFormat: types.OutputFormatMp3, Text: aws.String("Hello, world"), VoiceId: types.VoiceIdJoanna})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(s.AudioStream)
	if aws.ToString(s.ContentType) != "audio/mpeg" || s.RequestCharacters != 12 || len(body) != 0 {
		t.Fatalf("speech: type %q chars %d bytes %d", aws.ToString(s.ContentType), s.RequestCharacters, len(body))
	}
	s, err = c.SynthesizeSpeech(ctx, &polly.SynthesizeSpeechInput{OutputFormat: types.OutputFormatOggVorbis, TextType: types.TextTypeSsml, Text: aws.String("<speak>Hi <break/>there</speak>"), VoiceId: types.VoiceIdMatthew})
	if err != nil || aws.ToString(s.ContentType) != "audio/ogg" || s.RequestCharacters != 8 {
		t.Fatalf("ssml: %v %q %d", err, aws.ToString(s.ContentType), s.RequestCharacters)
	}
	if _, err := c.SynthesizeSpeech(ctx, &polly.SynthesizeSpeechInput{OutputFormat: types.OutputFormatMp3, VoiceId: types.VoiceIdJoanna}); err == nil {
		t.Fatal("speech without text succeeded")
	}
	v, err := c.DescribeVoices(ctx, &polly.DescribeVoicesInput{LanguageCode: types.LanguageCodeEnGb})
	if err != nil || len(v.Voices) == 0 {
		t.Fatalf("voices: %v %+v", err, v)
	}
	for _, x := range v.Voices {
		if x.LanguageCode != types.LanguageCodeEnGb {
			t.Fatalf("voice filter: %+v", x)
		}
	}

	if _, err := c.DeleteLexicon(ctx, &polly.DeleteLexiconInput{Name: aws.String("acme")}); err != nil {
		t.Fatal(err)
	}
	var nf *types.LexiconNotFoundException
	if _, err := c.GetLexicon(ctx, &polly.GetLexiconInput{Name: aws.String("acme")}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.DeleteLexicon(ctx, &polly.DeleteLexiconInput{Name: aws.String("acme")}); !errors.As(err, &nf) {
		t.Fatalf("second delete: %T %v", err, err)
	}
}
