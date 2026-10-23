package generic

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// Polly: lexicons are stored and read back, with the attributes AWS works out from the PLS file. Nothing is
// spoken: SynthesizeSpeech answers with an empty audio stream, the content type of the format asked for and the
// character count, so a client that plays or saves the audio gets a zero-byte file.
var (
	plsLang     = regexp.MustCompile(`xml:lang="([^"]*)"`)
	plsAlphabet = regexp.MustCompile(`alphabet="([^"]*)"`)
	plsLexeme   = regexp.MustCompile(`<lexeme[\s>]`)
	ssmlTag     = regexp.MustCompile(`<[^>]*>`)
)

var pollyTypes = map[string]string{"mp3": "audio/mpeg", "ogg_vorbis": "audio/ogg", "ogg_opus": "audio/ogg", "pcm": "audio/pcm", "json": "application/x-json-stream"}

// A few real voices, so that DescribeVoices is not empty. It is not the full list.
var pollyVoices = []map[string]any{
	{"Id": "Joanna", "Name": "Joanna", "Gender": "Female", "LanguageCode": "en-US", "LanguageName": "US English"},
	{"Id": "Matthew", "Name": "Matthew", "Gender": "Male", "LanguageCode": "en-US", "LanguageName": "US English"},
	{"Id": "Amy", "Name": "Amy", "Gender": "Female", "LanguageCode": "en-GB", "LanguageName": "British English"},
	{"Id": "Brian", "Name": "Brian", "Gender": "Male", "LanguageCode": "en-GB", "LanguageName": "British English"},
}

func init() {
	Override("polly", "PutLexicon", func(c *Call) (map[string]any, error) {
		content := str(c.In["Content"])
		if !strings.Contains(content, "<lexicon") {
			return nil, c.Err("InvalidLexiconException", "The lexicon content is not a valid PLS document.")
		}
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		d := map[string]any{"LexemesCount": float64(len(plsLexeme.FindAllString(content, -1))), "Size": float64(len(content))}
		if m := plsLang.FindStringSubmatch(content); m != nil {
			d["LanguageCode"] = m[1]
		}
		if m := plsAlphabet.FindStringSubmatch(content); m != nil {
			d["Alphabet"] = m[1]
		}
		s2Set(c, nil, d)
		return out, nil
	})
	Override("polly", "SynthesizeSpeech", func(c *Call) (map[string]any, error) {
		text := str(c.In["Text"])
		if str(c.In["TextType"]) == "ssml" {
			text = ssmlTag.ReplaceAllString(text, "")
		}
		return map[string]any{"AudioStream": "", "ContentType": pollyTypes[str(c.In["OutputFormat"])],
			"RequestCharacters": float64(utf8.RuneCountInString(text))}, nil
	})
	Override("polly", "DescribeVoices", func(c *Call) (map[string]any, error) {
		var voices []any
		for _, v := range pollyVoices {
			if l := str(c.In["LanguageCode"]); l == "" || l == v["LanguageCode"] {
				voices = append(voices, map[string]any{"Id": v["Id"], "Name": v["Name"], "Gender": v["Gender"], "LanguageCode": v["LanguageCode"],
					"LanguageName": v["LanguageName"], "SupportedEngines": []any{"standard"}, "AdditionalLanguageCodes": []any{}})
			}
		}
		return map[string]any{"Voices": voices}, nil
	})
}
