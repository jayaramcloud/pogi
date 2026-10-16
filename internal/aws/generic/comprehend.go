package generic

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"regexp"
	"strings"
	"unicode"
)

// Comprehend: sentiment, entities, key phrases and language use the local model when one is
// configured (a prompt that asks for JSON); otherwise, or when the model's answer is not usable,
// small deterministic rules answer. Syntax and PII are always rules. Document classifiers and the
// like are stored metadata.

// modelJSON asks the model for a JSON value; ok is false when there is no model or no usable answer.
func modelJSON(c *Call, id, system, prompt string) (v any, ok bool) {
	text, used, err := aiGateway(c).Complete(context.Background(), id, system, prompt)
	if !used || err != nil {
		return nil, false
	}
	start := strings.IndexAny(text, "{[")
	end := strings.LastIndexAny(text, "}]")
	if start < 0 || end < start || json.Unmarshal([]byte(text[start:end+1]), &v) != nil {
		return nil, false
	}
	return v, true
}

const jsonOnly = "You analyse text. Reply with only the JSON asked for, no other words."

var aiScores = map[string][4]float64{ // positive, negative, neutral, mixed
	"POSITIVE": {0.95, 0.01, 0.03, 0.01}, "NEGATIVE": {0.01, 0.95, 0.03, 0.01}, "NEUTRAL": {0.02, 0.02, 0.95, 0.01}, "MIXED": {0.3, 0.3, 0.1, 0.3},
}

func sentimentOf(c *Call, text string) (string, map[string]any) {
	label := ""
	if v, ok := modelJSON(c, "comprehend", jsonOnly, `Classify the sentiment of the text as POSITIVE, NEGATIVE, NEUTRAL or MIXED. Reply {"sentiment":"..."}.`+"\n\nText: "+text); ok {
		label = strings.ToUpper(str(aiAmap(v)["sentiment"]))
	}
	if _, ok := aiScores[label]; !ok {
		pos, neg := 0, 0
		for _, w := range aiTokens(text) {
			switch strings.ToLower(strings.Trim(w.text, ".,!?;:")) {
			case "good", "great", "love", "loved", "excellent", "happy", "wonderful", "amazing", "best", "nice", "fantastic", "like":
				pos++
			case "bad", "terrible", "hate", "hated", "awful", "sad", "horrible", "worst", "poor", "angry", "disappointing":
				neg++
			}
		}
		switch {
		case pos > 0 && neg > 0:
			label = "MIXED"
		case pos > 0:
			label = "POSITIVE"
		case neg > 0:
			label = "NEGATIVE"
		default:
			label = "NEUTRAL"
		}
	}
	s := aiScores[label]
	return label, map[string]any{"Positive": s[0], "Negative": s[1], "Neutral": s[2], "Mixed": s[3]}
}

type aiToken struct {
	text       string
	begin, end int
}

var wordRe = regexp.MustCompile(`\S+`)

// aiTokens are the whitespace separated words of a text with their offsets.
func aiTokens(s string) []aiToken {
	var out []aiToken
	for _, m := range wordRe.FindAllStringIndex(s, -1) {
		out = append(out, aiToken{s[m[0]:m[1]], len([]rune(s[:m[0]])), len([]rune(s[:m[1]]))})
	}
	return out
}

func isCapitalized(w string) bool {
	r := []rune(w)
	return len(r) > 1 && unicode.IsUpper(r[0]) && unicode.IsLetter(r[0])
}

func trimPunct(w string) string {
	return strings.TrimFunc(w, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
}

var aiMonths = map[string]bool{"january": true, "february": true, "march": true, "april": true, "may": true, "june": true, "july": true, "august": true,
	"september": true, "october": true, "november": true, "december": true}

func entitiesOf(c *Call, text string) []any {
	if v, ok := modelJSON(c, "comprehend", jsonOnly, `List the named entities of the text as [{"text":"...","type":"PERSON|LOCATION|ORGANIZATION|COMMERCIAL_ITEM|EVENT|DATE|QUANTITY|TITLE|OTHER"}].`+"\n\nText: "+text); ok {
		out := []any{}
		for _, e := range asList(v) {
			em := aiAmap(e)
			t := str(em["text"])
			if i := strings.Index(text, t); t != "" && i >= 0 {
				b := len([]rune(text[:i]))
				out = append(out, map[string]any{"Text": t, "Type": strings.ToUpper(aiFirstNonEmpty(str(em["type"]), "OTHER")), "Score": 0.99, "BeginOffset": b, "EndOffset": b + len([]rune(t))})
			}
		}
		return out
	}
	out := []any{}
	toks := aiTokens(text)
	for i := 0; i < len(toks); i++ {
		w := trimPunct(toks[i].text)
		first := i == 0 || strings.HasSuffix(toks[i-1].text, ".") || strings.HasSuffix(toks[i-1].text, "!") || strings.HasSuffix(toks[i-1].text, "?")
		kind := ""
		switch {
		case w == "":
			continue
		case unicode.IsDigit([]rune(w)[0]):
			kind = "QUANTITY"
			if len(w) == 4 {
				kind = "DATE"
			}
		case aiMonths[strings.ToLower(w)]:
			kind = "DATE"
		case isCapitalized(w) && !first:
			sum := sha256.Sum256([]byte(w))
			kind = []string{"PERSON", "LOCATION", "ORGANIZATION"}[int(sum[0])%3]
		default:
			continue
		}
		j, end := i, toks[i].end // a run of capitalized words is one entity
		for kind != "QUANTITY" && kind != "DATE" && j+1 < len(toks) && isCapitalized(trimPunct(toks[j+1].text)) && !strings.HasSuffix(toks[j].text, ".") {
			j++
			end = toks[j].end
		}
		r := []rune(text)
		phrase := strings.TrimRightFunc(string(r[toks[i].begin:end]), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
		out = append(out, map[string]any{"Text": phrase, "Type": kind, "Score": 0.99, "BeginOffset": toks[i].begin, "EndOffset": toks[i].begin + len([]rune(phrase))})
		i = j
	}
	return out
}

var aiStop = map[string]bool{"the": true, "a": true, "an": true, "and": true, "or": true, "but": true, "of": true, "in": true, "on": true, "at": true, "to": true, "for": true, "with": true,
	"is": true, "are": true, "was": true, "were": true, "it": true, "this": true, "that": true, "i": true, "you": true, "he": true, "she": true, "we": true, "they": true, "my": true, "your": true,
	"be": true, "as": true, "by": true, "from": true, "have": true, "has": true, "had": true, "not": true, "so": true}

func keyPhrasesOf(c *Call, text string) []any {
	if v, ok := modelJSON(c, "comprehend", jsonOnly, `List the key noun phrases of the text as ["...", "..."].`+"\n\nText: "+text); ok {
		out := []any{}
		for _, p := range asList(v) {
			if t := str(p); t != "" && strings.Contains(text, t) {
				b := len([]rune(text[:strings.Index(text, t)]))
				out = append(out, map[string]any{"Text": t, "Score": 0.99, "BeginOffset": b, "EndOffset": b + len([]rune(t))})
			}
		}
		return out
	}
	out := []any{}
	toks := aiTokens(text)
	r := []rune(text)
	for i := 0; i < len(toks) && len(out) < 10; i++ {
		if w := trimPunct(toks[i].text); w == "" || aiStop[strings.ToLower(w)] {
			continue
		}
		j := i
		for j+1 < len(toks) && !aiStop[strings.ToLower(trimPunct(toks[j+1].text))] && trimPunct(toks[j+1].text) != "" && !strings.ContainsAny(toks[j].text[len(toks[j].text)-1:], ".,;:!?") {
			j++
		}
		phrase := strings.TrimFunc(string(r[toks[i].begin:toks[j].end]), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
		out = append(out, map[string]any{"Text": phrase, "Score": 0.99, "BeginOffset": toks[i].begin, "EndOffset": toks[i].begin + len([]rune(phrase))})
		i = j
	}
	return out
}

var langWords = map[string]string{
	"en": " the and is of to in that it for ", "es": " el la los de que y en un una es por ", "fr": " le la les de et un une est des que ",
	"de": " der die das und ist nicht ein eine zu ", "it": " il lo gli di che e un una per non ", "pt": " o os de que e um uma para não ",
}

func languagesOf(c *Call, text string) []any {
	if v, ok := modelJSON(c, "comprehend", jsonOnly, `Name the language of the text as an ISO 639-1 code. Reply {"language":"xx"}.`+"\n\nText: "+text); ok {
		if code := strings.ToLower(str(aiAmap(v)["language"])); code != "" {
			return []any{map[string]any{"LanguageCode": code, "Score": 0.99}}
		}
	}
	best, bestN := "en", 0
	for _, code := range []string{"en", "es", "fr", "de", "it", "pt"} {
		n := 0
		for _, w := range strings.Fields(strings.ToLower(text)) {
			if strings.Contains(langWords[code], " "+trimPunct(w)+" ") && w != "" {
				n++
			}
		}
		if n > bestN {
			best, bestN = code, n
		}
	}
	return []any{map[string]any{"LanguageCode": best, "Score": 0.99}}
}

func syntaxOf(text string) []any {
	out := []any{}
	for i, t := range aiTokens(text) {
		w := strings.ToLower(trimPunct(t.text))
		tag := "NOUN"
		switch {
		case w == "":
			tag = "PUNCT"
		case unicode.IsDigit([]rune(w)[0]):
			tag = "NUM"
		case w == "a" || w == "an" || w == "the" || w == "this" || w == "that":
			tag = "DET"
		case w == "in" || w == "on" || w == "of" || w == "at" || w == "to" || w == "for" || w == "with" || w == "from" || w == "by":
			tag = "ADP"
		case w == "and" || w == "or" || w == "but":
			tag = "CCONJ"
		case w == "i" || w == "you" || w == "he" || w == "she" || w == "it" || w == "we" || w == "they":
			tag = "PRON"
		case w == "is" || w == "are" || w == "was" || w == "were" || w == "be" || strings.HasSuffix(w, "ing") || strings.HasSuffix(w, "ed"):
			tag = "VERB"
		case isCapitalized(trimPunct(t.text)) && i > 0:
			tag = "PROPN"
		}
		out = append(out, map[string]any{"TokenId": i + 1, "Text": t.text, "BeginOffset": t.begin, "EndOffset": t.end, "PartOfSpeech": map[string]any{"Tag": tag, "Score": 0.99}})
	}
	return out
}

var piiRes = []struct {
	kind string
	re   *regexp.Regexp
}{
	{"EMAIL", regexp.MustCompile(`[\w.+-]+@[\w-]+(\.[\w-]+)+`)},
	{"SSN", regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)},
	{"CREDIT_DEBIT_NUMBER", regexp.MustCompile(`\b(?:\d{4}[ -]?){3}\d{4}\b`)},
	{"PHONE", regexp.MustCompile(`(?:\+?\d{1,3}[ .-])?\(?\d{3}\)?[ .-]\d{3}[ .-]\d{4}\b`)},
}

func piiOf(text string) []any {
	out := []any{}
	for _, p := range piiRes {
		for _, m := range p.re.FindAllStringIndex(text, -1) {
			out = append(out, map[string]any{"Type": p.kind, "Score": 0.99, "BeginOffset": len([]rune(text[:m[0]])), "EndOffset": len([]rune(text[:m[1]]))})
		}
	}
	return out
}

func init() {
	const svc = "comprehend"
	text := func(c *Call) string { return str(c.In["Text"]) }

	Override(svc, "DetectSentiment", func(c *Call) (map[string]any, error) {
		s, sc := sentimentOf(c, text(c))
		return map[string]any{"Sentiment": s, "SentimentScore": sc}, nil
	})
	Override(svc, "DetectEntities", func(c *Call) (map[string]any, error) { return map[string]any{"Entities": entitiesOf(c, text(c))}, nil })
	Override(svc, "DetectKeyPhrases", func(c *Call) (map[string]any, error) {
		return map[string]any{"KeyPhrases": keyPhrasesOf(c, text(c))}, nil
	})
	Override(svc, "DetectDominantLanguage", func(c *Call) (map[string]any, error) {
		return map[string]any{"Languages": languagesOf(c, text(c))}, nil
	})
	Override(svc, "DetectSyntax", func(c *Call) (map[string]any, error) { return map[string]any{"SyntaxTokens": syntaxOf(text(c))}, nil })
	Override(svc, "DetectPiiEntities", func(c *Call) (map[string]any, error) { return map[string]any{"Entities": piiOf(text(c))}, nil })
	Override(svc, "ContainsPiiEntities", func(c *Call) (map[string]any, error) {
		seen, labels := map[string]bool{}, []any{}
		for _, e := range piiOf(text(c)) {
			if k := str(aiAmap(e)["Type"]); !seen[k] {
				seen[k] = true
				labels = append(labels, map[string]any{"Name": k, "Score": 0.99})
			}
		}
		return map[string]any{"Labels": labels}, nil
	})

	// The batch calls run the single one on every text.
	batch := func(op string, one func(c *Call, text string) map[string]any) {
		Override(svc, op, func(c *Call) (map[string]any, error) {
			results := []any{}
			for i, t := range asList(c.In["TextList"]) {
				r := one(c, str(t))
				r["Index"] = i
				results = append(results, r)
			}
			return map[string]any{"ResultList": results, "ErrorList": []any{}}, nil
		})
	}
	batch("BatchDetectSentiment", func(c *Call, t string) map[string]any {
		s, sc := sentimentOf(c, t)
		return map[string]any{"Sentiment": s, "SentimentScore": sc}
	})
	batch("BatchDetectEntities", func(c *Call, t string) map[string]any { return map[string]any{"Entities": entitiesOf(c, t)} })
	batch("BatchDetectKeyPhrases", func(c *Call, t string) map[string]any { return map[string]any{"KeyPhrases": keyPhrasesOf(c, t)} })
	batch("BatchDetectDominantLanguage", func(c *Call, t string) map[string]any { return map[string]any{"Languages": languagesOf(c, t)} })
	batch("BatchDetectSyntax", func(c *Call, t string) map[string]any { return map[string]any{"SyntaxTokens": syntaxOf(t)} })

	// Training is not done: a classifier or recognizer is trained at once. Their ARNs carry the name.
	for _, x := range []struct{ typ, prefix, nameMember string }{{"DocumentClassifier", "document-classifier", "DocumentClassifierName"}, {"EntityRecognizer", "entity-recognizer", "RecognizerName"}} {
		aiSeed(svc, "Create"+x.typ, func(c *Call, r *Resource) {
			arn := c.arnOf(x.prefix+"/"+str(c.In[x.nameMember]), nil)
			c.Svc.Delete(c.Account, c.Region, r.Type, r.Key)
			for k := range r.IDs {
				r.IDs[k] = arn
			}
			r.Key, r.ARN = arn, arn
			r.Data["Status"], r.Data[x.typ+"Arn"] = "TRAINED", arn
		}, func(c *Call) error {
			if c.Get(x.typ, c.arnOf(x.prefix+"/"+str(c.In[x.nameMember]), nil)) != nil {
				return c.Exists("%s %s already exists", x.typ, str(c.In[x.nameMember]))
			}
			return nil
		})
	}
}
