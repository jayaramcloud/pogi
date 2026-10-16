package generic

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"
)

// Translate: TranslateText uses the local model when one is configured, else a marked canned
// translation. Terminologies are stored (their languages and term count come from a CSV file).

var translateLanguages = []string{"ar:Arabic", "zh:Chinese (Simplified)", "nl:Dutch", "en:English", "fr:French", "de:German", "hi:Hindi", "it:Italian", "ja:Japanese", "ko:Korean",
	"pl:Polish", "pt:Portuguese (Brazil)", "ru:Russian", "es:Spanish", "sv:Swedish", "tr:Turkish"}

func init() {
	const svc = "translate"

	Override(svc, "TranslateText", func(c *Call) (map[string]any, error) {
		text, src, dst := str(c.In["Text"]), str(c.In["SourceLanguageCode"]), str(c.In["TargetLanguageCode"])
		if src == "auto" {
			src = str(aiAmap(languagesOf(c, text)[0])["LanguageCode"])
		}
		out, ok, err := aiGateway(c).Complete(context.Background(), "translate", "You are a translator. Reply with only the translation, nothing else.",
			fmt.Sprintf("Translate from %s to %s:\n\n%s", src, dst, text))
		if err != nil || !ok || strings.TrimSpace(out) == "" {
			out = fmt.Sprintf("[pogi canned translation %s->%s] %s", src, dst, text) // ponytail: a failing local model also gives the canned text
		}
		return map[string]any{"TranslatedText": strings.TrimSpace(out), "SourceLanguageCode": src, "TargetLanguageCode": dst}, nil
	})

	Override(svc, "ListLanguages", func(c *Call) (map[string]any, error) {
		var out []any
		for _, l := range translateLanguages {
			code, name, _ := strings.Cut(l, ":")
			out = append(out, map[string]any{"LanguageCode": code, "LanguageName": name})
		}
		return map[string]any{"Languages": out, "DisplayLanguageCode": "en"}, nil
	})

	// Importing a terminology under an existing name replaces it (MergeStrategy only has OVERWRITE).
	Override(svc, "ImportTerminology", func(c *Call) (map[string]any, error) {
		name := str(c.In["Name"])
		td := aiAmap(c.In["TerminologyData"])
		file, _ := base64.StdEncoding.DecodeString(str(td["File"]))
		lines := strings.Split(strings.TrimSpace(string(file)), "\n")
		langs := []any{"es"}
		src := "en"
		if header := strings.Split(strings.TrimSpace(lines[0]), ","); len(header) > 1 {
			src, langs = strings.TrimSpace(header[0]), nil
			for _, l := range header[1:] {
				langs = append(langs, strings.TrimSpace(l))
			}
		}
		now := time.Now().UTC()
		r := &Resource{Type: "Terminology", Key: name, IDs: map[string]string{"Name": name}, Name: name, Account: c.Account, Region: c.Region, Created: now, Updated: now,
			ARN: c.arnOf("terminology/"+name+"/LATEST", nil), Tags: tagMap(c.In["Tags"]),
			Data: map[string]any{"Description": c.In["Description"], "Format": td["Format"], "Directionality": aiFirstNonEmpty(str(td["Directionality"]), "UNI"),
				"SourceLanguageCode": src, "TargetLanguageCodes": langs, "TermCount": float64(len(lines) - 1), "SizeBytes": float64(len(file)), "SkippedTermCount": 0.0, "EncryptionKey": c.In["EncryptionKey"]}}
		if old := c.Get("Terminology", name); old != nil {
			r.Created = old.Created
		}
		c.Put(r)
		c.Res = r
		return map[string]any{"TerminologyProperties": c.Fill(r)["TerminologyProperties"]}, nil
	})
	Override(svc, "GetTerminology", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		out["TerminologyDataLocation"] = map[string]any{"RepositoryType": "S3", "Location": "https://s3.us-east-1.amazonaws.com/pogi-translate/" + str(c.In["Name"])}
		return out, nil
	})
}
