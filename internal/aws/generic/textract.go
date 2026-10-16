package generic

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/core"
)

// Textract: no OCR. A document whose bytes are text gets one LINE block per non-empty line (with
// its WORDs); any other document gets one canned line that depends on the bytes. Jobs succeed at
// once and answer with the same blocks.

func documentOf(c *Call, doc map[string]any) []byte {
	if b, err := base64.StdEncoding.DecodeString(str(doc["Bytes"])); err == nil && len(b) > 0 {
		return b
	}
	if o := aiAmap(doc["S3Object"]); o != nil {
		b, _ := aiS3Get(c, str(o["Bucket"]), str(o["Name"]))
		return b
	}
	return nil
}

func aiIsText(b []byte) bool {
	return len(b) > 0 && utf8.Valid(b) && !strings.ContainsRune(string(b), 0)
}

func blockID(aiSeed []byte, n int) string {
	sum := sha256.Sum256(append(append([]byte{}, aiSeed...), byte(n), byte(n>>8)))
	return fmt.Sprintf("%x-%x-%x-%x-%x", sum[:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}

func aiGeometry(top, left, width, height float64) map[string]any {
	return map[string]any{
		"BoundingBox": map[string]any{"Top": top, "Left": left, "Width": width, "Height": height},
		"Polygon": []any{map[string]any{"X": left, "Y": top}, map[string]any{"X": left + width, "Y": top},
			map[string]any{"X": left + width, "Y": top + height}, map[string]any{"X": left, "Y": top + height}},
	}
}

// textBlocks is the PAGE, LINE and WORD blocks of a document.
func textBlocks(data []byte) []any {
	var lines []string
	if aiIsText(data) {
		for _, l := range strings.Split(string(data), "\n") {
			if l = strings.TrimSpace(l); l != "" {
				lines = append(lines, l)
			}
		}
	}
	if len(lines) == 0 {
		sum := sha256.Sum256(data)
		lines = []string{fmt.Sprintf("POGI CANNED DOCUMENT %X", sum[:4])}
	}
	n := 0
	next := func() string { n++; return blockID(data, n) }
	page := map[string]any{"BlockType": "PAGE", "Id": next(), "Page": 1, "Geometry": aiGeometry(0, 0, 1, 1), "Confidence": 99.9}
	var out, children []any
	out = append(out, page)
	for i, l := range lines {
		top := min(0.04+float64(i)*0.03, 0.95)
		line := map[string]any{"BlockType": "LINE", "Id": next(), "Page": 1, "Text": l, "Confidence": 99.5, "Geometry": aiGeometry(top, 0.05, 0.9, 0.025)}
		var words []any
		var wordIDs []any
		for j, w := range strings.Fields(l) {
			id := next()
			wordIDs = append(wordIDs, id)
			words = append(words, map[string]any{"BlockType": "WORD", "Id": id, "Page": 1, "Text": w, "TextType": "PRINTED", "Confidence": 99.5,
				"Geometry": aiGeometry(top, 0.05+float64(j)*0.05, 0.045, 0.025)})
		}
		line["Relationships"] = []any{map[string]any{"Type": "CHILD", "Ids": wordIDs}}
		children = append(children, line["Id"])
		out = append(out, line)
		out = append(out, words...)
	}
	page["Relationships"] = []any{map[string]any{"Type": "CHILD", "Ids": children}}
	return out
}

func init() {
	const svc = "textract"
	meta := map[string]any{"Pages": 1}

	Override(svc, "DetectDocumentText", func(c *Call) (map[string]any, error) {
		data := documentOf(c, aiAmap(c.In["Document"]))
		return map[string]any{"Blocks": textBlocks(data), "DocumentMetadata": meta, "DetectDocumentTextModelVersion": "1.0"}, nil
	})
	Override(svc, "AnalyzeDocument", func(c *Call) (map[string]any, error) {
		data := documentOf(c, aiAmap(c.In["Document"]))
		return map[string]any{"Blocks": textBlocks(data), "DocumentMetadata": meta, "AnalyzeDocumentModelVersion": "1.0"}, nil
	})

	// Jobs: the document is read when the job is asked about, and the job has always succeeded.
	for _, op := range []string{"StartDocumentTextDetection", "StartDocumentAnalysis"} {
		Override(svc, op, func(c *Call) (map[string]any, error) {
			id := randHex(64)
			now := time.Now().UTC()
			c.Put(&Resource{Type: "Job", Key: id, ID: id, Account: c.Account, Region: c.Region, Created: now, Updated: now, Data: map[string]any{"DocumentLocation": c.In["DocumentLocation"]}})
			return map[string]any{"JobId": id}, nil
		})
	}
	for op, version := range map[string]string{"GetDocumentTextDetection": "DetectDocumentTextModelVersion", "GetDocumentAnalysis": "AnalyzeDocumentModelVersion"} {
		Override(svc, op, func(c *Call) (map[string]any, error) {
			job := c.Get("Job", str(c.In["JobId"]))
			if job == nil {
				return nil, core.Errorf(400, "InvalidJobIdException", "Could not aiFind JobId: %s", str(c.In["JobId"]))
			}
			o := aiAmap(aiAmap(job.Data["DocumentLocation"])["S3Object"])
			data, _ := aiS3Get(c, str(o["Bucket"]), str(o["Name"]))
			return map[string]any{"JobStatus": "SUCCEEDED", "Blocks": textBlocks(data), "DocumentMetadata": meta, version: "1.0"}, nil
		})
	}
}
