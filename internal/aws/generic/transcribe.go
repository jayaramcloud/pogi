package generic

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Transcribe: a transcription job is in progress for a moment, then completed, and its transcript
// JSON is written to the output bucket. The transcript is canned (nothing listens to the media):
// it depends on the media only through its hash. Vocabularies and the like are the engine's.

const jobTime = 300 * time.Millisecond

// transcribeJob is the TranscriptionJob structure of a stored job; its status follows from its age.
func transcribeJob(r *Resource) map[string]any {
	job := map[string]any{}
	for k, v := range r.Data {
		job[k] = v
	}
	job["TranscriptionJobName"] = r.Name
	job["CreationTime"] = r.Created.Format(time.RFC3339Nano)
	job["StartTime"] = r.Created.Format(time.RFC3339Nano)
	if time.Since(r.Created) < jobTime {
		job["TranscriptionJobStatus"] = "IN_PROGRESS"
	} else {
		job["TranscriptionJobStatus"] = "COMPLETED"
		job["CompletionTime"] = r.Created.Add(jobTime).Format(time.RFC3339Nano)
	}
	for _, k := range core.SortedKeys(r.Tags) {
		job["Tags"] = append(asList(job["Tags"]), map[string]any{"Key": k, "Value": r.Tags[k]})
	}
	return job
}

// transcript is the JSON that Transcribe writes for a job.
func transcript(c *Call, name, mediaURI string) []byte {
	aiSeed := []byte(mediaURI)
	if b, ok := s3Object(c, mediaURI); ok {
		aiSeed = b
	}
	sum := sha256.Sum256(aiSeed)
	text := fmt.Sprintf("[pogi canned transcript %x] This is a canned transcription of %s.", sum[:4], path.Base(mediaURI))
	var items []any
	at := 0.0
	for _, w := range strings.Fields(text) {
		word := strings.TrimRight(w, ".")
		items = append(items, map[string]any{"start_time": strconv.FormatFloat(at, 'f', 2, 64), "end_time": strconv.FormatFloat(at+0.3, 'f', 2, 64), "type": "pronunciation",
			"alternatives": []any{map[string]any{"confidence": "0.9900", "content": word}}})
		at += 0.35
	}
	items = append(items, map[string]any{"type": "punctuation", "alternatives": []any{map[string]any{"confidence": "0.0", "content": "."}}})
	b, _ := json.Marshal(map[string]any{"jobName": name, "accountId": c.Account, "status": "COMPLETED",
		"results": map[string]any{"transcripts": []any{map[string]any{"transcript": text}}, "items": items}})
	return b
}

// s3Object reads an s3://bucket/key or https://s3.region.amazonaws.com/bucket/key media URI.
func s3Object(c *Call, uri string) ([]byte, bool) {
	rest := strings.TrimPrefix(uri, "s3://")
	if rest == uri {
		i := strings.Index(uri, ".amazonaws.com/")
		if i < 0 {
			return nil, false
		}
		rest = uri[i+len(".amazonaws.com/"):]
	}
	bucket, key, _ := strings.Cut(rest, "/")
	return aiS3Get(c, bucket, key)
}

func init() {
	const svc = "transcribe"

	Override(svc, "StartTranscriptionJob", func(c *Call) (map[string]any, error) {
		name := str(c.In["TranscriptionJobName"])
		if c.Get("TranscriptionJob", name) != nil {
			return nil, c.Exists("The requested job name already exists. Use a different job name.")
		}
		uri := str(aiAmap(c.In["Media"])["MediaFileUri"])
		tr := map[string]any{}
		bucket := str(c.In["OutputBucketName"])
		if bucket != "" {
			key := str(c.In["OutputKey"])
			if key == "" || strings.HasSuffix(key, "/") {
				key += name + ".json"
			}
			if !aiS3Put(c, bucket, key, transcript(c, name, uri), "application/json") {
				return nil, c.Invalid("The specified S3 bucket doesn't exist or isn't accessible: %s", bucket)
			}
			tr["TranscriptFileUri"] = "https://s3." + c.Region + ".amazonaws.com/" + bucket + "/" + key
		} else { // ponytail: the service's own bucket is not written, so this link leads nowhere
			tr["TranscriptFileUri"] = "https://s3." + c.Region + ".amazonaws.com/aws-transcribe-" + c.Region + "-prod/" + c.Account + "/" + name + "/asrOutput.json"
		}
		now := time.Now().UTC()
		data := map[string]any{"Media": c.In["Media"], "Transcript": tr, "Settings": c.In["Settings"], "LanguageCode": c.In["LanguageCode"],
			"MediaFormat": c.In["MediaFormat"], "MediaSampleRateHertz": c.In["MediaSampleRateHertz"], "IdentifyLanguage": c.In["IdentifyLanguage"],
			"ContentRedaction": c.In["ContentRedaction"], "JobExecutionSettings": c.In["JobExecutionSettings"], "Subtitles": c.In["Subtitles"], "OutputBucketName": bucket}
		for k, v := range data {
			if v == nil {
				delete(data, k)
			}
		}
		r := &Resource{Type: "TranscriptionJob", Key: name, IDs: map[string]string{"TranscriptionJobName": name}, Name: name, Account: c.Account, Region: c.Region,
			ARN: c.arnOf("transcription-job/"+name, nil), Tags: tagMap(c.In["Tags"]), Data: data, Created: now, Updated: now}
		c.Put(r)
		job := transcribeJob(r)
		job["TranscriptionJobStatus"] = "IN_PROGRESS" // the answer to Start is the job as it starts
		delete(job, "CompletionTime")
		return map[string]any{"TranscriptionJob": job}, nil
	})

	Override(svc, "GetTranscriptionJob", func(c *Call) (map[string]any, error) {
		r := c.Get("TranscriptionJob", str(c.In["TranscriptionJobName"]))
		if r == nil {
			return nil, c.NotFound("The requested job couldn't be found. Check the job name and try your request again.")
		}
		return map[string]any{"TranscriptionJob": transcribeJob(r)}, nil
	})

	Override(svc, "DeleteTranscriptionJob", func(c *Call) (map[string]any, error) {
		if !c.Svc.Delete(c.Account, c.Region, "TranscriptionJob", str(c.In["TranscriptionJobName"])) {
			return nil, c.NotFound("The requested job couldn't be found. Check the job name and try your request again.")
		}
		return map[string]any{}, nil
	})

	// Newest first, filtered by status and name, paged by an offset aiToken.
	Override(svc, "ListTranscriptionJobs", func(c *Call) (map[string]any, error) {
		rs := c.List("TranscriptionJob")
		slices.Reverse(rs)
		off, _ := strconv.Atoi(str(c.In["NextToken"]))
		limit := int(aiNum(c.In["MaxResults"]))
		if limit <= 0 {
			limit = 100
		}
		var out []any
		skipped, more := 0, false
		for _, r := range rs {
			j := transcribeJob(r)
			if s := str(c.In["Status"]); s != "" && s != j["TranscriptionJobStatus"] || !strings.Contains(r.Name, str(c.In["JobNameContains"])) {
				continue
			}
			if skipped < off {
				skipped++
				continue
			}
			if len(out) == limit {
				more = true
				break
			}
			j["OutputLocationType"] = "SERVICE_BUCKET"
			if j["OutputBucketName"] != nil {
				j["OutputLocationType"] = "CUSTOMER_BUCKET"
			}
			out = append(out, j)
		}
		res := map[string]any{"TranscriptionJobSummaries": out}
		if s := str(c.In["Status"]); s != "" {
			res["Status"] = s
		}
		if more {
			res["NextToken"] = strconv.Itoa(off + limit)
		}
		return res, nil
	})
}
