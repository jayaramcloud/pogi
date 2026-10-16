package s3

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// controlTags serves S3 Control's ListTagsForResource, TagResource and
// UntagResource on bucket ARNs (/v20180820/tags/<arn>), which the AWS
// Terraform provider uses for bucket tags. They read and write the same tag
// set as Get/PutBucketTagging.
func (s *Service) controlTags(w http.ResponseWriter, r *http.Request, c *core.Ctx, arn string) error {
	arn, _ = url.PathUnescape(arn)
	_, bucket, ok := strings.Cut(arn, ":::")
	if !ok || !strings.HasPrefix(arn, "arn:") {
		return core.Errorf(400, "InvalidRequest", "Invalid ARN %q", arn)
	}
	read := func() (t taggingXML, err error) {
		err = s.withBucket(bucket, false, func(b *Bucket) error {
			return xml.Unmarshal([]byte(b.Config["tagging"]), &t)
		})
		if err != nil && core.AsError(err).Code == "NoSuchBucket" {
			return t, err
		}
		return t, nil // unset: no tags
	}
	write := func(t taggingXML) error {
		return s.withBucket(bucket, true, func(b *Bucket) error {
			if len(t.TagSet) == 0 {
				delete(b.Config, "tagging")
			} else {
				out, _ := xml.Marshal(t)
				b.Config["tagging"] = string(out)
			}
			s.journalBucket(b)
			return nil
		})
	}
	t, err := read()
	if err != nil {
		return err
	}
	switch r.Method {
	case http.MethodGet:
		c.Op = "ListTagsForResource"
		out := struct {
			XMLName xml.Name `xml:"ListTagsForResourceResult"`
			Xmlns   string   `xml:"xmlns,attr"`
			Tags    []tagXML `xml:"Tags>Tag"`
		}{Xmlns: "http://awss3control.amazonaws.com/doc/2018-08-20/", Tags: t.TagSet}
		w.Header().Set("Content-Type", "application/xml")
		b, _ := xml.Marshal(out)
		w.Write(append([]byte(xml.Header), b...))
		return nil
	case http.MethodPost:
		c.Op = "TagResource"
		var in struct {
			Tags []tagXML `xml:"Tags>Tag"`
		}
		if err := xml.Unmarshal(core.Body(r), &in); err != nil {
			return core.Errorf(400, "MalformedXML", "The XML you provided was not well-formed")
		}
		for _, n := range in.Tags {
			i := 0
			for ; i < len(t.TagSet) && t.TagSet[i].Key != n.Key; i++ {
			}
			if i < len(t.TagSet) {
				t.TagSet[i] = n
			} else {
				t.TagSet = append(t.TagSet, n)
			}
		}
	case http.MethodDelete:
		c.Op = "UntagResource"
		drop := r.URL.Query()["tagKeys"]
		kept := t.TagSet[:0]
		for _, kv := range t.TagSet {
			if !strings.Contains("\x00"+strings.Join(drop, "\x00")+"\x00", "\x00"+kv.Key+"\x00") {
				kept = append(kept, kv)
			}
		}
		t.TagSet = kept
	default:
		return core.Errorf(405, "MethodNotAllowed", "The specified method is not allowed against this resource.")
	}
	if err := write(t); err != nil {
		return err
	}
	w.WriteHeader(map[bool]int{true: http.StatusNoContent, false: http.StatusOK}[r.Method == http.MethodDelete])
	return nil
}
