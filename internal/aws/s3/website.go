package s3

import (
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// isWebsiteHost is true for the static website endpoint, <bucket>.s3-website.<host>
// or <bucket>.s3-website-<region>.<host>.
func isWebsiteHost(host string) bool {
	return strings.Contains(strings.ToLower(host), ".s3-website")
}

// website serves a bucket as a static website (GET and HEAD): the index document for
// "/" and for folders, a redirect when a folder is asked for without its slash, and the
// error document with status 404 when a key is missing.
//
// ponytail: the website endpoint is open, with no bucket policy check (S3 needs a public
// policy); routing rules and per-object redirects are not applied.
func (s *Service) website(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return core.Errorf(405, "MethodNotAllowed", "The specified method is not allowed against this resource.")
	}
	var cfg struct {
		Index    string `xml:"IndexDocument>Suffix"`
		Error    string `xml:"ErrorDocument>Key"`
		Redirect struct {
			Host     string `xml:"HostName"`
			Protocol string `xml:"Protocol"`
		} `xml:"RedirectAllRequestsTo"`
	}
	var errDoc *Object
	slash := false // a folder asked for without its trailing slash
	hasKey := func(k string) bool { return k != "" && !strings.HasSuffix(k, "/") }
	err := s.withBucket(bucket, false, func(b *Bucket) error {
		raw := b.Config["website"]
		if raw == "" {
			return core.Errorf(404, "NoSuchWebsiteConfiguration", "The specified bucket does not have a website configuration")
		}
		if err := xml.Unmarshal([]byte(raw), &cfg); err != nil {
			return err
		}
		if cfg.Redirect.Host == "" && cfg.Index != "" && !hasKey(key) {
			key += cfg.Index
		} else if hasKey(key) && b.latest(key) == nil && cfg.Index != "" && b.latest(key+"/"+cfg.Index) != nil {
			slash = true
		}
		if p := b.latest(cfg.Error); cfg.Error != "" && p != nil {
			o := *p
			errDoc = &o
		}
		return nil
	})
	if err != nil {
		return err
	}
	if slash {
		http.Redirect(w, r, "/"+key+"/", http.StatusFound)
		return nil
	}
	if h := cfg.Redirect.Host; h != "" {
		proto := cfg.Redirect.Protocol
		if proto == "" {
			proto = "http"
		}
		http.Redirect(w, r, proto+"://"+h+r.URL.RequestURI(), http.StatusMovedPermanently)
		return nil
	}
	err = s.getObject(w, r, c, bucket, key)
	if e, ok := err.(*core.Error); !ok || e.Code != "NoSuchKey" {
		return err
	}
	if errDoc == nil {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("<html><head><title>404 Not Found</title></head><body><h1>404 Not Found</h1><ul><li>Code: NoSuchKey</li><li>Message: The specified key does not exist.</li><li>Key: " + xmlEscape(key) + "</li></ul></body></html>"))
		return nil
	}
	w.Header().Set("Content-Type", errDoc.ContentType)
	w.WriteHeader(http.StatusNotFound)
	if r.Method != http.MethodHead {
		w.Write(errDoc.Data)
	}
	return nil
}

func xmlEscape(s string) string {
	var b strings.Builder
	xml.EscapeText(&b, []byte(s))
	return b.String()
}
