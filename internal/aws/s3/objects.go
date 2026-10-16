package s3

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// withBucket runs f with the bucket under the read or write lock.
func (s *Service) withBucket(name string, write bool, f func(b *Bucket) error) error {
	if write {
		s.mu.Lock()
		defer s.mu.Unlock()
	} else {
		s.mu.RLock()
		defer s.mu.RUnlock()
	}
	b := s.st.Buckets[name]
	if b == nil {
		return errNoBucket(name)
	}
	return f(b)
}

var storedHeaders = []string{"Content-Encoding", "Content-Disposition", "Content-Language", "Cache-Control", "Expires",
	"X-Amz-Website-Redirect-Location", "X-Amz-Server-Side-Encryption", "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id",
	"X-Amz-Server-Side-Encryption-Bucket-Key-Enabled", "X-Amz-Object-Lock-Mode", "X-Amz-Object-Lock-Retain-Until-Date",
	"X-Amz-Object-Lock-Legal-Hold"}

// newObject builds an object from request headers (no versioning applied yet).
func newObject(h http.Header, key string, data []byte) *Object {
	o := &Object{Key: key, Data: data, ETag: etag(data), Modified: time.Now().UTC(), ContentType: h.Get("Content-Type")}
	if o.ContentType == "" {
		o.ContentType = "binary/octet-stream"
	}
	for _, name := range storedHeaders {
		v := h.Get(name)
		if name == "Content-Encoding" {
			var keep []string
			for _, e := range strings.Split(v, ",") {
				if e = strings.TrimSpace(e); e != "" && e != "aws-chunked" {
					keep = append(keep, e)
				}
			}
			v = strings.Join(keep, ",")
		}
		if v != "" {
			if o.Headers == nil {
				o.Headers = map[string]string{}
			}
			o.Headers[name] = v
		}
	}
	for k, v := range h {
		if m, ok := strings.CutPrefix(k, "X-Amz-Meta-"); ok {
			if o.Meta == nil {
				o.Meta = map[string]string{}
			}
			o.Meta[strings.ToLower(m)] = strings.Join(v, ",")
		}
	}
	o.Grants = grantsOf(h)
	if t := h.Get("X-Amz-Tagging"); t != "" {
		vals, _ := url.ParseQuery(t)
		o.Tags = map[string]string{}
		for k := range vals {
			o.Tags[k] = vals.Get(k)
		}
	}
	if sc := h.Get("X-Amz-Storage-Class"); sc != "" && sc != "STANDARD" {
		o.StorageClass = sc
	}
	return o
}

func trimETag(s string) string { return strings.Trim(strings.TrimSpace(s), `"`) }

func etagMatch(header, etag string) bool {
	for _, e := range strings.Split(header, ",") {
		if e = strings.TrimSpace(e); e == "*" || trimETag(e) == trimETag(etag) {
			return true
		}
	}
	return false
}

func errPrecondition() error {
	return core.Errorf(412, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold")
}

// writePrecondition implements conditional writes (If-None-Match: *, If-Match).
func writePrecondition(h http.Header, key string, cur *Object) error {
	if h.Get("If-None-Match") == "*" && cur != nil {
		return errPrecondition()
	}
	if im := h.Get("If-Match"); im != "" {
		if cur == nil {
			return errNoKey(key)
		}
		if !etagMatch(im, cur.ETag) {
			return errPrecondition()
		}
	}
	return nil
}

func (s *Service) putObject(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	if len(key) > 1024 {
		return core.Errorf(400, "KeyTooLongError", "Your key is too long")
	}
	data, trailers, err := readBody(r)
	if err != nil {
		return err
	}
	algo, sum, err := verifyChecksums(r, trailers, data)
	if err != nil {
		return err
	}
	if algo == "" { // S3 adds a CRC64NVME checksum to every object that arrives without one (floci #858)
		algo = "CRC64NVME"
		sum = checksum(algo, data)
	}
	o := newObject(r.Header, key, data)
	o.ChecksumAlgo, o.Checksum, o.ChecksumType = algo, sum, "FULL_OBJECT"
	var ev []pending
	var expiration string
	versioned := false
	err = s.withBucket(bucket, true, func(b *Bucket) error {
		if err := writePrecondition(r.Header, key, b.latest(key)); err != nil {
			return err
		}
		if err := b.checkLockHeaders(r.Header); err != nil {
			return err
		}
		if b.Versioning != "Enabled" && lockedBy(b.version(key, "null"), time.Now(), false) != "" { // the write would replace a locked version
			return errLocked()
		}
		b.put(o)
		s.journal(b, key)
		versioned = b.Versioning != ""
		expiration = b.expirationHeader(o)
		ev = s.events(b, "ObjectCreated:Put", o)
		return nil
	})
	if err != nil {
		return err
	}
	s.notify(c.Trace, ev)
	if expiration != "" {
		w.Header().Set("x-amz-expiration", expiration)
	}
	if versioned {
		w.Header().Set("x-amz-version-id", o.VersionID)
	}
	w.Header().Set("ETag", o.ETag)
	w.Header().Set("x-amz-checksum-"+strings.ToLower(algo), sum)
	w.Header().Set("x-amz-checksum-type", "FULL_OBJECT")
	for _, name := range []string{"X-Amz-Server-Side-Encryption", "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", "X-Amz-Server-Side-Encryption-Bucket-Key-Enabled"} {
		if v := o.Headers[name]; v != "" { // the response names the encryption, as Head and Get do (floci #2144)
			w.Header().Set(name, v)
		}
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

// lookup finds the requested version (or the latest) of a key.
func lookup(w http.ResponseWriter, b *Bucket, key, versionID string) (*Object, error) {
	if versionID != "" {
		o := b.version(key, versionID)
		if o == nil {
			return nil, core.Errorf(404, "NoSuchVersion", "The specified version does not exist.")
		}
		if o.DeleteMarker {
			w.Header().Set("x-amz-delete-marker", "true")
			return nil, core.Errorf(405, "MethodNotAllowed", "The specified method is not allowed against this resource.")
		}
		return o, nil
	}
	vs := b.Objects[key]
	if len(vs) == 0 {
		return nil, errNoKey(key)
	}
	if o := vs[len(vs)-1]; !o.DeleteMarker {
		return o, nil
	}
	w.Header().Set("x-amz-delete-marker", "true")
	w.Header().Set("x-amz-version-id", vs[len(vs)-1].VersionID)
	return nil, errNoKey(key)
}

func (s *Service) getObject(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	q := r.URL.Query()
	if r.Header.Get("Authorization") == "" && !q.Has("X-Amz-Signature") && !c.Internal { // floci #918
		for param := range q {
			if strings.HasPrefix(param, "response-") {
				return core.Errorf(400, "InvalidRequest", "Request specific response headers cannot be used for anonymous GET requests.")
			}
		}
	}
	var o Object
	var expiration string
	versioned := false
	err := s.withBucket(bucket, false, func(b *Bucket) error {
		p, err := lookup(w, b, key, q.Get("versionId"))
		if err != nil {
			return err
		}
		o, versioned = *p, b.Versioning != ""
		if b.latest(key) == p {
			expiration = b.expirationHeader(p)
		}
		return nil
	})
	if err != nil {
		return err
	}
	h := w.Header()
	h.Set("ETag", o.ETag)
	h.Set("Last-Modified", o.Modified.Format(http.TimeFormat))
	mod := o.Modified.Truncate(time.Second)
	if im := r.Header.Get("If-Match"); im != "" && !etagMatch(im, o.ETag) {
		return errPrecondition()
	} else if t, err := http.ParseTime(r.Header.Get("If-Unmodified-Since")); im == "" && err == nil && mod.After(t) {
		return errPrecondition()
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" && etagMatch(inm, o.ETag) {
		return core.Errorf(304, "NotModified", "Not Modified")
	} else if t, err := http.ParseTime(r.Header.Get("If-Modified-Since")); inm == "" && err == nil && !mod.After(t) {
		return core.Errorf(304, "NotModified", "Not Modified")
	}

	data, status := o.Data, http.StatusOK
	if pn := q.Get("partNumber"); pn != "" {
		if pn != "1" {
			return core.Errorf(416, "InvalidPartNumber", "The requested partnumber is not satisfiable")
		}
		h.Set("x-amz-mp-parts-count", "1")
	}
	// a suffix range on an empty object is a plain 200 on S3 (floci #1141)
	if rg := r.Header.Get("Range"); rg != "" && !(len(data) == 0 && strings.HasPrefix(rg, "bytes=-")) {
		start, end, ok := parseRange(rg, len(data))
		if !ok {
			h.Set("Content-Range", fmt.Sprintf("bytes */%d", len(data)))
			return core.Errorf(416, "InvalidRange", "The requested range is not satisfiable")
		}
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		data, status = data[start:end+1], http.StatusPartialContent
	}
	h.Set("Content-Type", o.ContentType)
	h.Set("Content-Length", strconv.Itoa(len(data)))
	h.Set("Accept-Ranges", "bytes")
	if expiration != "" {
		h.Set("x-amz-expiration", expiration)
	}
	for k, v := range o.Headers {
		h.Set(k, v)
	}
	for k, v := range o.Meta {
		h.Set("x-amz-meta-"+k, v)
	}
	if versioned {
		h.Set("x-amz-version-id", o.VersionID)
	}
	if len(o.Tags) > 0 {
		h.Set("x-amz-tagging-count", strconv.Itoa(len(o.Tags)))
	}
	if o.StorageClass != "" {
		h.Set("x-amz-storage-class", o.StorageClass)
	}
	if strings.EqualFold(r.Header.Get("x-amz-checksum-mode"), "ENABLED") && o.Checksum != "" && status == http.StatusOK {
		h.Set("x-amz-checksum-"+strings.ToLower(o.ChecksumAlgo), o.Checksum)
		h.Set("x-amz-checksum-type", o.ChecksumType)
	}
	for param, header := range map[string]string{"response-content-type": "Content-Type", "response-content-language": "Content-Language",
		"response-expires": "Expires", "response-cache-control": "Cache-Control", "response-content-disposition": "Content-Disposition",
		"response-content-encoding": "Content-Encoding"} {
		if v := q.Get(param); v != "" {
			h.Set(header, v)
		}
	}
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		w.Write(data)
	}
	return nil
}

// parseRange handles "bytes=a-b", "bytes=a-" and "bytes=-n".
func parseRange(h string, size int) (int, int, bool) {
	spec, ok := strings.CutPrefix(h, "bytes=")
	if !ok {
		return 0, 0, false
	}
	spec, _, _ = strings.Cut(spec, ",")
	a, b, _ := strings.Cut(strings.TrimSpace(spec), "-")
	if a == "" {
		n, err := strconv.Atoi(b)
		if err != nil || n <= 0 || size == 0 {
			return 0, 0, false
		}
		return max(0, size-n), size - 1, true
	}
	start, err := strconv.Atoi(a)
	if err != nil || start >= size {
		return 0, 0, false
	}
	end := size - 1
	if b != "" {
		if e, err := strconv.Atoi(b); err == nil && e < end {
			end = e
		}
	}
	return start, end, end >= start
}

type removed struct {
	VersionID, MarkerVersion string
	Marker                   bool
	Event                    string
	Obj                      *Object
}

// remove deletes a key (or one version) following the bucket's versioning.
func (b *Bucket) remove(key, versionID string) removed {
	now := time.Now().UTC()
	if versionID != "" {
		vs := b.Objects[key]
		for i, v := range vs {
			if v.VersionID == versionID {
				b.Objects[key] = append(vs[:i:i], vs[i+1:]...)
				if len(b.Objects[key]) == 0 {
					delete(b.Objects, key)
				}
				return removed{VersionID: versionID, Marker: v.DeleteMarker, Event: "ObjectRemoved:Delete", Obj: v}
			}
		}
		return removed{VersionID: versionID}
	}
	switch b.Versioning {
	case "Enabled", "Suspended":
		dm := &Object{Key: key, DeleteMarker: true, Modified: now}
		b.put(dm)
		return removed{Marker: true, MarkerVersion: dm.VersionID, Event: "ObjectRemoved:DeleteMarkerCreated", Obj: dm}
	default:
		if o := b.latest(key); o != nil {
			delete(b.Objects, key)
			return removed{Event: "ObjectRemoved:Delete", Obj: o}
		}
		return removed{}
	}
}

func (s *Service) deleteObject(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	var res removed
	var ev []pending
	err := s.withBucket(bucket, true, func(b *Bucket) error {
		version := r.URL.Query().Get("versionId")
		if lockedBy(b.version(key, version), time.Now(), bypassGovernance(r)) != "" {
			return errLocked()
		}
		res = b.remove(key, version)
		if res.Event != "" {
			s.journal(b, key)
			ev = s.events(b, res.Event, res.Obj)
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.notify(c.Trace, ev)
	if res.Marker {
		w.Header().Set("x-amz-delete-marker", "true")
	}
	if v := res.VersionID + res.MarkerVersion; v != "" {
		w.Header().Set("x-amz-version-id", v)
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Service) deleteObjects(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, _ string) error {
	body, trailers, err := readBody(r)
	if err != nil {
		return err
	}
	if _, _, err := verifyChecksums(r, trailers, body); err != nil {
		return err
	}
	var req struct {
		Quiet   bool `xml:"Quiet"`
		Objects []struct {
			Key       string `xml:"Key"`
			VersionID string `xml:"VersionId"`
		} `xml:"Object"`
	}
	if err := xml.Unmarshal(body, &req); err != nil {
		return core.Errorf(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema")
	}
	if len(req.Objects) == 0 { // floci #2198: S3 refuses an empty Delete element
		return core.Errorf(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema")
	}
	type deleted struct {
		Key                   string `xml:"Key"`
		VersionID             string `xml:"VersionId,omitempty"`
		DeleteMarker          bool   `xml:"DeleteMarker,omitempty"`
		DeleteMarkerVersionID string `xml:"DeleteMarkerVersionId,omitempty"`
	}
	type refused struct {
		Key       string `xml:"Key"`
		VersionID string `xml:"VersionId,omitempty"`
		Code      string `xml:"Code"`
		Message   string `xml:"Message"`
	}
	out := struct {
		XMLName xml.Name  `xml:"DeleteResult"`
		Xmlns   string    `xml:"xmlns,attr"`
		Deleted []deleted `xml:"Deleted"`
		Errors  []refused `xml:"Error"`
	}{Xmlns: xmlns}
	var ev []pending
	err = s.withBucket(bucket, true, func(b *Bucket) error {
		for _, o := range req.Objects {
			if c.Denied[o.Key] { // IAM refused this key (floci #5405)
				out.Errors = append(out.Errors, refused{o.Key, o.VersionID, "AccessDenied", "Access Denied"})
				continue
			}
			if lockedBy(b.version(o.Key, o.VersionID), time.Now(), bypassGovernance(r)) != "" {
				out.Errors = append(out.Errors, refused{o.Key, o.VersionID, "AccessDenied", "Access Denied because object protected by object lock."})
				continue
			}
			res := b.remove(o.Key, o.VersionID)
			if res.Event != "" {
				s.journal(b, o.Key)
				ev = append(ev, s.events(b, res.Event, res.Obj)...)
			}
			if !req.Quiet {
				out.Deleted = append(out.Deleted, deleted{o.Key, o.VersionID, res.Marker, res.MarkerVersion})
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.notify(c.Trace, ev)
	return writeXML(w, 200, out)
}

func parseCopySource(src string) (bucket, key, version string, err error) {
	src, version, _ = strings.Cut(src, "?versionId=")
	if src, err = url.PathUnescape(strings.TrimPrefix(src, "/")); err != nil {
		return
	}
	bucket, key, _ = strings.Cut(src, "/")
	if bucket == "" || key == "" {
		err = core.Errorf(400, "InvalidArgument", "Copy Source must mention the source bucket and key: sourcebucket/sourcekey")
	}
	return
}

// source reads the object named by x-amz-copy-source.
func (s *Service) source(w http.ResponseWriter, r *http.Request) (Object, string, error) {
	sb, sk, sv, err := parseCopySource(r.Header.Get("x-amz-copy-source"))
	if err != nil {
		return Object{}, "", err
	}
	var o Object
	versioned := false
	err = s.withBucket(sb, false, func(b *Bucket) error {
		p, err := lookup(httpDiscard{}, b, sk, sv)
		if err != nil {
			return err
		}
		o, versioned = *p, b.Versioning != ""
		return nil
	})
	if err != nil {
		return o, "", err
	}
	h := r.Header
	if v := h.Get("x-amz-copy-source-if-match"); v != "" && !etagMatch(v, o.ETag) {
		return o, "", errPrecondition()
	}
	if v := h.Get("x-amz-copy-source-if-none-match"); v != "" && etagMatch(v, o.ETag) {
		return o, "", errPrecondition()
	}
	if versioned {
		w.Header().Set("x-amz-copy-source-version-id", o.VersionID)
	}
	return o, sb + "/" + sk, nil
}

type httpDiscard struct{}

func (httpDiscard) Header() http.Header         { return http.Header{} }
func (httpDiscard) Write(b []byte) (int, error) { return len(b), nil }
func (httpDiscard) WriteHeader(int)             {}

func (s *Service) copyObject(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	src, srcName, err := s.source(w, r)
	if err != nil {
		return err
	}
	replace := r.Header.Get("x-amz-metadata-directive") == "REPLACE"
	// a source version of its own (a rollback) is not a copy onto itself (floci #748)
	if srcName == bucket+"/"+key && !replace && r.Header.Get("x-amz-storage-class") == "" && !strings.Contains(r.Header.Get("x-amz-copy-source"), "?versionId=") {
		return core.Errorf(400, "InvalidRequest", "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.")
	}
	o := newObject(r.Header, key, src.Data)
	if !replace {
		o.ContentType, o.Headers, o.Meta = src.ContentType, src.Headers, src.Meta
	}
	if r.Header.Get("x-amz-tagging-directive") != "REPLACE" {
		o.Tags = src.Tags
	}
	if o.StorageClass == "" && r.Header.Get("x-amz-storage-class") == "" {
		o.StorageClass = src.StorageClass
	}
	if a := strings.ToUpper(r.Header.Get("x-amz-checksum-algorithm")); checksum(a, nil) != "" {
		o.ChecksumAlgo, o.Checksum, o.ChecksumType = a, checksum(a, o.Data), "FULL_OBJECT"
	} else if src.ChecksumType == "FULL_OBJECT" {
		o.ChecksumAlgo, o.Checksum, o.ChecksumType = src.ChecksumAlgo, src.Checksum, src.ChecksumType
	}
	var ev []pending
	versioned := false
	err = s.withBucket(bucket, true, func(b *Bucket) error {
		if err := writePrecondition(r.Header, key, b.latest(key)); err != nil {
			return err
		}
		b.put(o)
		s.journal(b, key)
		versioned = b.Versioning != ""
		ev = s.events(b, "ObjectCreated:Copy", o)
		return nil
	})
	if err != nil {
		return err
	}
	s.notify(c.Trace, ev)
	if versioned {
		w.Header().Set("x-amz-version-id", o.VersionID)
	}
	out := struct {
		XMLName      xml.Name `xml:"CopyObjectResult"`
		Xmlns        string   `xml:"xmlns,attr"`
		ETag         string   `xml:"ETag"`
		LastModified string   `xml:"LastModified"`
		checksums
	}{Xmlns: xmlns, ETag: o.ETag, LastModified: o.Modified.Format(core.ISO), checksums: checksumsOf(o.ChecksumAlgo, o.Checksum, o.ChecksumType)}
	return writeXML(w, 200, out)
}

// checksums is embedded into XML results that echo a checksum.
type checksums struct {
	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
	ChecksumType      string `xml:"ChecksumType,omitempty"`
}

func checksumsOf(algo, value, typ string) checksums {
	var c checksums
	switch algo {
	case "CRC32":
		c.ChecksumCRC32 = value
	case "CRC32C":
		c.ChecksumCRC32C = value
	case "CRC64NVME":
		c.ChecksumCRC64NVME = value
	case "SHA1":
		c.ChecksumSHA1 = value
	case "SHA256":
		c.ChecksumSHA256 = value
	default:
		return c
	}
	c.ChecksumType = typ
	return c
}

type tagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type taggingXML struct {
	XMLName xml.Name `xml:"Tagging"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	TagSet  []tagXML `xml:"TagSet>Tag"`
}

// ObjectTags returns a copy of the tags of an object version (the latest when
// version is empty); nil when it has none or does not exist. IAM enforcement
// uses it for the s3:ExistingObjectTag condition keys.
func (s *Service) ObjectTags(bucket, key, version string) map[string]string {
	var tags map[string]string
	s.withBucket(bucket, false, func(b *Bucket) error {
		if o, err := lookup(httpDiscard{}, b, key, version); err == nil {
			tags = maps.Clone(o.Tags)
		}
		return nil
	})
	return tags
}

func (s *Service) getObjectTagging(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	out := taggingXML{Xmlns: xmlns, TagSet: []tagXML{}}
	err := s.withBucket(bucket, false, func(b *Bucket) error {
		o, err := lookup(w, b, key, r.URL.Query().Get("versionId"))
		if err != nil {
			return err
		}
		for _, k := range core.SortedKeys(o.Tags) {
			out.TagSet = append(out.TagSet, tagXML{k, o.Tags[k]})
		}
		if b.Versioning != "" {
			w.Header().Set("x-amz-version-id", o.VersionID)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return writeXML(w, 200, out)
}

func (s *Service) putObjectTagging(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	body, _, err := readBody(r)
	if err != nil {
		return err
	}
	var in taggingXML
	if err := xml.Unmarshal(body, &in); err != nil {
		return core.Errorf(400, "MalformedXML", "The XML you provided was not well-formed")
	}
	if len(in.TagSet) > 10 {
		return core.Errorf(400, "BadRequest", "Object tags cannot be greater than 10")
	}
	return s.withBucket(bucket, true, func(b *Bucket) error {
		o, err := lookup(w, b, key, r.URL.Query().Get("versionId"))
		if err != nil {
			return err
		}
		tags := map[string]string{}
		for _, t := range in.TagSet {
			tags[t.Key] = t.Value
		}
		o.Tags = tags
		s.journal(b, key)
		if b.Versioning != "" {
			w.Header().Set("x-amz-version-id", o.VersionID)
		}
		w.WriteHeader(http.StatusOK)
		return nil
	})
}

func (s *Service) deleteObjectTagging(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	return s.withBucket(bucket, true, func(b *Bucket) error {
		o, err := lookup(w, b, key, r.URL.Query().Get("versionId"))
		if err != nil {
			return err
		}
		o.Tags = nil
		s.journal(b, key)
		w.WriteHeader(http.StatusNoContent)
		return nil
	})
}

// grantHeaders are the x-amz-grant-* request headers and the permission each gives.
var grantHeaders = map[string]string{"X-Amz-Grant-Read": "READ", "X-Amz-Grant-Read-Acp": "READ_ACP",
	"X-Amz-Grant-Write-Acp": "WRITE_ACP", "X-Amz-Grant-Full-Control": "FULL_CONTROL"}

// cannedGrants are the extra grants of a canned ACL (the owner's full control is always listed).
var cannedGrants = map[string]map[string]string{
	"public-read":        {"READ": `uri="http://acs.amazonaws.com/groups/global/AllUsers"`},
	"public-read-write":  {"READ": `uri="http://acs.amazonaws.com/groups/global/AllUsers"`, "WRITE": `uri="http://acs.amazonaws.com/groups/global/AllUsers"`},
	"authenticated-read": {"READ": `uri="http://acs.amazonaws.com/groups/global/AuthenticatedUsers"`},
}

// grantsOf reads the grants a request asks for: a canned ACL (x-amz-acl) and the x-amz-grant-* headers.
func grantsOf(h http.Header) map[string]string {
	var grants map[string]string
	add := func(perm, v string) {
		if grants == nil {
			grants = map[string]string{}
		}
		grants[perm] = strings.Join(slices.DeleteFunc([]string{grants[perm], v}, func(s string) bool { return s == "" }), ", ")
	}
	for perm, v := range cannedGrants[h.Get("X-Amz-Acl")] {
		add(perm, v)
	}
	for header, perm := range grantHeaders {
		if v := h.Get(header); v != "" {
			add(perm, v)
		}
	}
	return grants
}

// putObjectAcl replaces the grants of an object with those of the request.
func (s *Service) putObjectAcl(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	return s.withBucket(bucket, true, func(b *Bucket) error {
		o, err := lookup(w, b, key, r.URL.Query().Get("versionId"))
		if err != nil {
			return err
		}
		o.Grants = grantsOf(r.Header)
		s.journal(b, key)
		w.WriteHeader(http.StatusOK)
		return nil
	})
}

// grantsXML renders the <Grant> elements for the grantees in x-amz-grant-* header
// values such as `uri="http://acs.amazonaws.com/groups/global/AllUsers", id="abc"`.
func grantsXML(grants map[string]string) string {
	var b strings.Builder
	for _, perm := range core.SortedKeys(grants) {
		for _, g := range strings.Split(grants[perm], ",") {
			kind, val, ok := strings.Cut(strings.TrimSpace(g), "=")
			if !ok {
				continue
			}
			var v strings.Builder
			xml.EscapeText(&v, []byte(strings.Trim(val, `"`)))
			grantee := ""
			switch strings.ToLower(kind) {
			case "uri":
				grantee = `xsi:type="Group"><URI>` + v.String() + `</URI>`
			case "id":
				grantee = `xsi:type="CanonicalUser"><ID>` + v.String() + `</ID>`
			case "emailaddress":
				grantee = `xsi:type="AmazonCustomerByEmail"><EmailAddress>` + v.String() + `</EmailAddress>`
			default:
				continue
			}
			fmt.Fprintf(&b, `<Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" %s</Grantee><Permission>%s</Permission></Grant>`, grantee, perm)
		}
	}
	return b.String()
}

func (s *Service) getACL(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	acct := c.Account
	var grants map[string]string
	err := s.withBucket(bucket, false, func(b *Bucket) error {
		acct = b.Account
		if key != "" {
			o, err := lookup(w, b, key, r.URL.Query().Get("versionId"))
			if err == nil {
				grants = o.Grants
			}
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	id := ownerID(acct)
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<AccessControlPolicy xmlns="%s"><Owner><ID>%s</ID><DisplayName>pogi</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>%s</ID><DisplayName>pogi</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant>%s</AccessControlList></AccessControlPolicy>`, xmlns, id, id, grantsXML(grants))
	return nil
}

func (s *Service) getObjectAttributes(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	var o Object
	versioned := false
	err := s.withBucket(bucket, false, func(b *Bucket) error {
		p, err := lookup(w, b, key, r.URL.Query().Get("versionId"))
		if err == nil {
			o, versioned = *p, b.Versioning != ""
		}
		return err
	})
	if err != nil {
		return err
	}
	want := strings.ToLower(strings.Join(r.Header.Values("x-amz-object-attributes"), ",")) // the SDK sends one header per attribute
	var b strings.Builder
	b.WriteString(xml.Header + `<GetObjectAttributesResponse xmlns="` + xmlns + `">`)
	if strings.Contains(want, "etag") {
		b.WriteString("<ETag>" + trimETag(o.ETag) + "</ETag>")
	}
	if strings.Contains(want, "checksum") && o.Checksum != "" {
		fmt.Fprintf(&b, "<Checksum><Checksum%s>%s</Checksum%s><ChecksumType>%s</ChecksumType></Checksum>", o.ChecksumAlgo, o.Checksum, o.ChecksumAlgo, o.ChecksumType)
	}
	if strings.Contains(want, "objectparts") && o.Parts > 0 {
		fmt.Fprintf(&b, "<ObjectParts><PartsCount>%d</PartsCount></ObjectParts>", o.Parts)
	}
	if strings.Contains(want, "storageclass") {
		b.WriteString("<StorageClass>" + storageClass(o.StorageClass) + "</StorageClass>")
	}
	if strings.Contains(want, "objectsize") {
		fmt.Fprintf(&b, "<ObjectSize>%d</ObjectSize>", len(o.Data))
	}
	b.WriteString("</GetObjectAttributesResponse>")
	w.Header().Set("Last-Modified", o.Modified.Format(http.TimeFormat))
	if versioned {
		w.Header().Set("x-amz-version-id", o.VersionID)
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Write([]byte(b.String()))
	return nil
}

func storageClass(sc string) string {
	if sc == "" {
		return "STANDARD"
	}
	return sc
}

// checkFormPolicy enforces the conditions of a POST policy (a base64 JSON document)
// on the form fields and the file size: eq and starts-with on a field, exact-match
// objects, and content-length-range. No policy means no conditions.
func checkFormPolicy(policy, bucket string, form map[string][]string, size int) error {
	if policy == "" {
		return nil
	}
	raw, err := base64.StdEncoding.DecodeString(policy)
	var doc struct{ Conditions []any }
	if err != nil || json.Unmarshal(raw, &doc) != nil {
		return core.Errorf(400, "InvalidPolicyDocument", "Invalid Policy: Invalid JSON.")
	}
	field := func(name string) string {
		if strings.EqualFold(name, "bucket") {
			return bucket
		}
		for k, v := range form {
			if strings.EqualFold(k, name) && len(v) > 0 {
				return v[0]
			}
		}
		return ""
	}
	failed := func(cond any) error {
		b, _ := json.Marshal(cond)
		return core.Errorf(403, "AccessDenied", "Invalid according to Policy: Policy Condition failed: %s", b)
	}
	for _, cond := range doc.Conditions {
		switch c := cond.(type) {
		case map[string]any: // {"acl": "public-read"} means eq
			for k, v := range c {
				if want, _ := v.(string); field(k) != want {
					return failed(map[string]any{k: v})
				}
			}
		case []any:
			if len(c) != 3 {
				return core.Errorf(400, "InvalidPolicyDocument", "Invalid Policy: Invalid Simple-Condition: Simple-Conditions must have exactly one property specified.")
			}
			op, _ := c[0].(string)
			if strings.EqualFold(op, "content-length-range") {
				lo, ok1 := c[1].(float64)
				hi, ok2 := c[2].(float64)
				switch {
				case !ok1 || !ok2:
					return core.Errorf(400, "InvalidPolicyDocument", "Invalid Policy: content-length-range needs two numbers.")
				case float64(size) < lo:
					return core.Errorf(400, "EntityTooSmall", "Your proposed upload is smaller than the minimum allowed size")
				case float64(size) > hi:
					return core.Errorf(400, "EntityTooLarge", "Your proposed upload exceeds the maximum allowed size")
				}
				continue
			}
			name, _ := c[1].(string)
			want, _ := c[2].(string)
			have := field(strings.TrimPrefix(name, "$"))
			if strings.EqualFold(op, "eq") && have != want || strings.EqualFold(op, "starts-with") && !strings.HasPrefix(have, want) {
				return failed(cond)
			}
		}
	}
	return nil
}

// postObject handles browser form uploads (POST policy).
func (s *Service) postObject(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, _ string) error {
	r.Body = io.NopCloser(bytes.NewReader(core.Body(r)))
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		return core.Errorf(400, "MalformedPOSTRequest", "The body of your POST request is not well-formed multipart/form-data.")
	}
	field := func(name string) string {
		for k, v := range r.MultipartForm.Value {
			if strings.EqualFold(k, name) && len(v) > 0 {
				return v[0]
			}
		}
		return ""
	}
	if err := s.hub.VerifyPolicy(field("x-amz-credential"), field("policy"), field("x-amz-signature")); err != nil {
		return err
	}
	if field("x-amz-signature") == "" && s.hub.Cfg.ValidateSigs { // no credentials: only a public bucket policy lets it in
		if err := s.publicAccess(c, bucket); err != nil {
			return err
		}
	}
	f, fh, err := r.FormFile("file")
	if err != nil {
		return core.Errorf(400, "InvalidArgument", "POST requires exactly one file upload per request.")
	}
	defer f.Close()
	data, _ := io.ReadAll(f)
	if err := checkFormPolicy(field("policy"), bucket, r.MultipartForm.Value, len(data)); err != nil {
		return err
	}
	key := strings.ReplaceAll(field("key"), "${filename}", fh.Filename)
	if key == "" {
		return core.Errorf(400, "InvalidArgument", "Bucket POST must contain a field named 'key'.")
	}
	h := http.Header{}
	for k, v := range r.MultipartForm.Value {
		if lk := strings.ToLower(k); strings.HasPrefix(lk, "x-amz-meta-") || lk == "content-type" || lk == "cache-control" ||
			lk == "content-disposition" || lk == "content-encoding" || lk == "expires" || lk == "x-amz-storage-class" || lk == "tagging" {
			if lk == "tagging" {
				continue
			}
			h.Set(k, v[0])
		}
	}
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", fh.Header.Get("Content-Type"))
	}
	o := newObject(h, key, data)
	var ev []pending
	versioned := false
	err = s.withBucket(bucket, true, func(b *Bucket) error {
		b.put(o)
		s.journal(b, key)
		versioned = b.Versioning != ""
		ev = s.events(b, "ObjectCreated:Post", o)
		return nil
	})
	if err != nil {
		return err
	}
	s.notify(c.Trace, ev)
	if versioned { // floci #2607
		w.Header().Set("x-amz-version-id", o.VersionID)
	}
	loc := "http://" + r.Host + "/" + bucket + "/" + url.PathEscape(key)
	w.Header().Set("ETag", o.ETag)
	w.Header().Set("Location", loc)
	if redirect := field("success_action_redirect"); redirect != "" {
		u, err := url.Parse(redirect)
		if err == nil {
			q := u.Query()
			q.Set("bucket", bucket)
			q.Set("key", key)
			q.Set("etag", o.ETag)
			u.RawQuery = q.Encode()
			http.Redirect(w, r, u.String(), http.StatusSeeOther)
			return nil
		}
	}
	switch field("success_action_status") {
	case "200":
		w.WriteHeader(http.StatusOK)
	case "201":
		return writeXML(w, 201, struct {
			XMLName  xml.Name `xml:"PostResponse"`
			Location string
			Bucket   string
			Key      string
			ETag     string
		}{Location: loc, Bucket: bucket, Key: key, ETag: o.ETag})
	default:
		w.WriteHeader(http.StatusNoContent)
	}
	return nil
}

type contentXML struct {
	Key               string `xml:"Key"`
	LastModified      string `xml:"LastModified"`
	ETag              string `xml:"ETag"`
	Size              int    `xml:"Size"`
	StorageClass      string `xml:"StorageClass"`
	ChecksumAlgorithm string `xml:"ChecksumAlgorithm,omitempty"`
	ChecksumType      string `xml:"ChecksumType,omitempty"`
	Owner             *owner `xml:"Owner,omitempty"`
}

type prefixXML struct {
	Prefix string `xml:"Prefix"`
}

// listObjects serves ListObjects (v1) and ListObjectsV2.
func (s *Service) listObjects(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, _ string) error {
	q := r.URL.Query()
	v2 := q.Get("list-type") == "2"
	prefix, delim := q.Get("prefix"), q.Get("delimiter")
	maxKeys := 1000
	if v := q.Get("max-keys"); v != "" {
		n, _ := strconv.Atoi(v) // checkPaging has refused a bad number
		maxKeys = min(n, 1000)
	}
	enc := func(s string) string { return s }
	if q.Get("encoding-type") == "url" {
		enc = urlEncode
	}
	start, resume := q.Get("marker"), true
	if v2 {
		start, resume = q.Get("start-after"), false
		if tok := q.Get("continuation-token"); tok != "" {
			b, err := base64.RawURLEncoding.DecodeString(tok)
			if err != nil {
				return core.Errorf(400, "InvalidArgument", "The continuation token provided is incorrect")
			}
			start, resume = string(b), true
		}
	}
	withOwner := !v2 || q.Get("fetch-owner") == "true"
	var contents []contentXML
	var prefixes []prefixXML
	truncated, next, count := false, "", 0
	err := s.withBucket(bucket, false, func(b *Bucket) error {
		lastCP := ""
		for _, k := range sortedKeys(b.Objects) {
			if !strings.HasPrefix(k, prefix) || start != "" && (k <= start || resume && delim != "" && strings.HasSuffix(start, delim) && strings.HasPrefix(k, start)) {
				continue
			}
			o := b.latest(k)
			if o == nil {
				continue
			}
			if delim != "" {
				if i := strings.Index(k[len(prefix):], delim); i >= 0 {
					cp := k[:len(prefix)+i+len(delim)]
					if cp == lastCP {
						continue
					}
					if count == maxKeys {
						truncated = true
						break
					}
					prefixes = append(prefixes, prefixXML{enc(cp)})
					lastCP, next = cp, cp
					count++
					continue
				}
			}
			if count == maxKeys {
				truncated = true
				break
			}
			ct := contentXML{Key: enc(k), LastModified: o.Modified.Format(core.ISO), ETag: o.ETag, Size: len(o.Data),
				StorageClass: storageClass(o.StorageClass), ChecksumAlgorithm: o.ChecksumAlgo}
			if o.ChecksumAlgo != "" {
				ct.ChecksumType = o.ChecksumType
			}
			if withOwner {
				ct.Owner = ownerOf(b.Account)
			}
			contents = append(contents, ct)
			next = k
			count++
		}
		return nil
	})
	if err != nil {
		return err
	}
	encType := q.Get("encoding-type")
	if v2 {
		out := struct {
			XMLName               xml.Name     `xml:"ListBucketResult"`
			Xmlns                 string       `xml:"xmlns,attr"`
			Name                  string       `xml:"Name"`
			Prefix                string       `xml:"Prefix"`
			Delimiter             string       `xml:"Delimiter,omitempty"`
			MaxKeys               int          `xml:"MaxKeys"`
			EncodingType          string       `xml:"EncodingType,omitempty"`
			KeyCount              int          `xml:"KeyCount"`
			IsTruncated           bool         `xml:"IsTruncated"`
			ContinuationToken     string       `xml:"ContinuationToken,omitempty"`
			NextContinuationToken string       `xml:"NextContinuationToken,omitempty"`
			StartAfter            string       `xml:"StartAfter,omitempty"`
			Contents              []contentXML `xml:"Contents"`
			CommonPrefixes        []prefixXML  `xml:"CommonPrefixes"`
		}{Xmlns: xmlns, Name: bucket, Prefix: enc(prefix), Delimiter: enc(delim), MaxKeys: maxKeys, EncodingType: encType,
			KeyCount: count, IsTruncated: truncated, ContinuationToken: q.Get("continuation-token"), StartAfter: enc(q.Get("start-after")),
			Contents: contents, CommonPrefixes: prefixes}
		if truncated {
			out.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(next))
		}
		return writeXML(w, 200, out)
	}
	out := struct {
		XMLName        xml.Name     `xml:"ListBucketResult"`
		Xmlns          string       `xml:"xmlns,attr"`
		Name           string       `xml:"Name"`
		Prefix         string       `xml:"Prefix"`
		Marker         string       `xml:"Marker"`
		NextMarker     string       `xml:"NextMarker,omitempty"`
		Delimiter      string       `xml:"Delimiter,omitempty"`
		MaxKeys        int          `xml:"MaxKeys"`
		EncodingType   string       `xml:"EncodingType,omitempty"`
		IsTruncated    bool         `xml:"IsTruncated"`
		Contents       []contentXML `xml:"Contents"`
		CommonPrefixes []prefixXML  `xml:"CommonPrefixes"`
	}{Xmlns: xmlns, Name: bucket, Prefix: enc(prefix), Marker: enc(q.Get("marker")), Delimiter: enc(delim), MaxKeys: maxKeys,
		EncodingType: encType, IsTruncated: truncated, Contents: contents, CommonPrefixes: prefixes}
	if truncated {
		out.NextMarker = enc(next)
	}
	return writeXML(w, 200, out)
}

func (s *Service) listVersions(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, _ string) error {
	q := r.URL.Query()
	prefix, delim := q.Get("prefix"), q.Get("delimiter")
	keyMarker, vidMarker := q.Get("key-marker"), q.Get("version-id-marker")
	maxKeys := 1000
	if n, err := strconv.Atoi(q.Get("max-keys")); err == nil && n >= 0 {
		maxKeys = min(n, 1000)
	}
	enc := func(s string) string { return s }
	if q.Get("encoding-type") == "url" {
		enc = urlEncode
	}
	type version struct {
		Key               string `xml:"Key"`
		VersionID         string `xml:"VersionId"`
		IsLatest          bool   `xml:"IsLatest"`
		LastModified      string `xml:"LastModified"`
		ETag              string `xml:"ETag,omitempty"`
		Size              *int   `xml:"Size,omitempty"`
		StorageClass      string `xml:"StorageClass,omitempty"`
		ChecksumAlgorithm string `xml:"ChecksumAlgorithm,omitempty"`
		Owner             *owner `xml:"Owner"`
	}
	out := struct {
		XMLName             xml.Name    `xml:"ListVersionsResult"`
		Xmlns               string      `xml:"xmlns,attr"`
		Name                string      `xml:"Name"`
		Prefix              string      `xml:"Prefix"`
		KeyMarker           string      `xml:"KeyMarker"`
		VersionIDMarker     string      `xml:"VersionIdMarker"`
		NextKeyMarker       string      `xml:"NextKeyMarker,omitempty"`
		NextVersionIDMarker string      `xml:"NextVersionIdMarker,omitempty"`
		MaxKeys             int         `xml:"MaxKeys"`
		Delimiter           string      `xml:"Delimiter,omitempty"`
		EncodingType        string      `xml:"EncodingType,omitempty"`
		IsTruncated         bool        `xml:"IsTruncated"`
		Versions            []version   `xml:"Version"`
		DeleteMarkers       []version   `xml:"DeleteMarker"`
		CommonPrefixes      []prefixXML `xml:"CommonPrefixes"`
	}{Xmlns: xmlns, Name: bucket, Prefix: enc(prefix), KeyMarker: keyMarker, VersionIDMarker: vidMarker, MaxKeys: maxKeys,
		Delimiter: enc(delim), EncodingType: q.Get("encoding-type")}
	err := s.withBucket(bucket, false, func(b *Bucket) error {
		count, skipping, lastCP := 0, vidMarker != "", ""
		for _, k := range sortedKeys(b.Objects) {
			if !strings.HasPrefix(k, prefix) || keyMarker != "" && (k < keyMarker || k == keyMarker && !skipping) {
				continue
			}
			if delim != "" {
				if i := strings.Index(k[len(prefix):], delim); i >= 0 {
					if cp := k[:len(prefix)+i+len(delim)]; cp != lastCP {
						out.CommonPrefixes = append(out.CommonPrefixes, prefixXML{enc(cp)})
						lastCP = cp
					}
					continue
				}
			}
			vs := b.Objects[k]
			for i := len(vs) - 1; i >= 0; i-- {
				o := vs[i]
				if k == keyMarker && skipping {
					if o.VersionID == vidMarker {
						skipping = false
					}
					continue
				}
				if count == maxKeys {
					out.IsTruncated = true
					return nil
				}
				v := version{Key: enc(k), VersionID: o.VersionID, IsLatest: i == len(vs)-1, LastModified: o.Modified.Format(core.ISO), Owner: ownerOf(b.Account)}
				if o.DeleteMarker {
					out.DeleteMarkers = append(out.DeleteMarkers, v)
				} else {
					size := len(o.Data)
					v.ETag, v.Size, v.StorageClass, v.ChecksumAlgorithm = o.ETag, &size, storageClass(o.StorageClass), o.ChecksumAlgo
					out.Versions = append(out.Versions, v)
				}
				out.NextKeyMarker, out.NextVersionIDMarker = enc(k), o.VersionID
				count++
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !out.IsTruncated {
		out.NextKeyMarker, out.NextVersionIDMarker = "", ""
	}
	return writeXML(w, 200, out)
}

// ---- multipart uploads ----

// Upload is an in-progress multipart upload.
type Upload struct {
	ID        string        `json:"id"`
	Key       string        `json:"key"`
	Initiated time.Time     `json:"initiated"`
	Proto     Object        `json:"proto"` // metadata for the final object
	Parts     map[int]*Part `json:"parts"`
}

// Part is one uploaded part.
type Part struct {
	Data     []byte    `json:"data"`
	ETag     string    `json:"etag"`
	Modified time.Time `json:"modified"`
	Checksum string    `json:"checksum,omitempty"`
}

func (s *Service) createUpload(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	o := newObject(r.Header, key, nil)
	algo := strings.ToUpper(r.Header.Get("x-amz-checksum-algorithm"))
	if checksum(algo, nil) == "" {
		algo = ""
	}
	typ := strings.ToUpper(r.Header.Get("x-amz-checksum-type"))
	if algo != "" && typ == "" {
		typ = map[bool]string{true: "FULL_OBJECT", false: "COMPOSITE"}[algo == "CRC64NVME"]
	}
	o.ChecksumAlgo, o.ChecksumType = algo, typ
	u := &Upload{ID: core.RandString(64, core.AlphaNum), Key: key, Initiated: time.Now().UTC(), Proto: *o, Parts: map[int]*Part{}}
	err := s.withBucket(bucket, true, func(b *Bucket) error {
		if b.Uploads == nil {
			b.Uploads = map[string]*Upload{}
		}
		b.Uploads[u.ID] = u
		s.j.Put("u/"+bucket+"/"+u.ID, u)
		return nil
	})
	if err != nil {
		return err
	}
	if algo != "" {
		w.Header().Set("x-amz-checksum-algorithm", algo)
		w.Header().Set("x-amz-checksum-type", typ)
	}
	return writeXML(w, 200, struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}{Xmlns: xmlns, Bucket: bucket, Key: key, UploadID: u.ID})
}

func errNoUpload() error {
	return core.Errorf(404, "NoSuchUpload", "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.")
}

func (s *Service) uploadPart(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	q := r.URL.Query()
	n, err := strconv.Atoi(q.Get("partNumber"))
	if err != nil || n < 1 || n > 10000 {
		return core.Errorf(400, "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive")
	}
	var data []byte
	copyPart := r.Header.Get("x-amz-copy-source") != ""
	var trailers map[string]string
	if copyPart {
		src, _, err := s.source(w, r)
		if err != nil {
			return err
		}
		data = src.Data
		if rg := r.Header.Get("x-amz-copy-source-range"); rg != "" {
			start, end, ok := parseRange(rg, len(data))
			if !ok {
				return core.Errorf(400, "InvalidArgument", "The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy")
			}
			data = data[start : end+1]
		}
	} else if data, trailers, err = readBody(r); err != nil {
		return err
	}
	p := &Part{Data: data, ETag: etag(data), Modified: time.Now().UTC()}
	var algo string
	err = s.withBucket(bucket, true, func(b *Bucket) error {
		u := b.Uploads[q.Get("uploadId")]
		if u == nil {
			return errNoUpload()
		}
		if !copyPart {
			a, sum, err := verifyChecksums(r, trailers, data)
			if err != nil {
				return err
			}
			if a == "" && u.Proto.ChecksumAlgo != "" {
				a, sum = u.Proto.ChecksumAlgo, checksum(u.Proto.ChecksumAlgo, data)
			}
			algo, p.Checksum = a, sum
		} else if u.Proto.ChecksumAlgo != "" {
			algo, p.Checksum = u.Proto.ChecksumAlgo, checksum(u.Proto.ChecksumAlgo, data)
		}
		u.Parts[n] = p
		s.j.Put("p/"+bucket+"/"+u.ID+"/"+strconv.Itoa(n), p)
		return nil
	})
	if err != nil {
		return err
	}
	if copyPart {
		return writeXML(w, 200, struct {
			XMLName      xml.Name `xml:"CopyPartResult"`
			Xmlns        string   `xml:"xmlns,attr"`
			ETag         string   `xml:"ETag"`
			LastModified string   `xml:"LastModified"`
			checksums
		}{Xmlns: xmlns, ETag: p.ETag, LastModified: p.Modified.Format(core.ISO), checksums: checksumsOf(algo, p.Checksum, "")})
	}
	w.Header().Set("ETag", p.ETag)
	if algo != "" {
		w.Header().Set("x-amz-checksum-"+strings.ToLower(algo), p.Checksum)
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Service) completeUpload(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	body, _, err := readBody(r)
	if err != nil {
		return err
	}
	var req struct {
		Parts []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}
	empty := len(strings.TrimSpace(string(body))) == 0
	if err := xml.Unmarshal(body, &req); err != nil && !empty {
		return core.Errorf(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema")
	}
	if len(req.Parts) == 0 {
		return core.Errorf(400, "InvalidRequest", "You must specify at least one part")
	}
	id := r.URL.Query().Get("uploadId")
	var o *Object
	var ev []pending
	versioned := false
	err = s.withBucket(bucket, true, func(b *Bucket) error {
		u := b.Uploads[id]
		if u == nil {
			return errNoUpload()
		}
		var data, md5s, sums []byte
		prev := 0
		limit := s.hub.MaxUpload()
		for i, rp := range req.Parts {
			if rp.PartNumber <= prev {
				return core.Errorf(400, "InvalidPartOrder", "The list of parts was not in ascending order. The parts list must be specified in order by part number.")
			}
			prev = rp.PartNumber
			p := u.Parts[rp.PartNumber]
			if p == nil || trimETag(rp.ETag) != trimETag(p.ETag) {
				return core.Errorf(400, "InvalidPart", "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.")
			}
			if i < len(req.Parts)-1 && len(p.Data) < 5<<20 {
				return core.Errorf(400, "EntityTooSmall", "Your proposed upload is smaller than the minimum allowed object size (5 MiB per part, except the last).")
			}
			if size := int64(len(data) + len(p.Data)); size > limit {
				// the parts add up to a file over the limit: count the rest of the parts for the message
				for _, rest := range req.Parts[i+1:] {
					if q := u.Parts[rest.PartNumber]; q != nil {
						size += int64(len(q.Data))
					}
				}
				return core.Errorf(400, "EntityTooLarge", "%s", s.hub.TooBig("s3", bucket+"/"+key, size, limit))
			}
			data = append(data, p.Data...)
			h := md5.Sum(p.Data)
			md5s = append(md5s, h[:]...)
			if raw, err := base64.StdEncoding.DecodeString(p.Checksum); err == nil {
				sums = append(sums, raw...)
			}
		}
		// floci #5635: x-amz-mp-object-size is the size the client expects the finished object to have
		if want := r.Header.Get("x-amz-mp-object-size"); want != "" {
			if n, err := strconv.ParseInt(want, 10, 64); err != nil || n != int64(len(data)) {
				return core.Errorf(400, "InvalidRequest", "The provided 'x-amz-mp-object-size' header value %s does not match what was computed: %d", want, len(data))
			}
		}
		if err := writePrecondition(r.Header, key, b.latest(key)); err != nil {
			return err
		}
		obj := u.Proto
		h := md5.Sum(md5s)
		obj.Data, obj.Parts, obj.Modified = data, len(req.Parts), time.Now().UTC()
		obj.ETag = fmt.Sprintf(`"%s-%d"`, hex.EncodeToString(h[:]), len(req.Parts))
		switch {
		case obj.ChecksumAlgo == "":
		case obj.ChecksumType == "FULL_OBJECT":
			obj.Checksum = checksum(obj.ChecksumAlgo, data)
		default:
			obj.Checksum = fmt.Sprintf("%s-%d", checksum(obj.ChecksumAlgo, sums), len(req.Parts))
		}
		// floci #1950: a checksum the client sends with the request must match the finished object
		if want := r.Header.Get("x-amz-checksum-" + strings.ToLower(obj.ChecksumAlgo)); want != "" && obj.ChecksumAlgo != "" && want != obj.Checksum {
			return core.Errorf(400, "BadDigest", "The %s you specified did not match the calculated checksum.", obj.ChecksumAlgo)
		}
		b.put(&obj)
		delete(b.Uploads, id)
		s.journal(b, key)
		s.j.Delete("u/" + bucket + "/" + id)
		o, versioned = &obj, b.Versioning != ""
		ev = s.events(b, "ObjectCreated:CompleteMultipartUpload", o)
		return nil
	})
	if err != nil {
		return err
	}
	s.notify(c.Trace, ev)
	if versioned {
		w.Header().Set("x-amz-version-id", o.VersionID)
	}
	return writeXML(w, 200, struct {
		XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Location string   `xml:"Location"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		ETag     string   `xml:"ETag"`
		checksums
	}{Xmlns: xmlns, Location: "http://" + r.Host + "/" + bucket + "/" + url.PathEscape(key), Bucket: bucket, Key: key,
		ETag: o.ETag, checksums: checksumsOf(o.ChecksumAlgo, o.Checksum, o.ChecksumType)})
}

func (s *Service) abortUpload(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	return s.withBucket(bucket, true, func(b *Bucket) error {
		id := r.URL.Query().Get("uploadId")
		if b.Uploads[id] == nil {
			return errNoUpload()
		}
		delete(b.Uploads, id)
		s.j.Delete("u/" + bucket + "/" + id)
		w.WriteHeader(http.StatusNoContent)
		return nil
	})
}

func (s *Service) listParts(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	q := r.URL.Query()
	maxParts := 1000
	if n, err := strconv.Atoi(q.Get("max-parts")); err == nil && n > 0 {
		maxParts = min(n, 1000)
	}
	marker, _ := strconv.Atoi(q.Get("part-number-marker"))
	type partXML struct {
		PartNumber   int    `xml:"PartNumber"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int    `xml:"Size"`
		checksums
	}
	out := struct {
		XMLName              xml.Name  `xml:"ListPartsResult"`
		Xmlns                string    `xml:"xmlns,attr"`
		Bucket               string    `xml:"Bucket"`
		Key                  string    `xml:"Key"`
		UploadID             string    `xml:"UploadId"`
		PartNumberMarker     int       `xml:"PartNumberMarker"`
		NextPartNumberMarker int       `xml:"NextPartNumberMarker"`
		MaxParts             int       `xml:"MaxParts"`
		IsTruncated          bool      `xml:"IsTruncated"`
		Parts                []partXML `xml:"Part"`
		Initiator            *owner    `xml:"Initiator"`
		Owner                *owner    `xml:"Owner"`
		StorageClass         string    `xml:"StorageClass"`
		ChecksumAlgorithm    string    `xml:"ChecksumAlgorithm,omitempty"`
		ChecksumType         string    `xml:"ChecksumType,omitempty"`
	}{Xmlns: xmlns, Bucket: bucket, Key: key, UploadID: q.Get("uploadId"), PartNumberMarker: marker, MaxParts: maxParts}
	err := s.withBucket(bucket, false, func(b *Bucket) error {
		u := b.Uploads[q.Get("uploadId")]
		if u == nil {
			return errNoUpload()
		}
		out.Initiator, out.Owner, out.StorageClass = ownerOf(b.Account), ownerOf(b.Account), storageClass(u.Proto.StorageClass)
		out.ChecksumAlgorithm, out.ChecksumType = u.Proto.ChecksumAlgo, u.Proto.ChecksumType
		var nums []int
		for n := range u.Parts {
			if n > marker {
				nums = append(nums, n)
			}
		}
		sort.Ints(nums)
		for _, n := range nums {
			if len(out.Parts) == maxParts {
				out.IsTruncated = true
				break
			}
			p := u.Parts[n]
			out.Parts = append(out.Parts, partXML{n, p.Modified.Format(core.ISO), p.ETag, len(p.Data), checksumsOf(u.Proto.ChecksumAlgo, p.Checksum, "")})
			out.NextPartNumberMarker = n
		}
		return nil
	})
	if err != nil {
		return err
	}
	return writeXML(w, 200, out)
}

func (s *Service) listUploads(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, _ string) error {
	q := r.URL.Query()
	type uploadXML struct {
		Key          string `xml:"Key"`
		UploadID     string `xml:"UploadId"`
		Initiated    string `xml:"Initiated"`
		StorageClass string `xml:"StorageClass"`
		Initiator    *owner `xml:"Initiator"`
		Owner        *owner `xml:"Owner"`
	}
	out := struct {
		XMLName     xml.Name    `xml:"ListMultipartUploadsResult"`
		Xmlns       string      `xml:"xmlns,attr"`
		Bucket      string      `xml:"Bucket"`
		KeyMarker   string      `xml:"KeyMarker"`
		Prefix      string      `xml:"Prefix"`
		MaxUploads  int         `xml:"MaxUploads"`
		IsTruncated bool        `xml:"IsTruncated"`
		Uploads     []uploadXML `xml:"Upload"`
	}{Xmlns: xmlns, Bucket: bucket, KeyMarker: q.Get("key-marker"), Prefix: q.Get("prefix"), MaxUploads: 1000}
	err := s.withBucket(bucket, false, func(b *Bucket) error {
		for _, u := range b.Uploads {
			if strings.HasPrefix(u.Key, out.Prefix) {
				out.Uploads = append(out.Uploads, uploadXML{u.Key, u.ID, u.Initiated.Format(core.ISO), storageClass(u.Proto.StorageClass), ownerOf(b.Account), ownerOf(b.Account)})
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(out.Uploads, func(i, j int) bool {
		return out.Uploads[i].Key < out.Uploads[j].Key || out.Uploads[i].Key == out.Uploads[j].Key && out.Uploads[i].Initiated < out.Uploads[j].Initiated
	})
	return writeXML(w, 200, out)
}
