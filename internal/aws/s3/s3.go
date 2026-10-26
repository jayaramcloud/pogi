// Package s3 emulates Amazon S3 (REST-XML): buckets, objects, versioning,
// multipart uploads, checksums, aws-chunked bodies, CORS, presigned URLs,
// bucket configuration and event notifications.
package s3

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/iam"
	"github.com/nrndev/pogi/internal/core"
)

const xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"

// Service is the S3 emulator.
type Service struct {
	hub  *core.Hub
	stop func() // ends the lifecycle sweep
	mu   sync.RWMutex
	st   state
	j    *core.Journal // wal mode
}

type state struct {
	Buckets map[string]*Bucket `json:"buckets"`
}

// Bucket holds objects (all versions) and raw configuration documents.
type Bucket struct {
	Name       string               `json:"name"`
	Account    string               `json:"account"`
	Region     string               `json:"region"`
	Created    time.Time            `json:"created"`
	Versioning string               `json:"versioning,omitempty"` // Enabled | Suspended
	Objects    map[string][]*Object `json:"objects"`              // key → versions, newest last
	Uploads    map[string]*Upload   `json:"uploads,omitempty"`
	Config     map[string]string    `json:"config,omitempty"` // subresource → raw document
}

// Object is one version of an object (or a delete marker).
type Object struct {
	Key          string            `json:"key"`
	VersionID    string            `json:"versionId"`
	Data         []byte            `json:"data,omitempty"`
	ETag         string            `json:"etag"`
	Modified     time.Time         `json:"modified"`
	ContentType  string            `json:"contentType,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	Meta         map[string]string `json:"meta,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	Grants       map[string]string `json:"grants,omitempty"` // permission → x-amz-grant-* header value
	ChecksumAlgo string            `json:"checksumAlgo,omitempty"`
	Checksum     string            `json:"checksum,omitempty"`
	ChecksumType string            `json:"checksumType,omitempty"`
	StorageClass string            `json:"storageClass,omitempty"`
	Parts        int               `json:"parts,omitempty"`
	DeleteMarker bool              `json:"deleteMarker,omitempty"`
}

// New creates the S3 service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.stop = core.Every(time.Second, s.Sweep)
	return s
}

// Close stops the lifecycle sweep.
func (s *Service) Close() error { s.stop(); return nil }

func (s *Service) Info() core.Info {
	return core.Info{Name: "s3", Cloud: core.AWS, Title: "S3", Signing: []string{"s3"}, Protocol: "rest-xml"}
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Buckets: map[string]*Bucket{}}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Buckets == nil {
		st.Buckets = map[string]*Bucket{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// SetJournal logs every change of buckets, objects and uploads (wal mode).
func (s *Service) SetJournal(j *core.Journal) {
	s.mu.Lock()
	s.j = j
	s.mu.Unlock()
}

// Apply replays a journal record: b/ buckets (without objects and uploads),
// o/ the versions of a key, u/ multipart uploads and p/ their parts.
func (s *Service) Apply(key string, v []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := strings.SplitN(key, "/", 3) // kind, bucket, key or upload id
	if len(p) < 2 {
		return fmt.Errorf("bad key %q", key)
	}
	b := s.st.Buckets[p[1]]
	switch {
	case p[0] == "b":
		if err := core.Replay(s.st.Buckets, p[1], v); err != nil || v == nil {
			return err
		}
		n := s.st.Buckets[p[1]]
		n.Objects, n.Uploads = map[string][]*Object{}, nil
		if b != nil {
			n.Objects, n.Uploads = b.Objects, b.Uploads
		}
	case b == nil || len(p) < 3: // the bucket went later
	case p[0] == "o":
		return core.Replay(b.Objects, p[2], v)
	case p[0] == "u":
		if b.Uploads == nil {
			b.Uploads = map[string]*Upload{}
		}
		return core.Replay(b.Uploads, p[2], v)
	case p[0] == "p":
		id, n, _ := strings.Cut(p[2], "/")
		num, _ := strconv.Atoi(n)
		if u := b.Uploads[id]; u != nil {
			part := &Part{}
			if err := json.Unmarshal(v, part); err != nil {
				return err
			}
			u.Parts[num] = part
		}
	}
	return nil
}

// journal logs the versions of a key after a change; lock held.
// ponytail: every version rides along, data inline; log one version per
// record (or move data to a blob store) if versioned keys get big.
func (s *Service) journal(b *Bucket, key string) {
	if vs := b.Objects[key]; len(vs) > 0 {
		s.j.Put("o/"+b.Name+"/"+key, vs)
	} else {
		s.j.Delete("o/" + b.Name + "/" + key)
	}
}

// journalBucket logs a bucket's settings without its objects and uploads; lock held.
func (s *Service) journalBucket(b *Bucket) {
	def := *b
	def.Objects, def.Uploads = nil, nil
	s.j.Put("b/"+b.Name, &def)
}

type handler func(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	if arn, ok := strings.CutPrefix(r.URL.EscapedPath(), "/v20180820/tags/"); ok { // S3 Control tags of a bucket
		c.Op = "ListTagsForResource"
		w.Header().Set("x-amz-request-id", c.RequestID)
		err := c.Intercept()
		if err == nil {
			err = s.controlTags(w, r, c, arn)
		}
		if err != nil {
			writeErr(w, r, c, "", err)
		}
		return
	}
	bucket, key := target(r, s.hub.Cfg.Hostname)
	if r.Method == http.MethodPut || r.Method == http.MethodPost {
		hoistQuery(r)
	}
	op, h := s.route(r, bucket, key)
	if bucket != "" && isWebsiteHost(r.Host) {
		op, h = "GetObject", s.website
	}
	c.Op = op
	w.Header().Set("x-amz-request-id", c.RequestID)
	w.Header().Set("x-amz-id-2", c.RequestID)
	if r.Header.Get("Origin") != "" && s.cors(w, r, bucket) {
		return
	}
	if h == nil {
		writeErr(w, r, c, bucket, core.Errorf(501, "NotImplemented", "pogi does not implement this S3 request (%s %s)", r.Method, r.URL.RequestURI()))
		return
	}
	if err := c.Intercept(); err != nil {
		writeErr(w, r, c, bucket, err)
		return
	}
	if slices.ContainsFunc(strings.Split(r.URL.EscapedPath(), "/"), func(seg string) bool { return seg == "." || seg == ".." }) {
		writeErr(w, r, c, bucket, core.Errorf(400, "InvalidURI", "Couldn't parse the specified URI.")) // floci #1231
		return
	}
	if err := presignExpired(r.URL.Query()); err != nil {
		writeErr(w, r, c, bucket, err)
		return
	}
	if !isWebsiteHost(r.Host) { // the website endpoint is open, see website
		if err := s.anonymous(r, c, bucket); err != nil {
			writeErr(w, r, c, bucket, err)
			return
		}
	}
	if err := checkPaging(r.URL.Query()); err != nil {
		writeErr(w, r, c, bucket, err)
		return
	}
	if err := h(w, r, c, bucket, key); err != nil {
		writeErr(w, r, c, bucket, err)
	}
}

// checkPaging refuses a paging parameter that is not a whole number from 0 to 2^31-1, as S3 does.
// A value above 1000 is fine: the list calls cap it. max-buckets must be 1 to 10000.
func checkPaging(q url.Values) error {
	for _, name := range []string{"max-keys", "max-uploads", "max-parts", "part-number-marker", "max-buckets"} {
		v := q.Get(name)
		if v == "" {
			continue
		}
		n, err := strconv.ParseInt(v, 10, 32)
		switch {
		case name == "max-buckets" && (err != nil || n < 1 || n > 10000):
			return core.Errorf(400, "InvalidArgument", "Argument max-buckets must be an integer between 1 and 10000")
		case err != nil || n < 0:
			return core.Errorf(400, "InvalidArgument", "Provided %s not an integer or within integer range", name)
		}
	}
	return nil
}

// route maps method + subresource to an operation.
func (s *Service) route(r *http.Request, bucket, key string) (string, handler) {
	q := r.URL.Query()
	has := q.Has
	switch {
	case bucket == "":
		if r.Method == http.MethodGet {
			return "ListBuckets", s.listBuckets
		}
		return "Unknown", nil
	case key == "":
		switch r.Method {
		case http.MethodHead:
			return "HeadBucket", s.headBucket
		case http.MethodPut:
			if sub := subresource(q); sub != "" {
				return "PutBucket" + opName(sub), s.putConfig(sub)
			}
			return "CreateBucket", s.createBucket
		case http.MethodDelete:
			if sub := subresource(q); sub != "" {
				return "DeleteBucket" + opName(sub), s.deleteConfig(sub)
			}
			return "DeleteBucket", s.deleteBucket
		case http.MethodPost:
			if has("delete") {
				return "DeleteObjects", s.deleteObjects
			}
			if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
				return "PostObject", s.postObject
			}
		case http.MethodGet:
			switch {
			case has("location"):
				return "GetBucketLocation", s.getLocation
			case has("uploads"):
				return "ListMultipartUploads", s.listUploads
			case has("versions"):
				return "ListObjectVersions", s.listVersions
			case q.Get("list-type") == "2":
				return "ListObjectsV2", s.listObjects
			}
			if sub := subresource(q); sub != "" {
				return "GetBucket" + opName(sub), s.getConfig(sub)
			}
			return "ListObjects", s.listObjects
		}
	default:
		switch r.Method {
		case http.MethodPut:
			switch {
			case has("uploadId") && r.Header.Get("x-amz-copy-source") != "":
				return "UploadPartCopy", s.uploadPart
			case has("uploadId"):
				return "UploadPart", s.uploadPart
			case has("tagging"):
				return "PutObjectTagging", s.putObjectTagging
			case has("retention"):
				return "PutObjectRetention", s.putObjectRetention
			case has("legal-hold"):
				return "PutObjectLegalHold", s.putObjectLegalHold
			case has("acl"):
				return "PutObjectAcl", s.putObjectAcl
			case r.Header.Get("x-amz-copy-source") != "":
				return "CopyObject", s.copyObject
			}
			return "PutObject", s.putObject
		case http.MethodGet, http.MethodHead:
			switch {
			case has("uploadId"):
				return "ListParts", s.listParts
			case has("tagging"):
				return "GetObjectTagging", s.getObjectTagging
			case has("retention"):
				return "GetObjectRetention", s.getObjectRetention
			case has("legal-hold"):
				return "GetObjectLegalHold", s.getObjectLegalHold
			case has("acl"):
				return "GetObjectAcl", s.getACL
			case has("attributes"):
				return "GetObjectAttributes", s.getObjectAttributes
			}
			if r.Method == http.MethodHead {
				return "HeadObject", s.getObject
			}
			return "GetObject", s.getObject
		case http.MethodDelete:
			switch {
			case has("uploadId"):
				return "AbortMultipartUpload", s.abortUpload
			case has("tagging"):
				return "DeleteObjectTagging", s.deleteObjectTagging
			}
			return "DeleteObject", s.deleteObject
		case http.MethodPost:
			switch {
			case has("uploads"):
				return "CreateMultipartUpload", s.createUpload
			case has("uploadId"):
				return "CompleteMultipartUpload", s.completeUpload
			case has("restore"):
				return "RestoreObject", s.accept
			}
		}
	}
	return "Unknown", nil
}

// target finds bucket and key from a virtual-host or path-style request.
func target(r *http.Request, hostname string) (bucket, key string) {
	host := strings.ToLower(r.Host)
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if b := virtualBucket(host, hostname); b != "" {
		return b, strings.TrimPrefix(r.URL.Path, "/")
	}
	bucket, key, _ = strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	return bucket, key
}

// virtualBucket is the bucket of <bucket>.s3.<host>, <bucket>.<hostname> and
// <bucket>.localhost.
func virtualBucket(host, hostname string) string {
	if net.ParseIP(host) != nil || strings.HasPrefix(host, "s3.") || strings.HasPrefix(host, "s3-") {
		return ""
	}
	for _, sep := range []string{".s3.", ".s3-"} {
		if i := strings.Index(host, sep); i > 0 {
			return host[:i]
		}
	}
	if b, base := core.SplitHost(host, hostname); base == hostname || base == "localhost" {
		return b
	}
	return ""
}

// bucketErrors are the error codes that name the bucket in a <BucketName> element, as S3 does.
var bucketErrors = map[string]bool{"NoSuchBucket": true, "BucketNotEmpty": true, "BucketAlreadyOwnedByYou": true, "BucketAlreadyExists": true,
	"InvalidBucketName": true, "NoSuchTagSet": true, "NoSuchBucketPolicy": true, "NoSuchLifecycleConfiguration": true, "NoSuchCORSConfiguration": true,
	"NoSuchWebsiteConfiguration": true, "ReplicationConfigurationNotFoundError": true, "ObjectLockConfigurationNotFoundError": true,
	"NoSuchPublicAccessBlockConfiguration": true, "OwnershipControlsNotFoundError": true}

func writeErr(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket string, err error) {
	e := core.AsError(err)
	c.ErrCode = e.Code
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(e.Status)
	if r.Method == http.MethodHead || e.Status == http.StatusNotModified {
		return
	}
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	control := strings.HasPrefix(r.URL.Path, "/v20180820/tags/") // S3 Control wraps its errors in <ErrorResponse> (floci #557)
	if control {
		b.WriteString(`<ErrorResponse xmlns="http://awss3control.amazonaws.com/doc/2018-08-20/">`)
	}
	b.WriteString("<Error><Code>")
	xml.EscapeText(&b, []byte(e.Code))
	b.WriteString("</Code><Message>")
	xml.EscapeText(&b, []byte(e.Msg))
	b.WriteString("</Message>")
	if bucket != "" && bucketErrors[e.Code] {
		b.WriteString("<BucketName>")
		xml.EscapeText(&b, []byte(bucket))
		b.WriteString("</BucketName>")
	}
	b.WriteString("<RequestId>" + c.RequestID + "</RequestId></Error>")
	if control {
		b.WriteString("<RequestId>" + c.RequestID + "</RequestId></ErrorResponse>")
	}
	w.Write([]byte(b.String()))
}

func writeXML(w http.ResponseWriter, status int, v any) error {
	b, err := xml.Marshal(v)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	w.Write([]byte(xml.Header))
	w.Write(b)
	return nil
}

func (s *Service) accept(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	if _, err := s.bucket(bucket); err != nil {
		return err
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func errNoBucket(name string) error {
	return core.Errorf(404, "NoSuchBucket", "The specified bucket does not exist: %s", name)
}

func errNoKey(key string) error {
	return core.Errorf(404, "NoSuchKey", "The specified key does not exist: %s", key)
}

// bucket returns a bucket under the read lock's protection of the caller.
func (s *Service) bucket(name string) (*Bucket, error) {
	s.mu.RLock()
	b := s.st.Buckets[name]
	s.mu.RUnlock()
	if b == nil {
		return nil, errNoBucket(name)
	}
	return b, nil
}

// latest returns the current version of a key (nil if absent or deleted).
func (b *Bucket) latest(key string) *Object {
	vs := b.Objects[key]
	if len(vs) == 0 || vs[len(vs)-1].DeleteMarker {
		return nil
	}
	return vs[len(vs)-1]
}

// put stores a new version according to the bucket's versioning state.
func (b *Bucket) put(o *Object) {
	b.applyDefaultRetention(o)
	switch b.Versioning {
	case "Enabled":
		o.VersionID = core.RandString(32, core.AlphaNum)
		b.Objects[o.Key] = append(b.Objects[o.Key], o)
	case "Suspended":
		o.VersionID = "null"
		vs := b.Objects[o.Key][:0:0]
		for _, v := range b.Objects[o.Key] {
			if v.VersionID != "null" {
				vs = append(vs, v)
			}
		}
		b.Objects[o.Key] = append(vs, o)
	default:
		o.VersionID = "null"
		b.Objects[o.Key] = []*Object{o}
	}
}

func (b *Bucket) version(key, id string) *Object {
	for _, v := range b.Objects[key] {
		if v.VersionID == id {
			return v
		}
	}
	return nil
}

// TaggedResources lists buckets (ARN → tags) for the tagging API and Resource Explorer.
func (s *Service) TaggedResources(account, region string) map[string]map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := map[string]map[string]string{}
	for _, b := range s.st.Buckets {
		if b.Account == account && b.Region == region {
			var t taggingXML
			xml.Unmarshal([]byte(b.Config["tagging"]), &t) // unset: no tags
			tags := map[string]string{}
			for _, kv := range t.TagSet {
				tags[kv.Key] = kv.Value
			}
			out[core.GlobalARN("s3", region, "", b.Name)] = tags
		}
	}
	return out
}

// Resources lists buckets for the console.
func (s *Service) Resources() []core.Table {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t := core.Table{Name: "Buckets", Global: true, Columns: []string{"Name", "Region", "Account", "Objects", "Size", "Versioning", "Created"}}
	for _, name := range core.SortedKeys(s.st.Buckets) {
		b := s.st.Buckets[name]
		n, size := 0, 0
		for key := range b.Objects {
			if o := b.latest(key); o != nil {
				n++
				size += len(o.Data)
			}
		}
		t.Rows = append(t.Rows, []string{b.Name, b.Region, b.Account, strconv.Itoa(n), HumanSize(int64(size)), b.Versioning, b.Created.Format(time.DateTime)})
	}
	return []core.Table{t}
}

// Inspect returns a bucket's settings and object count.
func (s *Service) Inspect(table, id string) any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b := s.st.Buckets[id]
	if b == nil {
		return nil
	}
	return map[string]any{"name": b.Name, "region": b.Region, "account": b.Account, "created": b.Created,
		"versioning": b.Versioning, "keys": len(b.Objects), "uploads": len(b.Uploads), "config": b.Config}
}

// HumanSize formats a byte count.
func HumanSize(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

// ---- wire helpers: aws-chunked bodies, checksums, presigned URLs ----

// readBody returns the payload, decoding aws-chunked framing and its trailers.
func readBody(r *http.Request) ([]byte, map[string]string, error) {
	body := core.Body(r)
	if !strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") &&
		!strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
		return body, nil, nil
	}
	var out []byte
	for {
		i := bytes.Index(body, []byte("\r\n"))
		if i < 0 {
			return nil, nil, core.Errorf(400, "IncompleteBody", "malformed aws-chunked body")
		}
		size, _, _ := strings.Cut(string(body[:i]), ";")
		n, err := strconv.ParseInt(strings.TrimSpace(size), 16, 64)
		if err != nil || n < 0 || int64(len(body)-i-2) < n {
			return nil, nil, core.Errorf(400, "IncompleteBody", "malformed aws-chunked body")
		}
		body = body[i+2:]
		if n == 0 {
			break
		}
		out = append(out, body[:n]...)
		body = bytes.TrimPrefix(body[n:], []byte("\r\n"))
	}
	trailers := map[string]string{}
	for _, line := range strings.Split(string(body), "\r\n") {
		if k, v, ok := strings.Cut(line, ":"); ok {
			trailers[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
		}
	}
	if out == nil {
		out = []byte{}
	}
	return out, trailers, nil
}

var (
	algos          = []string{"CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256", "MD5", "SHA512"}
	crc32cTable    = crc32.MakeTable(crc32.Castagnoli)
	crc64nvmeTable = crc64.MakeTable(0x9a6c9329ac4bc9b5)
)

// checksum computes a base64 S3 checksum; "" for unknown algorithms.
func checksum(algo string, data []byte) string {
	var sum []byte
	switch algo {
	case "CRC32":
		sum = binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE(data))
	case "CRC32C":
		sum = binary.BigEndian.AppendUint32(nil, crc32.Checksum(data, crc32cTable))
	case "CRC64NVME":
		sum = binary.BigEndian.AppendUint64(nil, crc64.Checksum(data, crc64nvmeTable))
	case "SHA1":
		h := sha1.Sum(data)
		sum = h[:]
	case "SHA256":
		h := sha256.Sum256(data)
		sum = h[:]
	case "MD5":
		h := md5.Sum(data)
		sum = h[:]
	case "SHA512":
		h := sha512.Sum512(data)
		sum = h[:]
	default:
		return ""
	}
	return base64.StdEncoding.EncodeToString(sum)
}

// verifyChecksums checks Content-MD5 and any x-amz-checksum-* header or
// trailer, returning the algorithm and value to store.
func verifyChecksums(r *http.Request, trailers map[string]string, data []byte) (algo, value string, err error) {
	if want := r.Header.Get("Content-MD5"); want != "" {
		h := md5.Sum(data)
		if base64.StdEncoding.EncodeToString(h[:]) != want {
			return "", "", core.Errorf(400, "BadDigest", "The Content-MD5 you specified did not match what we received.")
		}
	}
	for _, a := range algos {
		name := "x-amz-checksum-" + strings.ToLower(a)
		want := r.Header.Get(name)
		if want == "" {
			want = trailers[name]
		}
		if want != "" {
			if got := checksum(a, data); got != want {
				return "", "", core.Errorf(400, "BadDigest", "The %s you specified did not match the calculated checksum.", a)
			}
			return a, want, nil
		}
	}
	for _, h := range []string{"x-amz-sdk-checksum-algorithm", "x-amz-checksum-algorithm"} {
		if a := strings.ToUpper(r.Header.Get(h)); checksum(a, nil) != "" {
			return a, checksum(a, data), nil
		}
	}
	return "", "", nil
}

// queryOnly are x-amz-* query parameters that are not request headers in disguise.
var queryOnly = []string{"x-amz-algorithm", "x-amz-credential", "x-amz-date", "x-amz-expires", "x-amz-signedheaders",
	"x-amz-signature", "x-amz-security-token", "x-amz-content-sha256", "x-amz-copy-source"}

// hoistQuery copies the x-amz-* query parameters of a presigned URL onto the request
// headers (metadata, tags, checksums, storage class and so on), where the handlers
// read them. A header the request already has wins.
func hoistQuery(r *http.Request) {
	for k, v := range r.URL.Query() {
		if l := strings.ToLower(k); strings.HasPrefix(l, "x-amz-") && !slices.Contains(queryOnly, l) && r.Header.Get(l) == "" {
			r.Header.Set(l, v[0])
		}
	}
}

// anonymous refuses a request without credentials when signatures are validated,
// unless the bucket policy lets everyone do it. Browser form uploads (POST policy)
// are checked in postObject.
func (s *Service) anonymous(r *http.Request, c *core.Ctx, bucket string) error {
	if !s.hub.Cfg.ValidateSigs || c.Internal || r.Header.Get("Authorization") != "" || r.URL.Query().Has("X-Amz-Signature") || c.Op == "PostObject" {
		return nil
	}
	return s.publicAccess(c, bucket)
}

// BucketPolicy returns a bucket's policy document and owner account (ok is false when
// the bucket does not exist). IAM enforcement evaluates it next to the caller's policies.
func (s *Service) BucketPolicy(bucket string) (policy, account string, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if b := s.st.Buckets[bucket]; b != nil {
		return b.Config["policy"], b.Account, true
	}
	return "", "", false
}

// publicAccess is nil when the bucket's policy allows the request without credentials.
func (s *Service) publicAccess(c *core.Ctx, bucket string) error {
	s.mu.RLock()
	var policy string
	if b := s.st.Buckets[bucket]; b != nil {
		policy = b.Config["policy"]
	}
	s.mu.RUnlock()
	if iam.AnonymousAllowed(c, policy) {
		return nil
	}
	return core.Errorf(403, "AccessDenied", "Access Denied")
}

func presignExpired(q url.Values) error {
	if q.Get("X-Amz-Signature") == "" {
		return nil
	}
	t, err1 := time.Parse("20060102T150405Z", q.Get("X-Amz-Date"))
	secs, err2 := strconv.Atoi(q.Get("X-Amz-Expires"))
	if err1 == nil && err2 == nil && time.Now().After(t.Add(time.Duration(secs)*time.Second)) {
		return core.Errorf(403, "AccessDenied", "Request has expired")
	}
	return nil
}

func etag(data []byte) string {
	h := md5.Sum(data)
	return `"` + hex.EncodeToString(h[:]) + `"`
}

func ownerID(account string) string {
	h := sha256.Sum256([]byte("pogi:" + account))
	return hex.EncodeToString(h[:])
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

func ownerOf(account string) *owner { return &owner{ownerID(account), "pogi"} }

// urlEncode encodes keys for EncodingType=url (decodable by unquote and unquote_plus).
func urlEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || '0' <= ch && ch <= '9' || strings.IndexByte("-_.~/", ch) >= 0 {
			b.WriteByte(ch)
		} else {
			fmt.Fprintf(&b, "%%%02X", ch)
		}
	}
	return b.String()
}

func sortedKeys(m map[string][]*Object) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
