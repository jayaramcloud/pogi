// Package storage emulates Google Cloud Storage: the JSON API (buckets,
// objects, media/multipart/resumable uploads, copy/rewrite/compose,
// generations and preconditions, notifications to Pub/Sub) and the XML API
// paths client libraries use for downloads and signed URLs. Point clients at
// it with STORAGE_EMULATOR_HOST=http://localhost:7644.
package storage

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
)

// Service is the Cloud Storage emulator.
type Service struct {
	hub     *core.Hub
	mu      sync.RWMutex
	st      state
	uploads map[string]*upload // resumable sessions (not persisted)
	lastGen int64
}

type state struct {
	Buckets map[string]*Bucket `json:"buckets"`
}

// Bucket is a GCS bucket; extra settings (cors, lifecycle, …) are kept raw.
type Bucket struct {
	Name           string                     `json:"name"`
	Project        string                     `json:"project"`
	Location       string                     `json:"location"`
	StorageClass   string                     `json:"storageClass"`
	Created        time.Time                  `json:"created"`
	Updated        time.Time                  `json:"updated"`
	Metageneration int64                      `json:"metageneration"`
	Versioning     bool                       `json:"versioning"`
	Labels         map[string]string          `json:"labels,omitempty"`
	Extra          map[string]json.RawMessage `json:"extra,omitempty"`
	IAM            json.RawMessage            `json:"iam,omitempty"`
	Objects        map[string][]*Object       `json:"objects"` // name → generations, live one last
	Notifications  map[string]*Notification   `json:"notifications,omitempty"`
}

// Object is one generation of an object.
type Object struct {
	Name               string            `json:"name"`
	Generation         int64             `json:"generation"`
	Metageneration     int64             `json:"metageneration"`
	Data               []byte            `json:"data,omitempty"`
	ContentType        string            `json:"contentType,omitempty"`
	ContentEncoding    string            `json:"contentEncoding,omitempty"`
	ContentDisposition string            `json:"contentDisposition,omitempty"`
	ContentLanguage    string            `json:"contentLanguage,omitempty"`
	CacheControl       string            `json:"cacheControl,omitempty"`
	CustomTime         string            `json:"customTime,omitempty"`
	StorageClass       string            `json:"storageClass,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	Created            time.Time         `json:"created"`
	Updated            time.Time         `json:"updated"`
	Deleted            time.Time         `json:"deleted,omitzero"` // noncurrent since
	MD5                string            `json:"md5"`
	CRC32C             string            `json:"crc32c"`
	Components         int               `json:"components,omitempty"`
}

// Notification sends object changes to a Pub/Sub topic.
type Notification struct {
	ID               string            `json:"id"`
	Topic            string            `json:"topic"`
	EventTypes       []string          `json:"event_types,omitempty"`
	PayloadFormat    string            `json:"payload_format"`
	ObjectNamePrefix string            `json:"object_name_prefix,omitempty"`
	CustomAttributes map[string]string `json:"custom_attributes,omitempty"`
}

type upload struct {
	bucket string
	meta   objectMeta
	data   []byte
	pre    url.Values
}

// New creates the Cloud Storage service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, uploads: map[string]*upload{}}
	s.Reset()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "gcs", Cloud: core.GCP, Title: "Cloud Storage", Protocol: "gcs-json", Match: match}
}

// BodyLimit raises the upload limit for the source zip of a Cloud Function: generateUploadUrl hands
// out a URL in a gcf-v2-uploads-* bucket, and the zip is PUT there.
func (s *Service) BodyLimit(r *http.Request) int64 {
	if r.Method == http.MethodPut && strings.HasPrefix(strings.TrimPrefix(r.URL.Path, "/"), "gcf-v2-uploads-") {
		return s.hub.CodeLimit(false)
	}
	return 0
}

// match claims JSON API paths and XML API traffic from Google clients.
func match(r *http.Request) bool {
	p := r.URL.Path
	for _, prefix := range []string{"/storage/v1/", "/upload/storage/v1/", "/download/storage/v1/"} {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	if core.APIVersionPath(p) || strings.HasPrefix(p, "/oauth2/") {
		return false // "/v1/projects/..." is another Google API, not a bucket
	}
	auth := r.Header.Get("Authorization")
	if auth != "" && !strings.HasPrefix(auth, "Bearer") {
		return false // AWS-signed traffic belongs to S3
	}
	ua := r.UserAgent()
	return r.Header.Get("X-Goog-Api-Client") != "" || r.URL.Query().Has("X-Goog-Signature") ||
		strings.Contains(ua, "gcloud") || strings.Contains(ua, "google-cloud") || strings.Contains(ua, "google-api-go-client")
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Buckets: map[string]*Bucket{}}
	s.uploads = map[string]*upload{}
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

// apiError renders the GCS JSON error shape.
func apiError(w http.ResponseWriter, c *core.Ctx, err error) {
	e := core.AsError(err)
	c.ErrCode = e.Code
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(e.Status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": e.Status, "message": e.Msg, "status": core.GoogleStatus(e.Status),
		"errors": []map[string]string{{"message": e.Msg, "domain": "global", "reason": e.Code}}}})
}

func notFound(format string, a ...any) error { return core.Errorf(404, "notFound", format, a...) }
func badRequest(format string, a ...any) error {
	return core.Errorf(400, "invalid", format, a...)
}
func conditionNotMet() error {
	return core.Errorf(412, "conditionNotMet", "At least one of the pre-conditions you specified did not hold.")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(status)
	if v != nil {
		json.NewEncoder(w).Encode(v)
	}
}

// segments splits the escaped path so object names may contain %2F.
func segments(r *http.Request) []string {
	var out []string
	for _, p := range strings.Split(strings.Trim(r.URL.EscapedPath(), "/"), "/") {
		u, err := url.PathUnescape(p)
		if err != nil {
			u = p
		}
		out = append(out, u)
	}
	return out
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	seg := segments(r)
	var op string
	var h func() error
	switch {
	case len(seg) >= 2 && seg[0] == "storage" && seg[1] == "v1":
		op, h = s.routeJSON(w, r, c, seg[2:])
	case len(seg) >= 3 && seg[0] == "upload" && seg[1] == "storage":
		op, h = "objects.insert", func() error { return s.upload(w, r, c, seg) }
	case len(seg) >= 3 && seg[0] == "download" && seg[1] == "storage":
		op, h = "objects.get", func() error {
			if len(seg) < 7 || seg[3] != "b" || seg[5] != "o" {
				return notFound("Not Found")
			}
			return s.getObject(w, r, c, seg[4], strings.Join(seg[6:], "/"), true)
		}
	default:
		op, h = s.routeXML(w, r, c, seg)
	}
	c.Op = op
	if op == "buckets.list" || op == "buckets.insert" {
		c.Resource = "projects/" + r.URL.Query().Get("project") // for IAM: the project the call is on
	}
	if err := c.Intercept(); err != nil {
		apiError(w, c, err)
		return
	}
	if h == nil {
		apiError(w, c, core.Errorf(501, "notImplemented", "pogi does not implement %s %s", r.Method, r.URL.Path))
		return
	}
	if !gapi.PageSizeOK(r.URL.Query().Get("maxResults")) {
		apiError(w, c, badRequest("Invalid value for maxResults: must be a whole number, 0 or more"))
		return
	}
	if err := h(); err != nil {
		if op != "" && strings.HasPrefix(op, "xml.") {
			xmlError(w, c, err)
			return
		}
		apiError(w, c, err)
	}
}

func (s *Service) routeJSON(w http.ResponseWriter, r *http.Request, c *core.Ctx, seg []string) (string, func() error) {
	n, m := len(seg), r.Method
	if n == 0 || seg[0] != "b" {
		return "", nil
	}
	switch {
	case n == 1 && m == http.MethodGet:
		return "buckets.list", func() error { return s.listBuckets(w, r) }
	case n == 1 && m == http.MethodPost:
		return "buckets.insert", func() error { return s.insertBucket(w, r) }
	case n == 2 && m == http.MethodGet:
		return "buckets.get", func() error { return s.getBucket(w, r, seg[1]) }
	case n == 2 && (m == http.MethodPatch || m == http.MethodPut):
		return "buckets.patch", func() error { return s.patchBucket(w, r, seg[1]) }
	case n == 2 && m == http.MethodDelete:
		return "buckets.delete", func() error { return s.deleteBucket(w, seg[1]) }
	case n >= 3 && seg[2] == "iam":
		op := "buckets.getIamPolicy"
		if n > 3 {
			op = "buckets.testIamPermissions"
		} else if m != http.MethodGet {
			op = "buckets.setIamPolicy"
		}
		return op, func() error { return s.iam(w, r, seg[1], n > 3) }
	case n == 3 && seg[2] == "storageLayout" && m == http.MethodGet: // gcloud storage cp asks before each upload
		return "buckets.getStorageLayout", func() error { return s.storageLayout(w, seg[1]) }
	case n >= 3 && seg[2] == "notificationConfigs":
		id := ""
		if n > 3 {
			id = seg[3]
		}
		return "notifications." + strings.ToLower(m), func() error { return s.notifications(w, r, seg[1], id) }
	case n == 3 && seg[2] == "o" && m == http.MethodGet:
		return "objects.list", func() error { return s.listObjects(w, r, seg[1]) }
	case n == 3 && seg[2] == "o" && m == http.MethodPost: // simple upload without the /upload prefix
		return "objects.insert", func() error { return s.upload(w, r, c, append([]string{"upload", "storage", "v1"}, seg...)) }
	case n >= 4 && seg[2] == "o":
		name := strings.Join(seg[3:], "/")
		if i := indexOf(seg, "copyTo"); i > 3 && n >= i+5 {
			return "objects.copy", func() error {
				return s.copy(w, r, seg[1], strings.Join(seg[3:i], "/"), seg[i+2], strings.Join(seg[i+4:], "/"), false)
			}
		}
		if i := indexOf(seg, "rewriteTo"); i > 3 && n >= i+5 {
			return "objects.rewrite", func() error {
				return s.copy(w, r, seg[1], strings.Join(seg[3:i], "/"), seg[i+2], strings.Join(seg[i+4:], "/"), true)
			}
		}
		if seg[n-1] == "compose" && m == http.MethodPost {
			return "objects.compose", func() error { return s.compose(w, r, seg[1], strings.Join(seg[3:n-1], "/")) }
		}
		switch m {
		case http.MethodGet:
			return "objects.get", func() error { return s.getObject(w, r, c, seg[1], name, r.URL.Query().Get("alt") == "media") }
		case http.MethodPatch, http.MethodPut:
			return "objects.patch", func() error { return s.patchObject(w, r, seg[1], name) }
		case http.MethodDelete:
			return "objects.delete", func() error { return s.deleteObject(w, r, seg[1], name) }
		}
	}
	return "", nil
}

func indexOf(seg []string, s string) int {
	for i, x := range seg {
		if x == s {
			return i
		}
	}
	return -1
}

// ---- buckets ----

var bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$`)

func (b *Bucket) resource(host string) map[string]any {
	out := map[string]any{"kind": "storage#bucket", "id": b.Name, "name": b.Name, "selfLink": "http://" + host + "/storage/v1/b/" + b.Name,
		"projectNumber": "0", "timeCreated": b.Created.Format(time.RFC3339Nano), "updated": b.Updated.Format(time.RFC3339Nano),
		"metageneration": strconv.FormatInt(b.Metageneration, 10), "location": b.Location, "locationType": "multi-region",
		"storageClass": b.StorageClass, "etag": "CAE=", "versioning": map[string]bool{"enabled": b.Versioning}}
	if len(b.Labels) > 0 {
		out["labels"] = b.Labels
	}
	for k, v := range b.Extra {
		out[k] = v
	}
	return out
}

type bucketMeta struct {
	Name         string
	Location     string
	StorageClass string
	Versioning   *struct{ Enabled bool }
	Labels       map[string]string
}

// applyBucket copies settings from a request body (known fields + raw extras).
func applyBucket(b *Bucket, body []byte) error {
	var m bucketMeta
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return badRequest("Invalid JSON payload: %v", err)
	}
	json.Unmarshal(body, &raw)
	if m.Location != "" {
		b.Location = strings.ToUpper(m.Location)
	}
	if m.StorageClass != "" {
		b.StorageClass = m.StorageClass
	}
	if m.Versioning != nil {
		b.Versioning = m.Versioning.Enabled
	}
	if m.Labels != nil {
		b.Labels = m.Labels
	}
	for k, v := range raw {
		switch k {
		case "name", "location", "storageClass", "versioning", "labels", "id", "kind", "selfLink", "etag", "timeCreated", "updated", "metageneration", "projectNumber":
		default:
			if b.Extra == nil {
				b.Extra = map[string]json.RawMessage{}
			}
			b.Extra[k] = v
		}
	}
	return nil
}

func (s *Service) insertBucket(w http.ResponseWriter, r *http.Request) error {
	body := core.Body(r)
	var m bucketMeta
	json.Unmarshal(body, &m)
	if !bucketName.MatchString(m.Name) {
		return badRequest("Invalid bucket name: '%s'", m.Name)
	}
	now := time.Now().UTC()
	b := &Bucket{Name: m.Name, Project: r.URL.Query().Get("project"), Location: "US", StorageClass: "STANDARD", Created: now, Updated: now,
		Metageneration: 1, Objects: map[string][]*Object{}}
	if err := applyBucket(b, body); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Buckets[b.Name] != nil {
		return core.Errorf(409, "conflict", "Your previous request to create the named bucket succeeded and you already own it.")
	}
	s.st.Buckets[b.Name] = b
	writeJSON(w, 200, b.resource(r.Host))
	return nil
}

func (s *Service) listBuckets(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := []any{}
	for _, name := range core.SortedKeys(s.st.Buckets) {
		b := s.st.Buckets[name]
		if (q.Get("project") == "" || b.Project == "" || b.Project == q.Get("project")) && strings.HasPrefix(name, q.Get("prefix")) && name > q.Get("pageToken") {
			items = append(items, b.resource(r.Host))
		}
	}
	writeJSON(w, 200, map[string]any{"kind": "storage#buckets", "items": items})
	return nil
}

func (s *Service) bucket(name string) (*Bucket, error) {
	if b := s.st.Buckets[name]; b != nil {
		return b, nil
	}
	return nil, notFound("The specified bucket does not exist.")
}

func (s *Service) getBucket(w http.ResponseWriter, r *http.Request, name string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := s.bucket(name)
	if err != nil {
		return err
	}
	writeJSON(w, 200, b.resource(r.Host))
	return nil
}

// storageLayout says where the bucket lives and that it has a flat namespace (no hierarchical folders).
func (s *Service) storageLayout(w http.ResponseWriter, name string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := s.bucket(name)
	if err != nil {
		return err
	}
	writeJSON(w, 200, map[string]any{"kind": "storage#storageLayout", "bucket": b.Name, "location": b.Location,
		"locationType": "multi-region", "hierarchicalNamespace": map[string]any{"enabled": false}})
	return nil
}

func (s *Service) patchBucket(w http.ResponseWriter, r *http.Request, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucket(name)
	if err != nil {
		return err
	}
	if v := r.URL.Query().Get("ifMetagenerationMatch"); v != "" && v != strconv.FormatInt(b.Metageneration, 10) {
		return conditionNotMet()
	}
	if err := applyBucket(b, core.Body(r)); err != nil {
		return err
	}
	b.Metageneration++
	b.Updated = time.Now().UTC()
	writeJSON(w, 200, b.resource(r.Host))
	return nil
}

func (s *Service) deleteBucket(w http.ResponseWriter, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucket(name)
	if err != nil {
		return err
	}
	for _, gens := range b.Objects {
		if len(gens) > 0 {
			return core.Errorf(409, "conflict", "The bucket you tried to delete is not empty.")
		}
	}
	delete(s.st.Buckets, name)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Service) iam(w http.ResponseWriter, r *http.Request, name string, test bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucket(name)
	if err != nil {
		return err
	}
	switch {
	case test:
		writeJSON(w, 200, map[string]any{"kind": "storage#testIamPermissionsResponse", "permissions": r.URL.Query()["permissions"]})
	case r.Method == http.MethodPut:
		b.IAM = json.RawMessage(core.Body(r))
		w.Header().Set("Content-Type", "application/json")
		w.Write(b.IAM)
	default:
		if b.IAM == nil {
			writeJSON(w, 200, map[string]any{"kind": "storage#policy", "resourceId": "projects/_/buckets/" + name, "version": 1, "etag": "CAE=",
				"bindings": []map[string]any{{"role": "roles/storage.legacyBucketOwner", "members": []string{"projectOwner:" + b.Project}}}})
			return nil
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(b.IAM)
	}
	return nil
}

func (s *Service) notifications(w http.ResponseWriter, r *http.Request, name, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucket(name)
	if err != nil {
		return err
	}
	if b.Notifications == nil {
		b.Notifications = map[string]*Notification{}
	}
	res := func(n *Notification) map[string]any {
		return map[string]any{"kind": "storage#notification", "id": n.ID, "topic": "//pubsub.googleapis.com/" + strings.TrimPrefix(n.Topic, "//pubsub.googleapis.com/"),
			"event_types": n.EventTypes, "payload_format": n.PayloadFormat, "object_name_prefix": n.ObjectNamePrefix,
			"custom_attributes": n.CustomAttributes, "etag": n.ID, "selfLink": "http://" + r.Host + "/storage/v1/b/" + name + "/notificationConfigs/" + n.ID}
	}
	switch {
	case r.Method == http.MethodPost:
		var n Notification
		if err := json.Unmarshal(core.Body(r), &n); err != nil || n.Topic == "" {
			return badRequest("A notification configuration needs a topic")
		}
		n.ID = strconv.Itoa(len(b.Notifications) + 1)
		if n.PayloadFormat == "" {
			n.PayloadFormat = "JSON_API_V1"
		}
		b.Notifications[n.ID] = &n
		writeJSON(w, 200, res(&n))
	case r.Method == http.MethodGet && id == "":
		items := []any{}
		for _, k := range core.SortedKeys(b.Notifications) {
			items = append(items, res(b.Notifications[k]))
		}
		writeJSON(w, 200, map[string]any{"kind": "storage#notifications", "items": items})
	case r.Method == http.MethodGet:
		n := b.Notifications[id]
		if n == nil {
			return notFound("Notification %s not found", id)
		}
		writeJSON(w, 200, res(n))
	case r.Method == http.MethodDelete:
		delete(b.Notifications, id)
		w.WriteHeader(http.StatusNoContent)
	}
	return nil
}

// ---- objects ----

func (o *Object) resource(bucket, host string) map[string]any {
	gen := strconv.FormatInt(o.Generation, 10)
	out := map[string]any{"kind": "storage#object", "id": bucket + "/" + o.Name + "/" + gen, "name": o.Name, "bucket": bucket,
		"selfLink":   "http://" + host + "/storage/v1/b/" + bucket + "/o/" + url.PathEscape(o.Name),
		"mediaLink":  "http://" + host + "/download/storage/v1/b/" + bucket + "/o/" + url.PathEscape(o.Name) + "?generation=" + gen + "&alt=media",
		"generation": gen, "metageneration": strconv.FormatInt(o.Metageneration, 10), "contentType": o.ContentType,
		"storageClass": or(o.StorageClass, "STANDARD"), "size": strconv.Itoa(len(o.Data)), "md5Hash": o.MD5, "crc32c": o.CRC32C,
		"etag": etag(o), "timeCreated": o.Created.Format(time.RFC3339Nano), "updated": o.Updated.Format(time.RFC3339Nano),
		"timeStorageClassUpdated": o.Created.Format(time.RFC3339Nano)}
	for k, v := range map[string]string{"contentEncoding": o.ContentEncoding, "contentDisposition": o.ContentDisposition,
		"contentLanguage": o.ContentLanguage, "cacheControl": o.CacheControl, "customTime": o.CustomTime} {
		if v != "" {
			out[k] = v
		}
	}
	if len(o.Metadata) > 0 {
		out["metadata"] = o.Metadata
	}
	if !o.Deleted.IsZero() {
		out["timeDeleted"] = o.Deleted.Format(time.RFC3339Nano)
	}
	if o.Components > 0 {
		out["componentCount"] = o.Components
	}
	return out
}

func etag(o *Object) string {
	return base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint64(nil, uint64(o.Generation)))
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func hashes(data []byte) (md5b64, crc string) {
	m := md5.Sum(data)
	c := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	return base64.StdEncoding.EncodeToString(m[:]), base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint32(nil, c))
}

// live returns the current generation of an object (nil if none).
func (b *Bucket) live(name string) *Object {
	gens := b.Objects[name]
	if len(gens) == 0 || !gens[len(gens)-1].Deleted.IsZero() {
		return nil
	}
	return gens[len(gens)-1]
}

// find returns a specific generation (or the live one when gen is "").
func (b *Bucket) find(name, gen string) *Object {
	if gen == "" {
		return b.live(name)
	}
	for _, o := range b.Objects[name] {
		if strconv.FormatInt(o.Generation, 10) == gen {
			return o
		}
	}
	return nil
}

// check applies ifGeneration*/ifMetageneration* preconditions.
func check(q url.Values, cur *Object) error {
	gen, meta := int64(0), int64(0)
	if cur != nil {
		gen, meta = cur.Generation, cur.Metageneration
	}
	for k, test := range map[string]func(int64) bool{
		"ifGenerationMatch":        func(v int64) bool { return gen == v },
		"ifGenerationNotMatch":     func(v int64) bool { return gen != v },
		"ifMetagenerationMatch":    func(v int64) bool { return cur != nil && meta == v },
		"ifMetagenerationNotMatch": func(v int64) bool { return cur == nil || meta != v },
	} {
		if s := q.Get(k); s != "" {
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil || !test(v) {
				return conditionNotMet()
			}
		}
	}
	return nil
}

func (s *Service) nextGen() int64 {
	g := time.Now().UnixMicro()
	if g <= s.lastGen {
		g = s.lastGen + 1
	}
	s.lastGen = g
	return g
}

type objectMeta struct {
	Name               string
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	ContentLanguage    string
	CacheControl       string
	CustomTime         string
	StorageClass       string
	Metadata           map[string]string
	Md5Hash            string
	Crc32c             string
}

// store writes a new live generation; the lock must be held. It returns pending notifications.
func (s *Service) store(b *Bucket, m objectMeta, data []byte, q url.Values) (*Object, []pending, error) {
	if m.Name == "" {
		return nil, nil, core.Errorf(400, "required", "Required")
	}
	cur := b.live(m.Name)
	if err := check(q, cur); err != nil {
		return nil, nil, err
	}
	md5b, crc := hashes(data)
	if m.Md5Hash != "" && m.Md5Hash != md5b || m.Crc32c != "" && m.Crc32c != crc {
		return nil, nil, core.Errorf(400, "invalid", "Provided MD5 hash or CRC32C does not match the calculated value")
	}
	now := time.Now().UTC()
	o := &Object{Name: m.Name, Generation: s.nextGen(), Metageneration: 1, Data: data, ContentType: or(m.ContentType, "application/octet-stream"),
		ContentEncoding: m.ContentEncoding, ContentDisposition: m.ContentDisposition, ContentLanguage: m.ContentLanguage,
		CacheControl: m.CacheControl, CustomTime: m.CustomTime, StorageClass: m.StorageClass, Metadata: m.Metadata,
		Created: now, Updated: now, MD5: md5b, CRC32C: crc}
	var ev []pending
	if b.Versioning {
		if cur != nil {
			cur.Deleted = now
			ev = append(ev, s.events(b, cur, "OBJECT_ARCHIVE")...)
		}
		b.Objects[m.Name] = append(b.Objects[m.Name], o)
	} else {
		b.Objects[m.Name] = []*Object{o}
	}
	return o, append(ev, s.events(b, o, "OBJECT_FINALIZE")...), nil
}

// upload handles media, multipart and resumable uploads.
func (s *Service) upload(w http.ResponseWriter, r *http.Request, c *core.Ctx, seg []string) error {
	if len(seg) < 6 || seg[3] != "b" || seg[5] != "o" {
		return notFound("Not Found")
	}
	bucket, q := seg[4], r.URL.Query()
	if id := q.Get("upload_id"); id != "" {
		return s.resume(w, r, bucket, id)
	}
	body := core.Body(r)
	var m objectMeta
	data := body
	switch q.Get("uploadType") {
	case "multipart":
		// gcloud (Python's email package) quotes the boundary with single quotes, which
		// mime does not accept: boundary='===...=='. Real GCS takes it, so turn them into double quotes.
		ct := r.Header.Get("Content-Type")
		if i := strings.Index(ct, "boundary='"); i >= 0 && strings.HasSuffix(ct, "'") {
			ct = ct[:i] + `boundary="` + ct[i+len("boundary='"):len(ct)-1] + `"`
		}
		_, params, err := mime.ParseMediaType(ct)
		if err != nil {
			return badRequest("multipart upload needs a boundary")
		}
		mr := multipart.NewReader(bytes.NewReader(body), params["boundary"])
		meta, err := mr.NextPart()
		if err != nil {
			return badRequest("missing metadata part")
		}
		mb, _ := io.ReadAll(meta)
		if err := json.Unmarshal(mb, &m); err != nil {
			return badRequest("Invalid JSON metadata: %v", err)
		}
		media, err := mr.NextPart()
		if err != nil {
			return badRequest("missing media part")
		}
		data, _ = io.ReadAll(media)
		if m.ContentType == "" {
			m.ContentType = media.Header.Get("Content-Type")
		}
	case "resumable":
		if len(body) > 0 {
			if err := json.Unmarshal(body, &m); err != nil {
				return badRequest("Invalid JSON metadata: %v", err)
			}
		}
		if m.Name == "" {
			m.Name = q.Get("name")
		}
		if ct := r.Header.Get("X-Upload-Content-Type"); m.ContentType == "" && ct != "" {
			m.ContentType = ct
		}
		id := core.RandString(48, core.AlphaNum)
		s.mu.Lock()
		s.uploads[id] = &upload{bucket: bucket, meta: m, pre: q}
		s.mu.Unlock()
		loc := "http://" + r.Host + "/upload/storage/v1/b/" + bucket + "/o?uploadType=resumable&upload_id=" + id
		w.Header().Set("Location", loc)
		w.Header().Set("X-GUploader-UploadID", id)
		writeJSON(w, 200, nil)
		return nil
	default: // media
		m.ContentType = r.Header.Get("Content-Type")
	}
	if m.Name == "" {
		m.Name = q.Get("name")
	}
	return s.finish(w, r, bucket, m, data, q)
}

func (s *Service) finish(w http.ResponseWriter, r *http.Request, bucket string, m objectMeta, data []byte, q url.Values) error {
	s.mu.Lock()
	b, err := s.bucket(bucket)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	o, ev, err := s.store(b, m, data, q)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	res := o.resource(bucket, r.Host)
	s.mu.Unlock()
	s.notify(core.C(r).Trace, ev)
	writeJSON(w, 200, res)
	return nil
}

// resume appends a chunk of a resumable upload (Content-Range: bytes a-b/total).
func (s *Service) resume(w http.ResponseWriter, r *http.Request, bucket, id string) error {
	s.mu.Lock()
	u := s.uploads[id]
	if u == nil {
		s.mu.Unlock()
		return notFound("No such upload session")
	}
	if r.Method == http.MethodDelete {
		delete(s.uploads, id)
		s.mu.Unlock()
		w.WriteHeader(499)
		return nil
	}
	limit, code := s.hub.MaxUpload(), strings.HasPrefix(bucket, "gcf-v2-uploads-")
	if code {
		limit = s.hub.CodeLimit(false)
	}
	if size := int64(len(u.data) + len(core.Body(r))); size > limit {
		delete(s.uploads, id)
		s.mu.Unlock()
		msg := ""
		if code {
			msg = s.hub.TooBigCode("gcs", bucket+"/"+u.meta.Name, "Function code", size, limit)
		} else {
			msg = s.hub.TooBig("gcs", bucket+"/"+u.meta.Name, size, limit)
		}
		return core.Errorf(413, "uploadTooLarge", "%s", msg)
	}
	u.data = append(u.data, core.Body(r)...)
	total := int64(-1)
	if cr := r.Header.Get("Content-Range"); cr != "" {
		if i := strings.LastIndex(cr, "/"); i >= 0 && cr[i+1:] != "*" {
			total, _ = strconv.ParseInt(cr[i+1:], 10, 64)
		}
	} else {
		total = int64(len(u.data)) // single-shot PUT
	}
	if total < 0 || int64(len(u.data)) < total {
		s.mu.Unlock()
		if len(u.data) > 0 {
			w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", len(u.data)-1))
		}
		if r.Header.Get("X-GUploader-No-308") == "yes" { // the Go client asks for 200 + override
			w.Header().Set("X-Http-Status-Code-Override", "308")
			w.WriteHeader(http.StatusOK)
			return nil
		}
		w.WriteHeader(308)
		return nil
	}
	delete(s.uploads, id)
	data, meta, pre := u.data, u.meta, u.pre
	s.mu.Unlock()
	return s.finish(w, r, bucket, meta, data, pre)
}

func (s *Service) getObject(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, name string, media bool) error {
	q := r.URL.Query()
	s.mu.RLock()
	b, err := s.bucket(bucket)
	if err != nil {
		s.mu.RUnlock()
		return err
	}
	p := b.find(name, q.Get("generation"))
	if p == nil {
		s.mu.RUnlock()
		return notFound("No such object: %s/%s", bucket, name)
	}
	o := *p
	s.mu.RUnlock()
	if err := check(q, &o); err != nil {
		return err
	}
	if !media {
		writeJSON(w, 200, o.resource(bucket, r.Host))
		return nil
	}
	return serveMedia(w, r, &o)
}

// serveMedia writes object bytes with the headers Google clients validate.
func serveMedia(w http.ResponseWriter, r *http.Request, o *Object) error {
	h := w.Header()
	h.Set("Content-Type", o.ContentType)
	h.Set("X-Goog-Generation", strconv.FormatInt(o.Generation, 10))
	h.Set("X-Goog-Metageneration", strconv.FormatInt(o.Metageneration, 10))
	h.Set("X-Goog-Hash", "crc32c="+o.CRC32C+",md5="+o.MD5)
	h.Set("X-Goog-Stored-Content-Length", strconv.Itoa(len(o.Data)))
	h.Set("X-Goog-Storage-Class", or(o.StorageClass, "STANDARD"))
	h.Set("ETag", `"`+hexMD5(o)+`"`)
	h.Set("Last-Modified", o.Updated.Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")
	if o.ContentEncoding != "" {
		h.Set("Content-Encoding", o.ContentEncoding)
		h.Set("X-Goog-Stored-Content-Encoding", o.ContentEncoding)
	}
	for k, v := range o.Metadata {
		h.Set("X-Goog-Meta-"+k, v)
	}
	data, status := o.Data, http.StatusOK
	if rg := r.Header.Get("Range"); rg != "" {
		start, end, ok := parseRange(rg, len(data))
		if !ok {
			h.Set("Content-Range", fmt.Sprintf("bytes */%d", len(data)))
			return core.Errorf(416, "requestedRangeNotSatisfiable", "The requested range cannot be satisfied.")
		}
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		data, status = data[start:end+1], http.StatusPartialContent
	}
	h.Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		w.Write(data)
	}
	return nil
}

func hexMD5(o *Object) string {
	b, _ := base64.StdEncoding.DecodeString(o.MD5)
	return fmt.Sprintf("%x", b)
}

func parseRange(h string, size int) (int, int, bool) {
	spec, ok := strings.CutPrefix(h, "bytes=")
	if !ok {
		return 0, 0, false
	}
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
	if e, err := strconv.Atoi(b); err == nil && e < end {
		end = e
	}
	return start, end, end >= start
}

func (s *Service) patchObject(w http.ResponseWriter, r *http.Request, bucket, name string) error {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(core.Body(r), &m); err != nil {
		return badRequest("Invalid JSON payload")
	}
	s.mu.Lock()
	b, err := s.bucket(bucket)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	o := b.find(name, r.URL.Query().Get("generation"))
	if o == nil {
		s.mu.Unlock()
		return notFound("No such object: %s/%s", bucket, name)
	}
	if err := check(r.URL.Query(), o); err != nil {
		s.mu.Unlock()
		return err
	}
	str := func(k string, dst *string) {
		if v, ok := m[k]; ok {
			var x string
			json.Unmarshal(v, &x)
			*dst = x
		}
	}
	str("contentType", &o.ContentType)
	str("contentEncoding", &o.ContentEncoding)
	str("contentDisposition", &o.ContentDisposition)
	str("contentLanguage", &o.ContentLanguage)
	str("cacheControl", &o.CacheControl)
	str("customTime", &o.CustomTime)
	if v, ok := m["metadata"]; ok {
		var md map[string]*string
		json.Unmarshal(v, &md)
		next := map[string]string{}
		if r.Method == http.MethodPatch {
			for k, x := range o.Metadata {
				next[k] = x
			}
		}
		for k, x := range md {
			if x == nil {
				delete(next, k)
			} else {
				next[k] = *x
			}
		}
		o.Metadata = next
	}
	o.Metageneration++
	o.Updated = time.Now().UTC()
	res := o.resource(bucket, r.Host)
	ev := s.events(b, o, "OBJECT_METADATA_UPDATE")
	s.mu.Unlock()
	s.notify(core.C(r).Trace, ev)
	writeJSON(w, 200, res)
	return nil
}

func (s *Service) deleteObject(w http.ResponseWriter, r *http.Request, bucket, name string) error {
	q := r.URL.Query()
	s.mu.Lock()
	b, err := s.bucket(bucket)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	o := b.find(name, q.Get("generation"))
	if o == nil {
		s.mu.Unlock()
		return notFound("No such object: %s/%s", bucket, name)
	}
	if err := check(q, o); err != nil {
		s.mu.Unlock()
		return err
	}
	var ev []pending
	if b.Versioning && q.Get("generation") == "" {
		o.Deleted = time.Now().UTC()
		ev = s.events(b, o, "OBJECT_ARCHIVE")
	} else {
		gens := b.Objects[name]
		for i, g := range gens {
			if g == o {
				b.Objects[name] = append(gens[:i:i], gens[i+1:]...)
			}
		}
		if len(b.Objects[name]) == 0 {
			delete(b.Objects, name)
		}
		ev = s.events(b, o, "OBJECT_DELETE")
	}
	s.mu.Unlock()
	s.notify(core.C(r).Trace, ev)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Service) listObjects(w http.ResponseWriter, r *http.Request, bucket string) error {
	q := r.URL.Query()
	prefix, delim, token := q.Get("prefix"), q.Get("delimiter"), q.Get("pageToken")
	versions := q.Get("versions") == "true"
	maxResults := 1000
	if n, err := strconv.Atoi(q.Get("maxResults")); err == nil && n > 0 {
		maxResults = min(n, 1000)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := s.bucket(bucket)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(b.Objects))
	for n := range b.Objects {
		names = append(names, n)
	}
	sort.Strings(names)
	items, prefixes := []any{}, []string{}
	seen := map[string]bool{}
	next := ""
	for _, n := range names {
		if !strings.HasPrefix(n, prefix) || token != "" && n <= token ||
			q.Get("startOffset") != "" && n < q.Get("startOffset") || q.Get("endOffset") != "" && n >= q.Get("endOffset") {
			continue
		}
		if delim != "" {
			if i := strings.Index(n[len(prefix):], delim); i >= 0 {
				p := n[:len(prefix)+i+len(delim)]
				if !seen[p] && (b.live(n) != nil || versions) {
					seen[p] = true
					prefixes = append(prefixes, p)
					if q.Get("includeTrailingDelimiter") == "true" && n == p && b.live(n) != nil {
						items = append(items, b.live(n).resource(bucket, r.Host))
					}
				}
				continue
			}
		}
		if len(items) >= maxResults {
			next = n
			break
		}
		if versions {
			for _, o := range b.Objects[n] {
				items = append(items, o.resource(bucket, r.Host))
			}
		} else if o := b.live(n); o != nil {
			items = append(items, o.resource(bucket, r.Host))
		}
	}
	out := map[string]any{"kind": "storage#objects", "items": items}
	if len(prefixes) > 0 {
		out["prefixes"] = prefixes
	}
	if next != "" {
		out["nextPageToken"] = names[sort.SearchStrings(names, next)-1]
	}
	writeJSON(w, 200, out)
	return nil
}

// copy implements copyTo and rewriteTo (rewrites always finish in one call).
func (s *Service) copy(w http.ResponseWriter, r *http.Request, srcBucket, srcName, dstBucket, dstName string, rewrite bool) error {
	q := r.URL.Query()
	s.mu.Lock()
	sb, err := s.bucket(srcBucket)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	src := sb.find(srcName, q.Get("sourceGeneration"))
	if src == nil {
		s.mu.Unlock()
		return notFound("No such object: %s/%s", srcBucket, srcName)
	}
	db, err := s.bucket(dstBucket)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	m := objectMeta{Name: dstName, ContentType: src.ContentType, ContentEncoding: src.ContentEncoding, ContentDisposition: src.ContentDisposition,
		ContentLanguage: src.ContentLanguage, CacheControl: src.CacheControl, Metadata: src.Metadata, StorageClass: src.StorageClass}
	if body := core.Body(r); len(body) > 0 {
		json.Unmarshal(body, &m)
		m.Name = dstName
	}
	o, ev, err := s.store(db, m, src.Data, q)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	res := o.resource(dstBucket, r.Host)
	s.mu.Unlock()
	s.notify(core.C(r).Trace, ev)
	if rewrite {
		size := strconv.Itoa(len(o.Data))
		writeJSON(w, 200, map[string]any{"kind": "storage#rewriteResponse", "totalBytesRewritten": size, "objectSize": size, "done": true, "resource": res})
		return nil
	}
	writeJSON(w, 200, res)
	return nil
}

func (s *Service) compose(w http.ResponseWriter, r *http.Request, bucket, name string) error {
	var in struct {
		SourceObjects []struct {
			Name       string
			Generation string
		}
		Destination *objectMeta
	}
	if err := json.Unmarshal(core.Body(r), &in); err != nil || len(in.SourceObjects) == 0 || len(in.SourceObjects) > 32 {
		return badRequest("compose needs 1 to 32 source objects")
	}
	s.mu.Lock()
	b, err := s.bucket(bucket)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	var data []byte
	for _, so := range in.SourceObjects {
		o := b.find(so.Name, so.Generation)
		if o == nil {
			s.mu.Unlock()
			return notFound("No such object: %s/%s", bucket, so.Name)
		}
		data = append(data, o.Data...)
	}
	m := objectMeta{Name: name}
	if in.Destination != nil {
		m = *in.Destination
		m.Name = name
	}
	o, ev, err := s.store(b, m, data, r.URL.Query())
	if err != nil {
		s.mu.Unlock()
		return err
	}
	o.Components = len(in.SourceObjects)
	res := o.resource(bucket, r.Host)
	s.mu.Unlock()
	s.notify(core.C(r).Trace, ev)
	writeJSON(w, 200, res)
	return nil
}

// ---- XML API (downloads, signed URLs) ----

func xmlError(w http.ResponseWriter, c *core.Ctx, err error) {
	e := core.AsError(err)
	code := map[int]string{404: "NoSuchKey", 412: "PreconditionFailed", 403: "AccessDenied", 416: "InvalidRange"}[e.Status]
	if code == "" {
		code = e.Code
	}
	c.ErrCode = code
	w.Header().Set("Content-Type", "application/xml; charset=UTF-8")
	w.WriteHeader(e.Status)
	fmt.Fprintf(w, `<?xml version='1.0' encoding='UTF-8'?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, e.Msg)
}

func (s *Service) routeXML(w http.ResponseWriter, r *http.Request, c *core.Ctx, seg []string) (string, func() error) {
	if len(seg) < 2 || seg[0] == "" {
		return "", nil
	}
	bucket, name := seg[0], strings.Join(seg[1:], "/")
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		return "xml.getObject", func() error {
			s.mu.RLock()
			b, err := s.bucket(bucket)
			var o Object
			if err == nil {
				if p := b.find(name, r.URL.Query().Get("generation")); p != nil {
					o = *p
				} else {
					err = notFound("The specified key does not exist.")
				}
			}
			s.mu.RUnlock()
			if err != nil {
				return err
			}
			return serveMedia(w, r, &o)
		}
	case http.MethodPut:
		return "xml.putObject", func() error {
			m := objectMeta{Name: name, ContentType: r.Header.Get("Content-Type"), CacheControl: r.Header.Get("Cache-Control"),
				ContentDisposition: r.Header.Get("Content-Disposition"), ContentEncoding: r.Header.Get("Content-Encoding")}
			for k, v := range r.Header {
				if meta, ok := strings.CutPrefix(k, "X-Goog-Meta-"); ok {
					if m.Metadata == nil {
						m.Metadata = map[string]string{}
					}
					m.Metadata[strings.ToLower(meta)] = v[0]
				}
			}
			s.mu.Lock()
			b, err := s.bucket(bucket)
			if err != nil {
				s.mu.Unlock()
				return err
			}
			o, ev, err := s.store(b, m, core.Body(r), url.Values{})
			s.mu.Unlock()
			if err != nil {
				return err
			}
			s.notify(c.Trace, ev)
			w.Header().Set("ETag", `"`+hexMD5(o)+`"`)
			w.Header().Set("X-Goog-Generation", strconv.FormatInt(o.Generation, 10))
			w.WriteHeader(http.StatusOK)
			return nil
		}
	case http.MethodDelete:
		return "xml.deleteObject", func() error { return s.deleteObject(w, r, bucket, name) }
	}
	return "", nil
}

// ---- notifications ----

type pending struct {
	topic string
	msg   *core.Message
}

// eventarcTypes maps GCS event names to the CloudEvent types Eventarc triggers filter on.
var eventarcTypes = map[string]string{"OBJECT_FINALIZE": "finalized", "OBJECT_DELETE": "deleted",
	"OBJECT_ARCHIVE": "archived", "OBJECT_METADATA_UPDATE": "metadataUpdated"}

// events builds the notifications for an object change, Pub/Sub ones and, when
// Eventarc runs, its CloudEvent; lock held.
func (s *Service) events(b *Bucket, o *Object, event string) []pending {
	var out []pending
	if typ, ok := eventarcTypes[event]; ok && s.hub.Service("eventarc") != nil {
		body, _ := json.Marshal(o.resource(b.Name, "localhost:7644"))
		out = append(out, pending{"//eventarc.googleapis.com/projects/" + or(b.Project, "_") + "/events", &core.Message{
			Source: "//storage.googleapis.com/projects/_/buckets/" + b.Name, DetailType: "google.cloud.storage.object.v1." + typ,
			Subject: "objects/" + o.Name, Body: string(body), From: "//storage.googleapis.com/projects/_/buckets/" + b.Name}})
	}
	for _, id := range core.SortedKeys(b.Notifications) {
		n := b.Notifications[id]
		if !strings.HasPrefix(o.Name, n.ObjectNamePrefix) || len(n.EventTypes) > 0 && !contains(n.EventTypes, event) {
			continue
		}
		attrs := map[string]core.MessageAttribute{}
		for k, v := range map[string]string{"notificationConfig": "projects/_/buckets/" + b.Name + "/notificationConfigs/" + id,
			"eventType": event, "payloadFormat": n.PayloadFormat, "bucketId": b.Name, "objectId": o.Name,
			"objectGeneration": strconv.FormatInt(o.Generation, 10), "eventTime": time.Now().UTC().Format(time.RFC3339Nano)} {
			attrs[k] = core.MessageAttribute{DataType: "String", StringValue: v}
		}
		for k, v := range n.CustomAttributes {
			attrs[k] = core.MessageAttribute{DataType: "String", StringValue: v}
		}
		body := ""
		if n.PayloadFormat == "JSON_API_V1" {
			b, _ := json.Marshal(o.resource(b.Name, "localhost:7644"))
			body = string(b)
		}
		out = append(out, pending{n.Topic, &core.Message{Body: body, Attributes: attrs, From: "//storage.googleapis.com/projects/_/buckets/" + b.Name}})
	}
	return out
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func (s *Service) notify(trace string, ev []pending) {
	for _, p := range ev {
		p.msg.Trace = trace
		if err := s.hub.Deliver(p.topic, p.msg); err != nil {
			s.hub.Log.Warn("gcs notification failed", "topic", p.topic, "err", err)
		}
	}
}

// ---- console ----

// Resources lists buckets.
func (s *Service) Resources() []core.Table {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t := core.Table{Name: "Buckets", Columns: []string{"Name", "Project", "Location", "Objects", "Size", "Versioning", "Created"}}
	for _, name := range core.SortedKeys(s.st.Buckets) {
		b := s.st.Buckets[name]
		n, size := 0, 0
		for key := range b.Objects {
			if o := b.live(key); o != nil {
				n++
				size += len(o.Data)
			}
		}
		t.Rows = append(t.Rows, []string{b.Name, b.Project, b.Location, strconv.Itoa(n), strconv.Itoa(size) + " B",
			strconv.FormatBool(b.Versioning), b.Created.Format(time.DateTime)})
	}
	return []core.Table{t}
}

// Inspect returns a bucket's resource and object names.
func (s *Service) Inspect(table, id string) any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b := s.st.Buckets[id]
	if b == nil {
		return nil
	}
	var objects []string
	for _, k := range core.SortedKeys(b.Objects) {
		if b.live(k) != nil {
			objects = append(objects, k)
		}
	}
	return map[string]any{"bucket": b.resource("localhost:7644"), "objects": objects, "notifications": b.Notifications}
}

// Browse lists one "directory" level of a bucket for the console.
func (s *Service) Browse(bucket, prefix string) ([]string, []core.ObjectInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := s.bucket(bucket)
	if err != nil {
		return nil, nil, err
	}
	var dirs []string
	var objs []core.ObjectInfo
	seen := map[string]bool{}
	for _, k := range core.SortedKeys(b.Objects) {
		o := b.live(k)
		if o == nil || !strings.HasPrefix(k, prefix) {
			continue
		}
		if i := strings.Index(k[len(prefix):], "/"); i >= 0 {
			if d := k[:len(prefix)+i+1]; !seen[d] {
				seen[d] = true
				dirs = append(dirs, d)
			}
			continue
		}
		objs = append(objs, core.ObjectInfo{Key: k, Size: len(o.Data), Modified: o.Updated, ContentType: o.ContentType})
	}
	return dirs, objs, nil
}

// Download returns an object's bytes and content type (console).
func (s *Service) Download(bucket, name string) ([]byte, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := s.bucket(bucket)
	if err != nil {
		return nil, "", err
	}
	o := b.live(name)
	if o == nil {
		return nil, "", notFound("No such object: %s/%s", bucket, name)
	}
	return o.Data, o.ContentType, nil
}

// Upload stores an object (console).
func (s *Service) Upload(bucket, name, contentType string, data []byte) error {
	s.mu.Lock()
	b, err := s.bucket(bucket)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	_, ev, err := s.store(b, objectMeta{Name: name, ContentType: contentType}, data, url.Values{})
	s.mu.Unlock()
	s.notify(core.NewTrace(), ev)
	return err
}

// Remove deletes the live generation of an object (console).
func (s *Service) Remove(bucket, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucket(bucket)
	if err != nil {
		return err
	}
	delete(b.Objects, name)
	return nil
}

// MakeBucket creates a bucket in the console's default project.
func (s *Service) MakeBucket(name string) error {
	if !bucketName.MatchString(name) {
		return badRequest("Invalid bucket name: '%s'", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Buckets[name] != nil {
		return core.Errorf(409, "conflict", "bucket %s already exists", name)
	}
	now := time.Now().UTC()
	s.st.Buckets[name] = &Bucket{Name: name, Project: "pogi-local", Location: "US", StorageClass: "STANDARD", Created: now, Updated: now,
		Metageneration: 1, Objects: map[string][]*Object{}}
	return nil
}

// DropBucket deletes a bucket and its objects (console).
func (s *Service) DropBucket(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.bucket(name); err != nil {
		return err
	}
	delete(s.st.Buckets, name)
	return nil
}
