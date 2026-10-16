// Package s3vectors emulates Amazon S3 Vectors (REST-JSON): vector buckets
// with their policies, indexes, and vectors with exact nearest-neighbour
// queries and metadata filters.
package s3vectors

import (
	"cmp"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"maps"
	"math"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the S3 Vectors emulator.
type Service struct {
	hub *core.Hub
	mux *http.ServeMux
	mu  sync.Mutex
	st  state
}

type state struct {
	Buckets map[string]*Bucket `json:"buckets"` // ARN
}

// Bucket is a vector bucket.
type Bucket struct {
	Name       string            `json:"name"`
	ARN        string            `json:"arn"`
	Account    string            `json:"account"`
	Region     string            `json:"region"`
	Created    time.Time         `json:"created"`
	Encryption *encryption       `json:"encryption"`
	Policy     string            `json:"policy,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
	Indexes    map[string]*Index `json:"indexes"`
}

// Index is a vector index. Queries compare the query vector with every
// vector in it.
type Index struct {
	Name          string             `json:"name"`
	Bucket        string             `json:"bucket"`
	ARN           string             `json:"arn"`
	Created       time.Time          `json:"created"`
	Dimension     int                `json:"dimension"`
	Metric        string             `json:"metric"` // cosine | euclidean
	NonFilterable []string           `json:"nonFilterable,omitempty"`
	Encryption    *encryption        `json:"encryption"`
	Tags          map[string]string  `json:"tags,omitempty"`
	Vectors       map[string]*vector `json:"vectors"`
}

type vector struct {
	Data     []float32      `json:"data"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type encryption struct {
	SseType   string `json:"sseType,omitempty"`
	KmsKeyArn string `json:"kmsKeyArn,omitempty"`
}

// input is the body of every operation.
type input struct {
	VectorBucketName, VectorBucketArn, IndexName, IndexArn string
	Policy, Prefix, NextToken, DataType, DistanceMetric    string
	Dimension, MaxResults, SegmentCount, SegmentIndex      *int
	TopK                                                   *int
	MetadataConfiguration                                  *struct{ NonFilterableMetadataKeys []string }
	EncryptionConfiguration                                *encryption
	Tags                                                   map[string]string
	Vectors                                                []struct {
		Key      string
		Data     struct{ Float32 []float32 }
		Metadata any
	}
	Keys                                       []string
	ReturnData, ReturnMetadata, ReturnDistance bool
	QueryVector                                struct{ Float32 []float32 }
	Filter                                     any
}

type handler func(*core.Ctx, *input, *http.Request) (any, error)

// New creates the S3 Vectors service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux()}
	s.Reset()
	for pattern, h := range map[string]http.HandlerFunc{
		"POST /CreateVectorBucket":       handle("CreateVectorBucket", s.createBucket),
		"POST /GetVectorBucket":          handle("GetVectorBucket", s.getBucket),
		"POST /ListVectorBuckets":        handle("ListVectorBuckets", s.listBuckets),
		"POST /DeleteVectorBucket":       handle("DeleteVectorBucket", s.deleteBucket),
		"POST /PutVectorBucketPolicy":    handle("PutVectorBucketPolicy", s.policy),
		"POST /GetVectorBucketPolicy":    handle("GetVectorBucketPolicy", s.policy),
		"POST /DeleteVectorBucketPolicy": handle("DeleteVectorBucketPolicy", s.policy),
		"POST /CreateIndex":              handle("CreateIndex", s.createIndex),
		"POST /GetIndex":                 handle("GetIndex", s.getIndex),
		"POST /ListIndexes":              handle("ListIndexes", s.listIndexes),
		"POST /DeleteIndex":              handle("DeleteIndex", s.deleteIndex),
		"POST /PutVectors":               handle("PutVectors", s.putVectors),
		"POST /GetVectors":               handle("GetVectors", s.getVectors),
		"POST /ListVectors":              handle("ListVectors", s.listVectors),
		"POST /DeleteVectors":            handle("DeleteVectors", s.deleteVectors),
		"POST /QueryVectors":             handle("QueryVectors", s.queryVectors),
		"GET /tags/{arn...}":             handle("ListTagsForResource", s.tags),
		"POST /tags/{arn...}":            handle("TagResource", s.tags),
		"DELETE /tags/{arn...}":          handle("UntagResource", s.tags),
		"/": handle("Unknown", func(*core.Ctx, *input, *http.Request) (any, error) {
			return nil, core.Errorf(404, "UnknownOperationException", "pogi does not implement this S3 Vectors request")
		}),
	} {
		s.mux.HandleFunc(pattern, h)
	}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "s3vectors", Cloud: core.AWS, Title: "S3 Vectors", Signing: []string{"s3vectors"}, Protocol: "rest-json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Buckets: map[string]*Bucket{}}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := state{Buckets: map[string]*Bucket{}}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func handle(op string, fn handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := core.C(r)
		c.Op = op
		var in input
		err := c.Intercept()
		if b := core.Body(r); err == nil && len(b) > 0 && json.Unmarshal(b, &in) != nil {
			err = invalid("The request body is not valid JSON for %s.", op)
		}
		var out any
		if err == nil {
			out, err = fn(c, &in, r)
		}
		if err != nil {
			core.RESTJSONError(w, c, err)
			return
		}
		if out == nil {
			out = struct{}{}
		}
		core.RESTJSON(w, c, http.StatusOK, out)
	}
}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}

func notFound(what string) error {
	return core.Errorf(404, "NotFoundException", "The specified %s could not be found", what)
}

var (
	bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)
	indexName  = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
)

// encrypt checks an encryption configuration; none means SSE-S3, or def.
func encrypt(e, def *encryption) (*encryption, error) {
	switch {
	case e == nil || e.SseType == "" && e.KmsKeyArn == "":
		return cmp.Or(def, &encryption{SseType: "AES256"}), nil
	case e.SseType == "AES256" && e.KmsKeyArn == "", e.SseType == "aws:kms" && e.KmsKeyArn != "":
		return e, nil
	}
	return nil, invalid("Encryption is AES256, or aws:kms with a kmsKeyArn.")
}

// page sorts names and returns at most limit of those after token, with
// the token of the next page.
func page(names []string, token string, limit int) ([]string, string, error) {
	after, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, "", invalid("The nextToken is not valid.")
	}
	slices.Sort(names)
	if token != "" {
		i, found := slices.BinarySearch(names, string(after))
		if found {
			i++
		}
		names = names[i:]
	}
	if len(names) <= limit {
		return names, "", nil
	}
	return names[:limit], base64.RawURLEncoding.EncodeToString([]byte(names[limit-1])), nil
}

// paged is a list output; the last page has no nextToken.
func paged(name string, items []any, next string) map[string]any {
	out := map[string]any{name: items}
	if next != "" {
		out["nextToken"] = next
	}
	return out
}

// limit reads maxResults: 1 to hi, def when absent.
func limit(n *int, def, hi int) (int, error) {
	if n == nil {
		return def, nil
	}
	if *n < 1 || *n > hi {
		return 0, invalid("maxResults must be between 1 and %d.", hi)
	}
	return *n, nil
}

// ---- vector buckets ----

// bucket finds the vector bucket an input names; lock held.
func (s *Service) bucket(c *core.Ctx, in *input) (*Bucket, error) {
	arn := in.VectorBucketArn
	if arn == "" {
		if in.VectorBucketName == "" {
			return nil, invalid("Specify vectorBucketName or vectorBucketArn.")
		}
		arn = c.ARN("s3vectors", "bucket/"+in.VectorBucketName)
	}
	if b := s.st.Buckets[arn]; b != nil {
		return b, nil
	}
	return nil, notFound("vector bucket")
}

func (b *Bucket) out(full bool) map[string]any {
	m := map[string]any{"vectorBucketName": b.Name, "vectorBucketArn": b.ARN, "creationTime": core.Time(b.Created)}
	if full {
		m["encryptionConfiguration"] = b.Encryption
	}
	return m
}

func (s *Service) createBucket(c *core.Ctx, in *input, _ *http.Request) (any, error) {
	if !bucketName.MatchString(in.VectorBucketName) {
		return nil, invalid("Vector bucket names have 3 to 63 lowercase letters, digits and hyphens: %q", in.VectorBucketName)
	}
	enc, err := encrypt(in.EncryptionConfiguration, nil)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	arn := c.ARN("s3vectors", "bucket/"+in.VectorBucketName)
	if s.st.Buckets[arn] != nil {
		return nil, core.Errorf(409, "ConflictException", "A vector bucket named %s already exists", in.VectorBucketName)
	}
	s.st.Buckets[arn] = &Bucket{Name: in.VectorBucketName, ARN: arn, Account: c.Account, Region: c.Region, Created: time.Now(),
		Encryption: enc, Tags: in.Tags, Indexes: map[string]*Index{}}
	return map[string]string{"vectorBucketArn": arn}, nil
}

func (s *Service) getBucket(c *core.Ctx, in *input, _ *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucket(c, in)
	if err != nil {
		return nil, err
	}
	return map[string]any{"vectorBucket": b.out(true)}, nil
}

func (s *Service) listBuckets(c *core.Ctx, in *input, _ *http.Request) (any, error) {
	n, err := limit(in.MaxResults, 500, 500)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	byName := map[string]*Bucket{}
	for _, b := range s.st.Buckets {
		if b.Account == c.Account && b.Region == c.Region && strings.HasPrefix(b.Name, in.Prefix) {
			byName[b.Name] = b
		}
	}
	names, next, err := page(slices.Collect(maps.Keys(byName)), in.NextToken, n)
	if err != nil {
		return nil, err
	}
	out := []any{}
	for _, name := range names {
		out = append(out, byName[name].out(false))
	}
	return paged("vectorBuckets", out, next), nil
}

func (s *Service) deleteBucket(c *core.Ctx, in *input, _ *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucket(c, in)
	if err != nil {
		return nil, err
	}
	if len(b.Indexes) > 0 {
		return nil, core.Errorf(409, "ConflictException", "The vector bucket %s is not empty: delete its indexes first", b.Name)
	}
	delete(s.st.Buckets, b.ARN)
	return nil, nil
}

// policy serves PutVectorBucketPolicy, GetVectorBucketPolicy and DeleteVectorBucketPolicy.
// ponytail: bucket policies are stored, not enforced.
func (s *Service) policy(c *core.Ctx, in *input, _ *http.Request) (any, error) {
	if c.Op == "PutVectorBucketPolicy" && !json.Valid([]byte(in.Policy)) {
		return nil, invalid("The policy is not valid JSON.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucket(c, in)
	if err != nil {
		return nil, err
	}
	switch c.Op {
	case "PutVectorBucketPolicy":
		b.Policy = in.Policy
	case "DeleteVectorBucketPolicy":
		b.Policy = ""
	default:
		if b.Policy == "" {
			return nil, notFound("vector bucket policy")
		}
		return map[string]string{"policy": b.Policy}, nil
	}
	return nil, nil
}

// ---- indexes ----

// index finds the index an input names; lock held.
func (s *Service) index(c *core.Ctx, in *input) (*Index, error) {
	if in.IndexArn != "" {
		bucket, name, _ := strings.Cut(in.IndexArn, "/index/")
		if b := s.st.Buckets[bucket]; b != nil && b.Indexes[name] != nil {
			return b.Indexes[name], nil
		}
		return nil, notFound("index")
	}
	if in.IndexName == "" {
		return nil, invalid("Specify indexName or indexArn.")
	}
	b, err := s.bucket(c, in)
	if err != nil {
		return nil, err
	}
	if x := b.Indexes[in.IndexName]; x != nil {
		return x, nil
	}
	return nil, notFound("index")
}

func (x *Index) out(full bool) map[string]any {
	m := map[string]any{"vectorBucketName": x.Bucket, "indexName": x.Name, "indexArn": x.ARN, "creationTime": core.Time(x.Created)}
	if full {
		m["dataType"], m["dimension"], m["distanceMetric"], m["encryptionConfiguration"] = "float32", x.Dimension, x.Metric, x.Encryption
		if len(x.NonFilterable) > 0 {
			m["metadataConfiguration"] = map[string]any{"nonFilterableMetadataKeys": x.NonFilterable}
		}
	}
	return m
}

func (s *Service) createIndex(c *core.Ctx, in *input, _ *http.Request) (any, error) {
	switch {
	case !indexName.MatchString(in.IndexName):
		return nil, invalid("Index names have 3 to 63 lowercase letters, digits, hyphens and dots: %q", in.IndexName)
	case in.DataType != "float32":
		return nil, invalid("dataType must be float32.")
	case in.Dimension == nil || *in.Dimension < 1 || *in.Dimension > 4096:
		return nil, invalid("dimension must be between 1 and 4096.")
	case in.DistanceMetric != "cosine" && in.DistanceMetric != "euclidean":
		return nil, invalid("distanceMetric must be cosine or euclidean.")
	}
	var nonFilterable []string
	if mc := in.MetadataConfiguration; mc != nil {
		if nonFilterable = mc.NonFilterableMetadataKeys; len(nonFilterable) < 1 || len(nonFilterable) > 10 {
			return nil, invalid("nonFilterableMetadataKeys takes 1 to 10 keys.")
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucket(c, in)
	if err != nil {
		return nil, err
	}
	enc, err := encrypt(in.EncryptionConfiguration, b.Encryption)
	if err != nil {
		return nil, err
	}
	if b.Indexes[in.IndexName] != nil {
		return nil, core.Errorf(409, "ConflictException", "An index named %s already exists in %s", in.IndexName, b.Name)
	}
	x := &Index{Name: in.IndexName, Bucket: b.Name, ARN: b.ARN + "/index/" + in.IndexName, Created: time.Now(), Dimension: *in.Dimension,
		Metric: in.DistanceMetric, NonFilterable: nonFilterable, Encryption: enc, Tags: in.Tags, Vectors: map[string]*vector{}}
	b.Indexes[x.Name] = x
	return map[string]string{"indexArn": x.ARN}, nil
}

func (s *Service) getIndex(c *core.Ctx, in *input, _ *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	x, err := s.index(c, in)
	if err != nil {
		return nil, err
	}
	return map[string]any{"index": x.out(true)}, nil
}

func (s *Service) listIndexes(c *core.Ctx, in *input, _ *http.Request) (any, error) {
	n, err := limit(in.MaxResults, 500, 500)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucket(c, in)
	if err != nil {
		return nil, err
	}
	var names []string
	for name := range b.Indexes {
		if strings.HasPrefix(name, in.Prefix) {
			names = append(names, name)
		}
	}
	names, next, err := page(names, in.NextToken, n)
	if err != nil {
		return nil, err
	}
	out := []any{}
	for _, name := range names {
		out = append(out, b.Indexes[name].out(false))
	}
	return paged("indexes", out, next), nil
}

func (s *Service) deleteIndex(c *core.Ctx, in *input, _ *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	x, err := s.index(c, in)
	if err != nil {
		return nil, err
	}
	b := s.st.Buckets[strings.TrimSuffix(x.ARN, "/index/"+x.Name)]
	delete(b.Indexes, x.Name)
	return nil, nil
}

// ---- vectors ----

func (v *vector) out(key string, data, metadata bool) map[string]any {
	m := map[string]any{"key": key}
	if data {
		m["data"] = map[string]any{"float32": v.Data}
	}
	if metadata && v.Metadata != nil {
		m["metadata"] = v.Metadata
	}
	return m
}

// filterable is the metadata filters see: all but the index's non-filterable keys.
func filterable(md map[string]any, skip []string) map[string]any {
	if len(skip) == 0 {
		return md
	}
	md = maps.Clone(md)
	for _, k := range skip {
		delete(md, k)
	}
	return md
}

func norm(v []float32) (n float64) {
	for _, f := range v {
		n += float64(f) * float64(f)
	}
	return math.Sqrt(n)
}

// putVectors writes (or overwrites) up to 500 vectors, all or none.
func (s *Service) putVectors(c *core.Ctx, in *input, _ *http.Request) (any, error) {
	if len(in.Vectors) == 0 || len(in.Vectors) > 500 {
		return nil, invalid("PutVectors takes 1 to 500 vectors.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	x, err := s.index(c, in)
	if err != nil {
		return nil, err
	}
	staged := map[string]*vector{}
	for i, v := range in.Vectors {
		md, isMap := v.Metadata.(map[string]any)
		all, _ := json.Marshal(md)
		some, _ := json.Marshal(filterable(md, x.NonFilterable))
		switch {
		case len(v.Key) == 0 || len(v.Key) > 1024:
			return nil, invalid("vectors[%d].key must have 1 to 1024 characters.", i)
		case len(v.Data.Float32) != x.Dimension:
			return nil, invalid("Invalid record for key '%s': vector must have length %d, but has length %d", v.Key, x.Dimension, len(v.Data.Float32))
		case x.Metric == "cosine" && norm(v.Data.Float32) == 0:
			return nil, invalid("Invalid record for key '%s': a zero vector has no cosine distance", v.Key)
		case v.Metadata != nil && !isMap:
			return nil, invalid("Invalid record for key '%s': metadata must be an object", v.Key)
		case len(all) > 40<<10 || len(some) > 2<<10:
			return nil, invalid("Invalid record for key '%s': metadata is limited to 40 KB, of which 2 KB filterable", v.Key)
		}
		staged[v.Key] = &vector{Data: v.Data.Float32, Metadata: md}
	}
	maps.Copy(x.Vectors, staged)
	return nil, nil
}

func (s *Service) getVectors(c *core.Ctx, in *input, _ *http.Request) (any, error) {
	if len(in.Keys) == 0 || len(in.Keys) > 100 {
		return nil, invalid("GetVectors takes 1 to 100 keys.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	x, err := s.index(c, in)
	if err != nil {
		return nil, err
	}
	out := []any{}
	for _, k := range in.Keys {
		if v := x.Vectors[k]; v != nil {
			out = append(out, v.out(k, in.ReturnData, in.ReturnMetadata))
		}
	}
	return map[string]any{"vectors": out}, nil
}

// listVectors pages through the vectors of one segment (segments split the
// keys by hash, for parallel listing).
func (s *Service) listVectors(c *core.Ctx, in *input, _ *http.Request) (any, error) {
	n, err := limit(in.MaxResults, 500, 1000)
	if err != nil {
		return nil, err
	}
	count, seg := 1, 0
	if in.SegmentCount != nil {
		count = *in.SegmentCount
	}
	if in.SegmentIndex != nil {
		seg = *in.SegmentIndex
	}
	if count < 1 || count > 16 || seg < 0 || seg >= count {
		return nil, invalid("segmentCount must be between 1 and 16, and segmentIndex less than it.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	x, err := s.index(c, in)
	if err != nil {
		return nil, err
	}
	var keys []string
	for k := range x.Vectors {
		h := fnv.New32a()
		h.Write([]byte(k))
		if int(h.Sum32()%uint32(count)) == seg {
			keys = append(keys, k)
		}
	}
	keys, next, err := page(keys, in.NextToken, n)
	if err != nil {
		return nil, err
	}
	out := []any{}
	for _, k := range keys {
		out = append(out, x.Vectors[k].out(k, in.ReturnData, in.ReturnMetadata))
	}
	return paged("vectors", out, next), nil
}

func (s *Service) deleteVectors(c *core.Ctx, in *input, _ *http.Request) (any, error) {
	if len(in.Keys) == 0 || len(in.Keys) > 500 {
		return nil, invalid("DeleteVectors takes 1 to 500 keys.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	x, err := s.index(c, in)
	if err != nil {
		return nil, err
	}
	for _, k := range in.Keys {
		delete(x.Vectors, k)
	}
	return nil, nil
}

// distance is the cosine distance (1 - cosine similarity) or the Euclidean distance.
func distance(metric string, a, b []float32) float64 {
	var dot, sq float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot, sq = dot+x*y, sq+(x-y)*(x-y)
	}
	if metric == "cosine" {
		return 1 - dot/(norm(a)*norm(b))
	}
	return math.Sqrt(sq)
}

// queryVectors returns the topK nearest vectors that pass the filter.
// ponytail: exact k-NN over every vector, under the service lock; an ANN
// index if anyone keeps millions of vectors here.
func (s *Service) queryVectors(c *core.Ctx, in *input, _ *http.Request) (any, error) {
	if in.TopK == nil || *in.TopK < 1 || *in.TopK > 100 {
		return nil, invalid("topK must be between 1 and 100.")
	}
	var filter map[string]any
	if in.Filter != nil {
		f, ok := in.Filter.(map[string]any)
		if !ok {
			return nil, invalid("The filter must be an object.")
		}
		if _, err := match(f, nil); err != nil {
			return nil, invalid("Invalid filter: %v", err)
		}
		filter = f
	}
	q := in.QueryVector.Float32
	s.mu.Lock()
	defer s.mu.Unlock()
	x, err := s.index(c, in)
	if err != nil {
		return nil, err
	}
	if len(q) != x.Dimension {
		return nil, invalid("The query vector must have length %d, but has length %d.", x.Dimension, len(q))
	}
	if x.Metric == "cosine" && norm(q) == 0 {
		return nil, invalid("A zero query vector has no cosine distance.")
	}
	type hit struct {
		key string
		d   float64
	}
	var hits []hit
	for k, v := range x.Vectors {
		if ok, _ := match(filter, filterable(v.Metadata, x.NonFilterable)); ok {
			hits = append(hits, hit{k, distance(x.Metric, q, v.Data)})
		}
	}
	slices.SortFunc(hits, func(a, b hit) int { return cmp.Or(cmp.Compare(a.d, b.d), strings.Compare(a.key, b.key)) })
	out := []any{}
	for _, h := range hits[:min(*in.TopK, len(hits))] {
		m := x.Vectors[h.key].out(h.key, false, in.ReturnMetadata)
		if in.ReturnDistance {
			m["distance"] = float32(h.d)
		}
		out = append(out, m)
	}
	return map[string]any{"vectors": out, "distanceMetric": x.Metric}, nil
}

// match evaluates the metadata filter language: a field equals a value or
// meets operators ($eq $ne $gt $gte $lt $lte $in $nin $exists), $and and
// $or combine filters, and several fields must all hold. Every clause is
// evaluated, so a malformed filter fails on any metadata, even none.
func match(f, md map[string]any) (bool, error) {
	ok := true
	for k, cond := range f {
		var r bool
		switch k {
		case "$and", "$or":
			list, isList := cond.([]any)
			if !isList || len(list) == 0 {
				return false, fmt.Errorf("%s takes a non-empty array of filters", k)
			}
			r = k == "$and"
			for _, sub := range list {
				sf, isMap := sub.(map[string]any)
				if !isMap {
					return false, fmt.Errorf("%s takes an array of filter objects", k)
				}
				m, err := match(sf, md)
				if err != nil {
					return false, err
				}
				if k == "$and" {
					r = r && m
				} else {
					r = r || m
				}
			}
		default:
			if strings.HasPrefix(k, "$") {
				return false, fmt.Errorf("unsupported operator %s", k)
			}
			v, present := md[k]
			var err error
			if r, err = test(v, present, cond); err != nil {
				return false, err
			}
		}
		ok = ok && r
	}
	return ok, nil
}

// test checks one field's condition: a value (equality) or an operator object.
func test(v any, present bool, cond any) (bool, error) {
	ops, isOps := cond.(map[string]any)
	if !isOps {
		ops = map[string]any{"$eq": cond}
	}
	if len(ops) == 0 {
		return false, fmt.Errorf("a condition needs an operator")
	}
	ok := true
	for op, arg := range ops {
		var r bool
		switch op {
		case "$eq", "$ne":
			if !scalar(arg) {
				return false, fmt.Errorf("%s takes a string, number or boolean", op)
			}
			r = (present && eq(v, arg)) == (op == "$eq")
		case "$gt", "$gte", "$lt", "$lte":
			n, isNum := arg.(float64)
			if !isNum {
				return false, fmt.Errorf("%s takes a number", op)
			}
			x, isX := v.(float64)
			switch op {
			case "$gt":
				r = x > n
			case "$gte":
				r = x >= n
			case "$lt":
				r = x < n
			default:
				r = x <= n
			}
			r = r && present && isX
		case "$in", "$nin":
			list, isList := arg.([]any)
			if !isList || slices.ContainsFunc(list, func(a any) bool { return !scalar(a) }) {
				return false, fmt.Errorf("%s takes an array of strings, numbers or booleans", op)
			}
			r = (present && slices.ContainsFunc(list, func(a any) bool { return eq(v, a) })) == (op == "$in")
		case "$exists":
			b, isBool := arg.(bool)
			if !isBool {
				return false, fmt.Errorf("$exists takes a boolean")
			}
			r = present == b
		default:
			return false, fmt.Errorf("unsupported operator %s", op)
		}
		ok = ok && r
	}
	return ok, nil
}

func scalar(v any) bool {
	switch v.(type) {
	case string, float64, bool:
		return true
	}
	return false
}

// eq compares a metadata value with a filter value; an array matches when one of its elements does.
func eq(v, x any) bool {
	if list, ok := v.([]any); ok {
		return slices.ContainsFunc(list, func(e any) bool { return eq(e, x) })
	}
	return scalar(v) && v == x
}

// ---- tags ----

// tags serves ListTagsForResource, TagResource and UntagResource for buckets and indexes.
func (s *Service) tags(c *core.Ctx, in *input, r *http.Request) (any, error) {
	arn := r.PathValue("arn")
	s.mu.Lock()
	defer s.mu.Unlock()
	bucket, index, isIndex := strings.Cut(arn, "/index/")
	var tags *map[string]string
	if b := s.st.Buckets[bucket]; b != nil && !isIndex {
		tags = &b.Tags
	} else if b != nil && b.Indexes[index] != nil {
		tags = &b.Indexes[index].Tags
	} else {
		return nil, notFound("resource " + arn)
	}
	if *tags == nil {
		*tags = map[string]string{}
	}
	switch c.Op {
	case "TagResource":
		maps.Copy(*tags, in.Tags)
	case "UntagResource":
		for _, k := range r.URL.Query()["tagKeys"] {
			delete(*tags, k)
		}
	default:
		return map[string]any{"tags": *tags}, nil
	}
	return nil, nil
}

// ---- console ----

// Resources lists vector buckets and indexes.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	bt := core.Table{Name: "Vector buckets", Columns: []string{"ARN", "Name", "Indexes", "Region", "Account"}}
	it := core.Table{Name: "Indexes", Columns: []string{"ARN", "Bucket", "Index", "Dimension", "Metric", "Vectors"}}
	for _, arn := range core.SortedKeys(s.st.Buckets) {
		b := s.st.Buckets[arn]
		bt.Rows = append(bt.Rows, []string{b.ARN, b.Name, strconv.Itoa(len(b.Indexes)), b.Region, b.Account})
		for _, name := range core.SortedKeys(b.Indexes) {
			x := b.Indexes[name]
			it.Rows = append(it.Rows, []string{x.ARN, b.Name, x.Name, strconv.Itoa(x.Dimension), x.Metric, strconv.Itoa(len(x.Vectors))})
		}
	}
	return []core.Table{bt, it}
}

// Inspect returns a bucket (with its policy and indexes) or an index (with
// its vector count).
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	bucket, index, isIndex := strings.Cut(id, "/index/")
	b := s.st.Buckets[bucket]
	switch {
	case b == nil:
	case table == "Vector buckets" && !isIndex:
		m := b.out(true)
		m["policy"], m["tags"], m["indexes"] = b.Policy, b.Tags, core.SortedKeys(b.Indexes)
		return m
	case table == "Indexes" && b.Indexes[index] != nil:
		x := b.Indexes[index]
		m := x.out(true)
		m["tags"], m["vectors"] = x.Tags, len(x.Vectors)
		return m
	}
	return nil
}
