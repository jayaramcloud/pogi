package s3vectors_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/document"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/types"
	"github.com/aws/smithy-go"
	svc "github.com/nrndev/pogi/internal/aws/s3vectors"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func code(err error) string {
	var e smithy.APIError
	if errors.As(err, &e) {
		return e.ErrorCode()
	}
	return fmt.Sprint(err)
}

const dim = 8

var genres = []string{"drama", "comedy", "news"}

// dist is the brute-force distance the queries must agree with.
func dist(metric types.DistanceMetric, a, b []float32) float64 {
	var dot, na, nb, sq float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot, na, nb, sq = dot+x*y, na+x*x, nb+y*y, sq+(x-y)*(x-y)
	}
	if metric == types.DistanceMetricCosine {
		return 1 - dot/math.Sqrt(na*nb)
	}
	return math.Sqrt(sq)
}

func random(r *rand.Rand) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = r.Float32()*2 - 1
	}
	return v
}

// setup makes bucket "vecs" with a cosine and a euclidean index, each
// holding the same 100 random vectors with metadata.
func setup(t *testing.T) (*core.Hub, *s3vectors.Client, map[string][]float32) {
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { return svc.New(h) })
	c := s3vectors.NewFromConfig(awstest.Config(t, url))
	must(c.CreateVectorBucket(ctx, &s3vectors.CreateVectorBucketInput{VectorBucketName: aws.String("vecs")}))
	r := rand.New(rand.NewPCG(1, 2))
	vecs := map[string][]float32{}
	var put []types.PutInputVector
	for i := range 100 {
		key := fmt.Sprintf("v%03d", i)
		vecs[key] = random(r)
		put = append(put, types.PutInputVector{Key: aws.String(key), Data: &types.VectorDataMemberFloat32{Value: vecs[key]},
			Metadata: document.NewLazyDocument(map[string]any{"genre": genres[i%3], "year": 2000 + i%20, "even": i%2 == 0, "tags": []string{"t" + fmt.Sprint(i%4)}, "note": "n"})})
	}
	for _, m := range []types.DistanceMetric{types.DistanceMetricCosine, types.DistanceMetricEuclidean} {
		must(c.CreateIndex(ctx, &s3vectors.CreateIndexInput{VectorBucketName: aws.String("vecs"), IndexName: aws.String(string(m)), DataType: types.DataTypeFloat32,
			Dimension: aws.Int32(dim), DistanceMetric: m, MetadataConfiguration: &types.MetadataConfiguration{NonFilterableMetadataKeys: []string{"note"}}}))
		must(c.PutVectors(ctx, &s3vectors.PutVectorsInput{VectorBucketName: aws.String("vecs"), IndexName: aws.String(string(m)), Vectors: put}))
	}
	return hub, c, vecs
}

func query(c *s3vectors.Client, index string, q []float32, k int32, filter any) *s3vectors.QueryVectorsOutput {
	in := &s3vectors.QueryVectorsInput{VectorBucketName: aws.String("vecs"), IndexName: aws.String(index), QueryVector: &types.VectorDataMemberFloat32{Value: q},
		TopK: aws.Int32(k), ReturnDistance: true, ReturnMetadata: true}
	if filter != nil {
		in.Filter = document.NewLazyDocument(filter)
	}
	return must(c.QueryVectors(ctx, in))
}

func TestQueryMatchesBruteForce(t *testing.T) {
	hub, c, vecs := setup(t)
	r := rand.New(rand.NewPCG(3, 4))
	for _, m := range []types.DistanceMetric{types.DistanceMetricCosine, types.DistanceMetricEuclidean} {
		for range 3 {
			q := random(r)
			keys := slices.SortedFunc(maps.Keys(vecs), func(a, b string) int { return cmp.Compare(dist(m, q, vecs[a]), dist(m, q, vecs[b])) })
			out := query(c, string(m), q, 5, nil)
			if out.DistanceMetric != m || len(out.Vectors) != 5 {
				t.Fatalf("%s: %s, %d results", m, out.DistanceMetric, len(out.Vectors))
			}
			for i, v := range out.Vectors {
				if want := dist(m, q, vecs[keys[i]]); *v.Key != keys[i] || math.Abs(float64(*v.Distance)-want) > 1e-5 {
					t.Fatalf("%s #%d: got %s at %v, want %s at %v", m, i, *v.Key, *v.Distance, keys[i], want)
				}
			}
		}
	}

	b, err := hub.Snapshot() // vectors survive Save and Load
	if err != nil {
		t.Fatal(err)
	}
	hub.Reset()
	if err := hub.Restore(b); err != nil {
		t.Fatal(err)
	}
	if out := query(c, "euclidean", vecs["v042"], 1, nil); *out.Vectors[0].Key != "v042" || *out.Vectors[0].Distance != 0 {
		t.Fatalf("after restore: %s %v", *out.Vectors[0].Key, *out.Vectors[0].Distance)
	}
}

func TestFilters(t *testing.T) {
	_, c, vecs := setup(t)
	q := vecs["v000"]
	for _, tc := range []struct {
		filter any
		want   func(i int) bool
	}{
		{map[string]any{"genre": "drama"}, func(i int) bool { return i%3 == 0 }},
		{map[string]any{"year": map[string]any{"$gte": 2015}}, func(i int) bool { return i%20 >= 15 }},
		{map[string]any{"year": map[string]any{"$gt": 2003, "$lte": 2005}}, func(i int) bool { return i%20 > 3 && i%20 <= 5 }},
		{map[string]any{"$and": []any{map[string]any{"genre": map[string]any{"$in": []any{"drama", "news"}}}, map[string]any{"even": true}}},
			func(i int) bool { return i%3 != 1 && i%2 == 0 }},
		{map[string]any{"$or": []any{map[string]any{"genre": "news"}, map[string]any{"year": map[string]any{"$lt": 2002}}}},
			func(i int) bool { return i%3 == 2 || i%20 < 2 }},
		{map[string]any{"genre": map[string]any{"$nin": []any{"drama"}}, "year": map[string]any{"$ne": 2001}},
			func(i int) bool { return i%3 != 0 && i%20 != 1 }},
		{map[string]any{"tags": "t1"}, func(i int) bool { return i%4 == 1 }}, // an array matches by element
		{map[string]any{"missing": map[string]any{"$exists": false}, "genre": map[string]any{"$exists": true}}, func(int) bool { return true }},
		{map[string]any{"note": "n"}, func(int) bool { return false }}, // not filterable
	} {
		n := 0
		for i := range 100 {
			if tc.want(i) {
				n++
			}
		}
		out := query(c, "cosine", q, 100, tc.filter)
		if len(out.Vectors) != n {
			t.Fatalf("filter %v: %d results, want %d", tc.filter, len(out.Vectors), n)
		}
		for _, v := range out.Vectors {
			var i int
			fmt.Sscanf(*v.Key, "v%d", &i)
			var md map[string]any
			if err := v.Metadata.UnmarshalSmithyDocument(&md); err != nil || !tc.want(i) || md["genre"] != genres[i%3] {
				t.Fatalf("filter %v returned %s %v (%v)", tc.filter, *v.Key, md, err)
			}
		}
	}
	for _, bad := range []any{
		map[string]any{"genre": map[string]any{"$regex": "dr"}},
		map[string]any{"$and": map[string]any{"genre": "drama"}},
		map[string]any{"year": map[string]any{"$gt": "2001"}},
		map[string]any{"genre": map[string]any{"$in": "drama"}},
	} {
		in := &s3vectors.QueryVectorsInput{VectorBucketName: aws.String("vecs"), IndexName: aws.String("cosine"),
			QueryVector: &types.VectorDataMemberFloat32{Value: q}, TopK: aws.Int32(3), Filter: document.NewLazyDocument(bad)}
		if _, err := c.QueryVectors(ctx, in); code(err) != "ValidationException" {
			t.Fatalf("filter %v: %v", bad, err)
		}
	}
}

func TestVectors(t *testing.T) {
	_, c, vecs := setup(t)
	b, x := aws.String("vecs"), aws.String("cosine")
	got := must(c.GetVectors(ctx, &s3vectors.GetVectorsInput{VectorBucketName: b, IndexName: x, Keys: []string{"v007", "nope"}, ReturnData: true, ReturnMetadata: true}))
	if len(got.Vectors) != 1 || !slices.Equal(got.Vectors[0].Data.(*types.VectorDataMemberFloat32).Value, vecs["v007"]) || got.Vectors[0].Metadata == nil {
		t.Fatalf("get %+v", got.Vectors)
	}

	seen := map[string]bool{}
	var token *string
	pages := 0
	for {
		out := must(c.ListVectors(ctx, &s3vectors.ListVectorsInput{VectorBucketName: b, IndexName: x, MaxResults: aws.Int32(30), NextToken: token}))
		pages++
		for _, v := range out.Vectors {
			if seen[*v.Key] || v.Data != nil || v.Metadata != nil {
				t.Fatalf("page %d: %s (data and metadata only on request)", pages, *v.Key)
			}
			seen[*v.Key] = true
		}
		if token = out.NextToken; token == nil {
			break
		}
	}
	if len(seen) != 100 || pages != 4 {
		t.Fatalf("listed %d vectors in %d pages", len(seen), pages)
	}
	clear(seen)
	for seg := range int32(3) {
		out := must(c.ListVectors(ctx, &s3vectors.ListVectorsInput{VectorBucketName: b, IndexName: x, SegmentCount: aws.Int32(3), SegmentIndex: seg}))
		for _, v := range out.Vectors {
			if seen[*v.Key] {
				t.Fatalf("%s is in two segments", *v.Key)
			}
			seen[*v.Key] = true
		}
	}
	if len(seen) != 100 {
		t.Fatalf("segments listed %d vectors", len(seen))
	}

	must(c.DeleteVectors(ctx, &s3vectors.DeleteVectorsInput{VectorBucketName: b, IndexName: x, Keys: []string{"v000", "v001"}}))
	if out := must(c.ListVectors(ctx, &s3vectors.ListVectorsInput{VectorBucketName: b, IndexName: x})); len(out.Vectors) != 98 {
		t.Fatalf("after delete: %d", len(out.Vectors))
	}
	// PutVectors overwrites a key
	must(c.PutVectors(ctx, &s3vectors.PutVectorsInput{VectorBucketName: b, IndexName: x, Vectors: []types.PutInputVector{
		{Key: aws.String("v002"), Data: &types.VectorDataMemberFloat32{Value: vecs["v003"]}}}}))
	if out := query(c, "cosine", vecs["v003"], 2, nil); *out.Vectors[0].Distance > 1e-6 || *out.Vectors[1].Distance > 1e-6 {
		t.Fatalf("overwritten: %+v", out.Vectors)
	}

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"dimension", func() error {
			_, err := c.PutVectors(ctx, &s3vectors.PutVectorsInput{VectorBucketName: b, IndexName: x, Vectors: []types.PutInputVector{
				{Key: aws.String("short"), Data: &types.VectorDataMemberFloat32{Value: []float32{1, 2}}}}})
			return err
		}()},
		{"key length", func() error {
			_, err := c.PutVectors(ctx, &s3vectors.PutVectorsInput{VectorBucketName: b, IndexName: x, Vectors: []types.PutInputVector{
				{Key: aws.String(strings.Repeat("k", 1025)), Data: &types.VectorDataMemberFloat32{Value: vecs["v003"]}}}})
			return err
		}()},
		{"zero cosine", func() error {
			_, err := c.PutVectors(ctx, &s3vectors.PutVectorsInput{VectorBucketName: b, IndexName: x, Vectors: []types.PutInputVector{
				{Key: aws.String("zero"), Data: &types.VectorDataMemberFloat32{Value: make([]float32, dim)}}}})
			return err
		}()},
		{"query dimension", func() error {
			_, err := c.QueryVectors(ctx, &s3vectors.QueryVectorsInput{VectorBucketName: b, IndexName: x, QueryVector: &types.VectorDataMemberFloat32{Value: []float32{1}}, TopK: aws.Int32(1)})
			return err
		}()},
		{"topK", func() error {
			_, err := c.QueryVectors(ctx, &s3vectors.QueryVectorsInput{VectorBucketName: b, IndexName: x, QueryVector: &types.VectorDataMemberFloat32{Value: vecs["v003"]}, TopK: aws.Int32(0)})
			return err
		}()},
		{"segment", func() error {
			_, err := c.ListVectors(ctx, &s3vectors.ListVectorsInput{VectorBucketName: b, IndexName: x, SegmentCount: aws.Int32(2), SegmentIndex: 2})
			return err
		}()},
	} {
		if code(tc.err) != "ValidationException" {
			t.Fatalf("%s: %v", tc.name, tc.err)
		}
	}
}

func TestBucketsAndIndexes(t *testing.T) {
	_, c, _ := setup(t)
	vb := must(c.GetVectorBucket(ctx, &s3vectors.GetVectorBucketInput{VectorBucketName: aws.String("vecs")})).VectorBucket
	if *vb.VectorBucketArn != core.ARN("s3vectors", "us-east-1", core.DefaultAccount, "bucket/vecs") || vb.EncryptionConfiguration.SseType != types.SseTypeAes256 {
		t.Fatalf("bucket %+v", vb)
	}
	ix := must(c.GetIndex(ctx, &s3vectors.GetIndexInput{IndexArn: aws.String(*vb.VectorBucketArn + "/index/cosine")})).Index
	if *ix.Dimension != dim || ix.DistanceMetric != types.DistanceMetricCosine || ix.MetadataConfiguration.NonFilterableMetadataKeys[0] != "note" || *ix.VectorBucketName != "vecs" {
		t.Fatalf("index %+v", ix)
	}
	first := must(c.ListIndexes(ctx, &s3vectors.ListIndexesInput{VectorBucketName: aws.String("vecs"), MaxResults: aws.Int32(1)}))
	second := must(c.ListIndexes(ctx, &s3vectors.ListIndexesInput{VectorBucketName: aws.String("vecs"), NextToken: first.NextToken}))
	if len(first.Indexes) != 1 || *first.Indexes[0].IndexName != "cosine" || len(second.Indexes) != 1 || *second.Indexes[0].IndexName != "euclidean" || second.NextToken != nil {
		t.Fatalf("pages %+v %+v", first, second)
	}
	must(c.CreateVectorBucket(ctx, &s3vectors.CreateVectorBucketInput{VectorBucketName: aws.String("other")}))
	if bs := must(c.ListVectorBuckets(ctx, &s3vectors.ListVectorBucketsInput{Prefix: aws.String("vec")})); len(bs.VectorBuckets) != 1 {
		t.Fatalf("buckets %+v", bs.VectorBuckets)
	}

	must(c.PutVectorBucketPolicy(ctx, &s3vectors.PutVectorBucketPolicyInput{VectorBucketName: aws.String("vecs"), Policy: aws.String(`{"Version":"2012-10-17","Statement":[]}`)}))
	if p := must(c.GetVectorBucketPolicy(ctx, &s3vectors.GetVectorBucketPolicyInput{VectorBucketName: aws.String("vecs")})); !strings.Contains(*p.Policy, "2012-10-17") {
		t.Fatalf("policy %s", *p.Policy)
	}
	must(c.DeleteVectorBucketPolicy(ctx, &s3vectors.DeleteVectorBucketPolicyInput{VectorBucketName: aws.String("vecs")}))

	must(c.TagResource(ctx, &s3vectors.TagResourceInput{ResourceArn: ix.IndexArn, Tags: map[string]string{"team": "ml", "env": "dev"}}))
	must(c.UntagResource(ctx, &s3vectors.UntagResourceInput{ResourceArn: ix.IndexArn, TagKeys: []string{"env"}}))
	if tags := must(c.ListTagsForResource(ctx, &s3vectors.ListTagsForResourceInput{ResourceArn: ix.IndexArn})).Tags; len(tags) != 1 || tags["team"] != "ml" {
		t.Fatalf("tags %v", tags)
	}

	for _, tc := range []struct {
		want string
		err  error
	}{
		{"NotFoundException", func() error { // no policy
			_, err := c.GetVectorBucketPolicy(ctx, &s3vectors.GetVectorBucketPolicyInput{VectorBucketName: aws.String("vecs")})
			return err
		}()},
		{"NotFoundException", func() error {
			_, err := c.GetIndex(ctx, &s3vectors.GetIndexInput{VectorBucketName: aws.String("vecs"), IndexName: aws.String("nope")})
			return err
		}()},
		{"ConflictException", func() error {
			_, err := c.CreateVectorBucket(ctx, &s3vectors.CreateVectorBucketInput{VectorBucketName: aws.String("vecs")})
			return err
		}()},
		{"ConflictException", func() error {
			_, err := c.CreateIndex(ctx, &s3vectors.CreateIndexInput{VectorBucketName: aws.String("vecs"), IndexName: aws.String("cosine"), DataType: types.DataTypeFloat32,
				Dimension: aws.Int32(dim), DistanceMetric: types.DistanceMetricCosine})
			return err
		}()},
		{"ConflictException", func() error { // not empty
			_, err := c.DeleteVectorBucket(ctx, &s3vectors.DeleteVectorBucketInput{VectorBucketName: aws.String("vecs")})
			return err
		}()},
		{"ValidationException", func() error {
			_, err := c.CreateVectorBucket(ctx, &s3vectors.CreateVectorBucketInput{VectorBucketName: aws.String("Bad_Name")})
			return err
		}()},
		{"ValidationException", func() error {
			_, err := c.CreateIndex(ctx, &s3vectors.CreateIndexInput{VectorBucketName: aws.String("vecs"), IndexName: aws.String("big"), DataType: types.DataTypeFloat32,
				Dimension: aws.Int32(5000), DistanceMetric: types.DistanceMetricCosine})
			return err
		}()},
	} {
		if code(tc.err) != tc.want {
			t.Fatalf("want %s, got %v", tc.want, tc.err)
		}
	}

	for _, ix := range []string{"cosine", "euclidean"} {
		must(c.DeleteIndex(ctx, &s3vectors.DeleteIndexInput{VectorBucketName: aws.String("vecs"), IndexName: aws.String(ix)}))
	}
	must(c.DeleteVectorBucket(ctx, &s3vectors.DeleteVectorBucketInput{VectorBucketName: aws.String("vecs")}))
	if _, err := c.GetVectorBucket(ctx, &s3vectors.GetVectorBucketInput{VectorBucketName: aws.String("vecs")}); code(err) != "NotFoundException" {
		t.Fatalf("deleted bucket: %v", err)
	}
}
