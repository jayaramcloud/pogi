package cloudfront_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	cfsvc "github.com/nrndev/pogi/internal/aws/cloudfront"
	iamsvc "github.com/nrndev/pogi/internal/aws/iam"
	s3svc "github.com/nrndev/pogi/internal/aws/s3"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

const (
	cachingOptimized    = "658327ea-f89d-4fab-a63d-7e88639e58f6"
	cachingDisabled     = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"
	allViewerExceptHost = "b689b0a8-53d0-40ab-baf2-68738e2966ac"
	securityHeaders     = "67f7725c-6f97-4210-82d7-5512b31e9d03"
)

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

func serve(t *testing.T) (*core.Hub, string, *cloudfront.Client, *s3.Client) {
	hub, u := core.ServeTest(t, func(h *core.Hub) core.Service { return s3svc.New(h) }, func(h *core.Hub) core.Service { return cfsvc.New(h) })
	cfg := awstest.Config(t, u)
	return hub, u, cloudfront.NewFromConfig(cfg), s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
}

func config(ref string, origins ...types.Origin) *types.DistributionConfig {
	return &types.DistributionConfig{CallerReference: aws.String(ref), Comment: aws.String(""), Enabled: aws.Bool(true),
		Origins: &types.Origins{Quantity: aws.Int32(int32(len(origins))), Items: origins},
		DefaultCacheBehavior: &types.DefaultCacheBehavior{TargetOriginId: origins[0].Id, ViewerProtocolPolicy: types.ViewerProtocolPolicyAllowAll,
			CachePolicyId: aws.String(cachingOptimized)}}
}

func s3Origin(id, bucket string) types.Origin {
	return types.Origin{Id: aws.String(id), DomainName: aws.String(bucket + ".s3.us-east-1.amazonaws.com"), S3OriginConfig: &types.S3OriginConfig{OriginAccessIdentity: aws.String("")}}
}

func customOrigin(id, rawURL string) types.Origin {
	u := must(url.Parse(rawURL))
	host, port, _ := net.SplitHostPort(u.Host)
	p, _ := strconv.Atoi(port)
	return types.Origin{Id: aws.String(id), DomainName: aws.String(host),
		CustomOriginConfig: &types.CustomOriginConfig{HTTPPort: aws.Int32(int32(p)), HTTPSPort: aws.Int32(443), OriginProtocolPolicy: types.OriginProtocolPolicyHttpOnly}}
}

// function creates and publishes a function and returns its ARN.
func function(cf *cloudfront.Client, name, runtime, code string) *string {
	created := must(cf.CreateFunction(ctx, &cloudfront.CreateFunctionInput{Name: aws.String(name), FunctionCode: []byte(code),
		FunctionConfig: &types.FunctionConfig{Comment: aws.String(name), Runtime: types.FunctionRuntime(runtime)}}))
	return must(cf.PublishFunction(ctx, &cloudfront.PublishFunctionInput{Name: aws.String(name), IfMatch: created.ETag})).FunctionSummary.FunctionMetadata.FunctionARN
}

// viewer sends a request to a distribution's domain on pogi's port.
func viewer(t *testing.T, base, domain, method, path string, header ...string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(method, base+path, nil)
	req.Host = domain
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res, string(b)
}

// TestEdge is the done-when: a CloudFront URL serves an S3 object, with
// caching, invalidation, path behaviors, functions and custom error pages.
func TestEdge(t *testing.T) {
	_, base, cf, s3c := serve(t)
	must(s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("site")}))
	for key, body := range map[string]string{"index.html": "home", "docs/index.html": "docs", "404.html": "not here", "hello.txt": "hello"} {
		must(s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("site"), Key: aws.String(key), Body: strings.NewReader(body)}))
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		fmt.Fprintf(w, "%s %s %s q=%s test=%s host=%s", r.Header.Get("User-Agent"), r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("X-Test"), r.Host)
	}))
	defer api.Close()

	rewrite := function(cf, "rewrite", "cloudfront-js-1.0",
		`function handler(event) { var r = event.request; if (r.uri !== '/' && r.uri.endsWith('/')) { r.uri += 'index.html'; } return r; }`)
	stamp := function(cf, "stamp", "cloudfront-js-2.0",
		`async function handler(event) { const res = event.response; res.headers['x-edge'] = {value: event.context.eventType}; return res; }`)
	oac := must(cf.CreateOriginAccessControl(ctx, &cloudfront.CreateOriginAccessControlInput{OriginAccessControlConfig: &types.OriginAccessControlConfig{
		Name: aws.String("site"), OriginAccessControlOriginType: types.OriginAccessControlOriginTypesS3,
		SigningBehavior: types.OriginAccessControlSigningBehaviorsAlways, SigningProtocol: types.OriginAccessControlSigningProtocolsSigv4}}))

	site := s3Origin("s3", "site")
	site.OriginAccessControlId = oac.OriginAccessControl.Id
	dc := config("edge", site, customOrigin("api", api.URL))
	dc.DefaultRootObject = aws.String("index.html")
	dc.DefaultCacheBehavior.ResponseHeadersPolicyId = aws.String(securityHeaders)
	dc.DefaultCacheBehavior.FunctionAssociations = &types.FunctionAssociations{Quantity: aws.Int32(2), Items: []types.FunctionAssociation{
		{EventType: types.EventTypeViewerRequest, FunctionARN: rewrite}, {EventType: types.EventTypeViewerResponse, FunctionARN: stamp}}}
	all := []types.Method{"GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"}
	dc.CacheBehaviors = &types.CacheBehaviors{Quantity: aws.Int32(1), Items: []types.CacheBehavior{{PathPattern: aws.String("/api/*"),
		TargetOriginId: aws.String("api"), ViewerProtocolPolicy: types.ViewerProtocolPolicyAllowAll, CachePolicyId: aws.String(cachingDisabled),
		OriginRequestPolicyId: aws.String(allViewerExceptHost), AllowedMethods: &types.AllowedMethods{Quantity: aws.Int32(7), Items: all}}}}
	dc.CustomErrorResponses = &types.CustomErrorResponses{Quantity: aws.Int32(1), Items: []types.CustomErrorResponse{{ErrorCode: aws.Int32(404),
		ResponsePagePath: aws.String("/404.html"), ResponseCode: aws.String("404"), ErrorCachingMinTTL: aws.Int64(0)}}}
	created := must(cf.CreateDistribution(ctx, &cloudfront.CreateDistributionInput{DistributionConfig: dc}))
	d := created.Distribution
	domain := *d.DomainName
	if domain != strings.ToLower(*d.Id)+".cloudfront.localhost" || *d.Status != "InProgress" || *created.ETag == "" {
		t.Fatalf("created %s %s %s", domain, *d.Status, aws.ToString(created.ETag))
	}
	if got := must(cf.GetDistribution(ctx, &cloudfront.GetDistributionInput{Id: d.Id})); *got.Distribution.Status != "Deployed" {
		t.Fatalf("status %s", *got.Distribution.Status)
	}

	check := func(res *http.Response, body string, status int, want, cache string) {
		t.Helper()
		if res.StatusCode != status || body != want || res.Header.Get("X-Cache") != cache+" from cloudfront" {
			t.Fatalf("got %d %q %q, want %d %q %s", res.StatusCode, body, res.Header.Get("X-Cache"), status, want, cache)
		}
	}
	res, body := viewer(t, base, domain, "GET", "/") // DefaultRootObject
	check(res, body, 200, "home", "Miss")
	if res.Header.Get("X-Edge") != "viewer-response" || res.Header.Get("X-Frame-Options") != "SAMEORIGIN" ||
		!strings.Contains(res.Header.Get("Via"), "(CloudFront)") || len(res.Header.Get("X-Amz-Cf-Id")) != 56 {
		t.Fatalf("headers %v", res.Header)
	}
	res, body = viewer(t, base, domain, "GET", "/hello.txt")
	check(res, body, 200, "hello", "Miss")
	res, body = viewer(t, base, domain, "GET", "/hello.txt?v=2") // the query string is not in CachingOptimized's key
	check(res, body, 200, "hello", "Hit")
	if res.Header.Get("Age") == "" {
		t.Fatal("a hit has an Age")
	}
	res, body = viewer(t, base, domain, "GET", "/docs/") // the viewer-request function adds index.html
	check(res, body, 200, "docs", "Miss")

	must(s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("site"), Key: aws.String("hello.txt"), Body: strings.NewReader("hello again")}))
	res, body = viewer(t, base, domain, "GET", "/hello.txt")
	check(res, body, 200, "hello", "Hit")
	inv := must(cf.CreateInvalidation(ctx, &cloudfront.CreateInvalidationInput{DistributionId: d.Id, InvalidationBatch: &types.InvalidationBatch{
		CallerReference: aws.String("inv-1"), Paths: &types.Paths{Quantity: aws.Int32(1), Items: []string{"/hel*"}}}}))
	if *inv.Invalidation.Status != "InProgress" {
		t.Fatalf("invalidation %s", *inv.Invalidation.Status)
	}
	res, body = viewer(t, base, domain, "GET", "/hello.txt")
	check(res, body, 200, "hello again", "Miss")
	got := must(cf.GetInvalidation(ctx, &cloudfront.GetInvalidationInput{DistributionId: d.Id, Id: inv.Invalidation.Id}))
	list := must(cf.ListInvalidations(ctx, &cloudfront.ListInvalidationsInput{DistributionId: d.Id}))
	if *got.Invalidation.Status != "Completed" || *list.InvalidationList.Quantity != 1 || got.Invalidation.InvalidationBatch.Paths.Items[0] != "/hel*" {
		t.Fatalf("invalidation %+v %+v", got.Invalidation, list.InvalidationList)
	}

	// /api/* goes to the custom origin with what AllViewerExceptHostHeader forwards, uncached
	res, body = viewer(t, base, domain, "GET", "/api/items?x=1", "X-Test", "yes")
	check(res, body, 200, "Go-http-client/1.1 GET /api/items q=x=1 test=yes host="+strings.TrimPrefix(api.URL, "http://"), "Miss")
	res, body = viewer(t, base, domain, "POST", "/api/items")
	check(res, body, 200, "Go-http-client/1.1 POST /api/items q= test= host="+strings.TrimPrefix(api.URL, "http://"), "Miss")
	if res.Header.Get("X-Edge") != "" || res.Header.Get("X-Frame-Options") != "" {
		t.Fatalf("the /api/* behavior has no functions or headers policy: %v", res.Header)
	}

	res, _ = viewer(t, base, domain, "POST", "/hello.txt") // the default behavior allows GET and HEAD
	if res.StatusCode != 403 || res.Header.Get("X-Cache") != "Error from cloudfront" {
		t.Fatalf("POST: %d %v", res.StatusCode, res.Header)
	}
	res, body = viewer(t, base, domain, "GET", "/missing") // the custom error page
	check(res, body, 404, "not here", "Error")
	res, _ = viewer(t, base, "nope.cloudfront.localhost", "GET", "/")
	if res.StatusCode != 404 {
		t.Fatalf("unknown distribution: %d", res.StatusCode)
	}
}

// TestOriginGroup fails over from an unreachable origin to one pogi serves in-process.
func TestOriginGroup(t *testing.T) {
	_, base, cf, s3c := serve(t)
	must(s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("assets")}))
	must(s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("assets"), Key: aws.String("app.js"), Body: strings.NewReader("js")}))
	// the custom origin reads S3 without credentials, so with signature checks on the bucket must be public
	must(s3c.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String("assets"),
		Policy: aws.String(`{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::assets/*"}]}`)}))
	dead := customOrigin("dead", "http://127.0.0.1:1")
	local := customOrigin("local", "http://assets.localhost:80") // pogi's S3 by virtual host
	dc := config("group", dead, local)
	dc.OriginGroups = &types.OriginGroups{Quantity: aws.Int32(1), Items: []types.OriginGroup{{Id: aws.String("group"),
		FailoverCriteria: &types.OriginGroupFailoverCriteria{StatusCodes: &types.StatusCodes{Quantity: aws.Int32(1), Items: []int32{503}}},
		Members:          &types.OriginGroupMembers{Quantity: aws.Int32(2), Items: []types.OriginGroupMember{{OriginId: aws.String("dead")}, {OriginId: aws.String("local")}}}}}}
	dc.DefaultCacheBehavior.TargetOriginId = aws.String("group")
	d := must(cf.CreateDistribution(ctx, &cloudfront.CreateDistributionInput{DistributionConfig: dc})).Distribution
	if res, body := viewer(t, base, *d.DomainName, "GET", "/app.js"); res.StatusCode != 200 || body != "js" {
		t.Fatalf("failover: %d %q", res.StatusCode, body)
	}
}

// floci #5242: ConnectionAttempts tries a dropped origin connection again (GET only).
func TestConnectionAttempts(t *testing.T) {
	_, base, cf, _ := serve(t)
	var hits int
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits++; hits < 3 {
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}
		io.WriteString(w, "ok")
	}))
	defer origin.Close()
	o := customOrigin("o", origin.URL)
	o.ConnectionAttempts = aws.Int32(3)
	d := must(cf.CreateDistribution(ctx, &cloudfront.CreateDistributionInput{DistributionConfig: config("attempts", o)})).Distribution
	if res, body := viewer(t, base, *d.DomainName, "GET", "/x"); res.StatusCode != 200 || body != "ok" || hits != 3 {
		t.Fatalf("3 attempts: %d %q after %d hits", res.StatusCode, body, hits)
	}
}

// floci #4875: an IAMCertificateId that IAM does not have is an InvalidViewerCertificate.
func TestMissingIAMCertificate(t *testing.T) {
	_, u := core.ServeTest(t, func(h *core.Hub) core.Service { return iamsvc.New(h) }, func(h *core.Hub) core.Service { return cfsvc.New(h) })
	cf := cloudfront.NewFromConfig(awstest.Config(t, u))
	dc := config("iamcert", customOrigin("o", "http://127.0.0.1:1"))
	dc.ViewerCertificate = &types.ViewerCertificate{IAMCertificateId: aws.String("ASCAFAKEFAKEFAKE"), SSLSupportMethod: types.SSLSupportMethodSniOnly}
	_, err := cf.CreateDistribution(ctx, &cloudfront.CreateDistributionInput{DistributionConfig: dc})
	if code(err) != "InvalidViewerCertificate" {
		t.Fatalf("want InvalidViewerCertificate, got %v", err)
	}
}

func TestDistributions(t *testing.T) {
	hub, base, cf, _ := serve(t)
	created := must(cf.CreateDistributionWithTags(ctx, &cloudfront.CreateDistributionWithTagsInput{DistributionConfigWithTags: &types.DistributionConfigWithTags{
		DistributionConfig: config("crud", s3Origin("s3", "bucket")), Tags: &types.Tags{Items: []types.Tag{{Key: aws.String("team"), Value: aws.String("web")}}}}}))
	d := created.Distribution
	_, err := cf.CreateDistribution(ctx, &cloudfront.CreateDistributionInput{DistributionConfig: config("crud", s3Origin("s3", "bucket"))})
	if code(err) != "DistributionAlreadyExists" {
		t.Fatalf("same caller reference: %v", err)
	}
	bad := config("bad", s3Origin("s3", "bucket"))
	bad.DefaultCacheBehavior.TargetOriginId = aws.String("nope")
	if _, err = cf.CreateDistribution(ctx, &cloudfront.CreateDistributionInput{DistributionConfig: bad}); code(err) != "NoSuchOrigin" {
		t.Fatalf("unknown origin: %v", err)
	}
	bad = config("bad", s3Origin("s3", "bucket"))
	bad.DefaultCacheBehavior.CachePolicyId = aws.String("00000000-0000-0000-0000-000000000000")
	if _, err = cf.CreateDistribution(ctx, &cloudfront.CreateDistributionInput{DistributionConfig: bad}); code(err) != "NoSuchCachePolicy" {
		t.Fatalf("unknown cache policy: %v", err)
	}

	tags := must(cf.ListTagsForResource(ctx, &cloudfront.ListTagsForResourceInput{Resource: d.ARN}))
	if len(tags.Tags.Items) != 1 || *tags.Tags.Items[0].Value != "web" {
		t.Fatalf("tags %+v", tags.Tags)
	}
	must(cf.TagResource(ctx, &cloudfront.TagResourceInput{Resource: d.ARN, Tags: &types.Tags{Items: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}}))
	must(cf.UntagResource(ctx, &cloudfront.UntagResourceInput{Resource: d.ARN, TagKeys: &types.TagKeys{Items: []string{"team"}}}))
	if tags = must(cf.ListTagsForResource(ctx, &cloudfront.ListTagsForResourceInput{Resource: d.ARN})); len(tags.Tags.Items) != 1 || *tags.Tags.Items[0].Key != "env" {
		t.Fatalf("tags %+v", tags.Tags)
	}

	cfg := must(cf.GetDistributionConfig(ctx, &cloudfront.GetDistributionConfigInput{Id: d.Id}))
	if *cfg.ETag != *created.ETag || *cfg.DistributionConfig.Origins.Items[0].DomainName != "bucket.s3.us-east-1.amazonaws.com" {
		t.Fatalf("config %s %+v", *cfg.ETag, cfg.DistributionConfig.Origins)
	}
	cfg.DistributionConfig.Comment = aws.String("updated")
	_, err = cf.UpdateDistribution(ctx, &cloudfront.UpdateDistributionInput{Id: d.Id, DistributionConfig: cfg.DistributionConfig})
	if code(err) != "InvalidIfMatchVersion" {
		t.Fatalf("no If-Match: %v", err)
	}
	_, err = cf.UpdateDistribution(ctx, &cloudfront.UpdateDistributionInput{Id: d.Id, DistributionConfig: cfg.DistributionConfig, IfMatch: aws.String("E000")})
	if code(err) != "PreconditionFailed" {
		t.Fatalf("stale If-Match: %v", err)
	}
	cfg.DistributionConfig.CallerReference = aws.String("other")
	_, err = cf.UpdateDistribution(ctx, &cloudfront.UpdateDistributionInput{Id: d.Id, DistributionConfig: cfg.DistributionConfig, IfMatch: cfg.ETag})
	if code(err) != "IllegalUpdate" {
		t.Fatalf("new caller reference: %v", err)
	}
	cfg.DistributionConfig.CallerReference = aws.String("crud")
	updated := must(cf.UpdateDistribution(ctx, &cloudfront.UpdateDistributionInput{Id: d.Id, DistributionConfig: cfg.DistributionConfig, IfMatch: cfg.ETag}))
	if *updated.ETag == *cfg.ETag || *updated.Distribution.DistributionConfig.Comment != "updated" {
		t.Fatalf("updated %s %+v", *updated.ETag, updated.Distribution.DistributionConfig)
	}

	must(cf.CreateMonitoringSubscription(ctx, &cloudfront.CreateMonitoringSubscriptionInput{DistributionId: d.Id, MonitoringSubscription: &types.MonitoringSubscription{
		RealtimeMetricsSubscriptionConfig: &types.RealtimeMetricsSubscriptionConfig{RealtimeMetricsSubscriptionStatus: types.RealtimeMetricsSubscriptionStatusEnabled}}}))
	ms := must(cf.GetMonitoringSubscription(ctx, &cloudfront.GetMonitoringSubscriptionInput{DistributionId: d.Id}))
	if ms.MonitoringSubscription.RealtimeMetricsSubscriptionConfig.RealtimeMetricsSubscriptionStatus != types.RealtimeMetricsSubscriptionStatusEnabled {
		t.Fatalf("monitoring %+v", ms.MonitoringSubscription)
	}
	must(cf.DeleteMonitoringSubscription(ctx, &cloudfront.DeleteMonitoringSubscriptionInput{DistributionId: d.Id}))
	if _, err = cf.GetMonitoringSubscription(ctx, &cloudfront.GetMonitoringSubscriptionInput{DistributionId: d.Id}); code(err) != "NoSuchMonitoringSubscription" {
		t.Fatalf("deleted monitoring: %v", err)
	}

	list := must(cf.ListDistributions(ctx, &cloudfront.ListDistributionsInput{}))
	if *list.DistributionList.Quantity != 1 || *list.DistributionList.Items[0].Comment != "updated" || *list.DistributionList.Items[0].Status != "Deployed" {
		t.Fatalf("list %+v", list.DistributionList)
	}

	b, err := hub.Snapshot() // state survives Save and Load
	if err != nil {
		t.Fatal(err)
	}
	hub.Reset()
	if err := hub.Restore(b); err != nil {
		t.Fatal(err)
	}
	if res, _ := viewer(t, base, *d.DomainName, "GET", "/x"); res.StatusCode != 404 || res.Header.Get("X-Cache") != "Error from cloudfront" {
		t.Fatalf("restored distribution: %d", res.StatusCode) // pogi's S3 has no such bucket
	}

	_, err = cf.DeleteDistribution(ctx, &cloudfront.DeleteDistributionInput{Id: d.Id, IfMatch: updated.ETag})
	if code(err) != "DistributionNotDisabled" {
		t.Fatalf("delete enabled: %v", err)
	}
	cfg.DistributionConfig.Enabled = aws.Bool(false)
	disabled := must(cf.UpdateDistribution(ctx, &cloudfront.UpdateDistributionInput{Id: d.Id, DistributionConfig: cfg.DistributionConfig, IfMatch: updated.ETag}))
	if res, _ := viewer(t, base, *d.DomainName, "GET", "/"); res.StatusCode != 403 {
		t.Fatalf("disabled distribution: %d", res.StatusCode)
	}
	if _, err = cf.DeleteDistribution(ctx, &cloudfront.DeleteDistributionInput{Id: d.Id, IfMatch: updated.ETag}); code(err) != "PreconditionFailed" {
		t.Fatalf("delete stale: %v", err)
	}
	must(cf.DeleteDistribution(ctx, &cloudfront.DeleteDistributionInput{Id: d.Id, IfMatch: disabled.ETag}))
	if _, err = cf.GetDistribution(ctx, &cloudfront.GetDistributionInput{Id: d.Id}); code(err) != "NoSuchDistribution" {
		t.Fatalf("deleted: %v", err)
	}
}

func TestPolicies(t *testing.T) {
	_, _, cf, _ := serve(t)
	managed := must(cf.ListCachePolicies(ctx, &cloudfront.ListCachePoliciesInput{Type: types.CachePolicyTypeManaged}))
	names := map[string]string{}
	for _, p := range managed.CachePolicyList.Items {
		names[*p.CachePolicy.CachePolicyConfig.Name] = *p.CachePolicy.Id
	}
	if names["Managed-CachingOptimized"] != cachingOptimized || names["Managed-CachingDisabled"] != cachingDisabled {
		t.Fatalf("managed cache policies %v", names)
	}
	disabled := must(cf.GetCachePolicy(ctx, &cloudfront.GetCachePolicyInput{Id: aws.String(cachingDisabled)}))
	if *disabled.CachePolicy.CachePolicyConfig.MaxTTL != 0 {
		t.Fatalf("CachingDisabled %+v", disabled.CachePolicy.CachePolicyConfig)
	}
	if _, err := cf.DeleteCachePolicy(ctx, &cloudfront.DeleteCachePolicyInput{Id: aws.String(cachingDisabled), IfMatch: disabled.ETag}); code(err) != "IllegalDelete" {
		t.Fatalf("delete managed: %v", err)
	}
	orp := must(cf.ListOriginRequestPolicies(ctx, &cloudfront.ListOriginRequestPoliciesInput{Type: types.OriginRequestPolicyTypeManaged}))
	rhp := must(cf.ListResponseHeadersPolicies(ctx, &cloudfront.ListResponseHeadersPoliciesInput{Type: types.ResponseHeadersPolicyTypeManaged}))
	if *orp.OriginRequestPolicyList.Quantity < 5 || *rhp.ResponseHeadersPolicyList.Quantity != 5 {
		t.Fatalf("managed: %d origin request, %d response headers", *orp.OriginRequestPolicyList.Quantity, *rhp.ResponseHeadersPolicyList.Quantity)
	}

	pc := &types.CachePolicyConfig{Name: aws.String("mine"), MinTTL: aws.Int64(0), DefaultTTL: aws.Int64(60), MaxTTL: aws.Int64(600),
		ParametersInCacheKeyAndForwardedToOrigin: &types.ParametersInCacheKeyAndForwardedToOrigin{EnableAcceptEncodingGzip: aws.Bool(true),
			HeadersConfig:      &types.CachePolicyHeadersConfig{HeaderBehavior: types.CachePolicyHeaderBehaviorNone},
			CookiesConfig:      &types.CachePolicyCookiesConfig{CookieBehavior: types.CachePolicyCookieBehaviorNone},
			QueryStringsConfig: &types.CachePolicyQueryStringsConfig{QueryStringBehavior: types.CachePolicyQueryStringBehaviorAll}}}
	mine := must(cf.CreateCachePolicy(ctx, &cloudfront.CreateCachePolicyInput{CachePolicyConfig: pc}))
	if _, err := cf.CreateCachePolicy(ctx, &cloudfront.CreateCachePolicyInput{CachePolicyConfig: pc}); code(err) != "CachePolicyAlreadyExists" {
		t.Fatalf("same name: %v", err)
	}
	custom := must(cf.ListCachePolicies(ctx, &cloudfront.ListCachePoliciesInput{Type: types.CachePolicyTypeCustom}))
	if *custom.CachePolicyList.Quantity != 1 || custom.CachePolicyList.Items[0].Type != types.CachePolicyTypeCustom {
		t.Fatalf("custom %+v", custom.CachePolicyList)
	}
	dc := config("uses-policy", s3Origin("s3", "bucket"))
	dc.DefaultCacheBehavior.CachePolicyId = mine.CachePolicy.Id
	d := must(cf.CreateDistribution(ctx, &cloudfront.CreateDistributionInput{DistributionConfig: dc}))
	if _, err := cf.DeleteCachePolicy(ctx, &cloudfront.DeleteCachePolicyInput{Id: mine.CachePolicy.Id, IfMatch: mine.ETag}); code(err) != "CachePolicyInUse" {
		t.Fatalf("in use: %v", err)
	}
	dc.Enabled = aws.Bool(false)
	dc.DefaultCacheBehavior.CachePolicyId = aws.String(cachingOptimized)
	up := must(cf.UpdateDistribution(ctx, &cloudfront.UpdateDistributionInput{Id: d.Distribution.Id, DistributionConfig: dc, IfMatch: d.ETag}))
	must(cf.DeleteDistribution(ctx, &cloudfront.DeleteDistributionInput{Id: d.Distribution.Id, IfMatch: up.ETag}))
	must(cf.DeleteCachePolicy(ctx, &cloudfront.DeleteCachePolicyInput{Id: mine.CachePolicy.Id, IfMatch: mine.ETag}))

	oai := must(cf.CreateCloudFrontOriginAccessIdentity(ctx, &cloudfront.CreateCloudFrontOriginAccessIdentityInput{
		CloudFrontOriginAccessIdentityConfig: &types.CloudFrontOriginAccessIdentityConfig{CallerReference: aws.String("oai"), Comment: aws.String("legacy")}}))
	oais := must(cf.ListCloudFrontOriginAccessIdentities(ctx, &cloudfront.ListCloudFrontOriginAccessIdentitiesInput{}))
	if len(*oai.CloudFrontOriginAccessIdentity.S3CanonicalUserId) != 96 || *oais.CloudFrontOriginAccessIdentityList.Items[0].Comment != "legacy" {
		t.Fatalf("OAI %+v", oais.CloudFrontOriginAccessIdentityList.Items)
	}
	oac := must(cf.CreateOriginAccessControl(ctx, &cloudfront.CreateOriginAccessControlInput{OriginAccessControlConfig: &types.OriginAccessControlConfig{
		Name: aws.String("oac"), OriginAccessControlOriginType: types.OriginAccessControlOriginTypesS3,
		SigningBehavior: types.OriginAccessControlSigningBehaviorsAlways, SigningProtocol: types.OriginAccessControlSigningProtocolsSigv4}}))
	oacs := must(cf.ListOriginAccessControls(ctx, &cloudfront.ListOriginAccessControlsInput{}))
	if *oacs.OriginAccessControlList.Items[0].Name != "oac" || *oacs.OriginAccessControlList.Items[0].Id != *oac.OriginAccessControl.Id {
		t.Fatalf("OACs %+v", oacs.OriginAccessControlList.Items)
	}
	if _, err := cf.GetOriginAccessControl(ctx, &cloudfront.GetOriginAccessControlInput{Id: aws.String("E000")}); code(err) != "NoSuchOriginAccessControl" {
		t.Fatalf("unknown OAC: %v", err)
	}

	pk := must(cf.CreatePublicKey(ctx, &cloudfront.CreatePublicKeyInput{PublicKeyConfig: &types.PublicKeyConfig{CallerReference: aws.String("pk"),
		Name: aws.String("signer"), EncodedKey: aws.String("-----BEGIN PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----\n")}}))
	kg := must(cf.CreateKeyGroup(ctx, &cloudfront.CreateKeyGroupInput{KeyGroupConfig: &types.KeyGroupConfig{Name: aws.String("signers"), Items: []string{*pk.PublicKey.Id}}}))
	if _, err := cf.DeletePublicKey(ctx, &cloudfront.DeletePublicKeyInput{Id: pk.PublicKey.Id, IfMatch: pk.ETag}); code(err) != "PublicKeyInUse" {
		t.Fatalf("key in a group: %v", err)
	}
	groups := must(cf.ListKeyGroups(ctx, &cloudfront.ListKeyGroupsInput{}))
	if groups.KeyGroupList.Items[0].KeyGroup.KeyGroupConfig.Items[0] != *pk.PublicKey.Id {
		t.Fatalf("key groups %+v", groups.KeyGroupList.Items)
	}
	must(cf.DeleteKeyGroup(ctx, &cloudfront.DeleteKeyGroupInput{Id: kg.KeyGroup.Id, IfMatch: kg.ETag}))
	must(cf.DeletePublicKey(ctx, &cloudfront.DeletePublicKeyInput{Id: pk.PublicKey.Id, IfMatch: pk.ETag}))
}

func TestFunctions(t *testing.T) {
	_, _, cf, _ := serve(t)
	code1 := `function handler(event) { console.log('uri', event.request.uri);
  return {statusCode: 302, statusDescription: 'Found', headers: {location: {value: 'https://example.com' + event.request.uri}}}; }`
	created := must(cf.CreateFunction(ctx, &cloudfront.CreateFunctionInput{Name: aws.String("redirect"), FunctionCode: []byte(code1),
		FunctionConfig: &types.FunctionConfig{Comment: aws.String("to example.com"), Runtime: types.FunctionRuntimeCloudfrontJs10}}))
	if *created.FunctionSummary.Status != "UNPUBLISHED" || created.FunctionSummary.FunctionMetadata.Stage != types.FunctionStageDevelopment {
		t.Fatalf("created %+v", created.FunctionSummary)
	}
	_, err := cf.CreateFunction(ctx, &cloudfront.CreateFunctionInput{Name: aws.String("redirect"), FunctionCode: []byte(code1),
		FunctionConfig: &types.FunctionConfig{Comment: aws.String(""), Runtime: types.FunctionRuntimeCloudfrontJs10}})
	if code(err) != "FunctionAlreadyExists" {
		t.Fatalf("same name: %v", err)
	}
	got := must(cf.GetFunction(ctx, &cloudfront.GetFunctionInput{Name: aws.String("redirect")}))
	if string(got.FunctionCode) != code1 || *got.ETag != *created.ETag {
		t.Fatalf("code %q", got.FunctionCode)
	}

	event := `{"version":"1.0","context":{"eventType":"viewer-request"},"viewer":{"ip":"1.2.3.4"},
  "request":{"method":"GET","uri":"/a","querystring":{},"headers":{},"cookies":{}}}`
	test := must(cf.TestFunction(ctx, &cloudfront.TestFunctionInput{Name: aws.String("redirect"), IfMatch: created.ETag, EventObject: []byte(event)}))
	r := test.TestResult
	if !strings.Contains(*r.FunctionOutput, `"response":{"statusCode":302`) || !strings.Contains(*r.FunctionOutput, "https://example.com/a") ||
		len(r.FunctionExecutionLogs) != 1 || r.FunctionExecutionLogs[0] != "uri /a" || r.ComputeUtilization == nil || r.FunctionErrorMessage != nil {
		t.Fatalf("test %+v", r)
	}

	updated := must(cf.UpdateFunction(ctx, &cloudfront.UpdateFunctionInput{Name: aws.String("redirect"), IfMatch: created.ETag,
		FunctionCode:   []byte("function handler(event) { throw new Error('boom'); }"),
		FunctionConfig: &types.FunctionConfig{Comment: aws.String("broken"), Runtime: types.FunctionRuntimeCloudfrontJs20}}))
	if *updated.ETag == *created.ETag {
		t.Fatal("an update changes the ETag")
	}
	test = must(cf.TestFunction(ctx, &cloudfront.TestFunctionInput{Name: aws.String("redirect"), IfMatch: updated.ETag, EventObject: []byte(event)}))
	if test.TestResult.FunctionErrorMessage == nil || !strings.Contains(*test.TestResult.FunctionErrorMessage, "boom") {
		t.Fatalf("error %+v", test.TestResult)
	}
	if _, err = cf.DescribeFunction(ctx, &cloudfront.DescribeFunctionInput{Name: aws.String("redirect"), Stage: types.FunctionStageLive}); code(err) != "NoSuchFunctionExists" {
		t.Fatalf("unpublished LIVE: %v", err)
	}
	if _, err = cf.PublishFunction(ctx, &cloudfront.PublishFunctionInput{Name: aws.String("redirect"), IfMatch: created.ETag}); code(err) != "PreconditionFailed" {
		t.Fatalf("stale publish: %v", err)
	}
	must(cf.PublishFunction(ctx, &cloudfront.PublishFunctionInput{Name: aws.String("redirect"), IfMatch: updated.ETag}))
	live := must(cf.DescribeFunction(ctx, &cloudfront.DescribeFunctionInput{Name: aws.String("redirect"), Stage: types.FunctionStageLive}))
	if *live.FunctionSummary.FunctionConfig.Comment != "broken" || *live.FunctionSummary.Status != "UNASSOCIATED" {
		t.Fatalf("live %+v", live.FunctionSummary)
	}
	fl := must(cf.ListFunctions(ctx, &cloudfront.ListFunctionsInput{Stage: types.FunctionStageLive}))
	if *fl.FunctionList.Quantity != 1 || *fl.FunctionList.Items[0].Name != "redirect" {
		t.Fatalf("list %+v", fl.FunctionList)
	}

	// the ARN of "redir" prefixes the one of "redirect" (associated below); deleting it must still work
	unpublished := must(cf.CreateFunction(ctx, &cloudfront.CreateFunctionInput{Name: aws.String("redir"), FunctionCode: []byte("function handler(e) { return e.request; }"),
		FunctionConfig: &types.FunctionConfig{Comment: aws.String(""), Runtime: types.FunctionRuntimeCloudfrontJs10}}))
	dc := config("fn", s3Origin("s3", "bucket"))
	dc.DefaultCacheBehavior.FunctionAssociations = &types.FunctionAssociations{Quantity: aws.Int32(1), Items: []types.FunctionAssociation{
		{EventType: types.EventTypeViewerRequest, FunctionARN: unpublished.FunctionSummary.FunctionMetadata.FunctionARN}}}
	if _, err = cf.CreateDistribution(ctx, &cloudfront.CreateDistributionInput{DistributionConfig: dc}); code(err) != "InvalidFunctionAssociation" {
		t.Fatalf("unpublished function: %v", err)
	}
	dc.DefaultCacheBehavior.FunctionAssociations.Items[0].FunctionARN = live.FunctionSummary.FunctionMetadata.FunctionARN
	must(cf.CreateDistribution(ctx, &cloudfront.CreateDistributionInput{DistributionConfig: dc}))
	if _, err = cf.DeleteFunction(ctx, &cloudfront.DeleteFunctionInput{Name: aws.String("redirect"), IfMatch: updated.ETag}); code(err) != "FunctionInUse" {
		t.Fatalf("delete in use: %v", err)
	}
	if _, err = cf.DeleteFunction(ctx, &cloudfront.DeleteFunctionInput{Name: aws.String("redir"), IfMatch: aws.String("E000")}); code(err) != "PreconditionFailed" {
		t.Fatalf("delete stale: %v", err)
	}
	must(cf.DeleteFunction(ctx, &cloudfront.DeleteFunctionInput{Name: aws.String("redir"), IfMatch: unpublished.ETag}))
	if _, err = cf.DescribeFunction(ctx, &cloudfront.DescribeFunctionInput{Name: aws.String("redir")}); code(err) != "NoSuchFunctionExists" {
		t.Fatalf("deleted: %v", err)
	}
}

// floci #3067, #3221: a cache behavior and a custom origin read back with CloudFront's defaults.
func TestConfigDefaults(t *testing.T) {
	_, _, cf, _ := serve(t)
	cfg := config("defaults", customOrigin("o", "http://origin.example.com:80"))
	cfg.CacheBehaviors = &types.CacheBehaviors{Quantity: aws.Int32(1), Items: []types.CacheBehavior{{PathPattern: aws.String("/api/*"),
		TargetOriginId: aws.String("o"), ViewerProtocolPolicy: types.ViewerProtocolPolicyAllowAll, CachePolicyId: aws.String(cachingOptimized),
		AllowedMethods: &types.AllowedMethods{Quantity: aws.Int32(3), Items: []types.Method{"GET", "HEAD", "OPTIONS"}}}}}
	d := must(cf.CreateDistribution(ctx, &cloudfront.CreateDistributionInput{DistributionConfig: cfg})).Distribution
	got := must(cf.GetDistribution(ctx, &cloudfront.GetDistributionInput{Id: d.Id})).Distribution.DistributionConfig
	b := got.DefaultCacheBehavior
	if b.TrustedSigners == nil || b.SmoothStreaming == nil || b.FieldLevelEncryptionId == nil || b.AllowedMethods == nil ||
		aws.ToInt32(b.AllowedMethods.Quantity) != 2 || b.AllowedMethods.CachedMethods == nil || b.LambdaFunctionAssociations == nil {
		t.Errorf("default behavior = %+v", b)
	}
	// a behavior that sets AllowedMethods keeps them and still gets CachedMethods
	p := got.CacheBehaviors.Items[0]
	if aws.ToInt32(p.AllowedMethods.Quantity) != 3 || p.AllowedMethods.CachedMethods == nil || p.TrustedSigners == nil {
		t.Errorf("path behavior = %+v", p)
	}
	cc := got.Origins.Items[0].CustomOriginConfig
	if aws.ToInt32(cc.OriginKeepaliveTimeout) != 5 || aws.ToInt32(cc.OriginReadTimeout) != 30 ||
		cc.OriginSslProtocols == nil || len(cc.OriginSslProtocols.Items) != 1 || cc.OriginSslProtocols.Items[0] != types.SslProtocolTLSv12 {
		t.Errorf("custom origin = %+v", cc)
	}
}
