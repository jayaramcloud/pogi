package cloudfront

import (
	"bytes"
	"cmp"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dop251/goja"

	"github.com/nrndev/pogi/internal/core"
)

// edgeBase is the domain of distributions: <id>.cloudfront.localhost.
const edgeBase = "cloudfront.localhost"

// isEdge claims viewer traffic: the Host is a distribution's domain name.
func isEdge(r *http.Request) bool {
	_, base := core.SplitHost(r.Host, "")
	return strings.EqualFold(base, edgeBase)
}

// response is what a viewer gets: from the cache, an origin, CloudFront or a function.
type response struct {
	status int
	header http.Header
	body   []byte
	cache  string // X-Cache: Hit, Error, FunctionGeneratedResponse; "" is Miss (or Error by status)
	age    int    // seconds in the cache, for hits
	failed bool   // the origin could not be reached
}

// entry is a cached response.
type entry struct {
	res             response
	dist, path      string
	stored, expires time.Time
}

// edge is one viewer request on its way through a distribution.
type edge struct {
	s                     *Service
	dist, account, domain string
	cfg                   distConfig
	rid                   string // X-Amz-Cf-Id, also the function event's requestId
	viewer                *http.Request
}

// errorCodes are the error statuses CloudFront caches for ErrorCachingMinTTL.
var errorCodes = []int{400, 403, 404, 405, 414, 416, 500, 501, 502, 503, 504}

// serveEdge answers a viewer: behavior by path pattern, allowed methods,
// the viewer-request function, the cache or the origin, custom error
// pages, the response headers policy and the viewer-response function.
// ponytail: the viewer protocol policy, compression, range requests and
// Lambda@Edge (associations are stored, never run) are not applied.
func (s *Service) serveEdge(w http.ResponseWriter, r *http.Request) {
	label, _ := core.SplitHost(r.Host, "")
	e := &edge{s: s, rid: core.RandString(54, core.AlphaNum+"-_") + "==", viewer: r}
	s.mu.Lock()
	d := s.st.Distributions[strings.ToUpper(label)]
	if d != nil {
		e.dist, e.account, e.domain = d.Id, d.Account, d.domain()
		parse(d.Config, &e.cfg)
	}
	s.mu.Unlock()
	var res *response
	switch {
	case d == nil:
		res = cfError(http.StatusNotFound, "No distribution has the domain name "+r.Host+".")
	case !e.cfg.Enabled:
		res = cfError(http.StatusForbidden, "The distribution is disabled.")
	case strings.Count(r.Header.Get("Via"), "(CloudFront)") >= 3:
		res = cfError(http.StatusLoopDetected, "The request loops through CloudFront.")
	default:
		res = e.serve()
	}
	e.write(w, res)
}

func (e *edge) serve() *response {
	b := e.cfg.behavior(e.viewer.URL.Path)
	methods := b.Methods
	if len(methods) == 0 {
		methods = []string{"GET", "HEAD"}
	}
	if !slices.Contains(methods, e.viewer.Method) {
		return cfError(http.StatusForbidden, "This distribution is not configured to allow the HTTP request method that was used for this request.")
	}
	req := e.viewer.Clone(e.viewer.Context())
	onRequest, onResponse := e.functions(b)
	if onRequest != nil {
		out, err := e.run(onRequest, "viewer-request", req, nil)
		if err != nil {
			return cfError(http.StatusServiceUnavailable, "The CloudFront function associated with the CloudFront distribution is invalid or could not run: "+err.Error())
		}
		var probe struct{ StatusCode *int }
		if json.Unmarshal(out, &probe); probe.StatusCode != nil {
			var gen eventResponse
			json.Unmarshal(out, &gen)
			res := &response{status: http.StatusOK, header: http.Header{}, cache: "FunctionGeneratedResponse"}
			gen.apply(res)
			return res
		}
		var q eventRequest
		json.Unmarshal(out, &q)
		q.apply(req)
	}
	res := e.fetch(b, req)
	if er := e.cfg.errorResponse(res.status); er != nil && er.ResponsePagePath != "" {
		page := req.Clone(req.Context())
		page.Method, page.URL.Path, page.URL.RawPath, page.URL.RawQuery = http.MethodGet, er.ResponsePagePath, "", ""
		if p := e.fetch(e.cfg.behavior(page.URL.Path), page); p.status < 400 {
			code := res.status
			if n, err := strconv.Atoi(er.ResponseCode); err == nil {
				code = n
			}
			res = &response{status: code, header: p.header.Clone(), body: p.body, cache: "Error"}
		}
	}
	var policy headersPolicy
	parse(e.config(headersPolicies, b.ResponseHeadersPolicyId), &policy)
	policy.apply(res.header, e.viewer)
	if onResponse != nil {
		out, err := e.run(onResponse, "viewer-response", req, res)
		if err != nil {
			return cfError(http.StatusServiceUnavailable, "The CloudFront function associated with the CloudFront distribution is invalid or could not run: "+err.Error())
		}
		var p eventResponse
		json.Unmarshal(out, &p)
		p.apply(res)
	}
	return res
}

var hopByHop = []string{"Connection", "Keep-Alive", "Proxy-Connection", "Te", "Trailer", "Transfer-Encoding", "Upgrade"}

func (e *edge) write(w http.ResponseWriter, res *response) {
	h := w.Header()
	for k, vs := range res.header {
		if !slices.Contains(hopByHop, k) {
			h[k] = vs
		}
	}
	x := res.cache
	switch {
	case x == "FunctionGeneratedResponse" || x == "Error":
	case res.status >= 400:
		x = "Error"
	case x == "Hit":
		h.Set("Age", strconv.Itoa(res.age))
	default:
		x = "Miss"
	}
	h.Set("X-Cache", x+" from cloudfront")
	h.Set("Via", "1.1 "+cmp.Or(e.domain, edgeBase)+" (CloudFront)")
	h.Set("X-Amz-Cf-Id", e.rid)
	if e.viewer.Method != http.MethodHead {
		h.Set("Content-Length", strconv.Itoa(len(res.body)))
	}
	w.WriteHeader(res.status)
	if e.viewer.Method != http.MethodHead {
		w.Write(res.body)
	}
}

// cfError is a response CloudFront makes itself.
func cfError(status int, msg string) *response {
	body := fmt.Sprintf(`<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<HTML><HEAD><META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=iso-8859-1">
<TITLE>ERROR: The request could not be satisfied</TITLE>
</HEAD><BODY>
<H1>%d ERROR</H1>
<H2>The request could not be satisfied.</H2>
<HR noshade size="1px">
%s
</BODY></HTML>`, status, html.EscapeString(msg))
	return &response{status: status, header: http.Header{"Content-Type": {"text/html"}, "Server": {"CloudFront"}}, body: []byte(body), cache: "Error"}
}

// behavior is the first cache behavior whose path pattern matches, else the default.
func (c *distConfig) behavior(path string) *behavior {
	for i, b := range c.Behaviors {
		if glob("/"+strings.TrimPrefix(b.PathPattern, "/"), path) {
			return &c.Behaviors[i]
		}
	}
	return &c.Default
}

func (c *distConfig) errorResponse(code int) *errorResponse {
	if i := slices.IndexFunc(c.ErrorResponses, func(e errorResponse) bool { return e.ErrorCode == code }); i >= 0 {
		return &c.ErrorResponses[i]
	}
	return nil
}

// glob matches path patterns and invalidation paths: * is any run of
// characters, slashes included, and ? is one character.
// ponytail: compiled per call; cache the patterns if they get hot.
func glob(pattern, s string) bool {
	re := "(?s)^" + strings.NewReplacer(`\*`, ".*", `\?`, ".").Replace(regexp.QuoteMeta(pattern)) + "$"
	ok, _ := regexp.MatchString(re, s)
	return ok
}

// config is the config document of a policy the distribution may use; "" if none.
func (e *edge) config(k *kind, id string) string {
	if id == "" {
		return ""
	}
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	if it := e.s.item(e.account, k, id); it != nil {
		return it.Config
	}
	return ""
}

// functions returns the LIVE code of a behavior's viewer-request and viewer-response functions.
func (e *edge) functions(b *behavior) (onRequest, onResponse []byte) {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	for _, a := range b.Functions {
		if f := e.s.st.Functions[a.FunctionARN]; f != nil && f.Live != nil {
			if a.EventType == "viewer-request" {
				onRequest = f.Live.Code
			} else {
				onResponse = f.Live.Code
			}
		}
	}
	return onRequest, onResponse
}

// ---- cache ----

// sel is what one part of a policy includes: headers, cookies or query strings.
type sel struct {
	behavior string
	names    []string
}

// params are the headers, cookies and query strings in the cache key, or
// sent to the origin (the cache and origin request policies together).
type params struct{ headers, cookies, query []sel }

func parseParams(doc string) params {
	var x struct {
		H  string   `xml:"HeadersConfig>HeaderBehavior"`
		HN []string `xml:"HeadersConfig>Headers>Items>Name"`
		C  string   `xml:"CookiesConfig>CookieBehavior"`
		CN []string `xml:"CookiesConfig>Cookies>Items>Name"`
		Q  string   `xml:"QueryStringsConfig>QueryStringBehavior"`
		QN []string `xml:"QueryStringsConfig>QueryStrings>Items>Name"`
	}
	parse(doc, &x)
	return params{[]sel{{x.H, x.HN}}, []sel{{x.C, x.CN}}, []sel{{x.Q, x.QN}}}
}

// forwarded is a behavior's legacy ForwardedValues.
type forwarded struct {
	QueryString bool
	QueryKeys   []string `xml:"QueryStringCacheKeys>Items>Name"`
	Headers     []string `xml:"Headers>Items>Name"`
	Cookies     string   `xml:"Cookies>Forward"`
	CookieNames []string `xml:"Cookies>WhitelistedNames>Items>Name"`
}

func (f *forwarded) params() params {
	q, h := sel{"none", nil}, sel{"whitelist", f.Headers}
	if f.QueryString {
		q = sel{"all", nil}
		if len(f.QueryKeys) > 0 {
			q = sel{"whitelist", f.QueryKeys}
		}
	}
	if slices.Contains(f.Headers, "*") {
		h.behavior = "all"
	}
	return params{[]sel{h}, []sel{{f.Cookies, f.CookieNames}}, []sel{q}}
}

// pick reports whether name passes any of the selections.
// ponytail: names match case-insensitively; CloudFront compares query
// string and cookie names exactly.
func pick(ss []sel, name string) bool {
	for _, s := range ss {
		in := slices.ContainsFunc(s.names, func(n string) bool { return strings.EqualFold(n, name) })
		switch s.behavior {
		case "all", "allViewer", "allViewerAndWhitelistCloudFront":
			return true
		case "whitelist":
			if in {
				return true
			}
		case "allExcept":
			if !in {
				return true
			}
		}
	}
	return false
}

// forwardQuery keeps the query string parameters selected, in order.
func forwardQuery(raw string, ss []sel) string {
	var keep []string
	for _, p := range strings.Split(raw, "&") {
		name, _, _ := strings.Cut(p, "=")
		if n, err := url.QueryUnescape(name); p != "" && err == nil && pick(ss, n) {
			keep = append(keep, p)
		}
	}
	return strings.Join(keep, "&")
}

// policies resolves a behavior's cache key, what goes to the origin and the
// TTLs (min, default, max): from its cache and origin request policies, or
// its legacy ForwardedValues and TTLs.
func (e *edge) policies(b *behavior) (key, fwd params, ttl [3]int64) {
	ttl = [3]int64{0, 86400, 31536000}
	if doc := e.config(cachePolicies, b.CachePolicyId); doc != "" {
		var cp struct {
			MinTTL, DefaultTTL, MaxTTL int64
			Params                     inner `xml:"ParametersInCacheKeyAndForwardedToOrigin"`
		}
		parse(doc, &cp)
		key, ttl = parseParams(cp.Params.XML), [3]int64{cp.MinTTL, cp.DefaultTTL, cp.MaxTTL}
	} else {
		if b.Forwarded != nil {
			key = b.Forwarded.params()
		}
		for i, t := range []*int64{b.MinTTL, b.DefaultTTL, b.MaxTTL} {
			if t != nil {
				ttl[i] = *t
			}
		}
	}
	fwd = key
	if doc := e.config(requestPolicies, b.OriginRequestPolicyId); doc != "" {
		p := parseParams(doc)
		fwd = params{slices.Concat(key.headers, p.headers), slices.Concat(key.cookies, p.cookies), slices.Concat(key.query, p.query)}
	}
	return key, fwd, ttl
}

// fetch answers req from the cache or the behavior's origin, and caches
// what CloudFront would.
func (e *edge) fetch(b *behavior, req *http.Request) *response {
	key, fwd, ttls := e.policies(b)
	ck := ""
	if m := req.Method; m == http.MethodGet || m == http.MethodHead || m == http.MethodOptions && slices.Contains(b.CachedMethods, m) {
		parts := []string{e.dist, m, req.URL.Path, forwardQuery(req.URL.RawQuery, key.query)}
		for _, name := range slices.Sorted(maps.Keys(req.Header)) {
			if name != "Cookie" && pick(key.headers, name) {
				parts = append(parts, name+": "+strings.Join(req.Header[name], ","))
			}
		}
		for _, c := range req.Cookies() {
			if pick(key.cookies, c.Name) {
				parts = append(parts, "cookie: "+c.Name+"="+c.Value)
			}
		}
		ck = strings.Join(parts, "\n")
		if res := e.s.lookup(ck); res != nil {
			return res
		}
	}
	res := e.origin(b, fwd, req)
	if ttl := e.ttl(res, ttls); ck != "" && ttl > 0 {
		e.s.store(ck, e.dist, req.URL.Path, res, ttl)
	}
	return res
}

// ttl is how long CloudFront keeps a response. Successes stay for the
// origin's s-maxage, max-age or Expires within [MinTTL, MaxTTL], else
// DefaultTTL; no-store, no-cache and private mean MinTTL. Errors stay for
// their custom error response's ErrorCachingMinTTL, 10 seconds by default.
func (e *edge) ttl(res *response, t [3]int64) time.Duration {
	if slices.Contains(errorCodes, res.status) {
		ttl := int64(10)
		if er := e.cfg.errorResponse(res.status); er != nil && er.ErrorCachingMinTTL != nil {
			ttl = *er.ErrorCachingMinTTL
		}
		return time.Duration(ttl) * time.Second
	}
	if !slices.Contains([]int{200, 203, 300, 301, 410}, res.status) {
		return 0
	}
	clamp := func(v int64) int64 { return min(max(v, t[0]), t[2]) }
	cc, age := strings.ToLower(res.header.Get("Cache-Control")), t[1]
	if v, ok := directive(cc, "s-maxage"); ok {
		age = clamp(v)
	} else if v, ok := directive(cc, "max-age"); ok {
		age = clamp(v)
	} else if exp, err := http.ParseTime(res.header.Get("Expires")); err == nil {
		age = clamp(int64(time.Until(exp).Seconds()))
	}
	if strings.Contains(cc, "no-store") || strings.Contains(cc, "no-cache") || strings.Contains(cc, "private") {
		age = t[0]
	}
	return time.Duration(age) * time.Second
}

func directive(cc, name string) (int64, bool) {
	for _, d := range strings.Split(cc, ",") {
		if k, v, ok := strings.Cut(strings.TrimSpace(d), "="); ok && k == name {
			n, err := strconv.ParseInt(strings.Trim(v, `"`), 10, 64)
			return n, err == nil
		}
	}
	return 0, false
}

func (s *Service) lookup(key string) *response {
	s.mu.Lock()
	defer s.mu.Unlock()
	en := s.cache[key]
	if en == nil || time.Now().After(en.expires) {
		delete(s.cache, key)
		return nil
	}
	res := en.res
	res.header, res.cache, res.age = res.header.Clone(), "Hit", int(time.Since(en.stored).Seconds())
	return &res
}

func (s *Service) store(key, dist, path string, res *response, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cache) >= 10000 {
		clear(s.cache) // ponytail: a crude bound; an LRU if big sites live here
	}
	cp := *res
	cp.header = res.header.Clone()
	now := time.Now()
	s.cache[key] = &entry{res: cp, dist: dist, path: path, stored: now, expires: now.Add(ttl)}
}

// purge drops a distribution's cached responses whose path matches an
// invalidation path; lock held.
func (s *Service) purge(dist string, paths []string) {
	for k, en := range s.cache {
		if en.dist == dist && slices.ContainsFunc(paths, func(p string) bool { return glob(p, en.path) }) {
			delete(s.cache, k)
		}
	}
}

// ---- origins ----

// client fetches from origins pogi does not serve itself. Redirects go
// back to the viewer, as from CloudFront.
// ponytail: origin certificates are not verified.
var client = &http.Client{
	Transport:     &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, MaxIdleConnsPerHost: 32, IdleConnTimeout: time.Minute},
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// origin sends req to the behavior's origin, or through its origin group:
// GET, HEAD and OPTIONS fail over to the next member on the group's status
// codes or when an origin cannot be reached.
func (e *edge) origin(b *behavior, fwd params, req *http.Request) *response {
	ids, codes := []string{b.TargetOriginId}, []int(nil)
	if i := slices.IndexFunc(e.cfg.OriginGroups, func(g originGroup) bool { return g.Id == b.TargetOriginId }); i >= 0 {
		ids, codes = e.cfg.OriginGroups[i].Members, e.cfg.OriginGroups[i].Codes
	}
	res := cfError(http.StatusBadGateway, "The origin group has no members.")
	for n, id := range ids {
		i := slices.IndexFunc(e.cfg.Origins, func(o origin) bool { return o.Id == id })
		if i < 0 {
			return cfError(http.StatusBadGateway, "No origin has the id "+id+".")
		}
		res = e.call(&e.cfg.Origins[i], fwd, req)
		retry := res.failed || slices.Contains(codes, res.status)
		if !retry || n == len(ids)-1 || !slices.Contains([]string{"GET", "HEAD", "OPTIONS"}, req.Method) {
			break
		}
	}
	return res
}

// call makes one origin request: what the policies forward of the viewer's
// request, the origin's custom headers and CloudFront's own. S3 origins and
// names pogi serves (*.localhost) are answered in-process.
func (e *edge) call(o *origin, fwd params, req *http.Request) *response {
	path := req.URL.Path
	if path == "/" && e.cfg.DefaultRootObject != "" {
		path += e.cfg.DefaultRootObject
	}
	target := &url.URL{Path: o.OriginPath + path, RawQuery: forwardQuery(req.URL.RawQuery, fwd.query)}
	h := http.Header{}
	for k, vs := range req.Header {
		if k != "Cookie" && pick(fwd.headers, k) {
			h[k] = vs
		}
	}
	var cookies []string
	for _, c := range req.Cookies() {
		if pick(fwd.cookies, c.Name) {
			cookies = append(cookies, c.Name+"="+c.Value)
		}
	}
	if len(cookies) > 0 {
		h.Set("Cookie", strings.Join(cookies, "; "))
	}
	for _, ch := range o.Headers {
		h.Set(ch.HeaderName, ch.HeaderValue)
	}
	if h.Get("User-Agent") == "" {
		h.Set("User-Agent", "Amazon CloudFront")
	}
	via := "1.1 " + e.domain + " (CloudFront)"
	if prior := req.Header.Get("Via"); prior != "" {
		via = prior + ", " + via
	}
	h.Set("Via", via)
	h.Set("X-Amz-Cf-Id", e.rid)
	if ip, _, err := net.SplitHostPort(e.viewer.RemoteAddr); err == nil {
		if prior := req.Header.Get("X-Forwarded-For"); prior != "" {
			ip = prior + ", " + ip
		}
		h.Set("X-Forwarded-For", ip)
	}
	body := core.Body(e.viewer)
	host := strings.ToLower(o.DomainName)
	if bucket, ok := s3Bucket(host); ok {
		// ponytail: origin access controls, identities and bucket policies are
		// not enforced; pogi reads the bucket as its owner, so a missing key is
		// a 404 rather than the 403 an OAC without s3:ListBucket gets.
		p := (&url.URL{Path: "/" + bucket + target.Path, RawQuery: target.RawQuery}).RequestURI()
		status, hdr, data := e.s.hub.Do(e.account, core.DefaultRegion, "s3", req.Method, p, h, body)
		return &response{status: status, header: hdr, body: data}
	}
	scheme, port := "http", 80
	if c := o.Custom; c != nil {
		if c.OriginProtocolPolicy == "https-only" || c.OriginProtocolPolicy == "match-viewer" && e.viewer.TLS != nil {
			scheme, port = "https", cmp.Or(c.HTTPSPort, 443)
		} else {
			port = cmp.Or(c.HTTPPort, 80)
		}
	}
	target.Scheme, target.Host = scheme, host
	if !(scheme == "http" && port == 80 || scheme == "https" && port == 443) {
		target.Host = net.JoinHostPort(host, strconv.Itoa(port))
	}
	out, err := http.NewRequest(req.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		return cfError(http.StatusBadGateway, err.Error())
	}
	out.Header = h
	if hn := e.s.hub.Cfg.Hostname; strings.HasSuffix(host, ".localhost") || hn != "localhost" && strings.HasSuffix("."+host, "."+hn) {
		out.RemoteAddr = e.viewer.RemoteAddr
		rec := httptest.NewRecorder()
		e.s.hub.ServeHTTP(rec, out)
		r := rec.Result()
		return &response{status: r.StatusCode, header: r.Header, body: rec.Body.Bytes()}
	}
	timeout := 30 * time.Second
	if c := o.Custom; c != nil && c.OriginReadTimeout > 0 {
		timeout = time.Duration(c.OriginReadTimeout) * time.Second
	}
	ctx, cancel := context.WithTimeout(e.viewer.Context(), timeout)
	defer cancel()
	// ConnectionAttempts (default 3) tries for a request that can be repeated; a timeout is not repeated (floci #5242)
	attempts := 1
	if slices.Contains([]string{"GET", "HEAD", "OPTIONS"}, req.Method) {
		attempts = cmp.Or(o.ConnectionAttempts, 3)
	}
	for range attempts {
		var r *http.Response
		if r, err = client.Do(out.WithContext(ctx)); err == nil {
			var data []byte
			data, err = io.ReadAll(r.Body)
			r.Body.Close()
			if err == nil {
				return &response{status: r.StatusCode, header: r.Header, body: data}
			}
		}
		if ctx.Err() != nil {
			break
		}
	}
	res := cfError(http.StatusBadGateway, "CloudFront wasn't able to connect to the origin: "+err.Error())
	if errors.Is(err, context.DeadlineExceeded) {
		res = cfError(http.StatusGatewayTimeout, "The origin did not respond in time.")
	}
	res.failed = true
	return res
}

// s3Bucket is the bucket of an S3 origin domain: <bucket>.s3.<region>.amazonaws.com,
// <bucket>.s3.amazonaws.com, or a legacy s3-<region> or website endpoint.
func s3Bucket(host string) (string, bool) {
	if strings.HasSuffix(host, ".amazonaws.com") || strings.HasSuffix(host, ".amazonaws.com.cn") {
		for _, sep := range []string{".s3.", ".s3-"} {
			if i := strings.Index(host, sep); i > 0 {
				return host[:i], true
			}
		}
	}
	return "", false
}

// ---- response headers policies ----

type headersPolicy struct {
	Cors *struct {
		Origins     []string `xml:"AccessControlAllowOrigins>Items>Origin"`
		Headers     []string `xml:"AccessControlAllowHeaders>Items>Header"`
		Methods     []string `xml:"AccessControlAllowMethods>Items>Method"`
		Expose      []string `xml:"AccessControlExposeHeaders>Items>Header"`
		Credentials bool     `xml:"AccessControlAllowCredentials"`
		MaxAge      *int     `xml:"AccessControlMaxAgeSec"`
		Override    bool     `xml:"OriginOverride"`
	} `xml:"CorsConfig"`
	Security struct {
		HSTS *struct {
			Override, IncludeSubdomains, Preload bool
			MaxAge                               int `xml:"AccessControlMaxAgeSec"`
		} `xml:"StrictTransportSecurity"`
		ContentType *struct{ Override bool } `xml:"ContentTypeOptions"`
		Frame       *struct {
			Override    bool
			FrameOption string
		} `xml:"FrameOptions"`
		XSS *struct {
			Override, Protection, ModeBlock bool
			ReportUri                       string
		} `xml:"XSSProtection"`
		Referrer *struct {
			Override       bool
			ReferrerPolicy string
		} `xml:"ReferrerPolicy"`
		CSP *struct {
			Override              bool
			ContentSecurityPolicy string
		} `xml:"ContentSecurityPolicy"`
	} `xml:"SecurityHeadersConfig"`
	Custom []struct {
		Header, Value string
		Override      bool
	} `xml:"CustomHeadersConfig>Items>ResponseHeadersPolicyCustomHeader"`
	Remove []string `xml:"RemoveHeadersConfig>Items>ResponseHeadersPolicyRemoveHeader>Header"`
}

// apply adds the policy's CORS, security and custom headers to a response
// to r; a header the origin sent stays unless the policy overrides it.
// ponytail: no Server-Timing header.
func (p *headersPolicy) apply(h http.Header, r *http.Request) {
	set := func(name, value string, override bool) {
		if override || h.Get(name) == "" {
			h.Set(name, value)
		}
	}
	if c, origin := p.Cors, r.Header.Get("Origin"); c != nil && origin != "" &&
		(slices.Contains(c.Origins, "*") || slices.Contains(c.Origins, origin)) {
		set("Access-Control-Allow-Origin", map[bool]string{true: "*", false: origin}[slices.Contains(c.Origins, "*")], c.Override)
		if c.Credentials {
			set("Access-Control-Allow-Credentials", "true", c.Override)
		}
		if len(c.Expose) > 0 {
			set("Access-Control-Expose-Headers", strings.Join(c.Expose, ", "), c.Override)
		}
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			methods := c.Methods
			if slices.Contains(methods, "ALL") {
				methods = []string{"GET", "POST", "OPTIONS", "PUT", "DELETE", "PATCH", "HEAD"}
			}
			set("Access-Control-Allow-Methods", strings.Join(methods, ", "), c.Override)
			set("Access-Control-Allow-Headers", strings.Join(c.Headers, ", "), c.Override)
			if c.MaxAge != nil {
				set("Access-Control-Max-Age", strconv.Itoa(*c.MaxAge), c.Override)
			}
		}
	}
	sec := p.Security
	if x := sec.HSTS; x != nil {
		v := "max-age=" + strconv.Itoa(x.MaxAge)
		if x.IncludeSubdomains {
			v += "; includeSubDomains"
		}
		if x.Preload {
			v += "; preload"
		}
		set("Strict-Transport-Security", v, x.Override)
	}
	if x := sec.ContentType; x != nil {
		set("X-Content-Type-Options", "nosniff", x.Override)
	}
	if x := sec.Frame; x != nil {
		set("X-Frame-Options", x.FrameOption, x.Override)
	}
	if x := sec.XSS; x != nil {
		v := "0"
		if x.Protection {
			v = "1"
			if x.ModeBlock {
				v += "; mode=block"
			}
			if x.ReportUri != "" {
				v += "; report=" + x.ReportUri
			}
		}
		set("X-XSS-Protection", v, x.Override)
	}
	if x := sec.Referrer; x != nil {
		set("Referrer-Policy", x.ReferrerPolicy, x.Override)
	}
	if x := sec.CSP; x != nil {
		set("Content-Security-Policy", x.ContentSecurityPolicy, x.Override)
	}
	for _, c := range p.Custom {
		set(c.Header, c.Value, c.Override)
	}
	for _, name := range p.Remove {
		h.Del(name)
	}
}

// ---- CloudFront Functions ----

// value is a header, query string parameter or cookie in a function event.
type value struct {
	Value      string  `json:"value"`
	Attributes string  `json:"attributes,omitempty"`
	MultiValue []value `json:"multiValue,omitempty"`
}

func (v value) all() []value {
	if len(v.MultiValue) > 0 {
		return v.MultiValue
	}
	return []value{v}
}

// add adds a value under k; a repeated name keeps its first value and lists all in multiValue.
func add(m map[string]value, k string, v value) {
	if old, ok := m[k]; ok {
		v = value{Value: old.Value, Attributes: old.Attributes, MultiValue: append(old.all(), v)}
	}
	m[k] = v
}

type eventRequest struct {
	Method      string           `json:"method"`
	URI         string           `json:"uri"`
	QueryString map[string]value `json:"querystring"`
	Headers     map[string]value `json:"headers"`
	Cookies     map[string]value `json:"cookies"`
}

type eventResponse struct {
	StatusCode        int              `json:"statusCode"`
	StatusDescription string           `json:"statusDescription,omitempty"`
	Headers           map[string]value `json:"headers"`
	Cookies           map[string]value `json:"cookies"`
	Body              any              `json:"body,omitempty"` // a generated response's: text, or {encoding, data}
}

func toEventRequest(r *http.Request) eventRequest {
	ev := eventRequest{Method: r.Method, URI: r.URL.EscapedPath(), QueryString: map[string]value{}, Headers: map[string]value{}, Cookies: map[string]value{}}
	for _, p := range strings.Split(r.URL.RawQuery, "&") {
		if k, v, _ := strings.Cut(p, "="); p != "" {
			add(ev.QueryString, k, value{Value: v})
		}
	}
	add(ev.Headers, "host", value{Value: r.Host})
	for k, vs := range r.Header {
		for _, v := range vs {
			if k != "Cookie" {
				add(ev.Headers, strings.ToLower(k), value{Value: v})
			}
		}
	}
	for _, c := range r.Cookies() {
		add(ev.Cookies, c.Name, value{Value: c.Value})
	}
	return ev
}

// apply makes r the request a viewer-request function returned.
func (q *eventRequest) apply(r *http.Request) {
	if p, err := url.PathUnescape(q.URI); err == nil && q.URI != "" {
		r.URL.Path, r.URL.RawPath = p, q.URI
	}
	var qs, cs []string
	for _, k := range core.SortedKeys(q.QueryString) {
		for _, v := range q.QueryString[k].all() {
			qs = append(qs, k+"="+v.Value)
		}
	}
	r.URL.RawQuery = strings.Join(qs, "&")
	r.Header = http.Header{}
	for k, v := range q.Headers {
		for _, x := range v.all() {
			if k != "host" {
				r.Header.Add(k, x.Value)
			}
		}
	}
	for _, k := range core.SortedKeys(q.Cookies) {
		for _, x := range q.Cookies[k].all() {
			cs = append(cs, k+"="+x.Value)
		}
	}
	if len(cs) > 0 {
		r.Header.Set("Cookie", strings.Join(cs, "; "))
	}
}

func toEventResponse(res *response) eventResponse {
	ev := eventResponse{StatusCode: res.status, StatusDescription: http.StatusText(res.status), Headers: map[string]value{}, Cookies: map[string]value{}}
	for k, vs := range res.header {
		for _, v := range vs {
			if k != "Set-Cookie" {
				add(ev.Headers, strings.ToLower(k), value{Value: v})
				continue
			}
			name, rest, _ := strings.Cut(v, "=")
			val, attrs, _ := strings.Cut(rest, ";")
			add(ev.Cookies, name, value{Value: val, Attributes: strings.TrimSpace(attrs)})
		}
	}
	return ev
}

// apply makes res the response a function returned.
func (p *eventResponse) apply(res *response) {
	if p.StatusCode != 0 {
		res.status = p.StatusCode
	}
	res.header = http.Header{}
	for k, v := range p.Headers {
		for _, x := range v.all() {
			res.header.Add(k, x.Value)
		}
	}
	for k, v := range p.Cookies {
		for _, x := range v.all() {
			c := k + "=" + x.Value
			if x.Attributes != "" {
				c += "; " + x.Attributes
			}
			res.header.Add("Set-Cookie", c)
		}
	}
	switch b := p.Body.(type) {
	case string:
		res.body = []byte(b)
	case map[string]any:
		data, _ := b["data"].(string)
		res.body = []byte(data)
		if b["encoding"] == "base64" {
			res.body, _ = base64.StdEncoding.DecodeString(data)
		}
	}
}

// run calls a function with the event of req (and res, for viewer-response).
func (e *edge) run(code []byte, kind string, req *http.Request, res *response) ([]byte, error) {
	ip, _, _ := net.SplitHostPort(e.viewer.RemoteAddr)
	ev := map[string]any{
		"version": "1.0",
		"context": map[string]string{"distributionDomainName": e.domain, "distributionId": e.dist, "eventType": kind, "requestId": e.rid},
		"viewer":  map[string]string{"ip": ip},
		"request": toEventRequest(req),
	}
	if res != nil {
		ev["response"] = toEventResponse(res)
	}
	b, _ := json.Marshal(ev)
	out, _, err := runFunction(code, b)
	return out, err
}

// cfImport is cloudfront-js-2.0's `import cf from 'cloudfront'`.
var cfImport = regexp.MustCompile(`(?m)^\s*import\s+(\w+)\s+from\s+['"]cloudfront['"]\s*;?`)

// runFunction calls a function's handler with event (JSON) on goja and
// returns the JSON of what it returned, with its console.log lines. Runtime
// 2.0's async handlers are awaited.
// ponytail: no KeyValueStore (cf.kvs) or origin helpers, and no require()
// modules (crypto, querystring); a handler gets one second, not 1 ms.
func runFunction(code, event []byte) (out []byte, logs []string, err error) {
	vm := goja.New()
	console := vm.NewObject()
	console.Set("log", func(call goja.FunctionCall) goja.Value {
		var parts []string
		for _, a := range call.Arguments {
			parts = append(parts, a.String())
		}
		logs = append(logs, strings.Join(parts, " "))
		return goja.Undefined()
	})
	vm.Set("console", console)
	timer := time.AfterFunc(time.Second, func() { vm.Interrupt("the function ran too long") })
	defer timer.Stop()
	if _, err = vm.RunString(cfImport.ReplaceAllString(string(code), "var $1 = {};")); err != nil {
		return nil, logs, err
	}
	handler, ok := goja.AssertFunction(vm.Get("handler"))
	if !ok {
		return nil, logs, errors.New("the function has no handler")
	}
	jsonObj := vm.Get("JSON").ToObject(vm)
	parseJSON, _ := goja.AssertFunction(jsonObj.Get("parse"))
	stringify, _ := goja.AssertFunction(jsonObj.Get("stringify"))
	ev, err := parseJSON(goja.Undefined(), vm.ToValue(string(event)))
	if err != nil {
		return nil, logs, err
	}
	v, err := handler(goja.Undefined(), ev)
	if err != nil {
		return nil, logs, err
	}
	if p, ok := v.Export().(*goja.Promise); ok {
		switch p.State() {
		case goja.PromiseStateFulfilled:
			v = p.Result()
		case goja.PromiseStateRejected:
			return nil, logs, fmt.Errorf("%v", p.Result())
		default:
			return nil, logs, errors.New("the handler's promise did not settle")
		}
	}
	s, err := stringify(goja.Undefined(), v)
	if err != nil || goja.IsUndefined(s) {
		return nil, logs, cmp.Or(err, errors.New("the handler returned nothing"))
	}
	return []byte(s.String()), logs, nil
}
