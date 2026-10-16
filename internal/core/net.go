package core

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"
	"time"
)

// SplitHost splits a request host into the label in front of a base and that
// base, without the port: "my-bucket.aws.localhost:7644" is ("my-bucket",
// "aws.localhost") and "abc.execute-api.localhost" is ("abc",
// "execute-api.localhost"). hostname (Config.Hostname, or "" for none) is a
// base of its own, and an SDK host prefix glued to it ("sync-aws.localhost",
// from Step Functions' StartSyncExecution) is dropped. IPs and single names
// have no label.
func SplitHost(host, hostname string) (label, base string) {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	switch {
	case host == hostname || net.ParseIP(host) != nil || !strings.Contains(host, "."):
		return "", host
	case hostname != "" && strings.HasSuffix(host, hostname) && !strings.HasSuffix(host, "."+hostname):
		return "", hostname
	}
	label, base, _ = strings.Cut(host, ".")
	return label, base
}

// sdkHeaders are the request headers AWS SDKs (and the Firebase and Google ones) send from browsers; a trailing
// "-" makes a prefix.
var sdkHeaders = []string{"x-amz-", "amz-sdk-", "content-", "if-", "authorization", "cache-control", "expires", "range", "x-firebase-", "x-client-version", "x-goog-"}

// CORS lets browser apps call pogi's APIs. Like LocalStack, it allows
// loopback origins (any scheme and port on localhost, *.localhost, 127.0.0.1
// and ::1), plus Cfg.CORSOrigins ("*" allows any), with the headers AWS SDKs
// send plus Cfg.CORSHeaders. It reports whether r was a preflight, which it
// has answered: 204, or 403 for any other origin. S3 buckets with a CORS
// configuration, API Gateway and Lambda URLs apply their own rules instead,
// and the admin API keeps its cross-origin protection.
func (h *Hub) CORS(w http.ResponseWriter, r *http.Request) (preflight bool) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	preflight = r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != ""
	hd := w.Header()
	hd.Add("Vary", "Origin")
	if !h.corsOrigin(origin) {
		if preflight {
			w.WriteHeader(http.StatusForbidden)
		}
		return preflight
	}
	hd.Set("Access-Control-Allow-Origin", origin)
	hd.Set("Access-Control-Expose-Headers", "*")
	if !preflight {
		return false
	}
	var allowed []string
	for _, name := range strings.Split(r.Header.Get("Access-Control-Request-Headers"), ",") {
		if name = strings.ToLower(strings.TrimSpace(name)); name != "" && h.corsHeader(name) {
			allowed = append(allowed, name)
		}
	}
	hd.Set("Access-Control-Allow-Methods", "GET, HEAD, PUT, POST, DELETE, PATCH")
	if len(allowed) > 0 {
		hd.Set("Access-Control-Allow-Headers", strings.Join(allowed, ", "))
	}
	hd.Set("Access-Control-Max-Age", "600")
	w.WriteHeader(http.StatusNoContent)
	return true
}

func (h *Hub) corsOrigin(origin string) bool {
	if slices.ContainsFunc(h.Cfg.CORSOrigins, func(o string) bool { return o == "*" || strings.EqualFold(o, origin) }) {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && Loopback(u.Hostname())
}

// Loopback reports whether host is localhost, *.localhost or a loopback IP.
func Loopback(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.IsLoopback()
}

// browserOK is the front door for web pages. A page the developer visits can
// send requests to pogi (cross-site writes) or reach it through a name that
// resolves to 127.0.0.1 (DNS rebinding). Only browser requests are checked:
// the ones with an Origin or Sec-Fetch-Site header, or a Mozilla/ User-Agent
// (a page cannot change its User-Agent; SDKs, CLIs and curl do not send
// Mozilla/). Anything else passes untouched. why names what was refused.
func (h *Hub) browserOK(r *http.Request) (ok bool, why string) {
	origin, site := r.Header.Get("Origin"), r.Header.Get("Sec-Fetch-Site")
	if origin == "" && site == "" && !strings.HasPrefix(r.Header.Get("User-Agent"), "Mozilla/") {
		return true, ""
	}
	if !h.hostOK(r.Host) {
		return false, "host " + r.Host
	}
	switch {
	case origin == "":
		// no Origin (an <img>, a link, a plain GET): only a cross-site one is a risk
		if site != "cross-site" {
			return true, ""
		}
		// A link or a typed address into the console is a navigation. Its GET only shows pages; API paths
		// stay refused, since a query-protocol GET (?Action=DeleteQueue) can change state.
		// The path is cleaned first: "/_pogi/../?Action=..." must not pass as a console path.
		p := path.Clean(r.URL.Path)
		if r.Header.Get("Sec-Fetch-Mode") == "navigate" && (r.Method == http.MethodGet || r.Method == http.MethodHead) &&
			(p == "/" && r.URL.RawQuery == "" || p == "/_pogi" || strings.HasPrefix(p, "/_pogi/")) {
			return true, ""
		}
		if u, err := url.Parse(r.Header.Get("Referer")); err == nil && u.Host != "" && h.corsOrigin(u.Scheme+"://"+u.Host) {
			return true, ""
		}
		return false, "a cross-site page"
	case origin == "null": // sandboxed frames and file:// pages
		return false, "origin null"
	case h.corsOrigin(origin):
		return true, ""
	}
	if u, err := url.Parse(origin); err == nil && strings.EqualFold(u.Host, r.Host) {
		return true, "" // same-origin: the Host already passed
	}
	return false, "origin " + origin
}

// hostOK allows the names pogi is reached by: IPs, single-label names
// (localhost, container names), *.localhost, host.docker.internal, Cfg.Hostname
// and its subdomains, the hosts of Cfg.CORSOrigins ("*" allows any) and the
// real cloud domains pogi routes by host. Any other name is a stranger's DNS
// pointing at this machine.
func (h *Hub) hostOK(hostport string) bool {
	host := hostport
	if hh, _, err := net.SplitHostPort(hostport); err == nil {
		host = hh
	}
	host = strings.ToLower(strings.Trim(strings.TrimSuffix(host, "."), "[]"))
	if net.ParseIP(host) != nil || !strings.Contains(host, ".") || Loopback(host) || host == "host.docker.internal" {
		return true
	}
	if _, base := SplitHost(host, h.Cfg.Hostname); base == h.Cfg.Hostname || strings.HasSuffix(host, "."+h.Cfg.Hostname) {
		return true
	}
	for _, o := range h.Cfg.CORSOrigins {
		if u, err := url.Parse(o); o == "*" || err == nil && strings.EqualFold(u.Hostname(), host) {
			return true
		}
	}
	for _, d := range []string{".amazonaws.com", ".amazonaws.com.cn", ".googleapis.com"} {
		if strings.HasSuffix(host, d) {
			return true
		}
	}
	return false
}

// refuse answers a request browserOK turned away.
func (h *Hub) refuse(w http.ResponseWriter, r *http.Request, why string) {
	from := cmp.Or(r.Header.Get("Origin"), r.Host)
	h.Log.Info("refused a browser request", "from", from, "why", why, "method", r.Method, "path", r.URL.Path)
	http.Error(w, "pogi: refused a browser request from "+from+"; add it to server.cors.origins in pogi.yaml", http.StatusForbidden)
}

func (h *Hub) corsHeader(name string) bool {
	for _, p := range slices.Concat(sdkHeaders, h.Cfg.CORSHeaders) {
		if p = strings.ToLower(p); p == name || p == "*" || strings.HasSuffix(p, "-") && strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// MaxUpload is the upload limit in bytes: Cfg.MaxUploadMB, or 10 MiB. It caps every request body
// and every assembled file (multipart, blocks, resumable chunks).
func (h *Hub) MaxUpload() int64 {
	if h == nil {
		return 10 << 20
	}
	if limit := int64(max(h.Cfg.MaxUploadMB, 0)) << 20; limit > 0 {
		return limit
	}
	return 10 << 20
}

// CodeLimit is the body limit for function code uploads: AWS's 50 MB direct-upload limit for a zip,
// or more when the owner raised the upload limit. base64 says the zip travels as base64 in JSON
// (Lambda), which needs a third more room.
func (h *Hub) CodeLimit(base64 bool) int64 {
	n := int64(50 << 20)
	if base64 {
		n = n/3*4 + 1<<20
	}
	return max(n, h.MaxUpload())
}

// Caution is the one sentence every refused upload carries.
func Caution(size, limit int64) string { return caution("uploads are", "", size, limit) }

// CodeCaution is Caution for function code, which is limited by the size of its zip:
// what names the code ("Lambda code", "Function code").
func CodeCaution(what string, zipSize, zipLimit int64) string {
	return caution(what+" is", " zipped", zipSize, zipLimit)
}

func caution(what, unit string, size, limit int64) string {
	mb := func(n int64) int64 { return (n + 1<<20 - 1) >> 20 }
	return fmt.Sprintf("pogi is a local emulator for tests, so %s limited to %d MB%s (this one is %d MB%s). "+
		"Raise the limit with --max-upload-mb or server.maxUploadMB in pogi.yaml.", what, mb(limit), unit, mb(size), unit)
}

// TooBig is the message for an upload that is over the limit. It logs the refusal (service and
// resource name what was refused) and returns the Caution sentence.
func (h *Hub) TooBig(service, resource string, size, limit int64) string {
	h.logTooBig(service, resource, size, limit)
	return Caution(size, limit)
}

// TooBigCode is TooBig for function code: sizes are of the zip.
func (h *Hub) TooBigCode(service, resource, what string, zipSize, zipLimit int64) string {
	h.logTooBig(service, resource, zipSize, zipLimit)
	return CodeCaution(what, zipSize, zipLimit)
}

func (h *Hub) logTooBig(service, resource string, size, limit int64) {
	if h != nil {
		h.Log.Warn("upload refused: over the limit", "service", service, "resource", resource, "bytes", size, "limit", limit)
	}
}

// BodyLimiter is for a service that needs another body limit for some requests:
// a bigger one (function code) or none at all (a negative number: image pushes).
// 0 means the default, MaxUpload.
type BodyLimiter interface{ BodyLimit(*http.Request) int64 }

// stallTimeout is how long an upload body may go without a single byte.
var stallTimeout = 30 * time.Second

// stallReader gives a body read a deadline that starts again at every Read, so a slow body that keeps
// moving is fine and a stalled one fails. It clears the deadline when the body ends: a deadline left on
// the connection would cancel a long handler (a 60 s Lambda invoke) after the body was read.
type stallReader struct {
	rc   *http.ResponseController
	r    io.Reader
	left int64 // bytes the length header promised and are still due; -1 when unknown
}

func (s *stallReader) Read(p []byte) (int, error) {
	s.rc.SetReadDeadline(time.Now().Add(stallTimeout))
	n, err := s.r.Read(p)
	if s.left >= 0 {
		s.left -= int64(n)
	}
	// A stall leaves the expired deadline: net/http reads what is left of the body before it answers,
	// and must not wait for the client again.
	if !errors.Is(err, os.ErrDeadlineExceeded) && (err != nil || s.left == 0) {
		s.rc.SetReadDeadline(time.Time{})
	}
	return n, err
}

// limitBody reads the whole body up front, up to the service's limit, so a refusal is clean: the
// service's Intercept answers a body that is too large (413) or stalled (408), and nothing keeps it.
// A negative limit lifts the cap: the body is then left for the service to stream.
func (h *Hub) limitBody(svc Service, w http.ResponseWriter, r *http.Request, c *Ctx) {
	if r.Body == nil || r.Body == http.NoBody || r.ContentLength == 0 {
		return
	}
	limit := h.MaxUpload()
	if l, ok := svc.(BodyLimiter); ok {
		if n := l.BodyLimit(r); n != 0 {
			limit = n
		}
	}
	raw := r.Body
	st := &stallReader{rc: http.NewResponseController(w), r: raw, left: r.ContentLength}
	r.Body = struct {
		io.Reader
		io.Closer
	}{st, raw}
	if limit < 0 {
		return
	}
	var body []byte
	var err error
	if r.ContentLength > limit {
		err = &http.MaxBytesError{Limit: limit}
	} else {
		body, err = io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	}
	c.read = true
	switch {
	case err == nil:
		c.body = body
		r.Body = io.NopCloser(bytes.NewReader(body))
		return
	case errors.Is(err, os.ErrDeadlineExceeded):
		h.Log.Warn("upload stalled", "service", c.Service, "path", r.URL.Path, "bytes", len(body))
		c.bodyErr = Errorf(http.StatusRequestTimeout, "RequestTimeout",
			"The upload stalled: no data arrived for %d seconds.", int(stallTimeout/time.Second))
		return
	}
	var tooBig *http.MaxBytesError
	if !errors.As(err, &tooBig) {
		c.bodyErr = Errorf(http.StatusBadRequest, "IncompleteBody", "The request body ended before its declared length.")
		return
	}
	// Drain it before answering: the Go SDK ignores Expect: 100-continue and drops the
	// connection when the answer comes while it still sends.
	n, _ := io.Copy(io.Discard, raw)
	size := max(r.ContentLength, int64(len(body))+n)
	code := "EntityTooLarge"
	if c.Cloud == Azure {
		code = "RequestBodyTooLarge"
	}
	msg := ""
	if limit > h.MaxUpload() { // a raised limit is for function code, which is counted as a zip
		what, zipSize := "Function code", size
		if c.Service == "lambda" { // the zip travels as base64 in JSON
			what, zipSize = "Lambda code", size/4*3
		}
		msg = h.TooBigCode(c.Service, r.URL.Path, what, zipSize, h.CodeLimit(false))
	} else {
		msg = h.TooBig(c.Service, r.URL.Path, size, limit)
	}
	c.bodyErr = Errorf(http.StatusRequestEntityTooLarge, code, "%s", msg)
}
