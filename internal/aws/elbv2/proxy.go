package elbv2

import (
	"bytes"
	"cmp"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/core"
)

// ---- ALB traffic ----

func hostPort(hp string) (host, port string) {
	if h, p, err := net.SplitHostPort(hp); err == nil {
		return h, p
	}
	return hp, ""
}

// isLB claims ALB traffic on pogi's port: the Host is a load balancer's DNS name.
func isLB(r *http.Request) bool {
	h, _ := hostPort(r.Host)
	return strings.HasSuffix(strings.ToLower(h), ".elb.localhost")
}

// serveLB answers a request that reached pogi's port for a load balancer:
// the listener on the Host's port, else 443 (TLS) or 80, else the first of
// the same protocol, else the first.
func (s *Service) serveLB(w http.ResponseWriter, r *http.Request) {
	host, port := hostPort(r.Host)
	want, proto := "80", "HTTP"
	if r.TLS != nil {
		want, proto = "443", "HTTPS"
	}
	s.mu.Lock()
	var lb *loadBalancer
	for _, x := range s.st.LBs {
		if strings.EqualFold(x.DNSName, host) {
			lb = x
		}
	}
	var arn, typ string
	var ports []string
	if lb != nil {
		typ = lb.Type
		ls := s.listenersOf(lb.LoadBalancerArn)
		for _, pick := range []func(*listener) bool{
			func(l *listener) bool { return strconv.Itoa(portOf(l)) == port },
			func(l *listener) bool { return strconv.Itoa(portOf(l)) == want },
			func(l *listener) bool { return l.Protocol == proto },
			func(l *listener) bool { return true },
		} {
			if i := slices.IndexFunc(ls, pick); i >= 0 && arn == "" {
				arn = ls[i].ListenerArn
			}
		}
		for _, p := range lb.Pogi.Ports {
			ports = append(ports, "127.0.0.1:"+strconv.Itoa(p))
		}
	}
	s.mu.Unlock()
	switch {
	case lb == nil:
		http.Error(w, "pogi: no load balancer has the DNS name "+host, http.StatusNotFound)
	case typ != "application":
		http.Error(w, fmt.Sprintf("pogi: %s is a %s load balancer, which does not speak HTTP on pogi's port; connect to %s", host, typ, strings.Join(ports, " or ")), http.StatusBadRequest)
	case arn == "":
		http.Error(w, "503 Service Temporarily Unavailable", http.StatusServiceUnavailable)
	default:
		s.route(w, r, arn)
	}
}

// route answers r with the actions of the listener's first matching rule.
func (s *Service) route(w http.ResponseWriter, r *http.Request, arn string) {
	host, _ := hostPort(r.Host)
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	s.mu.Lock()
	l, ok := s.st.Listeners[arn]
	if !ok {
		s.mu.Unlock()
		http.Error(w, "503 Service Temporarily Unavailable", http.StatusServiceUnavailable)
		return
	}
	lc := *l
	var a action
	for _, ru := range s.rulesOf(arn) {
		if !slices.ContainsFunc(ru.Conditions, func(c condition) bool { return !c.match(r, host, ip) }) {
			// the first forward, redirect or fixed-response; authenticate-* pass through
			i := slices.IndexFunc(ru.Actions, func(x action) bool {
				return x.Type == "forward" || x.Type == "redirect" || x.Type == "fixed-response"
			})
			if i >= 0 {
				a = ru.Actions[i]
			}
			break
		}
	}
	var to, tgArn, scheme string
	var lambda, multi bool
	var sticky *http.Cookie
	if a.Type == "forward" {
		var g *targetGroup
		g, sticky = s.pickGroup(a, r)
		if to = s.choose(g); g != nil {
			tgArn, lambda, scheme = g.TargetGroupArn, g.TargetType == "lambda", strings.ToLower(cmp.Or(g.Protocol, "http"))
			multi = g.attr("lambda.multi_value_headers.enabled") == "true"
		}
	}
	s.mu.Unlock()
	fwd := forwarded(r, &lc)
	switch a.Type {
	case "fixed-response":
		f := a.FixedResponseConfig
		if f.ContentType != "" {
			w.Header().Set("Content-Type", f.ContentType)
		}
		code, _ := strconv.Atoi(f.StatusCode)
		w.WriteHeader(code)
		io.WriteString(w, f.MessageBody)
	case "redirect":
		code := map[string]int{"HTTP_301": http.StatusMovedPermanently, "HTTP_302": http.StatusFound}[a.RedirectConfig.StatusCode]
		http.Redirect(w, r, redirect(a.RedirectConfig, r, &lc), code)
	case "forward":
		if sticky != nil {
			http.SetCookie(w, sticky)
		}
		switch {
		case to == "":
			http.Error(w, "503 Service Temporarily Unavailable", http.StatusServiceUnavailable)
		case lambda:
			s.invoke(w, r, to, tgArn, multi, fwd)
		default:
			proxy(w, r, scheme, to, fwd)
		}
	default:
		http.Error(w, "500 Internal Server Error", http.StatusInternalServerError) // no terminal action
	}
}

func (c *condition) match(r *http.Request, host, ip string) bool {
	switch c.Field {
	case "path-pattern":
		return c.PathPatternConfig.match(r.URL.Path, false)
	case "host-header":
		return c.HostHeaderConfig.match(host, true)
	case "http-header":
		v := values{c.HttpHeaderConfig.Values, c.HttpHeaderConfig.RegexValues}
		return slices.ContainsFunc(r.Header.Values(c.HttpHeaderConfig.HttpHeaderName), func(h string) bool { return v.match(h, true) })
	case "http-request-method":
		return slices.Contains(c.HttpRequestMethodConfig.Values, r.Method)
	case "query-string":
		q := r.URL.Query()
		return slices.ContainsFunc(c.QueryStringConfig.Values, func(p kv) bool {
			for k, vs := range q {
				if (p.Key == "" || wild(p.Key, k, true)) && slices.ContainsFunc(vs, func(v string) bool { return wild(p.Value, v, true) }) {
					return true
				}
			}
			return false
		})
	case "source-ip":
		a, err := netip.ParseAddr(ip)
		return err == nil && slices.ContainsFunc(c.SourceIpConfig.Values, func(cidr string) bool {
			p, err := netip.ParsePrefix(cidr)
			return err == nil && p.Contains(a.Unmap())
		})
	}
	return false
}

func (v *values) match(s string, fold bool) bool {
	return v != nil && (slices.ContainsFunc(v.Values, func(p string) bool { return wild(p, s, fold) }) ||
		slices.ContainsFunc(v.RegexValues, func(p string) bool { ok, _ := regexp.MatchString(p, s); return ok }))
}

// wild matches ELB's wildcards: * is any run of characters, ? any one.
// ponytail: compiled per request; cache the patterns if rules get hot.
func wild(pattern, s string, fold bool) bool {
	re := "(?s)^" + strings.NewReplacer(`\*`, ".*", `\?`, ".").Replace(regexp.QuoteMeta(pattern)) + "$"
	if fold {
		re = "(?i)" + re
	}
	ok, _ := regexp.MatchString(re, s)
	return ok
}

// redirect builds a redirect's URL: #{protocol}, #{host}, #{port}, #{path}
// and #{query} are the request's; #{port} is the port the client used.
func redirect(rc *struct{ Protocol, Port, Host, Path, Query, StatusCode string }, r *http.Request, l *listener) string {
	host, port := hostPort(r.Host)
	sub := strings.NewReplacer("#{protocol}", strings.ToLower(l.Protocol), "#{host}", host, "#{port}", cmp.Or(port, strconv.Itoa(portOf(l))),
		"#{path}", strings.TrimPrefix(r.URL.Path, "/"), "#{query}", r.URL.RawQuery)
	proto, p := strings.ToLower(sub.Replace(rc.Protocol)), sub.Replace(rc.Port)
	u := proto + "://" + sub.Replace(rc.Host)
	if !(proto == "http" && p == "80" || proto == "https" && p == "443") {
		u += ":" + p
	}
	u += sub.Replace(rc.Path)
	if q := sub.Replace(rc.Query); q != "" {
		u += "?" + q
	}
	return u
}

// pickGroup chooses a forward action's target group: the one a stickiness
// cookie names, else one by weight; lock held.
func (s *Service) pickGroup(a action, r *http.Request) (*targetGroup, *http.Cookie) {
	f := a.ForwardConfig
	if f == nil {
		return s.st.Groups[a.TargetGroupArn], nil
	}
	sticky := f.TargetGroupStickinessConfig != nil && f.TargetGroupStickinessConfig.Enabled
	if ck, err := r.Cookie("AWSALBTG"); sticky && err == nil &&
		slices.ContainsFunc(f.TargetGroups, func(t weighted) bool { return t.TargetGroupArn == ck.Value }) {
		return s.st.Groups[ck.Value], nil
	}
	total := 0
	for _, t := range f.TargetGroups {
		total += or(t.Weight, 1)
	}
	if total == 0 {
		return nil, nil
	}
	n := rand.IntN(total)
	for _, t := range f.TargetGroups {
		if n -= or(t.Weight, 1); n < 0 {
			if !sticky {
				return s.st.Groups[t.TargetGroupArn], nil
			}
			return s.st.Groups[t.TargetGroupArn], &http.Cookie{Name: "AWSALBTG", Value: t.TargetGroupArn, Path: "/",
				MaxAge: or(f.TargetGroupStickinessConfig.DurationSeconds, 3600)}
		}
	}
	return nil, nil
}

// choose picks a target of g round-robin among the healthy ones, or among
// all that pogi can reach when none is healthy (ELB fails open). It returns
// the address, or the function ARN of a Lambda target; lock held.
func (s *Service) choose(g *targetGroup) string {
	if g == nil {
		return ""
	}
	var healthy, all []string
	for _, t := range g.Targets {
		if a := s.addr(g, t, t.Port); a != "" && t.State != "draining" {
			all = append(all, a)
			if t.State == "healthy" {
				healthy = append(healthy, a)
			}
		}
	}
	if len(healthy) > 0 {
		all = healthy
	}
	if len(all) == 0 {
		return ""
	}
	n := s.rr[g.TargetGroupArn]
	s.rr[g.TargetGroupArn] = n + 1
	return all[n%len(all)]
}

// addr is where pogi reaches a target on port: the endpoint the owning
// service recorded (ECS tasks, EC2 instances), else an IP target's own
// address when this host can dial it, else the port serving an ALB target's
// listener; "" when there is none; lock held.
func (s *Service) addr(g *targetGroup, t *target, port int) string {
	switch g.TargetType {
	case "lambda":
		return t.Id
	case "alb":
		if lb := s.st.LBs[t.Id]; lb != nil && lb.Pogi.Ports[port] != 0 {
			return "127.0.0.1:" + strconv.Itoa(lb.Pogi.Ports[port])
		}
		return ""
	}
	kind := "instance"
	if g.TargetType == "ip" {
		kind = "ip"
	}
	if a, ok := s.hub.Endpoint(kind + ":" + t.Id + ":" + strconv.Itoa(port)); ok {
		return a
	}
	if kind == "ip" && local(t.Id) {
		return net.JoinHostPort(t.Id, strconv.Itoa(port))
	}
	return ""
}

// local reports whether pogi can dial ip itself: loopback, or an address of this host.
func local(ip string) bool {
	a := net.ParseIP(ip)
	if a == nil {
		return false
	}
	if a.IsLoopback() {
		return true
	}
	addrs, _ := net.InterfaceAddrs()
	return slices.ContainsFunc(addrs, func(x net.Addr) bool { n, ok := x.(*net.IPNet); return ok && n.IP.Equal(a) })
}

// forwarded are the headers ELB adds to what it passes on.
func forwarded(r *http.Request, l *listener) http.Header {
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	if prior := r.Header.Get("X-Forwarded-For"); prior != "" {
		ip = prior + ", " + ip
	}
	trace := r.Header.Get("X-Amzn-Trace-Id")
	if trace == "" {
		trace = fmt.Sprintf("Root=1-%08x-%s", time.Now().Unix(), core.RandHex(12))
	}
	return http.Header{"X-Forwarded-For": {ip}, "X-Forwarded-Proto": {strings.ToLower(l.Protocol)},
		"X-Forwarded-Port": {strconv.Itoa(portOf(l))}, "X-Amzn-Trace-Id": {trace}}
}

var upstream = &http.Transport{
	DialContext:         (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
	TLSClientConfig:     &tls.Config{InsecureSkipVerify: true}, // ELB does not verify target certificates
	MaxIdleConnsPerHost: 32,
	IdleConnTimeout:     time.Minute,
}

// proxy passes r to a target with the Host kept and ELB's headers added.
// ponytail: the request body is buffered (pogi's gateway reads it anyway).
func proxy(w http.ResponseWriter, r *http.Request, scheme, addr string, fwd http.Header) {
	body := core.Body(r)
	(&httputil.ReverseProxy{
		Transport: upstream,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme, pr.Out.URL.Host, pr.Out.Host = scheme, addr, pr.In.Host
			maps.Copy(pr.Out.Header, fwd)
			pr.Out.Body, pr.Out.ContentLength, pr.Out.TransferEncoding = http.NoBody, 0, nil
			if len(body) > 0 {
				pr.Out.Body, pr.Out.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
			}
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) {
				http.Error(w, "504 Gateway Time-out", http.StatusGatewayTimeout)
				return
			}
			http.Error(w, "502 Bad Gateway", http.StatusBadGateway)
		},
	}).ServeHTTP(w, r)
}

// invoke runs a Lambda target with the ALB event and answers with its
// result; a function error or a malformed result is a 502.
func (s *Service) invoke(w http.ResponseWriter, r *http.Request, fn, tgArn string, multi bool, fwd http.Header) {
	body := core.Body(r)
	headers := r.Header.Clone()
	maps.Copy(headers, fwd)
	headers.Set("Host", r.Host)
	ev := map[string]any{"requestContext": map[string]any{"elb": map[string]string{"targetGroupArn": tgArn}},
		"httpMethod": r.Method, "path": r.URL.Path, "body": string(body), "isBase64Encoded": false}
	if !utf8.Valid(body) {
		ev["body"], ev["isBase64Encoded"] = base64.StdEncoding.EncodeToString(body), true
	}
	query := map[string][]string{} // as sent: ELB does not decode the query string
	for _, p := range strings.Split(r.URL.RawQuery, "&") {
		if k, v, _ := strings.Cut(p, "="); p != "" {
			query[k] = append(query[k], v)
		}
	}
	if multi {
		mh := map[string][]string{}
		for k, v := range headers {
			mh[strings.ToLower(k)] = v
		}
		ev["multiValueHeaders"], ev["multiValueQueryStringParameters"] = mh, query
	} else { // the last value of a repeated header or parameter
		h, q := map[string]string{}, map[string]string{}
		for k, v := range headers {
			h[strings.ToLower(k)] = v[len(v)-1]
		}
		for k, v := range query {
			q[k] = v[len(v)-1]
		}
		ev["headers"], ev["queryStringParameters"] = h, q
	}
	payload, _ := json.Marshal(ev)
	out, err := s.hub.Invoke(fn, payload)
	var res struct {
		StatusCode        int                 `json:"statusCode"`
		Headers           map[string]string   `json:"headers"`
		MultiValueHeaders map[string][]string `json:"multiValueHeaders"`
		Body              string              `json:"body"`
		IsBase64Encoded   bool                `json:"isBase64Encoded"`
	}
	if err != nil || json.Unmarshal(out, &res) != nil || res.StatusCode < 100 || res.StatusCode > 999 {
		http.Error(w, "502 Bad Gateway", http.StatusBadGateway)
		return
	}
	for k, v := range res.Headers {
		w.Header().Set(k, v)
	}
	for k, vs := range res.MultiValueHeaders {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	b := []byte(res.Body)
	if res.IsBase64Encoded {
		b, _ = base64.StdEncoding.DecodeString(res.Body)
	}
	w.WriteHeader(res.StatusCode)
	w.Write(b)
}

// ---- listener ports ----

// bind opens a listener's own port on 127.0.0.1: ALBs serve HTTP (and HTTPS
// with pogi's certificate) there when the port is free; NLBs proxy TCP, on
// a random port when it is taken. The port goes in the load balancer's
// pogi metadata; lock held. Ports below 1024 on 127.0.0.1 need root, macOS
// included (it frees them only for 0.0.0.0): those ALB listeners are reached
// through pogi's port by Host.
func (s *Service) bind(l *listener) {
	lb := s.st.LBs[l.LoadBalancerArn]
	if lb == nil || l.Port == nil || lb.Type == "gateway" || l.Protocol == "UDP" {
		return // ponytail: no UDP, no GENEVE
	}
	ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(*l.Port))
	if err != nil && lb.Type == "network" {
		ln, err = net.Listen("tcp", "127.0.0.1:0")
	}
	if err != nil {
		s.hub.Log.Debug("elbv2: listener port unavailable", "listener", l.ListenerArn, "port", *l.Port, "err", err)
		return
	}
	if lb.Pogi.Ports == nil {
		lb.Pogi.Ports = map[int]int{}
	}
	lb.Pogi.Ports[*l.Port] = ln.Addr().(*net.TCPAddr).Port
	arn := l.ListenerArn
	if lb.Type == "network" { // ponytail: TLS listeners pass TLS through instead of terminating it
		s.open[arn] = ln
		core.Go("elbv2.serveTCP", func() { s.serveTCP(ln, arn) })
		return
	}
	if cfg, err := s.hub.TLS(); err == nil && l.Protocol == "HTTPS" {
		ln = core.SniffTLS(ln, cfg)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.route(w, r, arn) }),
		ReadHeaderTimeout: 30 * time.Second}
	s.open[arn] = srv
	go srv.Serve(ln)
}

// unbind closes a listener's port; lock held.
func (s *Service) unbind(l *listener) {
	if c := s.open[l.ListenerArn]; c != nil {
		c.Close()
		delete(s.open, l.ListenerArn)
	}
	if lb := s.st.LBs[l.LoadBalancerArn]; lb != nil {
		delete(lb.Pogi.Ports, portOf(l))
	}
}

// closeAll closes every listener port; lock held.
func (s *Service) closeAll() {
	for _, c := range s.open {
		c.Close()
	}
	clear(s.open)
	for _, lb := range s.st.LBs {
		lb.Pogi.Ports = nil
	}
}

// serveTCP is an NLB listener: each connection goes to a target of its
// default action, chosen like ALB's.
func (s *Service) serveTCP(ln net.Listener, arn string) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		core.Go("elbv2.serveTCP", func() {
			defer conn.Close()
			s.mu.Lock()
			to := ""
			if l := s.st.Listeners[arn]; l != nil && s.st.Rules[l.Rule] != nil {
				for _, a := range s.st.Rules[l.Rule].Actions { // NLBs only forward
					g, _ := s.pickGroup(a, &http.Request{})
					to = cmp.Or(to, s.choose(g))
				}
			}
			s.mu.Unlock()
			if to == "" {
				return
			}
			up, err := net.DialTimeout("tcp", to, 10*time.Second)
			if err != nil {
				return
			}
			defer up.Close()
			core.Go("elbv2.serveTCP", func() {
				io.Copy(up, conn)
				up.(*net.TCPConn).CloseWrite()
			})
			io.Copy(conn, up)
		})
	}
}

// ---- health ----

// Tick ends draining that is over and runs the health checks that are due:
// HTTP(S) GETs matched against the matcher, or TCP connects, to targets of
// groups a load balancer uses. Tests call it to move time.
func (s *Service) Tick(now time.Time) {
	type check struct {
		t                        *target
		proto, addr, path, codes string
		timeout                  time.Duration
		healthy, unhealthy       int
		reason, desc             string
	}
	var due []*check
	s.mu.Lock()
	used := s.usage()
	for _, g := range s.st.Groups {
		if slices.ContainsFunc(g.Targets, func(t *target) bool { return t.State == "draining" && now.After(t.Until) }) {
			g.Targets = slices.DeleteFunc(slices.Clone(g.Targets), func(t *target) bool { return t.State == "draining" && now.After(t.Until) })
		}
		if len(used[g.TargetGroupArn]) == 0 {
			continue
		}
		for _, t := range g.Targets {
			if t.State == "draining" || now.Before(t.next) {
				continue
			}
			t.next = now.Add(time.Duration(max(g.HealthCheckIntervalSeconds, 1)) * time.Second)
			port := t.Port
			if p, err := strconv.Atoi(g.HealthCheckPort); err == nil {
				port = p
			}
			switch addr := s.addr(g, t, port); {
			case g.TargetType == "lambda": // ponytail: Lambda targets are not health checked
				t.set("healthy", "", "")
			case !g.HealthCheckEnabled:
				t.set("unavailable", "Target.HealthCheckDisabled", "Health checks are disabled")
			case addr == "":
				t.set("unavailable", "Target.InvalidState", "pogi has no address for this target: no endpoint is recorded for it and its IP is not on this host")
			default:
				codes := "200"
				if g.Matcher != nil {
					codes = cmp.Or(g.Matcher.HttpCode, codes)
				}
				due = append(due, &check{t: t, proto: g.HealthCheckProtocol, addr: addr, path: g.HealthCheckPath, codes: codes,
					timeout: time.Duration(max(g.HealthCheckTimeoutSeconds, 1)) * time.Second, healthy: g.HealthyThresholdCount, unhealthy: g.UnhealthyThresholdCount})
			}
		}
	}
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, c := range due {
		wg.Go(func() { c.reason, c.desc = probe(c.proto, c.addr, c.path, c.codes, c.timeout) })
	}
	wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range due {
		c.t.observe(c.reason, c.desc, c.healthy, c.unhealthy)
	}
}

var checker = &http.Client{
	Transport:     &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, DisableKeepAlives: true},
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// probe runs one health check; it returns the failure reason, or "".
func probe(proto, addr, path, codes string, timeout time.Duration) (reason, desc string) {
	var err error
	if proto == "HTTP" || proto == "HTTPS" {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, strings.ToLower(proto)+"://"+addr+cmp.Or(path, "/"), nil)
		req.Header.Set("User-Agent", "ELB-HealthChecker/2.0")
		var res *http.Response
		if res, err = checker.Do(req); err == nil {
			res.Body.Close()
			if !codeIn(codes, res.StatusCode) {
				return "Target.ResponseCodeMismatch", fmt.Sprintf("Health checks failed with these codes: [%d]", res.StatusCode)
			}
			return "", ""
		}
	} else {
		var conn net.Conn
		if conn, err = net.DialTimeout("tcp", addr, timeout); err == nil {
			conn.Close()
			return "", ""
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) {
		return "Target.Timeout", "Request timed out"
	}
	return "Target.FailedHealthChecks", "Health checks failed"
}

// codeIn matches a status code against a matcher: "200", "200,202", "200-299".
func codeIn(codes string, code int) bool {
	for _, part := range strings.Split(codes, ",") {
		lo, hi, ok := strings.Cut(strings.TrimSpace(part), "-")
		if !ok {
			hi = lo
		}
		a, _ := strconv.Atoi(lo)
		b, _ := strconv.Atoi(hi)
		if code >= a && code <= b {
			return true
		}
	}
	return false
}

// observe moves a target's health after a check. A new target is healthy
// after one pass, an unhealthy one after HealthyThresholdCount passes, and
// UnhealthyThresholdCount failures make a target unhealthy.
// ponytail: ELB wants HealthyThresholdCount passes from new targets on some
// load balancer types; one keeps pogi quick.
func (t *target) observe(reason, desc string, healthy, unhealthy int) {
	switch {
	case t.State == "draining":
	case reason == "":
		t.ok, t.bad = t.ok+1, 0
		if t.State != "unhealthy" || t.ok >= healthy {
			t.set("healthy", "", "")
		}
	default:
		t.ok, t.bad = 0, t.bad+1
		if t.bad >= unhealthy {
			t.set("unhealthy", reason, desc)
		} else if t.State == "initial" {
			t.set("initial", "Elb.InitialHealthChecking", "Initial health checks in progress")
		}
	}
}
