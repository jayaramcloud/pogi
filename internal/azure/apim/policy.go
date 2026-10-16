package apim

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Policies are XML documents: <policies> with an <inbound>, <backend>, <outbound> and <on-error> section of
// elements. A level's section says where its parent level's section goes with <base/>.

// node is one element of a policy.
type node struct {
	name  string
	attr  map[string]string
	text  string
	kids  []*node
	scope string // the level it came from: counters are kept per scope
}

// sections of a policy document by name; a section the document leaves out is absent from the map.
type sections map[string][]*node

// parsePolicy reads a policy document.
func parsePolicy(doc, scope string) (sections, error) {
	d := xml.NewDecoder(strings.NewReader(doc))
	d.Strict = false // "rawxml" documents have bare & and < in their expressions
	var stack []*node
	var root *node
	for {
		t, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		switch t := t.(type) {
		case xml.StartElement:
			n := &node{name: t.Name.Local, attr: map[string]string{}, scope: scope}
			for _, a := range t.Attr {
				n.attr[a.Name.Local] = a.Value
			}
			if len(stack) > 0 {
				p := stack[len(stack)-1]
				p.kids = append(p.kids, n)
			} else if root == nil {
				root = n
			} else {
				return nil, errors.New("the policy document has more than one root element")
			}
			stack = append(stack, n)
		case xml.EndElement:
			stack = stack[:len(stack)-1]
		case xml.CharData:
			if len(stack) > 0 {
				stack[len(stack)-1].text += string(t)
			}
		}
	}
	if root == nil || root.name != "policies" {
		return nil, errors.New("the policy document must have a <policies> root element")
	}
	out := sections{}
	for _, k := range root.kids {
		out[k.name] = k.kids
	}
	return out, nil
}

// compose is the section of the innermost level levels[i], with <base/> replaced by the same section of the level outside it.
// A level that leaves a section out has nothing to add: it is as if it held <base/>.
func compose(levels []sections, i int, section string) []*node {
	if i < 0 {
		return nil
	}
	els, ok := levels[i][section]
	if !ok {
		return compose(levels, i-1, section)
	}
	var out []*node
	for _, n := range els {
		if n.name == "base" {
			out = append(out, compose(levels, i-1, section)...)
		} else {
			out = append(out, n)
		}
	}
	return out
}

// reply is a response a policy made, or the gateway's own.
type reply struct {
	status int
	hdr    http.Header
	body   []byte
}

func errReply(status int, msg string) *reply {
	b, _ := json.Marshal(map[string]any{"statusCode": status, "message": msg})
	return &reply{status, http.Header{"Content-Type": {"application/json"}}, b}
}

// exec is one request going through the policies.
type exec struct {
	s       *Service
	svc     string // the service's ID, for counters and named values
	r       *http.Request
	method  string
	path    string // under the API's path, which rewrite-uri changes
	query   url.Values
	hdr     http.Header
	body    []byte
	params  map[string]string // the operation's URL template parameters
	keepQ   map[string]bool   // the query parameters the operation's URL template names
	op      map[string]any    // the operation's properties
	backend string            // set-backend-service
	creds   map[string]any    // the credentials of the backend entity set-backend-service chose
	sub     string            // the subscription's name
	ip      string
	cors    *node
	ignored []string

	status int // the backend's response
	rhdr   http.Header
	rbody  []byte
}

var (
	exprHeader = regexp.MustCompile(`^@\(\s*context\.Request\.Headers\.GetValueOrDefault\(\s*"([^"]*)"\s*(?:,\s*"([^"]*)"\s*)?\)\s*\)$`)
	namedRef   = regexp.MustCompile(`\{\{([^{}]+)\}\}`)
	paramRef   = regexp.MustCompile(`\{[^{}]+\}`)
)

// val evaluates an attribute or a text of a policy: a literal, with {{named values}} replaced, or one of the few
// expressions pogi has.
// ponytail: the C# expression language is not here. Supported: context.Request.Headers.GetValueOrDefault("n", "d"),
// context.Subscription.Id and context.Request.IpAddress; any other @(...) or @{...} fails the request with a 500.
// Upgrade path: a Go interpreter for the common context.* members.
func (e *exec) val(s string) (string, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "@") {
		if m := exprHeader.FindStringSubmatch(s); m != nil {
			if v := e.hdr.Values(m[1]); len(v) > 0 {
				return strings.Join(v, ","), nil
			}
			return m[2], nil
		}
		switch strings.ReplaceAll(s, " ", "") {
		case "@(context.Subscription.Id)":
			return e.sub, nil
		case "@(context.Request.IpAddress)":
			return e.ip, nil
		}
		return "", fmt.Errorf("the policy expression %q is not supported by pogi", s)
	}
	var err error
	s = namedRef.ReplaceAllStringFunc(s, func(m string) string {
		v, ok := e.s.namedValue(e.svc, m[2:len(m)-2])
		if !ok && err == nil {
			err = fmt.Errorf("the named value %q does not exist", m[2:len(m)-2])
		}
		return v
	})
	return s, err
}

// attr is val of an attribute.
func (e *exec) attr(n *node, name string) (string, error) { return e.val(n.attr[name]) }

// values are the evaluated <value> children of a node.
func (e *exec) values(n *node) ([]string, error) {
	var out []string
	for _, k := range n.kids {
		if k.name == "value" {
			v, err := e.val(k.text)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
	}
	return out, nil
}

// apply sets key in m as exists-action says: override (the default), skip, append or delete.
func apply(m map[string][]string, key, action string, vals []string) {
	switch strings.ToLower(action) {
	case "skip":
		if _, ok := m[key]; !ok {
			m[key] = vals
		}
	case "append":
		m[key] = append(m[key], vals...)
	case "delete":
		delete(m, key)
	default:
		m[key] = vals
	}
}

// inbound runs the inbound policies; a non-nil reply ends the request.
func (e *exec) inbound(nodes []*node) (*reply, error) {
	for _, n := range nodes {
		var rep *reply
		var err error
		switch n.name {
		case "set-header":
			var vals []string
			if vals, err = e.values(n); err == nil {
				apply(e.hdr, http.CanonicalHeaderKey(n.attr["name"]), n.attr["exists-action"], vals)
			}
		case "set-query-parameter":
			var vals []string
			if vals, err = e.values(n); err == nil {
				apply(e.query, n.attr["name"], n.attr["exists-action"], vals)
			}
		case "rewrite-uri":
			err = e.rewrite(n)
		case "set-backend-service":
			err = e.setBackend(n)
		case "rate-limit":
			rep, err = e.rateLimit(n, false)
		case "rate-limit-by-key":
			rep, err = e.rateLimit(n, true)
		case "validate-jwt":
			rep, err = e.validateJWT(n)
		case "return-response":
			rep, err = e.returnResponse(n)
		case "mock-response":
			rep = e.mock(n)
		case "cors":
			rep = e.corsInbound(n)
		case "set-body":
			var b string
			if b, err = e.val(n.text); err == nil {
				e.body = []byte(b)
			}
		case "forward-request", "base":
		default:
			// ponytail: choose, send-request, quota, cache-lookup, retry and the rest are skipped, and named in
			// the X-Pogi-Apim-Ignored header; upgrade path: add them here.
			e.ignored = append(e.ignored, n.name)
		}
		if err != nil {
			return errReply(http.StatusInternalServerError, err.Error()), nil
		}
		if rep != nil {
			return rep, nil
		}
	}
	return nil, nil
}

// outbound runs the outbound policies on the backend's response.
func (e *exec) outbound(nodes []*node) (*reply, error) {
	for _, n := range nodes {
		var err error
		switch n.name {
		case "set-header":
			var vals []string
			if vals, err = e.values(n); err == nil {
				apply(e.rhdr, http.CanonicalHeaderKey(n.attr["name"]), n.attr["exists-action"], vals)
			}
		case "set-body":
			var b string
			if b, err = e.val(n.text); err == nil {
				e.rbody = []byte(b)
			}
		case "return-response":
			var rep *reply
			if rep, err = e.returnResponse(n); err == nil {
				return rep, nil
			}
		case "cors", "base", "forward-request":
		default:
			e.ignored = append(e.ignored, n.name)
		}
		if err != nil {
			return errReply(http.StatusInternalServerError, err.Error()), nil
		}
	}
	return nil, nil
}

// rewrite is rewrite-uri: the template replaces the path under the API's, with {parameters} of the operation filled in.
func (e *exec) rewrite(n *node) error {
	tmpl, err := e.attr(n, "template")
	if err != nil {
		return err
	}
	path, q, _ := strings.Cut(tmpl, "?")
	e.path = paramRef.ReplaceAllStringFunc(path, func(m string) string {
		if v, ok := e.params[m[1:len(m)-1]]; ok {
			return v
		}
		return m
	})
	if !strings.HasPrefix(e.path, "/") {
		e.path = "/" + e.path
	}
	if strings.EqualFold(n.attr["copy-unmatched-params"], "false") {
		for k := range e.query {
			if !e.keepQ[k] {
				delete(e.query, k)
			}
		}
	}
	if q != "" {
		in, _ := url.ParseQuery(paramRef.ReplaceAllStringFunc(q, func(m string) string { return e.params[m[1:len(m)-1]] }))
		for k, v := range in {
			e.query[k] = v
		}
	}
	return nil
}

func (e *exec) setBackend(n *node) error {
	if id, err := e.attr(n, "backend-id"); err != nil || id != "" {
		if err != nil {
			return err
		}
		p := e.s.backend(e.svc, id)
		if p == nil {
			return fmt.Errorf("the backend %q does not exist", id)
		}
		e.backend, _ = p["url"].(string)
		e.creds, _ = p["credentials"].(map[string]any)
		return nil
	}
	u, err := e.attr(n, "base-url")
	e.backend = u
	return err
}

// returnResponse builds the response of return-response: <set-status>, <set-header>s and a <set-body>.
func (e *exec) returnResponse(n *node) (*reply, error) {
	rep := &reply{status: http.StatusOK, hdr: http.Header{}}
	for _, k := range n.kids {
		switch k.name {
		case "set-status":
			if c, err := strconv.Atoi(k.attr["code"]); err == nil {
				rep.status = c
			}
		case "set-header":
			vals, err := e.values(k)
			if err != nil {
				return nil, err
			}
			apply(rep.hdr, http.CanonicalHeaderKey(k.attr["name"]), k.attr["exists-action"], vals)
		case "set-body":
			b, err := e.val(k.text)
			if err != nil {
				return nil, err
			}
			rep.body = []byte(b)
		}
	}
	return rep, nil
}

// mock reads the operation's example for the status code and content type of mock-response, if it has one.
func (e *exec) mock(n *node) *reply {
	code, ctype := http.StatusOK, n.attr["content-type"]
	if c, err := strconv.Atoi(n.attr["status-code"]); err == nil {
		code = c
	}
	rep := &reply{status: code, hdr: http.Header{}}
	resps, _ := e.op["responses"].([]any)
	for _, r := range resps {
		rm, _ := r.(map[string]any)
		if sc, _ := rm["statusCode"].(float64); int(sc) != code {
			continue
		}
		reps, _ := rm["representations"].([]any)
		for _, x := range reps {
			xm, _ := x.(map[string]any)
			ct, _ := xm["contentType"].(string)
			if ctype != "" && !strings.EqualFold(ct, ctype) {
				continue
			}
			sample := xm["sample"]
			if ex, _ := xm["examples"].(map[string]any); len(ex) > 0 {
				names := make([]string, 0, len(ex))
				for k := range ex {
					names = append(names, k)
				}
				sort.Strings(names)
				if em, _ := ex[names[0]].(map[string]any); em["value"] != nil {
					sample = em["value"]
				}
			}
			if sample == nil {
				continue
			}
			if s, ok := sample.(string); ok {
				rep.body = []byte(s)
			} else {
				rep.body, _ = json.Marshal(sample)
			}
			rep.hdr.Set("Content-Type", ct)
			return rep
		}
	}
	return rep
}

// ---- rate limits ----

// window is a rate limit's counter: calls since start.
type window struct {
	start time.Time
	n     int
}

// rateLimit is rate-limit (per subscription) and rate-limit-by-key (per counter-key).
// ponytail: a fixed window that starts at a key's first call, kept in memory; Azure's is sliding. Upgrade path: a
// sliding-window log. The increment-condition and the calls/api/operation children are ignored.
func (e *exec) rateLimit(n *node, byKey bool) (*reply, error) {
	calls, err := strconv.Atoi(n.attr["calls"])
	if err != nil {
		return nil, fmt.Errorf("%s needs a number in calls", n.name)
	}
	period, err := strconv.Atoi(n.attr["renewal-period"])
	if err != nil || period <= 0 {
		return nil, fmt.Errorf("%s needs a number in renewal-period", n.name)
	}
	key := e.sub
	if key == "" {
		key = e.ip
	}
	inc := 1
	if byKey {
		if key, err = e.attr(n, "counter-key"); err != nil {
			return nil, err
		}
		if v, err := strconv.Atoi(n.attr["increment-count"]); err == nil {
			inc = v
		}
	}
	key = e.svc + "|" + n.scope + "|" + key
	now := time.Now()
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	w := e.s.limits[key]
	if w == nil || now.Sub(w.start) >= time.Duration(period)*time.Second {
		w = &window{start: now}
		e.s.limits[key] = w
	}
	if w.n+inc > calls {
		wait := int((time.Duration(period)*time.Second - now.Sub(w.start) + time.Second - 1) / time.Second)
		rep := errReply(http.StatusTooManyRequests, fmt.Sprintf("Rate limit is exceeded. Try again in %d seconds.", wait))
		rep.hdr.Set("Retry-After", strconv.Itoa(wait))
		return rep, nil
	}
	w.n += inc
	return nil, nil
}

// ---- cors ----

// corsInbound answers a preflight itself and remembers the policy for the headers of the actual response.
func (e *exec) corsInbound(n *node) *reply {
	e.cors = n
	origin := e.hdr.Get("Origin")
	if e.method != http.MethodOptions || origin == "" || e.hdr.Get("Access-Control-Request-Method") == "" {
		return nil
	}
	rep := &reply{status: http.StatusOK, hdr: http.Header{}}
	e.corsHeaders(rep.hdr)
	if h := n.child("allowed-methods"); h != nil {
		if len(h.kidTexts("method")) > 0 {
			rep.hdr.Set("Access-Control-Allow-Methods", strings.Join(h.kidTexts("method"), ", "))
		}
		if age := h.attr["preflight-result-max-age"]; age != "" {
			rep.hdr.Set("Access-Control-Max-Age", age)
		}
	}
	if h := n.child("allowed-headers"); h != nil {
		if hs := h.kidTexts("header"); len(hs) > 0 {
			rep.hdr.Set("Access-Control-Allow-Headers", strings.Join(hs, ", "))
		}
	}
	return rep
}

// corsHeaders adds the headers every cross-origin response of the cors policy has.
func (e *exec) corsHeaders(h http.Header) {
	n, origin := e.cors, e.hdr.Get("Origin")
	if n == nil || origin == "" {
		return
	}
	allowed := n.child("allowed-origins").kidTexts("origin")
	creds := strings.EqualFold(n.attr["allow-credentials"], "true")
	for _, o := range allowed {
		if o == "*" || strings.EqualFold(o, origin) {
			if o == "*" && !creds {
				h.Set("Access-Control-Allow-Origin", "*")
			} else {
				h.Set("Access-Control-Allow-Origin", origin)
				h.Add("Vary", "Origin")
			}
			if creds {
				h.Set("Access-Control-Allow-Credentials", "true")
			}
			if hs := n.child("expose-headers").kidTexts("header"); len(hs) > 0 {
				h.Set("Access-Control-Expose-Headers", strings.Join(hs, ", "))
			}
			return
		}
	}
}

// child is the first kid of that name, or nil.
func (n *node) child(name string) *node {
	if n == nil {
		return nil
	}
	for _, k := range n.kids {
		if k.name == name {
			return k
		}
	}
	return nil
}

// kidTexts are the trimmed texts of the kids of that name.
func (n *node) kidTexts(name string) []string {
	if n == nil {
		return nil
	}
	var out []string
	for _, k := range n.kids {
		if k.name == name {
			out = append(out, strings.TrimSpace(k.text))
		}
	}
	return out
}
