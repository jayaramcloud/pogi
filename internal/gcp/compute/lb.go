package compute

// Load balancing and Cloud Armor, on Compute's own machinery (operations, selfLink, ids). The two
// services ("gcplb" and "cloudarmor") are views of one Compute Service, so the resources and the
// operations they make live in one state: a backend service can name a security policy, and an
// operation made by either is read at the same /operations path.
//
// ponytail: nothing carries traffic. Health checks never probe; forwarding rules get a made-up
// address; instance groups are not stored, so a backend's group is kept as given.

import (
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

var lbColls = []string{"healthChecks", "backendServices", "urlMaps", "targetHttpProxies", "targetHttpsProxies", "sslCertificates", "forwardingRules"}
var armorColls = []string{"securityPolicies"}

// claims reports whether a Compute path is in one of the collections: .../global/<c>, .../regions/<r>/<c>
// or .../aggregated/<c>.
func claims(path string, colls []string) bool {
	seg := strings.Split(strings.Trim(path, "/"), "/") // compute v1 projects <p> ...
	if len(seg) < 5 || seg[2] != "projects" {
		return false
	}
	rest := seg[4:]
	switch rest[0] {
	case "global", "aggregated":
		rest = rest[1:]
	case "regions":
		if len(rest) < 2 {
			return false
		}
		rest = rest[2:]
	}
	return len(rest) > 0 && slices.Contains(colls, rest[0])
}

// view is one registry entry that serves some collections of the Compute Service.
type view struct {
	s     *Service
	info  core.Info
	colls []string
}

// NewLoadBalancing and NewCloudArmor share the hub's Compute service, or one of their own when it is off.
func NewLoadBalancing(h *core.Hub) core.Service {
	return newView(h, core.Info{Name: "gcplb", Cloud: core.GCP, Title: "Cloud Load Balancing"}, lbColls)
}

func NewCloudArmor(h *core.Hub) core.Service {
	return newView(h, core.Info{Name: "cloudarmor", Cloud: core.GCP, Title: "Cloud Armor"}, armorColls)
}

func newView(h *core.Hub, info core.Info, colls []string) *view {
	s, ok := h.Service("compute").(*Service)
	if !ok {
		s = New(h)
	}
	info.Match = func(r *http.Request) bool {
		p := r.URL.Path
		return (strings.HasPrefix(p, "/compute/v1/") || strings.HasPrefix(p, "/compute/beta/")) && claims(p, colls)
	}
	return &view{s: s, info: info, colls: colls}
}

func (v *view) Info() core.Info                                  { return v.info }
func (v *view) ServeHTTP(w http.ResponseWriter, r *http.Request) { v.s.ServeHTTP(w, r) }

func init() {
	add := func(scope, coll string, k *kind) { kinds[scope+"/"+coll] = k }
	for _, scope := range []string{"global", "regions"} {
		add(scope, "healthChecks", &kind{kind: "compute#healthCheck", fill: fillHealthCheck, update: true, refill: true})
		add(scope, "backendServices", &kind{kind: "compute#backendService", fill: fillBackendService, update: true, refill: true,
			acts: map[string]func(*req, string) error{"setSecurityPolicy": setRef("securityPolicy", "securityPolicy", "global/securityPolicies", true)}})
		add(scope, "urlMaps", &kind{kind: "compute#urlMap", fill: fillURLMap, update: true, refill: true})
		add(scope, "targetHttpProxies", &kind{kind: "compute#targetHttpProxy", fill: fillHTTPProxy,
			acts: map[string]func(*req, string) error{"setUrlMap": setRef("urlMap", "urlMap", "urlMaps", true)}})
		add(scope, "targetHttpsProxies", &kind{kind: "compute#targetHttpsProxy", fill: fillHTTPSProxy,
			acts: map[string]func(*req, string) error{"setUrlMap": setRef("urlMap", "urlMap", "urlMaps", true), "setSslCertificates": setCerts}})
		add(scope, "sslCertificates", &kind{kind: "compute#sslCertificate", fill: fillCert})
		add(scope, "forwardingRules", &kind{kind: "compute#forwardingRule", fill: fillForwardingRule,
			acts: map[string]func(*req, string) error{"setTarget": setTarget, "setLabels": setLabels}})
		add(scope, "securityPolicies", &kind{kind: "compute#securityPolicy", fill: fillPolicy, update: true, refill: true,
			acts:  map[string]func(*req, string) error{"setLabels": setLabels, "addRule": addRule, "patchRule": patchRule, "removeRule": removeRule},
			reads: map[string]func(*req, string) (any, error){"getRule": getRule}})
	}
}

// scopeOf is the path of the project, region or "global" a resource is in: projects/p/global.
func scopeOf(path string) string {
	p := strings.Split(path, "/")
	return strings.Join(p[:len(p)-2], "/")
}

// link turns a reference into a full link under prefix (a collection under the resource's scope, or a
// scope-relative one such as global/securityPolicies); with check, the target must exist. mu is held.
func (q *req) link(in, prefix string, check bool) (string, error) {
	p := ref(in, prefix)
	if check && q.s.st.Res[p] == nil {
		return "", notFound(p)
	}
	return api + p, nil
}

// refField replaces doc[field] by its link; an absent field is left alone.
func (q *req) refField(doc map[string]any, field, prefix string, check bool) error {
	if str(doc[field]) == "" {
		return nil
	}
	l, err := q.link(str(doc[field]), prefix, check)
	doc[field] = l
	return err
}

func (q *req) refList(doc map[string]any, field, prefix string, check bool) error {
	for i, v := range arr(doc, field) {
		l, err := q.link(str(v), prefix, check)
		if err != nil {
			return err
		}
		arr(doc, field)[i] = l
	}
	return nil
}

func required(f string) error { return invalid("Required field '%s' not specified", f) }

func def(doc map[string]any, k string, v any) {
	if doc[k] == nil {
		doc[k] = v
	}
}

func oneOf(field, v string, ok ...string) error {
	if !slices.Contains(ok, v) {
		return invalid("Invalid value for field 'resource.%s': '%s'. Must be one of %s.", field, v, strings.Join(ok, ", "))
	}
	return nil
}

// ---- health checks ----

var hcBlocks = map[string]string{"HTTP": "httpHealthCheck", "HTTPS": "httpsHealthCheck", "HTTP2": "http2HealthCheck",
	"TCP": "tcpHealthCheck", "SSL": "sslHealthCheck", "GRPC": "grpcHealthCheck"}

func fillHealthCheck(q *req, path string, doc map[string]any) error {
	typ := str(doc["type"])
	if typ == "" { // the type follows from the one block that was sent
		for t, b := range hcBlocks {
			if doc[b] != nil {
				typ = t
			}
		}
	}
	block, ok := hcBlocks[typ]
	if !ok {
		return invalid("Invalid value for field 'resource.type': '%s'. Must be one of HTTP, HTTPS, HTTP2, TCP, SSL, GRPC.", typ)
	}
	doc["type"] = typ
	b := obj(doc, block)
	if b == nil {
		b = map[string]any{}
		doc[block] = b
	}
	if p := sizeOf(b["port"]); b["port"] != nil && (p < 1 || p > 65535) {
		return invalid("Invalid value for field 'resource.%s.port': '%d'. Must be between 1 and 65535.", block, p)
	}
	if typ != "GRPC" {
		def(b, "port", map[bool]int{true: 443, false: 80}[typ == "HTTPS" || typ == "SSL"])
		def(b, "proxyHeader", "NONE")
	}
	if strings.HasPrefix(typ, "HTTP") {
		def(b, "requestPath", "/")
	}
	def(doc, "checkIntervalSec", 5)
	def(doc, "timeoutSec", 5)
	def(doc, "healthyThreshold", 2)
	def(doc, "unhealthyThreshold", 2)
	return nil
}

// ---- backend services ----

func fillBackendService(q *req, path string, doc map[string]any) error {
	scope := scopeOf(path)
	def(doc, "protocol", "HTTP")
	def(doc, "loadBalancingScheme", "EXTERNAL")
	if err := oneOf("protocol", str(doc["protocol"]), "HTTP", "HTTPS", "HTTP2", "H2C", "TCP", "SSL", "UDP", "GRPC", "UNSPECIFIED"); err != nil {
		return err
	}
	if err := oneOf("loadBalancingScheme", str(doc["loadBalancingScheme"]), "EXTERNAL", "EXTERNAL_MANAGED", "INTERNAL", "INTERNAL_MANAGED", "INTERNAL_SELF_MANAGED"); err != nil {
		return err
	}
	if err := q.refList(doc, "healthChecks", scope+"/healthChecks", true); err != nil {
		return err
	}
	if err := q.refField(doc, "securityPolicy", "projects/"+q.project+"/global/securityPolicies", true); err != nil {
		return err
	}
	for _, v := range arr(doc, "backends") {
		if b, ok := v.(map[string]any); ok {
			if str(b["group"]) == "" {
				return required("backends[].group")
			}
			def(b, "balancingMode", "UTILIZATION")
			def(b, "capacityScaler", 1)
		}
	}
	def(doc, "timeoutSec", 30)
	def(doc, "port", 80)
	def(doc, "portName", "http")
	def(doc, "sessionAffinity", "NONE")
	def(doc, "connectionDraining", map[string]any{"drainingTimeoutSec": 0})
	doc["fingerprint"] = fingerprint()
	return nil
}

// ---- URL maps ----

// service checks a reference to a backend service (a backend bucket is kept as given).
func (q *req) service(doc map[string]any, field, scope string) error {
	if strings.Contains(str(doc[field]), "/backendBuckets/") {
		return q.refField(doc, field, scope+"/backendBuckets", false)
	}
	return q.refField(doc, field, scope+"/backendServices", true)
}

func fillURLMap(q *req, path string, doc map[string]any) error {
	scope := scopeOf(path)
	if str(doc["defaultService"]) == "" && doc["defaultRouteAction"] == nil && doc["defaultUrlRedirect"] == nil {
		return invalid("Invalid value for field 'resource': ''. Exactly one of defaultService, defaultRouteAction or defaultUrlRedirect must be set.")
	}
	if err := q.service(doc, "defaultService", scope); err != nil {
		return err
	}
	for _, v := range arr(doc, "pathMatchers") {
		pm, _ := v.(map[string]any)
		if pm == nil {
			continue
		}
		if err := q.service(pm, "defaultService", scope); err != nil {
			return err
		}
		for _, list := range []string{"pathRules", "routeRules"} {
			for _, r := range arr(pm, list) {
				if rm, _ := r.(map[string]any); rm != nil {
					if err := q.service(rm, "service", scope); err != nil {
						return err
					}
				}
			}
		}
	}
	doc["fingerprint"] = fingerprint()
	return nil
}

// ---- target proxies ----

func fillHTTPProxy(q *req, path string, doc map[string]any) error {
	if str(doc["urlMap"]) == "" {
		return required("urlMap")
	}
	if err := q.refField(doc, "urlMap", scopeOf(path)+"/urlMaps", true); err != nil {
		return err
	}
	def(doc, "proxyBind", false)
	return nil
}

func fillHTTPSProxy(q *req, path string, doc map[string]any) error {
	if err := fillHTTPProxy(q, path, doc); err != nil {
		return err
	}
	if len(arr(doc, "sslCertificates")) == 0 && str(doc["certificateMap"]) == "" {
		return required("sslCertificates")
	}
	def(doc, "quicOverride", "NONE")
	return q.refList(doc, "sslCertificates", scopeOf(path)+"/sslCertificates", true)
}

// setRef is the action that changes one reference of a resource to another stored one.
func setRef(field, bodyKey, coll string, check bool) func(*req, string) error {
	return func(q *req, path string) error {
		q.s.mu.Lock()
		defer q.s.mu.Unlock()
		if str(q.body[bodyKey]) == "" {
			return required(bodyKey)
		}
		prefix := scopeOf(path) + "/" + coll
		if strings.HasPrefix(coll, "global/") {
			prefix = "projects/" + q.project + "/" + coll
		}
		l, err := q.link(str(q.body[bodyKey]), prefix, check)
		if err == nil {
			q.s.st.Res[path][field] = l
		}
		return err
	}
}

func setCerts(q *req, path string) error {
	q.s.mu.Lock()
	defer q.s.mu.Unlock()
	d := map[string]any{"sslCertificates": q.body["sslCertificates"]}
	if len(arr(d, "sslCertificates")) == 0 {
		return required("sslCertificates")
	}
	if err := q.refList(d, "sslCertificates", scopeOf(path)+"/sslCertificates", true); err != nil {
		return err
	}
	q.s.st.Res[path]["sslCertificates"] = d["sslCertificates"]
	return nil
}

// ---- SSL certificates ----

func fillCert(q *req, path string, doc map[string]any) error {
	if m := obj(doc, "managed"); m != nil {
		domains := arr(m, "domains")
		if len(domains) == 0 {
			return required("managed.domains")
		}
		status := map[string]any{}
		for _, d := range domains {
			status[str(d)] = "ACTIVE"
		}
		m["status"], m["domainStatus"] = "ACTIVE", status
		doc["type"] = "MANAGED"
		return nil
	}
	cert, key := str(doc["certificate"]), str(doc["privateKey"])
	if cert == "" {
		return required("certificate")
	}
	if key == "" {
		return required("privateKey")
	}
	cb, _ := pem.Decode([]byte(cert))
	kb, _ := pem.Decode([]byte(key))
	if cb == nil {
		return invalid("Invalid value for field 'resource.certificate': 'xxx'. Certificate is not a valid PEM.")
	}
	if kb == nil {
		return invalid("Invalid value for field 'resource.privateKey': 'xxx'. Private key is not a valid PEM.")
	}
	c, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return invalid("Invalid value for field 'resource.certificate': 'xxx'. Certificate could not be parsed.")
	}
	delete(doc, "privateKey") // the API never returns it
	doc["type"], doc["expireTime"] = "SELF_MANAGED", c.NotAfter.Format(time.RFC3339)
	return nil
}

// ---- forwarding rules ----

// fillTarget links a forwarding rule's target; proxies must exist, other kinds of target are kept as given.
func (q *req) fillTarget(doc map[string]any, scope string) error {
	t := str(doc["target"])
	if t == "" {
		return nil
	}
	coll := "targetHttpProxies"
	for _, c := range []string{"targetHttpsProxies", "targetTcpProxies", "targetSslProxies", "targetPools", "targetInstances", "targetVpnGateways", "targetGrpcProxies"} {
		if strings.Contains(t, "/"+c+"/") {
			coll = c
		}
	}
	return q.refField(doc, "target", scope+"/"+coll, coll == "targetHttpProxies" || coll == "targetHttpsProxies")
}

func fillForwardingRule(q *req, path string, doc map[string]any) error {
	scope := scopeOf(path)
	global := strings.HasSuffix(scope, "/global")
	if str(doc["target"]) == "" && (global || str(doc["backendService"]) == "") {
		return required("target")
	}
	if pr := str(doc["portRange"]); pr != "" { // the API writes "80" as "80-80"
		lo, hi, ok := strings.Cut(pr, "-")
		a, e1 := strconv.Atoi(lo)
		b, e2 := strconv.Atoi(hi)
		if !ok {
			b, e2 = a, nil
		}
		if e1 != nil || e2 != nil || a < 1 || b > 65535 || a > b {
			return invalid("Invalid value for field 'resource.portRange': '%s'. Must be a port or a range of ports.", pr)
		}
		doc["portRange"] = strconv.Itoa(a) + "-" + strconv.Itoa(b)
	}
	if err := q.fillTarget(doc, scope); err != nil {
		return err
	}
	if err := q.refField(doc, "backendService", scope+"/backendServices", true); err != nil {
		return err
	}
	def(doc, "IPProtocol", "TCP")
	if err := oneOf("IPProtocol", str(doc["IPProtocol"]), "TCP", "UDP", "ESP", "AH", "SCTP", "ICMP", "L3_DEFAULT"); err != nil {
		return err
	}
	def(doc, "loadBalancingScheme", "EXTERNAL")
	def(doc, "networkTier", "PREMIUM")
	switch ip := str(doc["IPAddress"]); {
	case ip == "":
		doc["IPAddress"] = q.s.extIP()
	case !mustAddr(ip).IsValid(): // a name or link of a reserved address
		a := q.s.st.Res[ref(ip, scope+"/addresses")]
		if a == nil {
			return notFound(ref(ip, scope+"/addresses"))
		}
		doc["IPAddress"] = a["address"]
	}
	doc["fingerprint"], doc["labelFingerprint"] = fingerprint(), fingerprint()
	return nil
}

func mustAddr(s string) netip.Addr { a, _ := netip.ParseAddr(s); return a }

func setTarget(q *req, path string) error {
	q.s.mu.Lock()
	defer q.s.mu.Unlock()
	d := map[string]any{"target": q.body["target"]}
	if str(d["target"]) == "" {
		return required("target")
	}
	if err := q.fillTarget(d, scopeOf(path)); err != nil {
		return err
	}
	q.s.st.Res[path]["target"] = d["target"]
	return nil
}

// ---- Cloud Armor security policies ----

const defaultPriority = 2147483647

func fillPolicy(q *req, path string, doc map[string]any) error {
	def(doc, "type", "CLOUD_ARMOR")
	seen := map[int]bool{}
	rules := []any{}
	for _, v := range arr(doc, "rules") {
		r, _ := v.(map[string]any)
		if err := checkRule(r); err != nil {
			return err
		}
		if p := sizeOf(r["priority"]); seen[p] {
			return invalid("Invalid value for field 'resource.rules': duplicate priority '%d'.", p)
		} else {
			seen[p] = true
		}
		rules = append(rules, r)
	}
	if !seen[defaultPriority] {
		rules = append(rules, map[string]any{"kind": "compute#securityPolicyRule", "description": "Default rule, higher priority overrides it", "priority": defaultPriority,
			"match": map[string]any{"versionedExpr": "SRC_IPS_V1", "config": map[string]any{"srcIpRanges": []any{"*"}}}, "action": "allow", "preview": false})
	}
	doc["rules"], doc["fingerprint"] = rules, fingerprint()
	def(doc, "labelFingerprint", fingerprint())
	return nil
}

// checkRule validates a rule and completes it.
func checkRule(r map[string]any) error {
	if r == nil {
		return invalid("Invalid value for field 'resource.rules': rule must be an object.")
	}
	if p := sizeOf(r["priority"]); p < 0 || p > defaultPriority {
		return invalid("Invalid value for field 'resource.priority': '%d'. Must be between 0 and 2147483647.", p)
	}
	r["priority"] = sizeOf(r["priority"])
	a := str(r["action"])
	if a == "" {
		return required("action")
	}
	if !slices.ContainsFunc([]string{"allow", "deny", "redirect", "throttle", "rate_based_ban"}, func(p string) bool { return strings.HasPrefix(a, p) }) {
		return invalid("Invalid value for field 'resource.action': '%s'. Must be allow, deny(<status>), redirect, throttle or rate_based_ban.", a)
	}
	if len(obj(r, "match")) == 0 {
		return required("match")
	}
	r["kind"] = "compute#securityPolicyRule"
	def(r, "preview", false)
	return nil
}

// rule finds the rule of the request's priority query parameter in a policy; mu is held.
func rule(q *req, path string) (rules []any, i int, err error) {
	p, _ := strconv.Atoi(q.r.URL.Query().Get("priority"))
	rules = arr(q.s.st.Res[path], "rules")
	for i, v := range rules {
		if m, _ := v.(map[string]any); sizeOf(m["priority"]) == p {
			return rules, i, nil
		}
	}
	return nil, 0, notFound(path + "/rules/" + strconv.Itoa(p))
}

func addRule(q *req, path string) error {
	q.s.mu.Lock()
	defer q.s.mu.Unlock()
	r := clone(q.body)
	if err := checkRule(r); err != nil {
		return err
	}
	doc := q.s.st.Res[path]
	for _, v := range arr(doc, "rules") {
		if m, _ := v.(map[string]any); sizeOf(m["priority"]) == sizeOf(r["priority"]) {
			return invalid("Invalid value for field 'resource.priority': '%d'. A rule with this priority already exists.", sizeOf(r["priority"]))
		}
	}
	doc["rules"], doc["fingerprint"] = append(arr(doc, "rules"), r), fingerprint()
	return nil
}

func patchRule(q *req, path string) error {
	q.s.mu.Lock()
	defer q.s.mu.Unlock()
	rules, i, err := rule(q, path)
	if err != nil {
		return err
	}
	cur := rules[i].(map[string]any)
	patched := merge(clone(cur), clone(q.body))
	patched["priority"] = cur["priority"] // the priority names the rule; it does not change
	if err := checkRule(patched); err != nil {
		return err
	}
	rules[i] = patched
	q.s.st.Res[path]["fingerprint"] = fingerprint()
	return nil
}

func removeRule(q *req, path string) error {
	q.s.mu.Lock()
	defer q.s.mu.Unlock()
	rules, i, err := rule(q, path)
	if err != nil {
		return err
	}
	if sizeOf(rules[i].(map[string]any)["priority"]) == defaultPriority {
		return invalid("Invalid value for field 'priority': '2147483647'. The default rule cannot be removed.")
	}
	doc := q.s.st.Res[path]
	doc["rules"], doc["fingerprint"] = slices.Delete(rules, i, i+1), fingerprint()
	return nil
}

func getRule(q *req, path string) (any, error) {
	q.s.mu.Lock()
	defer q.s.mu.Unlock()
	rules, i, err := rule(q, path)
	if err != nil {
		return nil, err
	}
	return clone(rules[i].(map[string]any)), nil
}
