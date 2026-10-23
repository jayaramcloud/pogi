package compute_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi"
	computev1 "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func pogiStart(t *testing.T) *pogi.Running {
	t.Helper()
	return pogi.Start(t, pogi.Config{Services: []string{"compute", "gcplb", "cloudarmor"}})
}

func lbService(t *testing.T) *computev1.Service {
	t.Helper()
	srv := pogiStart(t)
	return must(computev1.NewService(ctx, option.WithEndpoint(srv.URL+"/compute/v1/"), option.WithoutAuthentication()))
}

// do runs a call that returns an operation and fails on an error or an operation error.
func do(call interface {
	Do(...googleapi.CallOption) (*computev1.Operation, error)
}) *computev1.Operation {
	op, err := call.Do()
	check(err)
	if op.Error != nil || op.Status != "DONE" {
		panic(op)
	}
	return op
}

// status is the HTTP status of a failed call, 0 when it did not fail.
func status(err error) int {
	if ge, ok := err.(*googleapi.Error); ok {
		return ge.Code
	}
	return 0
}

func selfSigned() (cert, key string) {
	k := must(ecdsa.GenerateKey(elliptic.P256(), rand.Reader))
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "example.com"}, NotBefore: time.Now(), NotAfter: time.Now().Add(24 * time.Hour)}
	der := must(x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k))
	kb := must(x509.MarshalPKCS8PrivateKey(k))
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}))
}

func TestLoadBalancing(t *testing.T) {
	svc := lbService(t)
	g := func(coll string) string {
		return "https://www.googleapis.com/compute/v1/projects/" + project + "/global/" + coll + "/"
	}

	// a policy with a rule, then a health check, backend service, URL map, proxies, certificate and rule
	do(svc.SecurityPolicies.Insert(project, &computev1.SecurityPolicy{Name: "pol", Rules: []*computev1.SecurityPolicyRule{
		{Priority: 1000, Action: "deny(403)", Match: &computev1.SecurityPolicyRuleMatcher{VersionedExpr: "SRC_IPS_V1", Config: &computev1.SecurityPolicyRuleMatcherConfig{SrcIpRanges: []string{"9.9.9.0/24"}}}}}}))
	pol := must(svc.SecurityPolicies.Get(project, "pol").Do())
	if len(pol.Rules) != 2 || pol.Rules[1].Priority != 2147483647 || pol.Type != "CLOUD_ARMOR" {
		t.Fatalf("policy %+v", pol)
	}
	do(svc.HealthChecks.Insert(project, &computev1.HealthCheck{Name: "hc", Type: "TCP", TcpHealthCheck: &computev1.TCPHealthCheck{Port: 80}}))
	hc := must(svc.HealthChecks.Get(project, "hc").Do())
	if hc.CheckIntervalSec != 5 || hc.TcpHealthCheck.ProxyHeader != "NONE" {
		t.Fatalf("health check %+v", hc)
	}
	do(svc.BackendServices.Insert(project, &computev1.BackendService{Name: "bs", HealthChecks: []string{"hc"}, SecurityPolicy: "pol"}))
	bs := must(svc.BackendServices.Get(project, "bs").Do())
	if bs.HealthChecks[0] != g("healthChecks")+"hc" || bs.Protocol != "HTTP" || bs.SecurityPolicy != g("securityPolicies")+"pol" {
		t.Fatalf("backend service %+v", bs)
	}
	do(svc.UrlMaps.Insert(project, &computev1.UrlMap{Name: "um", DefaultService: g("backendServices") + "bs"}))
	do(svc.TargetHttpProxies.Insert(project, &computev1.TargetHttpProxy{Name: "tp", UrlMap: "um"}))
	cert, key := selfSigned()
	do(svc.SslCertificates.Insert(project, &computev1.SslCertificate{Name: "cert", Certificate: cert, PrivateKey: key}))
	if c := must(svc.SslCertificates.Get(project, "cert").Do()); c.PrivateKey != "" || c.Type != "SELF_MANAGED" || c.ExpireTime == "" {
		t.Fatalf("certificate %+v", c)
	}
	do(svc.TargetHttpsProxies.Insert(project, &computev1.TargetHttpsProxy{Name: "tsp", UrlMap: "um", SslCertificates: []string{"cert"}}))
	do(svc.GlobalForwardingRules.Insert(project, &computev1.ForwardingRule{Name: "fr", Target: g("targetHttpProxies") + "tp", PortRange: "80"}))
	fr := must(svc.GlobalForwardingRules.Get(project, "fr").Do())
	if fr.PortRange != "80-80" || fr.IPAddress == "" || fr.IPProtocol != "TCP" || fr.Target != g("targetHttpProxies")+"tp" {
		t.Fatalf("forwarding rule %+v", fr)
	}
	if l := must(svc.GlobalForwardingRules.List(project).Do()); len(l.Items) != 1 {
		t.Fatalf("list %+v", l)
	}
	do(svc.TargetHttpProxies.SetUrlMap(project, "tp", &computev1.UrlMapReference{UrlMap: "um"}))

	// the rules of a policy
	rule := &computev1.SecurityPolicyRule{Priority: 2000, Action: "allow", Match: &computev1.SecurityPolicyRuleMatcher{VersionedExpr: "SRC_IPS_V1", Config: &computev1.SecurityPolicyRuleMatcherConfig{SrcIpRanges: []string{"1.2.3.4"}}}}
	do(svc.SecurityPolicies.AddRule(project, "pol", rule))
	do(svc.SecurityPolicies.PatchRule(project, "pol", &computev1.SecurityPolicyRule{Action: "deny(404)", Match: rule.Match}).Priority(2000))
	if r := must(svc.SecurityPolicies.GetRule(project, "pol").Priority(2000).Do()); r.Action != "deny(404)" {
		t.Fatalf("rule %+v", r)
	}
	do(svc.SecurityPolicies.RemoveRule(project, "pol").Priority(2000))

	// a global operation made here is read back through Compute's operations
	op := do(svc.UrlMaps.Patch(project, "um", &computev1.UrlMap{Description: "d"}))
	if got := must(svc.GlobalOperations.Get(project, op.Name).Do()); got.Status != "DONE" {
		t.Fatalf("operation %+v", got)
	}
	if must(svc.UrlMaps.Get(project, "um").Do()).Description != "d" {
		t.Fatal("patch lost")
	}

	// in use, then deleted in order
	for _, c := range []struct {
		name string
		del  func() error
	}{
		{"url map", func() error { _, err := svc.UrlMaps.Delete(project, "um").Do(); return err }},
		{"backend service", func() error { _, err := svc.BackendServices.Delete(project, "bs").Do(); return err }},
		{"health check", func() error { _, err := svc.HealthChecks.Delete(project, "hc").Do(); return err }},
		{"policy", func() error { _, err := svc.SecurityPolicies.Delete(project, "pol").Do(); return err }},
		{"certificate", func() error { _, err := svc.SslCertificates.Delete(project, "cert").Do(); return err }},
	} {
		if err := c.del(); status(err) != http.StatusBadRequest || !strings.Contains(err.Error(), "resourceInUseByAnotherResource") {
			t.Errorf("delete %s in use: %v", c.name, err)
		}
	}
	do(svc.GlobalForwardingRules.Delete(project, "fr"))
	do(svc.TargetHttpsProxies.Delete(project, "tsp"))
	do(svc.TargetHttpProxies.Delete(project, "tp"))
	do(svc.UrlMaps.Delete(project, "um"))
	do(svc.BackendServices.Delete(project, "bs"))
	do(svc.HealthChecks.Delete(project, "hc"))
	do(svc.SecurityPolicies.Delete(project, "pol"))
	do(svc.SslCertificates.Delete(project, "cert"))
}

func TestLoadBalancingFailures(t *testing.T) {
	svc := lbService(t)
	cert, key := selfSigned()
	armor := &computev1.SecurityPolicyRuleMatcher{VersionedExpr: "SRC_IPS_V1", Config: &computev1.SecurityPolicyRuleMatcherConfig{SrcIpRanges: []string{"*"}}}
	do(svc.HealthChecks.Insert(project, &computev1.HealthCheck{Name: "hc", Type: "TCP"}))
	do(svc.BackendServices.Insert(project, &computev1.BackendService{Name: "bs"}))
	do(svc.UrlMaps.Insert(project, &computev1.UrlMap{Name: "um", DefaultService: "bs"}))
	do(svc.SecurityPolicies.Insert(project, &computev1.SecurityPolicy{Name: "pol"}))

	fail := func(what string, want int, err error) {
		t.Helper()
		if status(err) != want {
			t.Errorf("%s: want %d, got %v", what, want, err)
		}
	}
	ins := func(f func(...googleapi.CallOption) (*computev1.Operation, error)) error { _, err := f(); return err }
	fail("bad name", 400, ins(svc.HealthChecks.Insert(project, &computev1.HealthCheck{Name: "Bad_Name", Type: "TCP"}).Do))
	fail("no type", 400, ins(svc.HealthChecks.Insert(project, &computev1.HealthCheck{Name: "x"}).Do))
	fail("unknown type", 400, ins(svc.HealthChecks.Insert(project, &computev1.HealthCheck{Name: "x", Type: "ICMP"}).Do))
	fail("bad port", 400, ins(svc.HealthChecks.Insert(project, &computev1.HealthCheck{Name: "x", Type: "TCP", TcpHealthCheck: &computev1.TCPHealthCheck{Port: 70000}}).Do))
	fail("duplicate", 409, ins(svc.HealthChecks.Insert(project, &computev1.HealthCheck{Name: "hc", Type: "TCP"}).Do))
	fail("health check missing", 404, ins(svc.BackendServices.Insert(project, &computev1.BackendService{Name: "x", HealthChecks: []string{"nope"}}).Do))
	fail("bad protocol", 400, ins(svc.BackendServices.Insert(project, &computev1.BackendService{Name: "x", Protocol: "FTP"}).Do))
	fail("policy missing", 404, ins(svc.BackendServices.Insert(project, &computev1.BackendService{Name: "x", SecurityPolicy: "nope"}).Do))
	fail("url map without service", 400, ins(svc.UrlMaps.Insert(project, &computev1.UrlMap{Name: "x"}).Do))
	fail("url map backend missing", 404, ins(svc.UrlMaps.Insert(project, &computev1.UrlMap{Name: "x", DefaultService: "nope"}).Do))
	fail("proxy without url map", 400, ins(svc.TargetHttpProxies.Insert(project, &computev1.TargetHttpProxy{Name: "x"}).Do))
	fail("proxy url map missing", 404, ins(svc.TargetHttpProxies.Insert(project, &computev1.TargetHttpProxy{Name: "x", UrlMap: "nope"}).Do))
	fail("https proxy without certificate", 400, ins(svc.TargetHttpsProxies.Insert(project, &computev1.TargetHttpsProxy{Name: "x", UrlMap: "um"}).Do))
	fail("certificate missing", 404, ins(svc.TargetHttpsProxies.Insert(project, &computev1.TargetHttpsProxy{Name: "x", UrlMap: "um", SslCertificates: []string{"nope"}}).Do))
	fail("certificate without key", 400, ins(svc.SslCertificates.Insert(project, &computev1.SslCertificate{Name: "x", Certificate: cert}).Do))
	fail("certificate not PEM", 400, ins(svc.SslCertificates.Insert(project, &computev1.SslCertificate{Name: "x", Certificate: "abc", PrivateKey: key}).Do))
	fail("forwarding rule without target", 400, ins(svc.GlobalForwardingRules.Insert(project, &computev1.ForwardingRule{Name: "x"}).Do))
	fail("forwarding rule target missing", 404, ins(svc.GlobalForwardingRules.Insert(project, &computev1.ForwardingRule{Name: "x", Target: "global/targetHttpProxies/nope"}).Do))
	fail("forwarding rule port", 400, ins(svc.GlobalForwardingRules.Insert(project, &computev1.ForwardingRule{Name: "x", Target: "global/targetHttpProxies/nope", PortRange: "99999"}).Do))
	fail("get missing", 404, func() error { _, err := svc.BackendServices.Get(project, "nope").Do(); return err }())
	fail("delete missing", 404, ins(svc.UrlMaps.Delete(project, "nope").Do))
	fail("policy bad action", 400, ins(svc.SecurityPolicies.AddRule(project, "pol", &computev1.SecurityPolicyRule{Priority: 1, Action: "maybe", Match: armor}).Do))
	fail("policy rule without match", 400, ins(svc.SecurityPolicies.AddRule(project, "pol", &computev1.SecurityPolicyRule{Priority: 1, Action: "allow"}).Do))
	fail("policy bad priority", 400, ins(svc.SecurityPolicies.AddRule(project, "pol", &computev1.SecurityPolicyRule{Priority: -1, Action: "allow", Match: armor, ForceSendFields: []string{"Priority"}}).Do))
	fail("policy duplicate priority", 400, ins(svc.SecurityPolicies.AddRule(project, "pol", &computev1.SecurityPolicyRule{Priority: 2147483647, Action: "allow", Match: armor}).Do))
	fail("policy remove default", 400, ins(svc.SecurityPolicies.RemoveRule(project, "pol").Priority(2147483647).Do))
	fail("policy remove missing rule", 404, ins(svc.SecurityPolicies.RemoveRule(project, "pol").Priority(5).Do))
	fail("policy patch missing rule", 404, ins(svc.SecurityPolicies.PatchRule(project, "pol", &computev1.SecurityPolicyRule{Action: "allow", Match: armor}).Priority(5).Do))
	fail("rule on missing policy", 404, ins(svc.SecurityPolicies.AddRule(project, "nope", &computev1.SecurityPolicyRule{Priority: 1, Action: "allow", Match: armor}).Do))
}

// A regional load balancer is stored in its region and listed in the aggregated list.
func TestRegionalLoadBalancing(t *testing.T) {
	svc := lbService(t)
	do(svc.RegionHealthChecks.Insert(project, "us-central1", &computev1.HealthCheck{Name: "rhc", Type: "HTTP"}))
	do(svc.RegionBackendServices.Insert(project, "us-central1", &computev1.BackendService{Name: "rbs", HealthChecks: []string{"rhc"}, LoadBalancingScheme: "INTERNAL_MANAGED"}))
	if bs := must(svc.RegionBackendServices.Get(project, "us-central1", "rbs").Do()); !strings.Contains(bs.HealthChecks[0], "/regions/us-central1/healthChecks/rhc") {
		t.Fatalf("%+v", bs)
	}
	agg := must(svc.BackendServices.AggregatedList(project).Do())
	if len(agg.Items["regions/us-central1"].BackendServices) != 1 {
		t.Fatalf("%+v", agg.Items)
	}
	if _, err := svc.RegionHealthChecks.Get(project, "nope-region", "rhc").Do(); status(err) != 404 {
		t.Errorf("unknown region: %v", err)
	}
}
