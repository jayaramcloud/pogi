package elbv2_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	elb "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	ltypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/smithy-go"
	ec2svc "github.com/nrndev/pogi/internal/aws/ec2"
	"github.com/nrndev/pogi/internal/aws/elbv2"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
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
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return fmt.Sprint(err)
}

type env struct {
	hub  *core.Hub
	svc  *elbv2.Service
	elb  *elb.Client
	cfg  aws.Config
	url  string // http://127.0.0.1:<port>
	port string
}

func setup(t *testing.T, more ...func(*core.Hub) core.Service) *env {
	e := &env{}
	build := append([]func(*core.Hub) core.Service{func(h *core.Hub) core.Service { e.svc = elbv2.New(h); return e.svc }}, more...)
	e.hub, e.url = core.ServeTest(t, build...)
	e.cfg = awstest.Config(t, e.url)
	e.elb = elb.NewFromConfig(e.cfg)
	e.port = e.url[strings.LastIndex(e.url, ":")+1:]
	return e
}

func (e *env) alb(t *testing.T, name string) types.LoadBalancer {
	t.Helper()
	return must(e.elb.CreateLoadBalancer(ctx, &elb.CreateLoadBalancerInput{Name: aws.String(name),
		Subnets: []string{"subnet-1", "subnet-2"}})).LoadBalancers[0]
}

func (e *env) group(t *testing.T, name string, in *elb.CreateTargetGroupInput) types.TargetGroup {
	t.Helper()
	if in == nil {
		in = &elb.CreateTargetGroupInput{Protocol: types.ProtocolEnumHttp, Port: aws.Int32(80), VpcId: aws.String("vpc-1"), TargetType: types.TargetTypeEnumIp}
	}
	in.Name = aws.String(name)
	return must(e.elb.CreateTargetGroup(ctx, in)).TargetGroups[0]
}

func forward(tg *string) []types.Action {
	return []types.Action{{Type: types.ActionTypeEnumForward, TargetGroupArn: tg}}
}

func fixed(status, body string) []types.Action {
	return []types.Action{{Type: types.ActionTypeEnumFixedResponse, FixedResponseConfig: &types.FixedResponseActionConfig{
		StatusCode: aws.String(status), ContentType: aws.String("text/plain"), MessageBody: aws.String(body)}}}
}

// freePort returns a port nothing listens on, for listener ports.
func freePort(t *testing.T) int32 {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return int32(ln.Addr().(*net.TCPAddr).Port)
}

var noRedirects = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

// call sends a request to url with Host host and returns status, body and headers.
func call(t *testing.T, method, url, host string, header map[string]string) (int, string, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(method, url, nil)
	req.Host = host
	for k, v := range header {
		req.Header.Set(k, v)
	}
	res, err := noRedirects.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(b), res.Header
}

func TestLoadBalancers(t *testing.T) {
	e := setup(t)
	out := must(e.elb.CreateLoadBalancer(ctx, &elb.CreateLoadBalancerInput{Name: aws.String("web"), Subnets: []string{"subnet-1", "subnet-2"},
		SecurityGroups: []string{"sg-1"}, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}))
	lb := out.LoadBalancers[0]
	if lb.State.Code != types.LoadBalancerStateEnumProvisioning || lb.Type != types.LoadBalancerTypeEnumApplication ||
		lb.Scheme != types.LoadBalancerSchemeEnumInternetFacing || *lb.CanonicalHostedZoneId != "Z35SXDOTRQ7X7K" ||
		!strings.HasPrefix(*lb.DNSName, "web-") || !strings.HasSuffix(*lb.DNSName, ".us-east-1.elb.localhost") ||
		!strings.Contains(*lb.LoadBalancerArn, ":loadbalancer/app/web/") || len(lb.AvailabilityZones) != 2 {
		t.Fatalf("created %+v", lb)
	}
	got := must(e.elb.DescribeLoadBalancers(ctx, &elb.DescribeLoadBalancersInput{Names: []string{"web"}})).LoadBalancers
	if len(got) != 1 || got[0].State.Code != types.LoadBalancerStateEnumActive || got[0].SecurityGroups[0] != "sg-1" {
		t.Fatalf("described %+v", got)
	}

	// same settings: the same load balancer; other settings: a clash
	again := must(e.elb.CreateLoadBalancer(ctx, &elb.CreateLoadBalancerInput{Name: aws.String("web")})).LoadBalancers[0]
	if *again.LoadBalancerArn != *lb.LoadBalancerArn {
		t.Fatal("create is not idempotent")
	}
	for name, c := range map[string]struct {
		err  error
		code string
	}{
		"duplicate": {must2(e.elb.CreateLoadBalancer(ctx, &elb.CreateLoadBalancerInput{Name: aws.String("web"), Type: types.LoadBalancerTypeEnumNetwork})), "DuplicateLoadBalancerName"},
		"bad name":  {must2(e.elb.CreateLoadBalancer(ctx, &elb.CreateLoadBalancerInput{Name: aws.String("-nope")})), "ValidationError"},
		"internal-": {must2(e.elb.CreateLoadBalancer(ctx, &elb.CreateLoadBalancerInput{Name: aws.String("internal-x")})), "ValidationError"},
		"missing":   {must2(e.elb.DescribeLoadBalancers(ctx, &elb.DescribeLoadBalancersInput{Names: []string{"gone"}})), "LoadBalancerNotFound"},
		"attr key":  {must2(e.elb.ModifyLoadBalancerAttributes(ctx, &elb.ModifyLoadBalancerAttributesInput{LoadBalancerArn: lb.LoadBalancerArn, Attributes: []types.LoadBalancerAttribute{{Key: aws.String("nope"), Value: aws.String("1")}}})), "ValidationError"},
		"tag dup":   {must2(e.elb.AddTags(ctx, &elb.AddTagsInput{ResourceArns: []string{*lb.LoadBalancerArn}, Tags: []types.Tag{{Key: aws.String("a")}, {Key: aws.String("a")}}})), "DuplicateTagKeys"},
		"tag gone":  {must2(e.elb.DescribeTags(ctx, &elb.DescribeTagsInput{ResourceArns: []string{strings.Replace(*lb.LoadBalancerArn, "/web/", "/gone/", 1)}})), "LoadBalancerNotFound"},
	} {
		if code(c.err) != c.code {
			t.Errorf("%s: got %v, want %s", name, c.err, c.code)
		}
	}

	attrs := must(e.elb.ModifyLoadBalancerAttributes(ctx, &elb.ModifyLoadBalancerAttributesInput{LoadBalancerArn: lb.LoadBalancerArn,
		Attributes: []types.LoadBalancerAttribute{{Key: aws.String("idle_timeout.timeout_seconds"), Value: aws.String("120")},
			{Key: aws.String("deletion_protection.enabled"), Value: aws.String("true")}}})).Attributes
	if a := attrMap(attrs); a["idle_timeout.timeout_seconds"] != "120" || a["routing.http2.enabled"] != "true" || a["access_logs.s3.enabled"] != "false" {
		t.Fatalf("attributes %v", a)
	}
	if _, err := e.elb.DeleteLoadBalancer(ctx, &elb.DeleteLoadBalancerInput{LoadBalancerArn: lb.LoadBalancerArn}); code(err) != "OperationNotPermitted" {
		t.Fatalf("deletion protection: %v", err)
	}
	must(e.elb.ModifyLoadBalancerAttributes(ctx, &elb.ModifyLoadBalancerAttributesInput{LoadBalancerArn: lb.LoadBalancerArn,
		Attributes: []types.LoadBalancerAttribute{{Key: aws.String("deletion_protection.enabled"), Value: aws.String("false")}}}))

	must(e.elb.SetSecurityGroups(ctx, &elb.SetSecurityGroupsInput{LoadBalancerArn: lb.LoadBalancerArn, SecurityGroups: []string{"sg-2", "sg-3"}}))
	must(e.elb.SetSubnets(ctx, &elb.SetSubnetsInput{LoadBalancerArn: lb.LoadBalancerArn, Subnets: []string{"subnet-1", "subnet-2", "subnet-3"}}))
	must(e.elb.SetIpAddressType(ctx, &elb.SetIpAddressTypeInput{LoadBalancerArn: lb.LoadBalancerArn, IpAddressType: types.IpAddressTypeDualstack}))
	if _, err := e.elb.SetIpAddressType(ctx, &elb.SetIpAddressTypeInput{LoadBalancerArn: lb.LoadBalancerArn, IpAddressType: "ipv5"}); code(err) != "ValidationError" {
		t.Fatalf("ip type: %v", err)
	}
	got = must(e.elb.DescribeLoadBalancers(ctx, &elb.DescribeLoadBalancersInput{LoadBalancerArns: []string{*lb.LoadBalancerArn}})).LoadBalancers
	if len(got[0].SecurityGroups) != 2 || len(got[0].AvailabilityZones) != 3 || got[0].IpAddressType != types.IpAddressTypeDualstack {
		t.Fatalf("after set: %+v", got[0])
	}

	if c := must(e.elb.DescribeCapacityReservation(ctx, &elb.DescribeCapacityReservationInput{LoadBalancerArn: lb.LoadBalancerArn})); c.MinimumLoadBalancerCapacity != nil {
		t.Fatalf("capacity %+v", c.MinimumLoadBalancerCapacity)
	}
	must(e.elb.ModifyCapacityReservation(ctx, &elb.ModifyCapacityReservationInput{LoadBalancerArn: lb.LoadBalancerArn,
		MinimumLoadBalancerCapacity: &types.MinimumLoadBalancerCapacity{CapacityUnits: aws.Int32(100)}}))
	if c := must(e.elb.DescribeCapacityReservation(ctx, &elb.DescribeCapacityReservationInput{LoadBalancerArn: lb.LoadBalancerArn})); *c.MinimumLoadBalancerCapacity.CapacityUnits != 100 {
		t.Fatalf("capacity %+v", c.MinimumLoadBalancerCapacity)
	}
	must(e.elb.AddTags(ctx, &elb.AddTagsInput{ResourceArns: []string{*lb.LoadBalancerArn}, Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("a")}}}))
	must(e.elb.RemoveTags(ctx, &elb.RemoveTagsInput{ResourceArns: []string{*lb.LoadBalancerArn}, TagKeys: []string{"env"}}))
	tags := must(e.elb.DescribeTags(ctx, &elb.DescribeTagsInput{ResourceArns: []string{*lb.LoadBalancerArn}})).TagDescriptions[0].Tags
	if len(tags) != 1 || *tags[0].Key != "team" || *tags[0].Value != "a" {
		t.Fatalf("tags %+v", tags)
	}

	nlb := must(e.elb.CreateLoadBalancer(ctx, &elb.CreateLoadBalancerInput{Name: aws.String("tcp"), Type: types.LoadBalancerTypeEnumNetwork,
		Scheme: types.LoadBalancerSchemeEnumInternal, SubnetMappings: []types.SubnetMapping{{SubnetId: aws.String("subnet-1"), PrivateIPv4Address: aws.String("10.0.0.9")}}})).LoadBalancers[0]
	if !strings.HasPrefix(*nlb.DNSName, "internal-tcp-") || *nlb.CanonicalHostedZoneId != "Z26RNL4JYFTOTI" ||
		*nlb.AvailabilityZones[0].LoadBalancerAddresses[0].PrivateIPv4Address != "10.0.0.9" {
		t.Fatalf("nlb %+v", nlb)
	}
	if a := attrMap(must(e.elb.DescribeLoadBalancerAttributes(ctx, &elb.DescribeLoadBalancerAttributesInput{LoadBalancerArn: nlb.LoadBalancerArn})).Attributes); a["load_balancing.cross_zone.enabled"] != "false" || a["routing.http2.enabled"] != "" {
		t.Fatalf("nlb attributes %v", a)
	}
	page := must(e.elb.DescribeLoadBalancers(ctx, &elb.DescribeLoadBalancersInput{PageSize: aws.Int32(1)}))
	if len(page.LoadBalancers) != 1 || page.NextMarker == nil {
		t.Fatalf("page %+v", page)
	}
	page = must(e.elb.DescribeLoadBalancers(ctx, &elb.DescribeLoadBalancersInput{PageSize: aws.Int32(1), Marker: page.NextMarker}))
	if len(page.LoadBalancers) != 1 || page.NextMarker != nil || *page.LoadBalancers[0].LoadBalancerName != "web" {
		t.Fatalf("page 2 %+v", page)
	}

	for _, arn := range []*string{lb.LoadBalancerArn, nlb.LoadBalancerArn, lb.LoadBalancerArn} { // deleting twice succeeds
		must(e.elb.DeleteLoadBalancer(ctx, &elb.DeleteLoadBalancerInput{LoadBalancerArn: arn}))
	}
	if n := len(must(e.elb.DescribeLoadBalancers(ctx, &elb.DescribeLoadBalancersInput{})).LoadBalancers); n != 0 {
		t.Fatalf("%d load balancers left", n)
	}
	if lim := must(e.elb.DescribeAccountLimits(ctx, &elb.DescribeAccountLimitsInput{})).Limits; len(lim) < 10 {
		t.Fatalf("limits %+v", lim)
	}
}

func must2[T any](_ T, err error) error { return err }

func attrMap[T any](attrs []T) map[string]string {
	out := map[string]string{}
	for _, a := range attrs {
		b, _ := json.Marshal(a)
		var kv struct{ Key, Value *string }
		json.Unmarshal(b, &kv)
		out[aws.ToString(kv.Key)] = aws.ToString(kv.Value)
	}
	return out
}

func TestListenersAndRules(t *testing.T) {
	e := setup(t)
	lb := e.alb(t, "web")
	tg := e.group(t, "api", nil)
	if *tg.HealthCheckPath != "/" || *tg.Matcher.HttpCode != "200" || *tg.HealthCheckIntervalSeconds != 30 || *tg.HealthCheckTimeoutSeconds != 5 ||
		*tg.HealthyThresholdCount != 5 || *tg.UnhealthyThresholdCount != 2 || *tg.HealthCheckPort != "traffic-port" || tg.HealthCheckProtocol != types.ProtocolEnumHttp ||
		*tg.ProtocolVersion != "HTTP1" || tg.IpAddressType != types.TargetGroupIpAddressTypeEnumIpv4 {
		t.Fatalf("target group defaults %+v", tg)
	}
	tcp := e.group(t, "tcp", &elb.CreateTargetGroupInput{Protocol: types.ProtocolEnumTcp, Port: aws.Int32(5432), VpcId: aws.String("vpc-1")})
	if tcp.HealthCheckProtocol != types.ProtocolEnumTcp || *tcp.HealthCheckTimeoutSeconds != 10 || tcp.Matcher != nil || tcp.TargetType != types.TargetTypeEnumInstance {
		t.Fatalf("tcp defaults %+v", tcp)
	}
	fn := e.group(t, "fn", &elb.CreateTargetGroupInput{TargetType: types.TargetTypeEnumLambda})
	if *fn.HealthCheckEnabled || fn.Port != nil || *fn.HealthCheckIntervalSeconds != 35 {
		t.Fatalf("lambda defaults %+v", fn)
	}
	if a := attrMap(must(e.elb.DescribeTargetGroupAttributes(ctx, &elb.DescribeTargetGroupAttributesInput{TargetGroupArn: tcp.TargetGroupArn})).Attributes); a["preserve_client_ip.enabled"] != "true" || a["proxy_protocol_v2.enabled"] != "false" {
		t.Fatalf("tcp attributes %v", a)
	}
	a := attrMap(must(e.elb.ModifyTargetGroupAttributes(ctx, &elb.ModifyTargetGroupAttributesInput{TargetGroupArn: tg.TargetGroupArn,
		Attributes: []types.TargetGroupAttribute{{Key: aws.String("deregistration_delay.timeout_seconds"), Value: aws.String("0")}}})).Attributes)
	if a["deregistration_delay.timeout_seconds"] != "0" || a["stickiness.type"] != "lb_cookie" {
		t.Fatalf("attributes %v", a)
	}

	l := must(e.elb.CreateListener(ctx, &elb.CreateListenerInput{LoadBalancerArn: lb.LoadBalancerArn, Protocol: types.ProtocolEnumHttp,
		Port: aws.Int32(freePort(t)), DefaultActions: forward(tg.TargetGroupArn)})).Listeners[0]
	d := l.DefaultActions[0]
	if *d.TargetGroupArn != *tg.TargetGroupArn || *d.Order != 1 || *d.ForwardConfig.TargetGroups[0].Weight != 1 || *d.ForwardConfig.TargetGroupStickinessConfig.Enabled {
		t.Fatalf("default action %+v", d)
	}
	https := must(e.elb.CreateListener(ctx, &elb.CreateListenerInput{LoadBalancerArn: lb.LoadBalancerArn, Protocol: types.ProtocolEnumHttps,
		Port: aws.Int32(freePort(t)), Certificates: []types.Certificate{{CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/a")}},
		DefaultActions: fixed("404", "no")})).Listeners[0]
	if *https.SslPolicy != "ELBSecurityPolicy-2016-08" || *https.Certificates[0].CertificateArn != "arn:aws:acm:us-east-1:000000000000:certificate/a" {
		t.Fatalf("https %+v", https)
	}
	must(e.elb.AddListenerCertificates(ctx, &elb.AddListenerCertificatesInput{ListenerArn: https.ListenerArn,
		Certificates: []types.Certificate{{CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/b")}}}))
	certs := must(e.elb.DescribeListenerCertificates(ctx, &elb.DescribeListenerCertificatesInput{ListenerArn: https.ListenerArn})).Certificates
	if len(certs) != 2 || !*certs[0].IsDefault || *certs[1].IsDefault {
		t.Fatalf("certificates %+v", certs)
	}
	must(e.elb.RemoveListenerCertificates(ctx, &elb.RemoveListenerCertificatesInput{ListenerArn: https.ListenerArn,
		Certificates: []types.Certificate{{CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/b")}}}))
	if a := attrMap(must(e.elb.DescribeListenerAttributes(ctx, &elb.DescribeListenerAttributesInput{ListenerArn: https.ListenerArn})).Attributes); a["routing.http.request.x_amzn_tls_version.header_name"] != "X-Amzn-Tls-Version" {
		t.Fatalf("listener attributes %v", a)
	}

	r10 := must(e.elb.CreateRule(ctx, &elb.CreateRuleInput{ListenerArn: l.ListenerArn, Priority: aws.Int32(10), Actions: fixed("200", "api"),
		Conditions: []types.RuleCondition{{Field: aws.String("path-pattern"), PathPatternConfig: &types.PathPatternConditionConfig{Values: []string{"/api/*"}}}}})).Rules[0]
	if r10.Conditions[0].Values[0] != "/api/*" || *r10.Priority != "10" || *r10.IsDefault {
		t.Fatalf("rule %+v", r10)
	}
	r20 := must(e.elb.CreateRule(ctx, &elb.CreateRuleInput{ListenerArn: l.ListenerArn, Priority: aws.Int32(20), Actions: forward(tg.TargetGroupArn),
		Conditions: []types.RuleCondition{{Field: aws.String("host-header"), Values: []string{"api.example.com"}}}})).Rules[0]
	if r20.Conditions[0].HostHeaderConfig.Values[0] != "api.example.com" {
		t.Fatalf("legacy host condition %+v", r20.Conditions[0])
	}
	rules := must(e.elb.DescribeRules(ctx, &elb.DescribeRulesInput{ListenerArn: l.ListenerArn})).Rules
	if len(rules) != 3 || *rules[0].Priority != "10" || *rules[2].Priority != "default" || !*rules[2].IsDefault || *rules[2].Actions[0].TargetGroupArn != *tg.TargetGroupArn {
		t.Fatalf("rules %+v", rules)
	}
	swapped := must(e.elb.SetRulePriorities(ctx, &elb.SetRulePrioritiesInput{RulePriorities: []types.RulePriorityPair{
		{RuleArn: r10.RuleArn, Priority: aws.Int32(20)}, {RuleArn: r20.RuleArn, Priority: aws.Int32(10)}}})).Rules
	if *swapped[0].RuleArn != *r20.RuleArn || *swapped[1].Priority != "20" {
		t.Fatalf("swapped %+v", swapped)
	}
	mod := must(e.elb.ModifyRule(ctx, &elb.ModifyRuleInput{RuleArn: r10.RuleArn, Conditions: []types.RuleCondition{{Field: aws.String("http-request-method"),
		HttpRequestMethodConfig: &types.HttpRequestMethodConditionConfig{Values: []string{"DELETE"}}}}})).Rules[0]
	if *mod.Conditions[0].Field != "http-request-method" || *mod.Actions[0].FixedResponseConfig.MessageBody != "api" {
		t.Fatalf("modified %+v", mod)
	}

	tgs := must(e.elb.DescribeTargetGroups(ctx, &elb.DescribeTargetGroupsInput{LoadBalancerArn: lb.LoadBalancerArn})).TargetGroups
	if len(tgs) != 1 || tgs[0].LoadBalancerArns[0] != *lb.LoadBalancerArn {
		t.Fatalf("groups of the load balancer %+v", tgs)
	}

	missing := strings.Replace(*tg.TargetGroupArn, "/api/", "/gone/", 1)
	for name, c := range map[string]struct {
		err  error
		code string
	}{
		"no group":       {must2(e.elb.CreateListener(ctx, &elb.CreateListenerInput{LoadBalancerArn: lb.LoadBalancerArn, Protocol: types.ProtocolEnumHttp, Port: aws.Int32(81), DefaultActions: forward(&missing)})), "TargetGroupNotFound"},
		"same port":      {must2(e.elb.CreateListener(ctx, &elb.CreateListenerInput{LoadBalancerArn: lb.LoadBalancerArn, Protocol: types.ProtocolEnumHttp, Port: l.Port, DefaultActions: fixed("200", "x")})), "DuplicateListener"},
		"tcp on alb":     {must2(e.elb.CreateListener(ctx, &elb.CreateListenerInput{LoadBalancerArn: lb.LoadBalancerArn, Protocol: types.ProtocolEnumTcp, Port: aws.Int32(82), DefaultActions: fixed("200", "x")})), "ValidationError"},
		"no cert":        {must2(e.elb.CreateListener(ctx, &elb.CreateListenerInput{LoadBalancerArn: lb.LoadBalancerArn, Protocol: types.ProtocolEnumHttps, Port: aws.Int32(83), DefaultActions: fixed("200", "x")})), "ValidationError"},
		"bad policy":     {must2(e.elb.ModifyListener(ctx, &elb.ModifyListenerInput{ListenerArn: https.ListenerArn, SslPolicy: aws.String("Mine")})), "SSLPolicyNotFound"},
		"priority taken": {must2(e.elb.CreateRule(ctx, &elb.CreateRuleInput{ListenerArn: l.ListenerArn, Priority: aws.Int32(10), Actions: fixed("200", "x"), Conditions: []types.RuleCondition{{Field: aws.String("path-pattern"), Values: []string{"/x"}}}})), "PriorityInUse"},
		"priority swap":  {must2(e.elb.SetRulePriorities(ctx, &elb.SetRulePrioritiesInput{RulePriorities: []types.RulePriorityPair{{RuleArn: r10.RuleArn, Priority: aws.Int32(10)}}})), "PriorityInUse"},
		"no conditions":  {must2(e.elb.CreateRule(ctx, &elb.CreateRuleInput{ListenerArn: l.ListenerArn, Priority: aws.Int32(30), Actions: fixed("200", "x"), Conditions: []types.RuleCondition{}})), "ValidationError"},
		"bad field":      {must2(e.elb.CreateRule(ctx, &elb.CreateRuleInput{ListenerArn: l.ListenerArn, Priority: aws.Int32(30), Actions: fixed("200", "x"), Conditions: []types.RuleCondition{{Field: aws.String("cookie"), Values: []string{"x"}}}})), "ValidationError"},
		"bad action":     {must2(e.elb.ModifyRule(ctx, &elb.ModifyRuleInput{RuleArn: r10.RuleArn, Actions: []types.Action{{Type: "teleport"}}})), "ValidationError"},
		"bad status":     {must2(e.elb.ModifyRule(ctx, &elb.ModifyRuleInput{RuleArn: r10.RuleArn, Actions: fixed("4xx", "x")})), "ValidationError"},
		"delete default": {must2(e.elb.DeleteRule(ctx, &elb.DeleteRuleInput{RuleArn: rules[2].RuleArn})), "OperationNotPermitted"},
		"no rule":        {must2(e.elb.DescribeRules(ctx, &elb.DescribeRulesInput{RuleArns: []string{*r10.RuleArn + "0"}})), "RuleNotFound"},
		"no listener":    {must2(e.elb.DescribeListeners(ctx, &elb.DescribeListenersInput{ListenerArns: []string{*l.ListenerArn + "0"}})), "ListenerNotFound"},
		"group in use":   {must2(e.elb.DeleteTargetGroup(ctx, &elb.DeleteTargetGroupInput{TargetGroupArn: tg.TargetGroupArn})), "ResourceInUse"},
		"group dup":      {must2(e.elb.CreateTargetGroup(ctx, &elb.CreateTargetGroupInput{Name: aws.String("api"), Protocol: types.ProtocolEnumHttp, Port: aws.Int32(8080), VpcId: aws.String("vpc-1")})), "DuplicateTargetGroupName"},
		"group no vpc":   {must2(e.elb.CreateTargetGroup(ctx, &elb.CreateTargetGroupInput{Name: aws.String("x"), Protocol: types.ProtocolEnumHttp, Port: aws.Int32(80)})), "ValidationError"},
		"group missing":  {must2(e.elb.DescribeTargetGroups(ctx, &elb.DescribeTargetGroupsInput{Names: []string{"gone"}})), "TargetGroupNotFound"},
		"register bad":   {must2(e.elb.RegisterTargets(ctx, &elb.RegisterTargetsInput{TargetGroupArn: tg.TargetGroupArn, Targets: []types.TargetDescription{{Id: aws.String("i-123")}}})), "ValidationError"},
		"register none":  {must2(e.elb.RegisterTargets(ctx, &elb.RegisterTargetsInput{TargetGroupArn: &missing, Targets: []types.TargetDescription{{Id: aws.String("10.0.0.1")}}})), "TargetGroupNotFound"},
		"health missing": {must2(e.elb.DescribeTargetHealth(ctx, &elb.DescribeTargetHealthInput{TargetGroupArn: &missing})), "TargetGroupNotFound"},
		"ssl missing":    {must2(e.elb.DescribeSSLPolicies(ctx, &elb.DescribeSSLPoliciesInput{Names: []string{"Mine"}})), "SSLPolicyNotFound"},
	} {
		if code(c.err) != c.code {
			t.Errorf("%s: got %v, want %s", name, c.err, c.code)
		}
	}

	moved := must(e.elb.ModifyListener(ctx, &elb.ModifyListenerInput{ListenerArn: https.ListenerArn, Protocol: types.ProtocolEnumHttp, Port: aws.Int32(freePort(t))})).Listeners[0]
	if moved.SslPolicy != nil || moved.Certificates != nil || moved.Protocol != types.ProtocolEnumHttp {
		t.Fatalf("https → http keeps %+v", moved)
	}
	pols := must(e.elb.DescribeSSLPolicies(ctx, &elb.DescribeSSLPoliciesInput{Names: []string{"ELBSecurityPolicy-TLS13-1-2-2021-06"}})).SslPolicies
	if len(pols) != 1 || !slices.Contains(pols[0].SslProtocols, "TLSv1.3") || *pols[0].Ciphers[0].Name != "TLS_AES_128_GCM_SHA256" {
		t.Fatalf("ssl policies %+v", pols)
	}

	must(e.elb.DeleteRule(ctx, &elb.DeleteRuleInput{RuleArn: r10.RuleArn}))
	must(e.elb.DeleteListener(ctx, &elb.DeleteListenerInput{ListenerArn: l.ListenerArn}))
	if _, err := e.elb.DescribeRules(ctx, &elb.DescribeRulesInput{RuleArns: []string{*r20.RuleArn}}); code(err) != "RuleNotFound" {
		t.Fatalf("rules outlive their listener: %v", err)
	}
	must(e.elb.DeleteTargetGroup(ctx, &elb.DeleteTargetGroupInput{TargetGroupArn: tg.TargetGroupArn}))
	must(e.elb.DeleteTargetGroup(ctx, &elb.DeleteTargetGroupInput{TargetGroupArn: tg.TargetGroupArn}))
}

// backend is a target that names itself and echoes what ELB added.
func backend(t *testing.T, name string) (*httptest.Server, string, string) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, h := range []string{"X-Forwarded-For", "X-Forwarded-Proto", "X-Forwarded-Port", "X-Amzn-Trace-Id"} {
			w.Header().Set("Got-"+h, r.Header.Get(h))
		}
		w.Header().Set("Got-Host", r.Host)
		io.WriteString(w, name+" "+r.URL.RequestURI())
	}))
	t.Cleanup(srv.Close)
	host, port, _ := net.SplitHostPort(srv.Listener.Addr().String())
	return srv, host, port
}

func TestALB(t *testing.T) {
	e := setup(t)
	_, _, aPort := backend(t, "a")
	_, bHost, bPort := backend(t, "b")
	// a stands in for a task: ECS records where pogi reaches its private IP
	e.hub.SetEndpoint("ip:10.0.1.10:80", "127.0.0.1:"+aPort)
	tgA, tgB := e.group(t, "a", nil), e.group(t, "b", nil)
	both := e.group(t, "both", nil)
	p, _ := strconv.Atoi(bPort)
	must(e.elb.RegisterTargets(ctx, &elb.RegisterTargetsInput{TargetGroupArn: both.TargetGroupArn, Targets: []types.TargetDescription{
		{Id: aws.String("10.0.1.10")}, {Id: aws.String(bHost), Port: aws.Int32(int32(p))}}}))
	must(e.elb.RegisterTargets(ctx, &elb.RegisterTargetsInput{TargetGroupArn: tgA.TargetGroupArn, Targets: []types.TargetDescription{{Id: aws.String("10.0.1.10")}}}))
	must(e.elb.RegisterTargets(ctx, &elb.RegisterTargetsInput{TargetGroupArn: tgB.TargetGroupArn, Targets: []types.TargetDescription{{Id: aws.String(bHost), Port: aws.Int32(int32(p))}}}))

	lb := e.alb(t, "web")
	own := freePort(t)
	l := must(e.elb.CreateListener(ctx, &elb.CreateListenerInput{LoadBalancerArn: lb.LoadBalancerArn, Protocol: types.ProtocolEnumHttp,
		Port: aws.Int32(own), DefaultActions: forward(both.TargetGroupArn)})).Listeners[0]
	rule := func(prio int32, cond types.RuleCondition, acts []types.Action) {
		must(e.elb.CreateRule(ctx, &elb.CreateRuleInput{ListenerArn: l.ListenerArn, Priority: aws.Int32(prio), Conditions: []types.RuleCondition{cond}, Actions: acts}))
	}
	path := func(p string) types.RuleCondition {
		return types.RuleCondition{Field: aws.String("path-pattern"), PathPatternConfig: &types.PathPatternConditionConfig{Values: []string{p}}}
	}
	rule(1, path("/a/*"), forward(tgA.TargetGroupArn))
	rule(2, path("/b/?"), forward(tgB.TargetGroupArn))
	rule(3, types.RuleCondition{Field: aws.String("host-header"), HostHeaderConfig: &types.HostHeaderConditionConfig{Values: []string{"API.example.*"}}}, fixed("200", "api"))
	rule(4, path("/teapot"), fixed("418", "short and stout"))
	rule(5, path("/old/*"), []types.Action{{Type: types.ActionTypeEnumRedirect, RedirectConfig: &types.RedirectActionConfig{
		StatusCode: types.RedirectActionStatusCodeEnumHttp301, Protocol: aws.String("HTTPS"), Port: aws.String("443"), Path: aws.String("/new/#{path}")}}})
	rule(6, types.RuleCondition{Field: aws.String("http-header"), HttpHeaderConfig: &types.HttpHeaderConditionConfig{HttpHeaderName: aws.String("X-Env"), Values: []string{"blue"}}}, fixed("200", "blue"))
	rule(7, types.RuleCondition{Field: aws.String("query-string"), QueryStringConfig: &types.QueryStringConditionConfig{Values: []types.QueryStringKeyValuePair{{Key: aws.String("v"), Value: aws.String("2*")}}}}, fixed("200", "v2"))
	rule(8, types.RuleCondition{Field: aws.String("http-request-method"), HttpRequestMethodConfig: &types.HttpRequestMethodConditionConfig{Values: []string{"DELETE"}}}, fixed("405", "no"))
	rule(9, types.RuleCondition{Field: aws.String("source-ip"), SourceIpConfig: &types.SourceIpConditionConfig{Values: []string{"10.0.0.0/8"}}}, fixed("403", "vpc only"))
	rule(10, path("/split"), []types.Action{{Type: types.ActionTypeEnumForward, ForwardConfig: &types.ForwardActionConfig{
		TargetGroups:                []types.TargetGroupTuple{{TargetGroupArn: tgA.TargetGroupArn, Weight: aws.Int32(0)}, {TargetGroupArn: tgB.TargetGroupArn, Weight: aws.Int32(5)}},
		TargetGroupStickinessConfig: &types.TargetGroupStickinessConfig{Enabled: aws.Bool(true), DurationSeconds: aws.Int32(60)}}}})

	host := *lb.DNSName + ":" + e.port
	var seen []string
	for range 4 {
		status, body, h := call(t, "GET", e.url+"/x?q=1", host, nil)
		if status != 200 || !strings.HasSuffix(body, " /x?q=1") || h.Get("Got-X-Forwarded-Proto") != "http" || h.Get("Got-X-Forwarded-Port") != strconv.Itoa(int(own)) ||
			h.Get("Got-X-Forwarded-For") != "127.0.0.1" || !strings.HasPrefix(h.Get("Got-X-Amzn-Trace-Id"), "Root=1-") || h.Get("Got-Host") != host {
			t.Fatalf("forward: %d %q %v", status, body, h)
		}
		seen = append(seen, body[:1])
	}
	if strings.Join(seen, "") != "abab" && strings.Join(seen, "") != "baba" {
		t.Fatalf("round robin: %v", seen)
	}

	ownURL := "http://127.0.0.1:" + strconv.Itoa(int(own))
	for _, c := range []struct {
		method, url, host string
		header            map[string]string
		status            int
		body              string
	}{
		{"GET", e.url + "/a/1", host, nil, 200, "a /a/1"},
		{"GET", e.url + "/b/1", host, nil, 200, "b /b/1"},
		{"GET", e.url + "/b/12", host, nil, 200, "" /* ? is one character: falls to the default */},
		{"GET", ownURL + "/", "api.example.com", nil, 200, "api"}, // the listener's own port
		{"GET", e.url + "/teapot", host, nil, 418, "short and stout"},
		{"GET", e.url + "/", host, map[string]string{"X-Env": "BLUE"}, 200, "blue"},
		{"GET", e.url + "/?v=21", host, nil, 200, "v2"},
		{"DELETE", e.url + "/", host, nil, 405, "no"},
		{"GET", e.url + "/split", host, nil, 200, "b /split"},
	} {
		status, body, _ := call(t, c.method, c.url, c.host, c.header)
		if status != c.status || c.body != "" && body != c.body {
			t.Errorf("%s %s (Host %s): %d %q, want %d %q", c.method, c.url, c.host, status, body, c.status, c.body)
		}
	}
	status, _, h := call(t, "GET", e.url+"/old/page?x=1", host, nil)
	if want := "https://" + *lb.DNSName + "/new/old/page?x=1"; status != 301 || h.Get("Location") != want {
		t.Fatalf("redirect: %d %q, want %q", status, h.Get("Location"), want)
	}
	_, _, h = call(t, "GET", e.url+"/split", host, nil)
	if !strings.Contains(h.Get("Set-Cookie"), "AWSALBTG=") {
		t.Fatalf("no stickiness cookie: %v", h)
	}
	if status, _, _ := call(t, "GET", e.url+"/", "nope-1.us-east-1.elb.localhost", nil); status != 404 {
		t.Fatalf("unknown load balancer: %d", status)
	}

	// deregistered targets drain and get no traffic; no targets is a 503
	must(e.elb.DeregisterTargets(ctx, &elb.DeregisterTargetsInput{TargetGroupArn: tgA.TargetGroupArn, Targets: []types.TargetDescription{{Id: aws.String("10.0.1.10")}}}))
	th := must(e.elb.DescribeTargetHealth(ctx, &elb.DescribeTargetHealthInput{TargetGroupArn: tgA.TargetGroupArn})).TargetHealthDescriptions
	if len(th) != 1 || th[0].TargetHealth.State != types.TargetHealthStateEnumDraining {
		t.Fatalf("draining %+v", th)
	}
	if status, _, _ := call(t, "GET", e.url+"/a/1", host, nil); status != 503 {
		t.Fatalf("draining target got traffic: %d", status)
	}
	e.svc.Tick(time.Now().Add(301 * time.Second))
	if th := must(e.elb.DescribeTargetHealth(ctx, &elb.DescribeTargetHealthInput{TargetGroupArn: tgA.TargetGroupArn})).TargetHealthDescriptions; len(th) != 0 {
		t.Fatalf("drained target stays: %+v", th)
	}

	// the listener's port moves with it, and a dead target is a 502
	moved := freePort(t)
	must(e.elb.ModifyListener(ctx, &elb.ModifyListenerInput{ListenerArn: l.ListenerArn, Port: aws.Int32(moved)}))
	dead := e.group(t, "dead", nil)
	must(e.elb.RegisterTargets(ctx, &elb.RegisterTargetsInput{TargetGroupArn: dead.TargetGroupArn, Targets: []types.TargetDescription{{Id: aws.String("127.0.0.1"), Port: aws.Int32(freePort(t))}}}))
	must(e.elb.ModifyListener(ctx, &elb.ModifyListenerInput{ListenerArn: l.ListenerArn, DefaultActions: forward(dead.TargetGroupArn)}))
	if status, _, _ := call(t, "GET", "http://127.0.0.1:"+strconv.Itoa(int(moved))+"/", "x", nil); status != 502 {
		t.Fatalf("dead target: %d", status)
	}
	if _, err := http.Get(ownURL); err == nil {
		t.Fatal("the old port still answers")
	}
}

const handler = `
import json

def handler(event, context):
    if event["path"] == "/boom":
        raise Exception("boom")
    if "multiValueHeaders" in event:
        return {"statusCode": 200, "multiValueHeaders": {"Set-Cookie": ["a=1", "b=2"]}, "body": json.dumps(event)}
    return {"statusCode": 201, "statusDescription": "201 Created", "headers": {"Content-Type": "application/json", "X-From": "lambda"},
            "body": json.dumps(event), "isBase64Encoded": False}
`

func TestLambdaTarget(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	e := setup(t, func(h *core.Hub) core.Service { return lambdasvc.New(h) })
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.Create("app.py")
	io.WriteString(w, handler)
	zw.Close()
	fn := must(lambda.NewFromConfig(e.cfg).CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("alb"), Runtime: ltypes.RuntimePython312,
		Handler: aws.String("app.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"), Code: &ltypes.FunctionCode{ZipFile: buf.Bytes()}}))
	tg := e.group(t, "fn", &elb.CreateTargetGroupInput{TargetType: types.TargetTypeEnumLambda})
	must(e.elb.RegisterTargets(ctx, &elb.RegisterTargetsInput{TargetGroupArn: tg.TargetGroupArn, Targets: []types.TargetDescription{{Id: fn.FunctionArn}}}))
	lb := e.alb(t, "fn")
	must(e.elb.CreateListener(ctx, &elb.CreateListenerInput{LoadBalancerArn: lb.LoadBalancerArn, Protocol: types.ProtocolEnumHttp,
		Port: aws.Int32(freePort(t)), DefaultActions: forward(tg.TargetGroupArn)}))
	host := *lb.DNSName + ":" + e.port

	status, body, h := call(t, "GET", e.url+"/hello?a=1&a=2&b=x%20y", host, map[string]string{"X-Custom": "yes"})
	var ev struct {
		HTTPMethod            string `json:"httpMethod"`
		Path, Body            string
		IsBase64Encoded       bool `json:"isBase64Encoded"`
		Headers               map[string]string
		QueryStringParameters map[string]string   `json:"queryStringParameters"`
		MultiValueQuery       map[string][]string `json:"multiValueQueryStringParameters"`
		RequestContext        struct {
			ELB struct{ TargetGroupArn string }
		} `json:"requestContext"`
	}
	if err := json.Unmarshal([]byte(body), &ev); err != nil || status != 201 || h.Get("X-From") != "lambda" {
		t.Fatalf("lambda: %d %v %q", status, h, body)
	}
	if ev.HTTPMethod != "GET" || ev.Path != "/hello" || ev.QueryStringParameters["a"] != "2" || ev.QueryStringParameters["b"] != "x%20y" ||
		ev.Headers["x-custom"] != "yes" || ev.Headers["host"] != host || ev.Headers["x-forwarded-proto"] != "http" ||
		ev.RequestContext.ELB.TargetGroupArn != *tg.TargetGroupArn {
		t.Fatalf("event %+v", ev)
	}
	if status, _, _ := call(t, "GET", e.url+"/boom", host, nil); status != 502 {
		t.Fatalf("function error: %d", status)
	}

	must(e.elb.ModifyTargetGroupAttributes(ctx, &elb.ModifyTargetGroupAttributesInput{TargetGroupArn: tg.TargetGroupArn,
		Attributes: []types.TargetGroupAttribute{{Key: aws.String("lambda.multi_value_headers.enabled"), Value: aws.String("true")}}}))
	status, body, h = call(t, "GET", e.url+"/multi?a=1&a=2", host, nil)
	ev.MultiValueQuery = nil
	if json.Unmarshal([]byte(body), &ev); status != 200 || !slices.Equal(ev.MultiValueQuery["a"], []string{"1", "2"}) || len(h.Values("Set-Cookie")) != 2 {
		t.Fatalf("multi-value: %d %v %+v", status, h, ev)
	}
	e.svc.Tick(time.Now())
	th := must(e.elb.DescribeTargetHealth(ctx, &elb.DescribeTargetHealthInput{TargetGroupArn: tg.TargetGroupArn})).TargetHealthDescriptions
	if len(th) != 1 || th[0].TargetHealth.State != types.TargetHealthStateEnumHealthy || th[0].Target.Port != nil {
		t.Fatalf("lambda health %+v", th)
	}
}

func TestHealthChecks(t *testing.T) {
	e := setup(t)
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" || r.Header.Get("User-Agent") != "ELB-HealthChecker/2.0" {
			w.WriteHeader(404)
		}
		io.WriteString(w, "ok")
	}))
	defer ok.Close()
	sick := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer sick.Close()
	port := func(s *httptest.Server) *int32 {
		p, _ := strconv.Atoi(s.URL[strings.LastIndex(s.URL, ":")+1:])
		return aws.Int32(int32(p))
	}
	tg := e.group(t, "web", &elb.CreateTargetGroupInput{Protocol: types.ProtocolEnumHttp, Port: aws.Int32(80), VpcId: aws.String("vpc-1"),
		TargetType: types.TargetTypeEnumIp, HealthCheckPath: aws.String("/health"), HealthCheckIntervalSeconds: aws.Int32(5)})
	targets := []types.TargetDescription{{Id: aws.String("127.0.0.1"), Port: port(ok)}, {Id: aws.String("127.0.0.1"), Port: port(sick)},
		{Id: aws.String("127.0.0.1"), Port: aws.Int32(freePort(t))}, {Id: aws.String("10.255.0.1")}}
	must(e.elb.RegisterTargets(ctx, &elb.RegisterTargetsInput{TargetGroupArn: tg.TargetGroupArn, Targets: targets}))
	state := func() []string {
		var out []string
		for _, d := range must(e.elb.DescribeTargetHealth(ctx, &elb.DescribeTargetHealthInput{TargetGroupArn: tg.TargetGroupArn})).TargetHealthDescriptions {
			out = append(out, string(d.TargetHealth.State)+" "+string(d.TargetHealth.Reason))
		}
		return out
	}
	if s := state(); s[0] != "unused Target.NotInUse" {
		t.Fatalf("a group no load balancer uses: %v", s)
	}
	lb := e.alb(t, "web")
	must(e.elb.CreateListener(ctx, &elb.CreateListenerInput{LoadBalancerArn: lb.LoadBalancerArn, Protocol: types.ProtocolEnumHttp,
		Port: aws.Int32(freePort(t)), DefaultActions: forward(tg.TargetGroupArn)}))
	// the background checks may have run once; these runs are ahead of them
	now := time.Now().Add(time.Hour)
	e.svc.Tick(now)
	if s := state(); s[0] != "healthy " || s[3] != "unavailable Target.InvalidState" {
		t.Fatalf("after a check: %v", s)
	}
	e.svc.Tick(now.Add(5 * time.Second))
	if s := state(); !slices.Equal(s, []string{"healthy ", "unhealthy Target.ResponseCodeMismatch", "unhealthy Target.FailedHealthChecks", "unavailable Target.InvalidState"}) {
		t.Fatalf("after two checks: %v", s)
	}
	for range 4 { // the healthy target takes all traffic
		if status, body, _ := call(t, "GET", e.url+"/", *lb.DNSName, nil); status != 404 || body != "ok" {
			t.Fatalf("routed to %d %q", status, body)
		}
	}

	th := must(e.elb.DescribeTargetHealth(ctx, &elb.DescribeTargetHealthInput{TargetGroupArn: tg.TargetGroupArn,
		Targets: []types.TargetDescription{{Id: aws.String("127.0.0.1"), Port: port(ok)}, {Id: aws.String("10.9.9.9")}}})).TargetHealthDescriptions
	if len(th) != 2 || *th[0].HealthCheckPort != strconv.Itoa(int(*port(ok))) || th[1].TargetHealth.Reason != types.TargetHealthReasonEnumNotRegistered || *th[1].Target.Port != 80 {
		t.Fatalf("filtered %+v", th)
	}

	// instance targets need an endpoint; EC2 records one when it runs the instance
	inst := e.group(t, "inst", &elb.CreateTargetGroupInput{Protocol: types.ProtocolEnumHttp, Port: aws.Int32(8080), VpcId: aws.String("vpc-1"), HealthCheckPath: aws.String("/health")})
	must(e.elb.CreateRule(ctx, &elb.CreateRuleInput{ListenerArn: must(e.elb.DescribeListeners(ctx, &elb.DescribeListenersInput{LoadBalancerArn: lb.LoadBalancerArn})).Listeners[0].ListenerArn,
		Priority: aws.Int32(1), Conditions: []types.RuleCondition{{Field: aws.String("path-pattern"), Values: []string{"/i"}}}, Actions: forward(inst.TargetGroupArn)}))
	must(e.elb.RegisterTargets(ctx, &elb.RegisterTargetsInput{TargetGroupArn: inst.TargetGroupArn, Targets: []types.TargetDescription{{Id: aws.String("i-0abc")}}}))
	e.svc.Tick(now.Add(10 * time.Second))
	if th := must(e.elb.DescribeTargetHealth(ctx, &elb.DescribeTargetHealthInput{TargetGroupArn: inst.TargetGroupArn})).TargetHealthDescriptions; th[0].TargetHealth.State != types.TargetHealthStateEnumUnavailable {
		t.Fatalf("instance without endpoint %+v", th[0].TargetHealth)
	}
	e.hub.SetEndpoint("instance:i-0abc:8080", ok.Listener.Addr().String())
	e.svc.Tick(now.Add(time.Minute)) // the next check, 30s on
	if th := must(e.elb.DescribeTargetHealth(ctx, &elb.DescribeTargetHealthInput{TargetGroupArn: inst.TargetGroupArn})).TargetHealthDescriptions; th[0].TargetHealth.State != types.TargetHealthStateEnumHealthy {
		t.Fatalf("instance with endpoint %+v", th[0].TargetHealth)
	}
	if status, body, _ := call(t, "GET", e.url+"/i", *lb.DNSName, nil); status != 404 || body != "ok" {
		t.Fatalf("instance target: %d %q", status, body)
	}
}

func TestNLB(t *testing.T) {
	e := setup(t)
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		for {
			c, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { io.Copy(c, c); c.Close() }()
		}
	}()
	tg := e.group(t, "tcp", &elb.CreateTargetGroupInput{Protocol: types.ProtocolEnumTcp, Port: aws.Int32(int32(echo.Addr().(*net.TCPAddr).Port)),
		VpcId: aws.String("vpc-1"), TargetType: types.TargetTypeEnumIp})
	must(e.elb.RegisterTargets(ctx, &elb.RegisterTargetsInput{TargetGroupArn: tg.TargetGroupArn, Targets: []types.TargetDescription{{Id: aws.String("127.0.0.1")}}}))
	nlb := must(e.elb.CreateLoadBalancer(ctx, &elb.CreateLoadBalancerInput{Name: aws.String("tcp"), Type: types.LoadBalancerTypeEnumNetwork,
		Subnets: []string{"subnet-1"}})).LoadBalancers[0]
	taken, _ := net.Listen("tcp", "127.0.0.1:0") // the listener port is taken: pogi picks another
	defer taken.Close()
	must(e.elb.CreateListener(ctx, &elb.CreateListenerInput{LoadBalancerArn: nlb.LoadBalancerArn, Protocol: types.ProtocolEnumTcp,
		Port: aws.Int32(int32(taken.Addr().(*net.TCPAddr).Port)), DefaultActions: forward(tg.TargetGroupArn)}))
	ports := e.svc.Inspect("Load balancers", "tcp").(map[string]any)["ports"].(map[int]int)
	bound := ports[taken.Addr().(*net.TCPAddr).Port]
	if bound == 0 || bound == taken.Addr().(*net.TCPAddr).Port {
		t.Fatalf("ports %v", ports)
	}
	ping := func(port int) string {
		c, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		io.WriteString(c, "ping")
		c.(*net.TCPConn).CloseWrite()
		b, _ := io.ReadAll(c)
		return string(b)
	}
	if got := ping(bound); got != "ping" {
		t.Fatalf("echo through the NLB: %q", got)
	}
	e.svc.Tick(time.Now().Add(time.Hour))
	if th := must(e.elb.DescribeTargetHealth(ctx, &elb.DescribeTargetHealthInput{TargetGroupArn: tg.TargetGroupArn})).TargetHealthDescriptions; th[0].TargetHealth.State != types.TargetHealthStateEnumHealthy {
		t.Fatalf("tcp health %+v", th[0].TargetHealth)
	}
	if status, _, _ := call(t, "GET", e.url+"/", *nlb.DNSName, nil); status != 400 {
		t.Fatalf("HTTP to an NLB on pogi's port: %d", status)
	}

	// state survives a restart, and the port opens again
	b, err := e.svc.Save()
	if err != nil {
		t.Fatal(err)
	}
	e.svc.Reset()
	if n := len(must(e.elb.DescribeLoadBalancers(ctx, &elb.DescribeLoadBalancersInput{})).LoadBalancers); n != 0 {
		t.Fatalf("reset left %d", n)
	}
	if err := e.svc.Load(b); err != nil {
		t.Fatal(err)
	}
	bound = e.svc.Inspect("Load balancers", "tcp").(map[string]any)["ports"].(map[int]int)[taken.Addr().(*net.TCPAddr).Port]
	if got := ping(bound); got != "ping" {
		t.Fatalf("echo after load: %q", got)
	}
}

// ECS registers tasks in-process over JSON.
func TestInProcess(t *testing.T) {
	e := setup(t)
	tg := e.group(t, "svc", nil)
	in := map[string]any{"TargetGroupArn": *tg.TargetGroupArn, "Targets": []map[string]any{{"Id": "10.0.3.7", "Port": 8080}}}
	if err := e.hub.Call("000000000000", "us-east-1", "ElasticLoadBalancing_v10", "RegisterTargets", in, nil); err != nil {
		t.Fatal(err)
	}
	var out struct {
		TargetHealthDescriptions []struct {
			Target struct {
				Id   string
				Port int
			}
			TargetHealth struct{ State string }
		}
	}
	if err := e.hub.Call("000000000000", "us-east-1", "ElasticLoadBalancing_v10", "DescribeTargetHealth", map[string]string{"TargetGroupArn": *tg.TargetGroupArn}, &out); err != nil {
		t.Fatal(err)
	}
	if d := out.TargetHealthDescriptions; len(d) != 1 || d[0].Target.Id != "10.0.3.7" || d[0].Target.Port != 8080 || d[0].TargetHealth.State != "unused" {
		t.Fatalf("described %+v", out)
	}
	err := e.hub.Call("000000000000", "us-east-1", "ElasticLoadBalancing_v10", "DeregisterTargets", map[string]any{"TargetGroupArn": "nope"}, nil)
	if ae := core.AsError(err); ae.Code != "TargetGroupNotFound" {
		t.Fatalf("deregister: %v", err)
	}
}

// floci #1220 and #1219: subnets of two VPCs, two subnets in one zone, and an ALB in one zone are refused.
func TestLoadBalancerSubnetRules(t *testing.T) {
	e := setup(t, func(h *core.Hub) core.Service { return ec2svc.New(h) })
	c := ec2.NewFromConfig(e.cfg)
	subnet := func(vpc, cidr, az string) string {
		return *must(c.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: &vpc, CidrBlock: aws.String(cidr), AvailabilityZone: aws.String(az)})).Subnet.SubnetId
	}
	v1 := *must(c.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.65.0.0/16")})).Vpc.VpcId
	v2 := *must(c.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.66.0.0/16")})).Vpc.VpcId
	a, b, a2, other := subnet(v1, "10.65.1.0/24", "us-east-1a"), subnet(v1, "10.65.2.0/24", "us-east-1b"), subnet(v1, "10.65.3.0/24", "us-east-1a"), subnet(v2, "10.66.1.0/24", "us-east-1b")
	create := func(name string, subnets ...string) error {
		_, err := e.elb.CreateLoadBalancer(ctx, &elb.CreateLoadBalancerInput{Name: aws.String(name), Subnets: subnets})
		return err
	}
	for name, subnets := range map[string][]string{"two-vpcs": {a, other}, "one-zone": {a}, "same-zone": {a, a2}} {
		if err := create(name, subnets...); err == nil {
			t.Errorf("%s: load balancer accepted", name)
		}
	}
	if err := create("fine", a, b); err != nil {
		t.Fatal(err)
	}
	lb := must(e.elb.DescribeLoadBalancers(ctx, &elb.DescribeLoadBalancersInput{Names: []string{"fine"}})).LoadBalancers[0]
	if _, err := e.elb.SetSubnets(ctx, &elb.SetSubnetsInput{LoadBalancerArn: lb.LoadBalancerArn, Subnets: []string{a, other}}); err == nil {
		t.Error("SetSubnets accepted two VPCs")
	}
}

// floci #4454: HealthCheckPort must be "traffic-port" or 1 to 65535, on create and on modify.
func TestHealthCheckPortRange(t *testing.T) {
	e := setup(t)
	for _, p := range []string{"70000", "0", "abc"} {
		_, err := e.elb.CreateTargetGroup(ctx, &elb.CreateTargetGroupInput{Name: aws.String("bad"), Protocol: types.ProtocolEnumHttp, Port: aws.Int32(80), VpcId: aws.String("vpc-1"), HealthCheckPort: aws.String(p)})
		if code(err) != "ValidationError" {
			t.Errorf("create with HealthCheckPort %q: %v", p, err)
		}
	}
	g := e.group(t, "ok", &elb.CreateTargetGroupInput{Protocol: types.ProtocolEnumHttp, Port: aws.Int32(80), VpcId: aws.String("vpc-1"), HealthCheckPort: aws.String("8080")})
	if _, err := e.elb.ModifyTargetGroup(ctx, &elb.ModifyTargetGroupInput{TargetGroupArn: g.TargetGroupArn, HealthCheckPort: aws.String("65536")}); code(err) != "ValidationError" {
		t.Errorf("modify with HealthCheckPort 65536: %v", err)
	}
	must(e.elb.ModifyTargetGroup(ctx, &elb.ModifyTargetGroupInput{TargetGroupArn: g.TargetGroupArn, HealthCheckPort: aws.String("traffic-port")}))
}
