package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	elbtypes "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

// The generated long-tail services run beside the hand-written ones: they
// register after them, share signing names without stealing traffic (Classic
// ELB and ELB v2 both sign as elasticloadbalancing), honour signature checks,
// and keep their state across a snapshot.
func TestGeneratedServices(t *testing.T) {
	ctx := context.Background()
	srv := pogi.Start(t, pogi.Config{ValidateSigs: true})
	cfg := awstest.Config(t, srv.URL)

	names := map[string]string{}
	for _, s := range srv.Hub.Services() {
		names[s.Info().Name] = fmt.Sprintf("%T", s)
	}
	for _, n := range []string{"codeartifact", "swf", "elb", "servicecatalog", "route53resolver", "efs", "backup", "dlm", "globalaccelerator", "guardduty", "service-quotas"} {
		if names[n] != "*generic.Service" {
			t.Errorf("%s is %q, want a generated service", n, names[n])
		}
	}
	if names["route53"] == "*generic.Service" || names["elbv2"] == "*generic.Service" {
		t.Errorf("a hand-written service was replaced: route53 %s, elbv2 %s", names["route53"], names["elbv2"])
	}

	ca := codeartifact.NewFromConfig(cfg)
	if _, err := ca.CreateDomain(ctx, &codeartifact.CreateDomainInput{Domain: aws.String("acme")}); err != nil {
		t.Fatal(err)
	}
	classic := elasticloadbalancing.NewFromConfig(cfg)
	if _, err := classic.CreateLoadBalancer(ctx, &elasticloadbalancing.CreateLoadBalancerInput{LoadBalancerName: aws.String("classic"),
		Listeners: []elbtypes.Listener{{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(80)}}, AvailabilityZones: []string{"us-east-1a"}}); err != nil {
		t.Fatal(err)
	}
	d, err := classic.DescribeLoadBalancers(ctx, &elasticloadbalancing.DescribeLoadBalancersInput{})
	if err != nil || len(d.LoadBalancerDescriptions) != 1 {
		t.Fatalf("classic: %v %+v", err, d)
	}
	v2, err := elasticloadbalancingv2.NewFromConfig(cfg).DescribeLoadBalancers(ctx, &elasticloadbalancingv2.DescribeLoadBalancersInput{})
	if err != nil || len(v2.LoadBalancers) != 0 {
		t.Fatalf("v2 saw the classic load balancer: %v %+v", err, v2)
	}

	snap, err := srv.Hub.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	srv.Hub.Reset()
	if _, err := ca.DescribeDomain(ctx, &codeartifact.DescribeDomainInput{Domain: aws.String("acme")}); err == nil {
		t.Fatal("domain survived a reset")
	}
	if err := srv.Hub.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if _, err := ca.DescribeDomain(ctx, &codeartifact.DescribeDomainInput{Domain: aws.String("acme")}); err != nil {
		t.Fatalf("domain lost in the snapshot: %v", err)
	}
	// the console lists the inferred resource types
	tables := srv.Hub.Resources("codeartifact")["codeartifact"]
	if len(tables) != 1 || tables[0].Name != "Domain" || len(tables[0].Rows) != 1 {
		t.Fatalf("console: %+v", tables)
	}
}
