package generic_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	elbtypes "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing/types"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	ec2svc "github.com/nrndev/pogi/internal/aws/ec2"
	"github.com/nrndev/pogi/internal/aws/generic"
	iamsvc "github.com/nrndev/pogi/internal/aws/iam"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

// floci #3774: a classic load balancer in a VPC shows the VPC and its first security group as the source group.
func TestELBSourceSecurityGroup(t *testing.T) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return ec2svc.New(h) }, func(h *core.Hub) core.Service { return generic.New(h, "elb") })
	ctx := context.Background()
	cfg := awstest.Config(t, url)
	e := ec2.NewFromConfig(cfg)
	vpc, err := e.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.63.0.0/16")})
	if err != nil {
		t.Fatal(err)
	}
	sn, err := e.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: vpc.Vpc.VpcId, CidrBlock: aws.String("10.63.1.0/24")})
	if err != nil {
		t.Fatal(err)
	}
	sg, err := e.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{GroupName: aws.String("elb-sg"), Description: aws.String("t"), VpcId: vpc.Vpc.VpcId})
	if err != nil {
		t.Fatal(err)
	}
	c := elasticloadbalancing.NewFromConfig(cfg)
	if _, err := c.CreateLoadBalancer(ctx, &elasticloadbalancing.CreateLoadBalancerInput{LoadBalancerName: aws.String("my-elb"), Subnets: []string{*sn.Subnet.SubnetId},
		SecurityGroups: []string{*sg.GroupId}, Listeners: []elbtypes.Listener{{Protocol: aws.String("TCP"), LoadBalancerPort: 443, InstancePort: aws.Int32(8443)}}}); err != nil {
		t.Fatal(err)
	}
	d, err := c.DescribeLoadBalancers(ctx, &elasticloadbalancing.DescribeLoadBalancersInput{})
	if err != nil || len(d.LoadBalancerDescriptions) != 1 {
		t.Fatalf("%v %+v", err, d)
	}
	lb := d.LoadBalancerDescriptions[0]
	if aws.ToString(lb.VPCId) != *vpc.Vpc.VpcId || lb.SourceSecurityGroup == nil || aws.ToString(lb.SourceSecurityGroup.GroupName) != "elb-sg" || aws.ToString(lb.SourceSecurityGroup.OwnerAlias) == "" {
		t.Fatalf("VPCId %v, SourceSecurityGroup %+v", lb.VPCId, lb.SourceSecurityGroup)
	}
}

// Query protocol (Classic ELB)
func TestELB(t *testing.T) {
	_, url := start(t, "elb")
	ctx := context.Background()
	c := elasticloadbalancing.NewFromConfig(awstest.Config(t, url))
	for i := range 3 {
		out, err := c.CreateLoadBalancer(ctx, &elasticloadbalancing.CreateLoadBalancerInput{LoadBalancerName: aws.String(fmt.Sprintf("lb%d", i)),
			Listeners:         []elbtypes.Listener{{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)}},
			AvailabilityZones: []string{"us-east-1a"},
			Tags:              []elbtypes.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
		if err != nil {
			t.Fatal(err)
		}
		if aws.ToString(out.DNSName) == "" {
			t.Fatalf("no DNS name: %+v", out)
		}
	}
	_, err := c.CreateLoadBalancer(ctx, &elasticloadbalancing.CreateLoadBalancerInput{LoadBalancerName: aws.String("lb1"),
		Listeners: []elbtypes.Listener{{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)}}})
	var dup *elbtypes.DuplicateAccessPointNameException
	if !errors.As(err, &dup) {
		t.Fatalf("%T %v", err, err)
	}
	d, err := c.DescribeLoadBalancers(ctx, &elasticloadbalancing.DescribeLoadBalancersInput{LoadBalancerNames: []string{"lb1"}})
	if err != nil || len(d.LoadBalancerDescriptions) != 1 {
		t.Fatalf("%v %+v", err, d)
	}
	lb := d.LoadBalancerDescriptions[0]
	if aws.ToString(lb.LoadBalancerName) != "lb1" || len(lb.AvailabilityZones) != 1 || len(lb.ListenerDescriptions) != 1 || lb.ListenerDescriptions[0].Listener.LoadBalancerPort != 80 {
		t.Fatalf("%+v", lb)
	}
	pages, n := 0, 0
	p := elasticloadbalancing.NewDescribeLoadBalancersPaginator(c, &elasticloadbalancing.DescribeLoadBalancersInput{PageSize: aws.Int32(2)})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		n += len(out.LoadBalancerDescriptions)
	}
	if pages != 2 || n != 3 {
		t.Fatalf("pages %d n %d", pages, n)
	}
	if _, err := c.AddTags(ctx, &elasticloadbalancing.AddTagsInput{LoadBalancerNames: []string{"lb1"}, Tags: []elbtypes.Tag{{Key: aws.String("a"), Value: aws.String("1")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RemoveTags(ctx, &elasticloadbalancing.RemoveTagsInput{LoadBalancerNames: []string{"lb1"}, Tags: []elbtypes.TagKeyOnly{{Key: aws.String("env")}}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.DescribeTags(ctx, &elasticloadbalancing.DescribeTagsInput{LoadBalancerNames: []string{"lb1"}})
	if err != nil || len(tags.TagDescriptions) != 1 || len(tags.TagDescriptions[0].Tags) != 1 || aws.ToString(tags.TagDescriptions[0].Tags[0].Key) != "a" {
		t.Fatalf("%v %+v", err, tags)
	}
	if _, err := c.DeleteLoadBalancer(ctx, &elasticloadbalancing.DeleteLoadBalancerInput{LoadBalancerName: aws.String("lb1")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeLoadBalancers(ctx, &elasticloadbalancing.DescribeLoadBalancersInput{LoadBalancerNames: []string{"lb1"}})
	var nf *elbtypes.AccessPointNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("%T %v", err, err)
	}
	// Query validation: the SDK does not check required members of this call on the client
	req, _ := http.NewRequest("POST", url, strings.NewReader("Action=CreateLoadBalancer&Version=2012-06-01"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/elasticloadbalancing/aws4_request")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 || !strings.Contains(string(b), "<Code>") {
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
}

// REST-XML (the model of the hand-written Route 53, used here on its own)
func TestRESTXML(t *testing.T) {
	_, url := start(t, "route53")
	ctx := context.Background()
	c := route53.NewFromConfig(awstest.Config(t, url))
	var ids []string
	for i := range 3 {
		out, err := c.CreateHostedZone(ctx, &route53.CreateHostedZoneInput{Name: aws.String(fmt.Sprintf("z%d.example.com", i)), CallerReference: aws.String(fmt.Sprintf("ref%d", i)),
			HostedZoneConfig: &r53types.HostedZoneConfig{Comment: aws.String("test"), PrivateZone: false}})
		if err != nil {
			t.Fatal(err)
		}
		if aws.ToString(out.HostedZone.Name) != fmt.Sprintf("z%d.example.com", i) || out.ChangeInfo == nil {
			t.Fatalf("%+v", out)
		}
		ids = append(ids, aws.ToString(out.HostedZone.Id))
	}
	g, err := c.GetHostedZone(ctx, &route53.GetHostedZoneInput{Id: aws.String(ids[1])})
	if err != nil || aws.ToString(g.HostedZone.CallerReference) != "ref1" {
		t.Fatalf("%v %+v", err, g)
	}
	pages, n := 0, 0
	p := route53.NewListHostedZonesPaginator(c, &route53.ListHostedZonesInput{MaxItems: aws.Int32(2)})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		n += len(out.HostedZones)
	}
	if pages != 2 || n != 3 {
		t.Fatalf("pages %d n %d", pages, n)
	}
	if _, err := c.DeleteHostedZone(ctx, &route53.DeleteHostedZoneInput{Id: aws.String(ids[1])}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetHostedZone(ctx, &route53.GetHostedZoneInput{Id: aws.String(ids[1])})
	var nf *r53types.NoSuchHostedZone
	if !errors.As(err, &nf) {
		t.Fatalf("%T %v", err, err)
	}
}

// floci #4875: a classic load balancer listener needs an IAM server certificate that exists.
func TestELBMissingCertificate(t *testing.T) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return iamsvc.New(h) }, func(h *core.Hub) core.Service { return generic.New(h, "elb") })
	c := elasticloadbalancing.NewFromConfig(awstest.Config(t, url))
	_, err := c.CreateLoadBalancer(context.Background(), &elasticloadbalancing.CreateLoadBalancerInput{LoadBalancerName: aws.String("tls"), AvailabilityZones: []string{"us-east-1a"},
		Listeners: []elbtypes.Listener{{Protocol: aws.String("HTTPS"), LoadBalancerPort: 443, InstancePort: aws.Int32(80),
			SSLCertificateId: aws.String("arn:aws:iam::000000000000:server-certificate/nope")}}})
	var nf *elbtypes.CertificateNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("want CertificateNotFound, got %T %v", err, err)
	}
}
