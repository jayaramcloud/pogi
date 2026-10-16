package generic_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/globalaccelerator"
	gatypes "github.com/aws/aws-sdk-go-v2/service/globalaccelerator/types"
	"github.com/aws/aws-sdk-go-v2/service/route53resolver"
	rrtypes "github.com/aws/aws-sdk-go-v2/service/route53resolver/types"
	"github.com/aws/aws-sdk-go-v2/service/servicecatalog"
	sctypes "github.com/aws/aws-sdk-go-v2/service/servicecatalog/types"
	"github.com/aws/aws-sdk-go-v2/service/servicequotas"
	"github.com/aws/aws-sdk-go-v2/service/swf"
	swftypes "github.com/aws/aws-sdk-go-v2/service/swf/types"
	"github.com/nrndev/pogi/internal/aws/generic"
	"github.com/nrndev/pogi/internal/awstest"
)

// JSON 1.0
func TestSWF(t *testing.T) {
	_, url := start(t, "swf")
	ctx := context.Background()
	c := swf.NewFromConfig(awstest.Config(t, url))
	for i := range 3 {
		_, err := c.RegisterDomain(ctx, &swf.RegisterDomainInput{Name: aws.String(fmt.Sprintf("d%d", i)), WorkflowExecutionRetentionPeriodInDays: aws.String("7"),
			Tags: []swftypes.ResourceTag{{Key: aws.String("env"), Value: aws.String("dev")}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	d, err := c.DescribeDomain(ctx, &swf.DescribeDomainInput{Name: aws.String("d1")})
	if err != nil {
		t.Fatal(err)
	}
	arn := aws.ToString(d.DomainInfo.Arn)
	if aws.ToString(d.DomainInfo.Name) != "d1" || d.DomainInfo.Status != swftypes.RegistrationStatusRegistered ||
		aws.ToString(d.Configuration.WorkflowExecutionRetentionPeriodInDays) != "7" || arn == "" {
		t.Fatalf("%+v %+v", d.DomainInfo, d.Configuration)
	}
	if _, err := c.RegisterDomain(ctx, &swf.RegisterDomainInput{Name: aws.String("d1"), WorkflowExecutionRetentionPeriodInDays: aws.String("7")}); err == nil {
		t.Fatal("duplicate domain")
	} else {
		var ex *swftypes.DomainAlreadyExistsFault
		if !errors.As(err, &ex) {
			t.Fatalf("%T %v", err, err)
		}
	}
	pages, n := 0, 0
	p := swf.NewListDomainsPaginator(c, &swf.ListDomainsInput{RegistrationStatus: swftypes.RegistrationStatusRegistered, MaximumPageSize: 2})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		n += len(out.DomainInfos)
	}
	if pages != 2 || n != 3 {
		t.Fatalf("pages %d domains %d", pages, n)
	}
	if _, err := c.UntagResource(ctx, &swf.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.TagResource(ctx, &swf.TagResourceInput{ResourceArn: aws.String(arn), Tags: []swftypes.ResourceTag{{Key: aws.String("a"), Value: aws.String("1")}}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListTagsForResource(ctx, &swf.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	if err != nil || len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "a" {
		t.Fatalf("%v %+v", err, tags)
	}
	if _, err := c.DeprecateDomain(ctx, &swf.DeprecateDomainInput{Name: aws.String("d1")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeDomain(ctx, &swf.DescribeDomainInput{Name: aws.String("d1")})
	var nf *swftypes.UnknownResourceFault
	if !errors.As(err, &nf) {
		t.Fatalf("%T %v", err, err)
	}
	// required member: the SDK checks it on the client, so go around it
	_, err = c.RegisterDomain(ctx, &swf.RegisterDomainInput{Name: aws.String("d9")}) // WorkflowExecutionRetentionPeriodInDays is required
	if err == nil {
		t.Fatal("missing member accepted")
	}
}

// JSON 1.1, with an ARN as the identifier and no region in it: us-west-2 only
func TestGlobalAccelerator(t *testing.T) {
	_, url := start(t, "globalaccelerator")
	ctx := context.Background()
	cfg := awstest.Config(t, url)
	cfg.Region = "us-west-2"
	c := globalaccelerator.NewFromConfig(cfg)
	var arns []string
	for i := range 3 {
		out, err := c.CreateAccelerator(ctx, &globalaccelerator.CreateAcceleratorInput{Name: aws.String(fmt.Sprintf("acc%d", i)), IpAddressType: gatypes.IpAddressTypeIpv4,
			Tags: []gatypes.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
		if err != nil {
			t.Fatal(err)
		}
		a := out.Accelerator
		if a.Status != gatypes.AcceleratorStatusDeployed || aws.ToString(a.Name) != fmt.Sprintf("acc%d", i) || a.CreatedTime == nil {
			t.Fatalf("%+v", a)
		}
		arns = append(arns, aws.ToString(a.AcceleratorArn))
	}
	if want := "arn:aws:globalaccelerator::000000000000:accelerator/"; arns[0][:len(want)] != want {
		t.Fatalf("arn %s", arns[0])
	}
	d, err := c.DescribeAccelerator(ctx, &globalaccelerator.DescribeAcceleratorInput{AcceleratorArn: aws.String(arns[1])})
	if err != nil || aws.ToString(d.Accelerator.Name) != "acc1" {
		t.Fatalf("%v %+v", err, d)
	}
	pages, n := 0, 0
	p := globalaccelerator.NewListAcceleratorsPaginator(c, &globalaccelerator.ListAcceleratorsInput{MaxResults: aws.Int32(2)})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		n += len(out.Accelerators)
	}
	if pages != 2 || n != 3 {
		t.Fatalf("pages %d n %d", pages, n)
	}
	if _, err := c.UpdateAccelerator(ctx, &globalaccelerator.UpdateAcceleratorInput{AcceleratorArn: aws.String(arns[1]), Name: aws.String("renamed")}); err != nil {
		t.Fatal(err)
	}
	d, _ = c.DescribeAccelerator(ctx, &globalaccelerator.DescribeAcceleratorInput{AcceleratorArn: aws.String(arns[1])})
	if aws.ToString(d.Accelerator.Name) != "renamed" {
		t.Fatalf("update: %+v", d.Accelerator)
	}
	if _, err := c.UntagResource(ctx, &globalaccelerator.UntagResourceInput{ResourceArn: aws.String(arns[1]), TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.TagResource(ctx, &globalaccelerator.TagResourceInput{ResourceArn: aws.String(arns[1]), Tags: []gatypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListTagsForResource(ctx, &globalaccelerator.ListTagsForResourceInput{ResourceArn: aws.String(arns[1])})
	if err != nil || len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "k" {
		t.Fatalf("%v %+v", err, tags)
	}
	if _, err := c.DeleteAccelerator(ctx, &globalaccelerator.DeleteAcceleratorInput{AcceleratorArn: aws.String(arns[1])}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeAccelerator(ctx, &globalaccelerator.DescribeAcceleratorInput{AcceleratorArn: aws.String(arns[1])})
	var nf *gatypes.AcceleratorNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("%T %v", err, err)
	}
}

// JSON 1.1
func TestServiceCatalog(t *testing.T) {
	_, url := start(t, "servicecatalog")
	ctx := context.Background()
	c := servicecatalog.NewFromConfig(awstest.Config(t, url))
	var ids []string
	for i := range 3 {
		out, err := c.CreatePortfolio(ctx, &servicecatalog.CreatePortfolioInput{DisplayName: aws.String(fmt.Sprintf("pf%d", i)), ProviderName: aws.String("acme"),
			IdempotencyToken: aws.String(fmt.Sprintf("tok%d", i)), Tags: []sctypes.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, aws.ToString(out.PortfolioDetail.Id))
	}
	d, err := c.DescribePortfolio(ctx, &servicecatalog.DescribePortfolioInput{Id: aws.String(ids[1])})
	if err != nil || aws.ToString(d.PortfolioDetail.DisplayName) != "pf1" || aws.ToString(d.PortfolioDetail.ProviderName) != "acme" ||
		aws.ToString(d.PortfolioDetail.ARN) == "" || len(d.Tags) != 1 {
		t.Fatalf("%v %+v", err, d)
	}
	pages, n := 0, 0
	p := servicecatalog.NewListPortfoliosPaginator(c, &servicecatalog.ListPortfoliosInput{PageSize: 2})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		n += len(out.PortfolioDetails)
	}
	if pages != 2 || n != 3 {
		t.Fatalf("pages %d n %d", pages, n)
	}
	// this API tags through the update call
	if _, err := c.UpdatePortfolio(ctx, &servicecatalog.UpdatePortfolioInput{Id: aws.String(ids[1]), Description: aws.String("d"),
		AddTags: []sctypes.Tag{{Key: aws.String("a"), Value: aws.String("1")}}, RemoveTags: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	d, _ = c.DescribePortfolio(ctx, &servicecatalog.DescribePortfolioInput{Id: aws.String(ids[1])})
	if len(d.Tags) != 1 || aws.ToString(d.Tags[0].Key) != "a" || aws.ToString(d.PortfolioDetail.Description) != "d" {
		t.Fatalf("%+v", d)
	}
	if _, err := c.DeletePortfolio(ctx, &servicecatalog.DeletePortfolioInput{Id: aws.String(ids[1])}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribePortfolio(ctx, &servicecatalog.DescribePortfolioInput{Id: aws.String(ids[1])})
	var nf *sctypes.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("%T %v", err, err)
	}
}

func TestRoute53Resolver(t *testing.T) {
	_, url := start(t, "route53resolver")
	ctx := context.Background()
	c := route53resolver.NewFromConfig(awstest.Config(t, url))
	var ids []string
	var arn string
	for i := range 3 {
		out, err := c.CreateResolverEndpoint(ctx, &route53resolver.CreateResolverEndpointInput{CreatorRequestId: aws.String(fmt.Sprintf("r%d", i)),
			Name: aws.String(fmt.Sprintf("ep%d", i)), Direction: rrtypes.ResolverEndpointDirectionInbound, SecurityGroupIds: []string{"sg-1"},
			IpAddresses: []rrtypes.IpAddressRequest{{SubnetId: aws.String("subnet-1")}, {SubnetId: aws.String("subnet-2")}},
			Tags:        []rrtypes.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
		if err != nil {
			t.Fatal(err)
		}
		e := out.ResolverEndpoint
		if e.Status != rrtypes.ResolverEndpointStatusOperational || aws.ToString(e.Name) != fmt.Sprintf("ep%d", i) || aws.ToString(e.Id) == "" {
			t.Fatalf("%+v", e)
		}
		ids, arn = append(ids, aws.ToString(e.Id)), aws.ToString(e.Arn)
	}
	g, err := c.GetResolverEndpoint(ctx, &route53resolver.GetResolverEndpointInput{ResolverEndpointId: aws.String(ids[0])})
	if err != nil || aws.ToString(g.ResolverEndpoint.CreatorRequestId) != "r0" || len(g.ResolverEndpoint.SecurityGroupIds) != 1 {
		t.Fatalf("%v %+v", err, g)
	}
	pages, n := 0, 0
	p := route53resolver.NewListResolverEndpointsPaginator(c, &route53resolver.ListResolverEndpointsInput{MaxResults: aws.Int32(2)})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		n += len(out.ResolverEndpoints)
	}
	if pages != 2 || n != 3 {
		t.Fatalf("pages %d n %d", pages, n)
	}
	tags, err := c.ListTagsForResource(ctx, &route53resolver.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	if err != nil || len(tags.Tags) != 1 {
		t.Fatalf("%v %+v", err, tags)
	}
	c.TagResource(ctx, &route53resolver.TagResourceInput{ResourceArn: aws.String(arn), Tags: []rrtypes.Tag{{Key: aws.String("x"), Value: aws.String("y")}}})
	c.UntagResource(ctx, &route53resolver.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"env"}})
	tags, _ = c.ListTagsForResource(ctx, &route53resolver.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	if len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "x" {
		t.Fatalf("%+v", tags)
	}
	if _, err := c.DeleteResolverEndpoint(ctx, &route53resolver.DeleteResolverEndpointInput{ResolverEndpointId: aws.String(ids[0])}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetResolverEndpoint(ctx, &route53resolver.GetResolverEndpointInput{ResolverEndpointId: aws.String(ids[0])})
	var nf *rrtypes.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("%T %v", err, err)
	}
}

// Service Quotas has no Create operation: the template calls (Put…IntoTemplate, Get…FromTemplate) are its resource.
func TestServiceQuotas(t *testing.T) {
	h, url := start(t, "service-quotas")
	ctx := context.Background()
	c := servicequotas.NewFromConfig(awstest.Config(t, url))
	for i := range 3 {
		if _, err := c.PutServiceQuotaIncreaseRequestIntoTemplate(ctx, &servicequotas.PutServiceQuotaIncreaseRequestIntoTemplateInput{
			QuotaCode: aws.String(fmt.Sprintf("L-%d", i)), ServiceCode: aws.String("ec2"), AwsRegion: aws.String("us-east-1"), DesiredValue: aws.Float64(float64(10 + i))}); err != nil {
			t.Fatal(err)
		}
	}
	g, err := c.GetServiceQuotaIncreaseRequestFromTemplate(ctx, &servicequotas.GetServiceQuotaIncreaseRequestFromTemplateInput{QuotaCode: aws.String("L-1"), ServiceCode: aws.String("ec2"), AwsRegion: aws.String("us-east-1")})
	if err != nil || aws.ToFloat64(g.ServiceQuotaIncreaseRequestInTemplate.DesiredValue) != 11 {
		t.Fatalf("%v %+v", err, g.ServiceQuotaIncreaseRequestInTemplate)
	}
	pages, n := 0, 0
	p := servicequotas.NewListServiceQuotaIncreaseRequestsInTemplatePaginator(c, &servicequotas.ListServiceQuotaIncreaseRequestsInTemplateInput{ServiceCode: aws.String("ec2"), AwsRegion: aws.String("us-east-1"), MaxResults: aws.Int32(2)})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		n += len(out.ServiceQuotaIncreaseRequestInTemplateList)
	}
	if pages != 2 || n != 3 {
		t.Fatalf("pages %d n %d", pages, n)
	}
	// tags: through the engine's own API, for the ARN of the stored request
	svc := h.Service("service-quotas").(*generic.Service)
	rs := svc.List("000000000000", "us-east-1", "ServiceQuotaIncreaseRequest")
	if len(rs) != 3 {
		t.Fatalf("%d stored", len(rs))
	}
	if _, err := c.TagResource(ctx, &servicequotas.TagResourceInput{ResourceARN: aws.String(rs[0].ARN), Tags: nil}); err == nil {
		t.Log("tag with no tags accepted")
	}
	if _, err := c.DeleteServiceQuotaIncreaseRequestFromTemplate(ctx, &servicequotas.DeleteServiceQuotaIncreaseRequestFromTemplateInput{QuotaCode: aws.String("L-1"), ServiceCode: aws.String("ec2"), AwsRegion: aws.String("us-east-1")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetServiceQuotaIncreaseRequestFromTemplate(ctx, &servicequotas.GetServiceQuotaIncreaseRequestFromTemplateInput{QuotaCode: aws.String("L-1"), ServiceCode: aws.String("ec2"), AwsRegion: aws.String("us-east-1")})
	var nf interface{ ErrorCode() string }
	if !errors.As(err, &nf) || nf.ErrorCode() != "NoSuchResourceException" {
		t.Fatalf("%T %v", err, err)
	}
}

// floci #5349 and #4774: an endpoint needs two addresses, and lists them with ids and a status.
func TestRoute53ResolverIpAddresses(t *testing.T) {
	_, url := start(t, "route53resolver")
	ctx := context.Background()
	c := route53resolver.NewFromConfig(awstest.Config(t, url))
	two := []rrtypes.IpAddressRequest{{SubnetId: aws.String("subnet-1"), Ip: aws.String("10.0.1.10")}, {SubnetId: aws.String("subnet-2"), Ip: aws.String("10.0.2.10")}}
	in := &route53resolver.CreateResolverEndpointInput{CreatorRequestId: aws.String("r1"), Direction: rrtypes.ResolverEndpointDirectionInbound, SecurityGroupIds: []string{"sg-1"}, IpAddresses: two[:1]}
	// the SDK does not check the length itself
	if _, err := c.CreateResolverEndpoint(ctx, in); err == nil {
		t.Fatal("one IP address accepted")
	}
	in.IpAddresses = two
	out, err := c.CreateResolverEndpoint(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	l, err := c.ListResolverEndpointIpAddresses(ctx, &route53resolver.ListResolverEndpointIpAddressesInput{ResolverEndpointId: out.ResolverEndpoint.Id})
	if err != nil || len(l.IpAddresses) != 2 || aws.ToString(l.IpAddresses[0].IpId) == "" || l.IpAddresses[0].Status != rrtypes.IpAddressStatusAttached || aws.ToString(l.IpAddresses[1].Ip) != "10.0.2.10" {
		t.Fatalf("%v %+v", err, l)
	}
	_, err = c.ListResolverEndpointIpAddresses(ctx, &route53resolver.ListResolverEndpointIpAddressesInput{ResolverEndpointId: aws.String("rslvr-in-nope")})
	var nf *rrtypes.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("%T %v", err, err)
	}
}

// floci #5350: a quota increase request has an Id that can be read back; an unknown Id is not found.
func TestServiceQuotaIncreaseRequest(t *testing.T) {
	_, url := start(t, "service-quotas")
	ctx := context.Background()
	c := servicequotas.NewFromConfig(awstest.Config(t, url))
	r, err := c.RequestServiceQuotaIncrease(ctx, &servicequotas.RequestServiceQuotaIncreaseInput{ServiceCode: aws.String("lambda"), QuotaCode: aws.String("L-B99A9384"), DesiredValue: aws.Float64(5000)})
	if err != nil || aws.ToString(r.RequestedQuota.Id) == "" {
		t.Fatalf("%v %+v", err, r)
	}
	g, err := c.GetRequestedServiceQuotaChange(ctx, &servicequotas.GetRequestedServiceQuotaChangeInput{RequestId: r.RequestedQuota.Id})
	if err != nil || aws.ToFloat64(g.RequestedQuota.DesiredValue) != 5000 || aws.ToString(g.RequestedQuota.QuotaCode) != "L-B99A9384" {
		t.Fatalf("%v %+v", err, g)
	}
	_, err = c.GetRequestedServiceQuotaChange(ctx, &servicequotas.GetRequestedServiceQuotaChangeInput{RequestId: aws.String("0123456789abcdef0123456789abcdef")})
	var nf interface{ ErrorCode() string }
	if !errors.As(err, &nf) || nf.ErrorCode() != "NoSuchResourceException" {
		t.Fatalf("%T %v", err, err)
	}
}
