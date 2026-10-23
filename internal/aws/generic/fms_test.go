package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/fms"
	"github.com/aws/aws-sdk-go-v2/service/fms/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestFMS(t *testing.T) {
	_, url := start(t, "fms")
	ctx := context.Background()
	c := fms.NewFromConfig(awstest.Config(t, url))

	if _, err := c.PutPolicy(ctx, &fms.PutPolicyInput{}); err == nil {
		t.Fatal("put without a policy succeeded")
	}
	_, err := c.GetPolicy(ctx, &fms.GetPolicyInput{PolicyId: aws.String("00000000-0000-0000-0000-000000000000")})
	kCode(t, err, "ResourceNotFoundException")
	_, err = c.DeletePolicy(ctx, &fms.DeletePolicyInput{PolicyId: aws.String("00000000-0000-0000-0000-000000000000")})
	kCode(t, err, "ResourceNotFoundException")

	p, err := c.PutPolicy(ctx, &fms.PutPolicyInput{
		Policy: &types.Policy{
			PolicyName:          aws.String("waf"),
			ResourceType:        aws.String("AWS::ElasticLoadBalancingV2::LoadBalancer"),
			RemediationEnabled:  false,
			ExcludeResourceTags: false,
			SecurityServicePolicyData: &types.SecurityServicePolicyData{
				Type:               types.SecurityServiceTypeWafv2,
				ManagedServiceData: aws.String(`{"type":"WAFV2"}`),
			},
		},
		TagList: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := aws.ToString(p.Policy.PolicyId)
	if id == "" || aws.ToString(p.PolicyArn) == "" || aws.ToString(p.Policy.PolicyName) != "waf" {
		t.Fatalf("policy: %+v", p)
	}
	g, err := c.GetPolicy(ctx, &fms.GetPolicyInput{PolicyId: aws.String(id)})
	if err != nil || aws.ToString(g.Policy.PolicyName) != "waf" || g.Policy.SecurityServicePolicyData.Type != types.SecurityServiceTypeWafv2 {
		t.Fatalf("get: %v %+v", err, g)
	}
	l, err := c.ListPolicies(ctx, &fms.ListPoliciesInput{})
	if err != nil || len(l.PolicyList) != 1 || aws.ToString(l.PolicyList[0].PolicyId) != id {
		t.Fatalf("list: %v %+v", err, l)
	}
	tg, err := c.ListTagsForResource(ctx, &fms.ListTagsForResourceInput{ResourceArn: p.PolicyArn})
	if err != nil || len(tg.TagList) != 1 {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.DeletePolicy(ctx, &fms.DeletePolicyInput{PolicyId: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetPolicy(ctx, &fms.GetPolicyInput{PolicyId: aws.String(id)})
	kCode(t, err, "ResourceNotFoundException")
}
