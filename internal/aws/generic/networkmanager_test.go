package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestNetworkManager(t *testing.T) {
	_, url := start(t, "networkmanager")
	ctx := context.Background()
	c := networkmanager.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateGlobalNetwork(ctx, &networkmanager.CreateGlobalNetworkInput{
		Description: aws.String("wan"), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	if err != nil || o.GlobalNetwork == nil {
		t.Fatalf("create: %v %+v", err, o)
	}
	gn := o.GlobalNetwork
	if aws.ToString(gn.GlobalNetworkId) == "" || aws.ToString(gn.GlobalNetworkArn) == "" || gn.State != types.GlobalNetworkStateAvailable {
		t.Fatalf("global network: %q %q %q", aws.ToString(gn.GlobalNetworkId), aws.ToString(gn.GlobalNetworkArn), gn.State)
	}
	d, err := c.DescribeGlobalNetworks(ctx, &networkmanager.DescribeGlobalNetworksInput{GlobalNetworkIds: []string{*gn.GlobalNetworkId}})
	if err != nil || len(d.GlobalNetworks) != 1 || aws.ToString(d.GlobalNetworks[0].Description) != "wan" {
		t.Fatalf("describe: %v %+v", err, d)
	}
	s, err := c.CreateSite(ctx, &networkmanager.CreateSiteInput{GlobalNetworkId: gn.GlobalNetworkId, Description: aws.String("hq")})
	if err != nil || s.Site == nil || aws.ToString(s.Site.SiteId) == "" {
		t.Fatalf("site: %v %+v", err, s)
	}
	if _, err := c.CreateSite(ctx, &networkmanager.CreateSiteInput{}); err == nil {
		t.Fatal("site without a network succeeded")
	}
	if _, err := c.DeleteGlobalNetwork(ctx, &networkmanager.DeleteGlobalNetworkInput{GlobalNetworkId: gn.GlobalNetworkId}); err != nil {
		t.Fatal(err)
	}
	if d, err := c.DescribeGlobalNetworks(ctx, &networkmanager.DescribeGlobalNetworksInput{}); err != nil || len(d.GlobalNetworks) != 0 {
		t.Fatalf("after delete: %v %+v", err, d)
	}
	if d, err := c.DescribeGlobalNetworks(ctx, &networkmanager.DescribeGlobalNetworksInput{GlobalNetworkIds: []string{*gn.GlobalNetworkId}}); err != nil || len(d.GlobalNetworks) != 0 {
		t.Fatalf("deleted network by id: %v %+v", err, d)
	}
	if _, err := c.DeleteGlobalNetwork(ctx, &networkmanager.DeleteGlobalNetworkInput{GlobalNetworkId: gn.GlobalNetworkId}); err == nil {
		t.Fatal("second delete succeeded")
	}
}
