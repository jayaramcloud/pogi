package generic_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/odb"
	"github.com/aws/aws-sdk-go-v2/service/odb/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestODB(t *testing.T) {
	_, url := start(t, "odb")
	ctx := context.Background()
	c := odb.NewFromConfig(awstest.Config(t, url))

	// failures first
	if _, err := c.CreateOdbNetwork(ctx, &odb.CreateOdbNetworkInput{DisplayName: aws.String("n")}); err == nil {
		t.Fatal("network without a subnet CIDR succeeded")
	}
	_, err := c.GetOdbNetwork(ctx, &odb.GetOdbNetworkInput{OdbNetworkId: aws.String("odbnet_nope")})
	kCode(t, err, "ResourceNotFoundException")
	_, err = c.DeleteOdbNetwork(ctx, &odb.DeleteOdbNetworkInput{OdbNetworkId: aws.String("odbnet_nope"), DeleteAssociatedResources: aws.Bool(false)})
	kCode(t, err, "ResourceNotFoundException")

	n, err := c.CreateOdbNetwork(ctx, &odb.CreateOdbNetworkInput{
		DisplayName: aws.String("net"), AvailabilityZoneId: aws.String("use1-az1"), ClientSubnetCidr: aws.String("10.2.0.0/24"),
		BackupSubnetCidr: aws.String("10.2.1.0/24"), S3Access: types.AccessDisabled, ZeroEtlAccess: types.AccessDisabled,
		Tags: map[string]string{"env": "dev"},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := aws.ToString(n.OdbNetworkId)
	if id == "" {
		t.Fatalf("no id: %+v", n)
	}
	g, err := c.GetOdbNetwork(ctx, &odb.GetOdbNetworkInput{OdbNetworkId: aws.String(id)})
	if err != nil {
		t.Fatal(err)
	}
	o := g.OdbNetwork
	if aws.ToString(o.OdbNetworkId) != id || aws.ToString(o.DisplayName) != "net" || o.Status != types.ResourceStatusAvailable ||
		aws.ToString(o.ClientSubnetCidr) != "10.2.0.0/24" || o.ManagedServices == nil || o.ManagedServices.S3Access.Status != types.ManagedResourceStatusDisabled || !strings.Contains(aws.ToString(o.OdbNetworkArn), ":odb:") {
		t.Fatalf("network: %+v", o)
	}
	l, err := c.ListOdbNetworks(ctx, &odb.ListOdbNetworksInput{})
	if err != nil || len(l.OdbNetworks) != 1 || aws.ToString(l.OdbNetworks[0].OdbNetworkId) != id {
		t.Fatalf("list: %v %+v", err, l)
	}
	tg, err := c.ListTagsForResource(ctx, &odb.ListTagsForResourceInput{ResourceArn: o.OdbNetworkArn})
	if err != nil || tg.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.DeleteOdbNetwork(ctx, &odb.DeleteOdbNetworkInput{OdbNetworkId: aws.String(id), DeleteAssociatedResources: aws.Bool(false)}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetOdbNetwork(ctx, &odb.GetOdbNetworkInput{OdbNetworkId: aws.String(id)})
	kCode(t, err, "ResourceNotFoundException")
}
