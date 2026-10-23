package generic_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/vpclattice"
	"github.com/aws/aws-sdk-go-v2/service/vpclattice/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestVPCLattice(t *testing.T) {
	_, url := start(t, "vpc-lattice")
	ctx := context.Background()
	c := vpclattice.NewFromConfig(awstest.Config(t, url))

	sn, err := c.CreateServiceNetwork(ctx, &vpclattice.CreateServiceNetworkInput{Name: aws.String("mesh"), AuthType: types.AuthTypeNone, Tags: map[string]string{"env": "dev"}})
	if err != nil || aws.ToString(sn.Name) != "mesh" || !strings.HasPrefix(aws.ToString(sn.Id), "sn-") || !strings.Contains(aws.ToString(sn.Arn), ":servicenetwork/sn-") {
		t.Fatalf("service network: %v %s", err, aws.ToString(sn.Arn))
	}
	if _, err := c.CreateServiceNetwork(ctx, &vpclattice.CreateServiceNetworkInput{Name: aws.String("mesh")}); err == nil {
		t.Fatal("duplicate service network succeeded")
	}
	if _, err := c.CreateServiceNetwork(ctx, &vpclattice.CreateServiceNetworkInput{}); err == nil {
		t.Fatal("service network without a name succeeded")
	}
	if _, err := c.CreateServiceNetwork(ctx, &vpclattice.CreateServiceNetworkInput{Name: aws.String("ab")}); err == nil { // the name needs three characters
		t.Fatal("a short name succeeded")
	}
	// the name is not the identifier: a get by name is a bad value, a get by a valid unknown id is not found
	if _, err := c.GetServiceNetwork(ctx, &vpclattice.GetServiceNetworkInput{ServiceNetworkIdentifier: aws.String("mesh")}); err == nil {
		t.Fatal("get by name succeeded")
	}
	if _, err := c.GetServiceNetwork(ctx, &vpclattice.GetServiceNetworkInput{ServiceNetworkIdentifier: aws.String("sn-00000000000000000")}); err == nil {
		t.Fatal("unknown network read")
	}
	g, err := c.GetServiceNetwork(ctx, &vpclattice.GetServiceNetworkInput{ServiceNetworkIdentifier: sn.Id})
	if err != nil || aws.ToString(g.Name) != "mesh" {
		t.Fatalf("get: %v %+v", err, g)
	}

	svc, err := c.CreateService(ctx, &vpclattice.CreateServiceInput{Name: aws.String("orders")})
	if err != nil || !strings.HasPrefix(aws.ToString(svc.Id), "svc-") || svc.Status != types.ServiceStatusActive {
		t.Fatalf("service: %v %+v", err, svc)
	}
	l, err := c.ListServices(ctx, &vpclattice.ListServicesInput{})
	if err != nil || len(l.Items) != 1 || aws.ToString(l.Items[0].Name) != "orders" {
		t.Fatalf("list: %v %+v", err, l)
	}
	rg, err := c.CreateResourceGateway(ctx, &vpclattice.CreateResourceGatewayInput{Name: aws.String("gate"), VpcIdentifier: aws.String("vpc-123")})
	if err != nil || !strings.HasPrefix(aws.ToString(rg.Id), "rgw-") {
		t.Fatalf("resource gateway: %v %+v", err, rg)
	}
	if _, err := c.GetResourceGateway(ctx, &vpclattice.GetResourceGatewayInput{ResourceGatewayIdentifier: rg.Id}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListTagsForResource(ctx, &vpclattice.ListTagsForResourceInput{ResourceArn: sn.Arn})
	if err != nil || tags.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, tags)
	}

	if _, err := c.DeleteServiceNetwork(ctx, &vpclattice.DeleteServiceNetworkInput{ServiceNetworkIdentifier: sn.Id}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteServiceNetwork(ctx, &vpclattice.DeleteServiceNetworkInput{ServiceNetworkIdentifier: sn.Id}); err == nil {
		t.Fatal("second delete succeeded")
	}
	if _, err := c.DeleteService(ctx, &vpclattice.DeleteServiceInput{ServiceIdentifier: svc.Id}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetService(ctx, &vpclattice.GetServiceInput{ServiceIdentifier: svc.Id}); err == nil {
		t.Fatal("deleted service read")
	}
}
