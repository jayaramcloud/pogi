package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/gamelift"
	"github.com/aws/aws-sdk-go-v2/service/gamelift/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestGameLift(t *testing.T) {
	_, url := start(t, "gamelift")
	ctx := context.Background()
	c := gamelift.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateFleet(ctx, &gamelift.CreateFleetInput{Name: aws.String("fleet"), EC2InstanceType: types.EC2InstanceTypeC5Large,
		ComputeType: types.ComputeTypeEc2, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	f := o.FleetAttributes
	if aws.ToString(f.Name) != "fleet" || aws.ToString(f.FleetId) == "" || f.Status != types.FleetStatusActive {
		t.Fatalf("fleet: %+v", f)
	}
	d, err := c.DescribeFleetAttributes(ctx, &gamelift.DescribeFleetAttributesInput{FleetIds: []string{aws.ToString(f.FleetId)}})
	if err != nil || len(d.FleetAttributes) != 1 {
		t.Fatalf("describe: %v %+v", err, d)
	}
	a, err := c.CreateAlias(ctx, &gamelift.CreateAliasInput{Name: aws.String("prod"), RoutingStrategy: &types.RoutingStrategy{Type: types.RoutingStrategyTypeSimple, FleetId: f.FleetId}})
	if err != nil || aws.ToString(a.Alias.Name) != "prod" || !strings.HasPrefix(aws.ToString(a.Alias.AliasId), "alias-") {
		t.Fatalf("alias: %v %s", err, aws.ToString(a.Alias.AliasId))
	}
	if _, err := c.DeleteFleet(ctx, &gamelift.DeleteFleetInput{FleetId: f.FleetId}); err != nil {
		t.Fatal(err)
	}
	var nf *types.NotFoundException
	if _, err := c.DeleteFleet(ctx, &gamelift.DeleteFleetInput{FleetId: f.FleetId}); !errors.As(err, &nf) {
		t.Fatalf("second delete: %T %v", err, err)
	}
	if _, err := c.CreateFleet(ctx, &gamelift.CreateFleetInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}
