package generic_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/storagegateway"
	"github.com/aws/aws-sdk-go-v2/service/storagegateway/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestStorageGateway(t *testing.T) {
	_, url := start(t, "storagegateway")
	ctx := context.Background()
	c := storagegateway.NewFromConfig(awstest.Config(t, url))

	if _, err := c.CreateTapePool(ctx, &storagegateway.CreateTapePoolInput{PoolName: aws.String("p")}); err == nil {
		t.Fatal("pool without a storage class succeeded")
	}
	if _, err := c.CreateTapePool(ctx, &storagegateway.CreateTapePoolInput{PoolName: aws.String("p"), StorageClass: "TAPE"}); err == nil {
		t.Fatal("pool with a bad storage class succeeded")
	}
	_, err := c.DeleteTapePool(ctx, &storagegateway.DeleteTapePoolInput{PoolARN: aws.String("arn:aws:storagegateway:us-east-1:000000000000:tapepool/pool-none")})
	kCode(t, err, "InvalidGatewayRequestException")

	p, err := c.CreateTapePool(ctx, &storagegateway.CreateTapePoolInput{
		PoolName: aws.String("archive"), StorageClass: types.TapeStorageClassGlacier, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	if err != nil || !strings.Contains(aws.ToString(p.PoolARN), ":tapepool/") {
		t.Fatalf("create: %v %+v", err, p)
	}
	l, err := c.ListTapePools(ctx, &storagegateway.ListTapePoolsInput{})
	if err != nil || len(l.PoolInfos) != 1 || aws.ToString(l.PoolInfos[0].PoolName) != "archive" || l.PoolInfos[0].StorageClass != types.TapeStorageClassGlacier {
		t.Fatalf("list: %v %+v", err, l)
	}
	tg, err := c.ListTagsForResource(ctx, &storagegateway.ListTagsForResourceInput{ResourceARN: p.PoolARN})
	if err != nil || len(tg.Tags) != 1 {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.DeleteTapePool(ctx, &storagegateway.DeleteTapePoolInput{PoolARN: p.PoolARN}); err != nil {
		t.Fatal(err)
	}
	l, err = c.ListTapePools(ctx, &storagegateway.ListTapePoolsInput{})
	if err != nil || len(l.PoolInfos) != 0 {
		t.Fatalf("after delete: %v %+v", err, l)
	}
}

func TestStorageGatewayGateway(t *testing.T) {
	_, url := start(t, "storagegateway")
	ctx := context.Background()
	c := storagegateway.NewFromConfig(awstest.Config(t, url))

	if _, err := c.ActivateGateway(ctx, &storagegateway.ActivateGatewayInput{GatewayName: aws.String("g")}); err == nil {
		t.Fatal("activate without a key succeeded")
	}
	_, err := c.DescribeGatewayInformation(ctx, &storagegateway.DescribeGatewayInformationInput{GatewayARN: aws.String("arn:aws:storagegateway:us-east-1:000000000000:gateway/sgw-NONE0000")})
	kCode(t, err, "InvalidGatewayRequestException")
	_, err = c.DeleteGateway(ctx, &storagegateway.DeleteGatewayInput{GatewayARN: aws.String("arn:aws:storagegateway:us-east-1:000000000000:gateway/sgw-NONE0000")})
	kCode(t, err, "InvalidGatewayRequestException")

	a, err := c.ActivateGateway(ctx, &storagegateway.ActivateGatewayInput{
		ActivationKey: aws.String("AAAAA-BBBBB-CCCCC-DDDDD-EEEEE"), GatewayName: aws.String("gw"), GatewayTimezone: aws.String("GMT-5:00"),
		GatewayRegion: aws.String("us-east-1"), GatewayType: aws.String("FILE_S3"), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	if err != nil || !strings.Contains(aws.ToString(a.GatewayARN), ":gateway/sgw-") {
		t.Fatalf("activate: %v %+v", err, a)
	}
	d, err := c.DescribeGatewayInformation(ctx, &storagegateway.DescribeGatewayInformationInput{GatewayARN: a.GatewayARN})
	if err != nil || aws.ToString(d.GatewayState) != "RUNNING" || aws.ToString(d.GatewayType) != "FILE_S3" || aws.ToString(d.GatewayTimezone) != "GMT-5:00" || len(d.Tags) != 1 {
		t.Fatalf("describe: %v %+v", err, d)
	}
	if _, err := c.UpdateGatewayInformation(ctx, &storagegateway.UpdateGatewayInformationInput{GatewayARN: a.GatewayARN, GatewayName: aws.String("renamed"), GatewayTimezone: aws.String("GMT+1:00")}); err != nil {
		t.Fatal(err)
	}
	l, err := c.ListGateways(ctx, &storagegateway.ListGatewaysInput{})
	if err != nil || len(l.Gateways) != 1 || aws.ToString(l.Gateways[0].GatewayName) != "renamed" {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.DeleteGateway(ctx, &storagegateway.DeleteGatewayInput{GatewayARN: a.GatewayARN}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeGatewayInformation(ctx, &storagegateway.DescribeGatewayInformationInput{GatewayARN: a.GatewayARN})
	kCode(t, err, "InvalidGatewayRequestException")
}
