package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/securitylake"
	"github.com/aws/aws-sdk-go-v2/service/securitylake/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestSecurityLake(t *testing.T) {
	_, url := start(t, "securitylake")
	ctx := context.Background()
	c := securitylake.NewFromConfig(awstest.Config(t, url))
	role := aws.String("arn:aws:iam::000000000000:role/meta")

	if _, err := c.CreateDataLake(ctx, &securitylake.CreateDataLakeInput{MetaStoreManagerRoleArn: role}); err == nil {
		t.Fatal("create without a configuration succeeded")
	}
	if _, err := c.ListDataLakes(ctx, &securitylake.ListDataLakesInput{Regions: []string{"us-east-1"}}); err != nil {
		t.Fatal(err)
	}
	in := &securitylake.CreateDataLakeInput{MetaStoreManagerRoleArn: role, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
		Configurations: []types.DataLakeConfiguration{{Region: aws.String("us-east-1"),
			LifecycleConfiguration: &types.DataLakeLifecycleConfiguration{Expiration: &types.DataLakeLifecycleExpiration{Days: aws.Int32(30)}}}}}
	out, err := c.CreateDataLake(ctx, in)
	if err != nil || len(out.DataLakes) != 1 {
		t.Fatalf("create: %v %+v", err, out)
	}
	dl := out.DataLakes[0]
	if aws.ToString(dl.Region) != "us-east-1" || aws.ToString(dl.DataLakeArn) == "" || aws.ToString(dl.S3BucketArn) == "" || dl.CreateStatus != types.DataLakeStatusCompleted {
		t.Fatalf("data lake: %+v", dl)
	}
	if _, err := c.CreateDataLake(ctx, in); err == nil {
		t.Fatal("second lake in a region succeeded")
	}
	l, err := c.ListDataLakes(ctx, &securitylake.ListDataLakesInput{Regions: []string{"us-east-1"}})
	if err != nil || len(l.DataLakes) != 1 || aws.ToInt32(l.DataLakes[0].LifecycleConfiguration.Expiration.Days) != 30 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if l, err = c.ListDataLakes(ctx, &securitylake.ListDataLakesInput{Regions: []string{"eu-west-1"}}); err != nil || len(l.DataLakes) != 0 {
		t.Fatalf("list of another region: %v %+v", err, l)
	}
	u, err := c.UpdateDataLake(ctx, &securitylake.UpdateDataLakeInput{Configurations: []types.DataLakeConfiguration{{Region: aws.String("us-east-1"),
		LifecycleConfiguration: &types.DataLakeLifecycleConfiguration{Expiration: &types.DataLakeLifecycleExpiration{Days: aws.Int32(60)}}}}})
	if err != nil || aws.ToInt32(u.DataLakes[0].LifecycleConfiguration.Expiration.Days) != 60 {
		t.Fatalf("update: %v %+v", err, u)
	}
	if _, err := c.UpdateDataLake(ctx, &securitylake.UpdateDataLakeInput{Configurations: []types.DataLakeConfiguration{{Region: aws.String("eu-west-1")}}}); err == nil {
		t.Fatal("update of a missing lake succeeded")
	}
	tags, err := c.ListTagsForResource(ctx, &securitylake.ListTagsForResourceInput{ResourceArn: dl.DataLakeArn})
	if err != nil || len(tags.Tags) != 1 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.DeleteDataLake(ctx, &securitylake.DeleteDataLakeInput{Regions: []string{"us-east-1", "eu-west-1"}}); err == nil {
		t.Fatal("delete with a missing region succeeded")
	}
	if _, err := c.DeleteDataLake(ctx, &securitylake.DeleteDataLakeInput{Regions: []string{"us-east-1"}}); err != nil {
		t.Fatal(err)
	}
	if l, _ = c.ListDataLakes(ctx, &securitylake.ListDataLakesInput{}); len(l.DataLakes) != 0 {
		t.Fatalf("lake left after delete: %+v", l)
	}
}
