package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/directoryservice"
	"github.com/aws/aws-sdk-go-v2/service/directoryservice/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestDirectoryService(t *testing.T) {
	_, url := start(t, "ds")
	ctx := context.Background()
	c := directoryservice.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateDirectory(ctx, &directoryservice.CreateDirectoryInput{
		Name: aws.String("corp.example.com"), Password: aws.String("Sup3rSecret!"), Size: types.DirectorySizeSmall,
		ShortName: aws.String("CORP"), Description: aws.String("d"),
		VpcSettings: &types.DirectoryVpcSettings{VpcId: aws.String("vpc-0123"), SubnetIds: []string{"subnet-a", "subnet-b"}},
		Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	if err != nil || aws.ToString(o.DirectoryId) == "" || (*o.DirectoryId)[:2] != "d-" {
		t.Fatalf("create: %v %+v", err, o)
	}
	d, err := c.DescribeDirectories(ctx, &directoryservice.DescribeDirectoriesInput{DirectoryIds: []string{*o.DirectoryId}})
	if err != nil || len(d.DirectoryDescriptions) != 1 {
		t.Fatalf("describe: %v %+v", err, d)
	}
	x := d.DirectoryDescriptions[0]
	if aws.ToString(x.Name) != "corp.example.com" || aws.ToString(x.ShortName) != "CORP" || x.Stage != types.DirectoryStageActive ||
		x.Type != types.DirectoryTypeSimpleAd || x.VpcSettings == nil || len(x.VpcSettings.SubnetIds) != 2 {
		t.Fatalf("directory: %+v", x)
	}
	tg, err := c.ListTagsForResource(ctx, &directoryservice.ListTagsForResourceInput{ResourceId: o.DirectoryId})
	if err != nil || len(tg.Tags) != 1 {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.CreateDirectory(ctx, &directoryservice.CreateDirectoryInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
	if _, err := c.DescribeDirectories(ctx, &directoryservice.DescribeDirectoriesInput{DirectoryIds: []string{"d-0000000000"}}); err == nil {
		t.Fatal("unknown directory found")
	}
	if _, err := c.DeleteDirectory(ctx, &directoryservice.DeleteDirectoryInput{DirectoryId: o.DirectoryId}); err != nil {
		t.Fatal(err)
	}
	if d, err := c.DescribeDirectories(ctx, &directoryservice.DescribeDirectoriesInput{}); err != nil || len(d.DirectoryDescriptions) != 0 {
		t.Fatalf("after delete: %v %+v", err, d)
	}
}
