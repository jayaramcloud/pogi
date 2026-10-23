package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestFSx(t *testing.T) {
	_, url := start(t, "fsx")
	ctx := context.Background()
	c := fsx.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateFileSystem(ctx, &fsx.CreateFileSystemInput{FileSystemType: types.FileSystemTypeLustre, StorageCapacity: aws.Int32(1200),
		SubnetIds: []string{"subnet-0123456789abcdef0"}, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
		LustreConfiguration: &types.CreateFileSystemLustreConfiguration{DeploymentType: types.LustreDeploymentTypeScratch2}})
	if err != nil {
		t.Fatal(err)
	}
	f := o.FileSystem
	id := aws.ToString(f.FileSystemId)
	if !strings.HasPrefix(id, "fs-") || len(id) != 20 || f.Lifecycle != types.FileSystemLifecycleAvailable || f.FileSystemType != types.FileSystemTypeLustre ||
		aws.ToString(f.DNSName) != id+".fsx.us-east-1.amazonaws.com" || aws.ToString(f.ResourceARN) != "arn:aws:fsx:us-east-1:000000000000:file-system/"+id ||
		aws.ToInt32(f.StorageCapacity) != 1200 || aws.ToString(f.OwnerId) != "000000000000" || f.CreationTime == nil || len(f.Tags) != 1 {
		t.Fatalf("create: %+v", f)
	}
	if f.LustreConfiguration == nil || f.LustreConfiguration.DeploymentType != types.LustreDeploymentTypeScratch2 || aws.ToString(f.LustreConfiguration.MountName) == "" {
		t.Fatalf("lustre: %+v", f.LustreConfiguration)
	}
	d, err := c.DescribeFileSystems(ctx, &fsx.DescribeFileSystemsInput{FileSystemIds: []string{id}})
	if err != nil || len(d.FileSystems) != 1 || aws.ToString(d.FileSystems[0].DNSName) != aws.ToString(f.DNSName) || d.FileSystems[0].Lifecycle != types.FileSystemLifecycleAvailable {
		t.Fatalf("describe: %v %+v", err, d)
	}

	z, err := c.CreateFileSystem(ctx, &fsx.CreateFileSystemInput{FileSystemType: types.FileSystemTypeOpenzfs, StorageCapacity: aws.Int32(64), SubnetIds: []string{"subnet-0123456789abcdef0"},
		OpenZFSConfiguration: &types.CreateFileSystemOpenZFSConfiguration{DeploymentType: types.OpenZFSDeploymentTypeSingleAz1, ThroughputCapacity: aws.Int32(64)}})
	if err != nil || z.FileSystem.OpenZFSConfiguration == nil || !strings.HasPrefix(aws.ToString(z.FileSystem.OpenZFSConfiguration.RootVolumeId), "fsvol-") {
		t.Fatalf("openzfs: %v %+v", err, z)
	}
	all, err := c.DescribeFileSystems(ctx, &fsx.DescribeFileSystemsInput{})
	if err != nil || len(all.FileSystems) != 2 {
		t.Fatalf("describe all: %v %+v", err, all)
	}

	arn := f.ResourceARN
	if _, err := c.TagResource(ctx, &fsx.TagResourceInput{ResourceARN: arn, Tags: []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}}}); err != nil {
		t.Fatal(err)
	}
	tg, err := c.ListTagsForResource(ctx, &fsx.ListTagsForResourceInput{ResourceARN: arn})
	if err != nil || len(tg.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.UntagResource(ctx, &fsx.UntagResourceInput{ResourceARN: arn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	d, _ = c.DescribeFileSystems(ctx, &fsx.DescribeFileSystemsInput{FileSystemIds: []string{id}})
	if len(d.FileSystems[0].Tags) != 1 || aws.ToString(d.FileSystems[0].Tags[0].Key) != "a" {
		t.Fatalf("tags after: %+v", d.FileSystems[0].Tags)
	}

	del, err := c.DeleteFileSystem(ctx, &fsx.DeleteFileSystemInput{FileSystemId: aws.String(id)})
	if err != nil || del.Lifecycle != types.FileSystemLifecycleDeleting {
		t.Fatalf("delete: %v %+v", err, del)
	}
	var nf *types.FileSystemNotFound
	if _, err := c.DescribeFileSystems(ctx, &fsx.DescribeFileSystemsInput{FileSystemIds: []string{id}}); !errors.As(err, &nf) {
		t.Fatalf("describe deleted: %T %v", err, err)
	}
	if _, err := c.DeleteFileSystem(ctx, &fsx.DeleteFileSystemInput{FileSystemId: aws.String(id)}); !errors.As(err, &nf) {
		t.Fatalf("second delete: %T %v", err, err)
	}
	if _, err := c.CreateFileSystem(ctx, &fsx.CreateFileSystemInput{StorageCapacity: aws.Int32(1200), SubnetIds: []string{"subnet-0123456789abcdef0"}}); err == nil {
		t.Fatal("create without a type succeeded")
	}
	if _, err := c.CreateFileSystem(ctx, &fsx.CreateFileSystemInput{FileSystemType: "NOPE", StorageCapacity: aws.Int32(1200), SubnetIds: []string{"subnet-0123456789abcdef0"}}); err == nil {
		t.Fatal("create with a bad type succeeded")
	}
}
