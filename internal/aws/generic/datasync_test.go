package generic_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/datasync"
	"github.com/aws/aws-sdk-go-v2/service/datasync/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestDataSync(t *testing.T) {
	srv := pogi.Start(t)
	cfg := awstest.Config(t, srv.URL)
	ctx := context.Background()
	c := datasync.NewFromConfig(cfg)
	sc := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })

	for _, b := range []string{"ds-src", "ds-dst"} {
		if _, err := sc.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}); err != nil {
			t.Fatal(err)
		}
	}
	put := func(bucket, key, body string) {
		t.Helper()
		if _, err := sc.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader(body)}); err != nil {
			t.Fatal(err)
		}
	}
	read := func(bucket, key string) (string, error) {
		o, err := sc.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			return "", err
		}
		b, _ := io.ReadAll(o.Body)
		return string(b), nil
	}
	put("ds-src", "in/data/a.csv", "1,2,3")
	put("ds-src", "in/data/b.csv", "4,5")
	put("ds-src", "in/data/skip.tmp", "junk")
	put("ds-src", "in/logs/x.log", "log")
	put("ds-src", "other/z", "outside the source prefix")

	role := "arn:aws:iam::000000000000:role/datasync"
	src, err := c.CreateLocationS3(ctx, &datasync.CreateLocationS3Input{S3BucketArn: aws.String("arn:aws:s3:::ds-src"), Subdirectory: aws.String("/in"),
		S3Config: &types.S3Config{BucketAccessRoleArn: aws.String(role)}, Tags: []types.TagListEntry{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	dst, err := c.CreateLocationS3(ctx, &datasync.CreateLocationS3Input{S3BucketArn: aws.String("arn:aws:s3:::ds-dst"), Subdirectory: aws.String("copy"),
		S3Config: &types.S3Config{BucketAccessRoleArn: aws.String(role)}})
	if err != nil {
		t.Fatal(err)
	}
	ds, err := c.DescribeLocationS3(ctx, &datasync.DescribeLocationS3Input{LocationArn: src.LocationArn})
	if err != nil || aws.ToString(ds.LocationUri) != "s3://ds-src/in/" || aws.ToString(ds.S3Config.BucketAccessRoleArn) != role || ds.CreationTime == nil {
		t.Fatalf("describe location: %v %+v", err, ds)
	}
	if _, err := c.DescribeLocationNfs(ctx, &datasync.DescribeLocationNfsInput{LocationArn: src.LocationArn}); err == nil {
		t.Fatal("an S3 location described as NFS")
	}
	nfs, err := c.CreateLocationNfs(ctx, &datasync.CreateLocationNfsInput{ServerHostname: aws.String("nas.example.com"), Subdirectory: aws.String("/export"),
		OnPremConfig: &types.OnPremConfig{AgentArns: []string{"arn:aws:datasync:us-east-1:000000000000:agent/agent-0123456789abcdef0"}}})
	if err != nil {
		t.Fatal(err)
	}
	dn, err := c.DescribeLocationNfs(ctx, &datasync.DescribeLocationNfsInput{LocationArn: nfs.LocationArn})
	if err != nil || aws.ToString(dn.LocationUri) != "nfs://nas.example.com/export/" || len(dn.OnPremConfig.AgentArns) != 1 {
		t.Fatalf("describe nfs: %v %+v", err, dn)
	}
	if _, err := c.CreateLocationSmb(ctx, &datasync.CreateLocationSmbInput{ServerHostname: aws.String("smb.example.com"), Subdirectory: aws.String("/share"), User: aws.String("u"), Password: aws.String("p"),
		AgentArns: []string{"arn:aws:datasync:us-east-1:000000000000:agent/agent-0123456789abcdef0"}}); err != nil {
		t.Fatal(err)
	}
	ll, err := c.ListLocations(ctx, &datasync.ListLocationsInput{})
	if err != nil || len(ll.Locations) != 4 {
		t.Fatalf("list locations: %v %+v", err, ll)
	}

	tk, err := c.CreateTask(ctx, &datasync.CreateTaskInput{SourceLocationArn: src.LocationArn, DestinationLocationArn: dst.LocationArn, Name: aws.String("nightly"),
		Includes: []types.FilterRule{{FilterType: types.FilterTypeSimplePattern, Value: aws.String("/data|/logs/x.log")}},
		Excludes: []types.FilterRule{{FilterType: types.FilterTypeSimplePattern, Value: aws.String("*.tmp")}},
		Tags:     []types.TagListEntry{{Key: aws.String("team"), Value: aws.String("data")}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateTask(ctx, &datasync.CreateTaskInput{SourceLocationArn: src.LocationArn, DestinationLocationArn: aws.String("arn:aws:datasync:us-east-1:000000000000:location/loc-0")}); err == nil {
		t.Fatal("task with a missing location")
	}
	dt, err := c.DescribeTask(ctx, &datasync.DescribeTaskInput{TaskArn: tk.TaskArn})
	if err != nil || aws.ToString(dt.Name) != "nightly" || dt.Status != types.TaskStatusAvailable || aws.ToString(dt.SourceLocationArn) != aws.ToString(src.LocationArn) || len(dt.Includes) != 1 {
		t.Fatalf("describe task: %v %+v", err, dt)
	}
	if _, err := c.UpdateTask(ctx, &datasync.UpdateTaskInput{TaskArn: tk.TaskArn, Name: aws.String("nightly-2")}); err != nil {
		t.Fatal(err)
	}
	if dt, _ = c.DescribeTask(ctx, &datasync.DescribeTaskInput{TaskArn: tk.TaskArn}); aws.ToString(dt.Name) != "nightly-2" {
		t.Fatalf("updated task: %+v", dt)
	}

	// the execution copies the filtered objects
	ex, err := c.StartTaskExecution(ctx, &datasync.StartTaskExecutionInput{TaskArn: tk.TaskArn})
	if err != nil {
		t.Fatal(err)
	}
	de, err := c.DescribeTaskExecution(ctx, &datasync.DescribeTaskExecutionInput{TaskExecutionArn: ex.TaskExecutionArn})
	if err != nil {
		t.Fatal(err)
	}
	if de.Status != "SUCCESS" || de.FilesTransferred != 3 || de.BytesTransferred != int64(len("1,2,3")+len("4,5")+len("log")) || de.FilesSkipped != 0 || de.StartTime == nil {
		t.Fatalf("execution: %+v", de)
	}
	for key, want := range map[string]string{"copy/data/a.csv": "1,2,3", "copy/data/b.csv": "4,5", "copy/logs/x.log": "log"} {
		if got, err := read("ds-dst", key); err != nil || got != want {
			t.Fatalf("copied %s: %q %v", key, got, err)
		}
	}
	for _, key := range []string{"copy/data/skip.tmp", "copy/other/z", "copy/z"} {
		if _, err := read("ds-dst", key); err == nil {
			t.Fatalf("%s was copied", key)
		}
	}
	// a second run copies nothing that has not changed
	put("ds-src", "in/data/a.csv", "1,2,3,4")
	ex2, _ := c.StartTaskExecution(ctx, &datasync.StartTaskExecutionInput{TaskArn: tk.TaskArn})
	de2, _ := c.DescribeTaskExecution(ctx, &datasync.DescribeTaskExecutionInput{TaskExecutionArn: ex2.TaskExecutionArn})
	if de2.FilesTransferred != 1 || de2.FilesSkipped != 2 {
		t.Fatalf("second execution: %+v", de2)
	}
	if got, _ := read("ds-dst", "copy/data/a.csv"); got != "1,2,3,4" {
		t.Fatalf("changed file: %q", got)
	}
	// a source file that is gone leaves the destination, when asked
	if _, err := sc.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("ds-src"), Key: aws.String("in/data/b.csv")}); err != nil {
		t.Fatal(err)
	}
	ex3, _ := c.StartTaskExecution(ctx, &datasync.StartTaskExecutionInput{TaskArn: tk.TaskArn, OverrideOptions: &types.Options{PreserveDeletedFiles: types.PreserveDeletedFilesRemove}})
	de3, _ := c.DescribeTaskExecution(ctx, &datasync.DescribeTaskExecutionInput{TaskExecutionArn: ex3.TaskExecutionArn})
	if de3.FilesDeleted != 1 {
		t.Fatalf("third execution: %+v", de3)
	}
	if _, err := read("ds-dst", "copy/data/b.csv"); err == nil {
		t.Fatal("b.csv stayed in the destination")
	}
	le, err := c.ListTaskExecutions(ctx, &datasync.ListTaskExecutionsInput{TaskArn: tk.TaskArn})
	if err != nil || len(le.TaskExecutions) != 3 {
		t.Fatalf("list executions: %v %+v", err, le)
	}
	lt, err := c.ListTasks(ctx, &datasync.ListTasksInput{})
	if err != nil || len(lt.Tasks) != 1 || aws.ToString(lt.Tasks[0].Name) != "nightly-2" {
		t.Fatalf("list tasks: %v %+v", err, lt)
	}

	// a task over metadata-only locations ends in ERROR
	nt, _ := c.CreateTask(ctx, &datasync.CreateTaskInput{SourceLocationArn: nfs.LocationArn, DestinationLocationArn: dst.LocationArn})
	ex4, _ := c.StartTaskExecution(ctx, &datasync.StartTaskExecutionInput{TaskArn: nt.TaskArn})
	if de4, _ := c.DescribeTaskExecution(ctx, &datasync.DescribeTaskExecutionInput{TaskExecutionArn: ex4.TaskExecutionArn}); de4.Status != "ERROR" {
		t.Fatalf("nfs execution: %+v", de4)
	}

	// tags
	tags, err := c.ListTagsForResource(ctx, &datasync.ListTagsForResourceInput{ResourceArn: tk.TaskArn})
	if err != nil || len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "team" {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.TagResource(ctx, &datasync.TagResourceInput{ResourceArn: tk.TaskArn, Tags: []types.TagListEntry{{Key: aws.String("a"), Value: aws.String("b")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &datasync.UntagResourceInput{ResourceArn: tk.TaskArn, Keys: []string{"team"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ = c.ListTagsForResource(ctx, &datasync.ListTagsForResourceInput{ResourceArn: tk.TaskArn}); len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "a" {
		t.Fatalf("tags after: %+v", tags)
	}

	// delete; the not-found is InvalidRequestException, as DataSync has no other
	if _, err := c.DeleteTask(ctx, &datasync.DeleteTaskInput{TaskArn: tk.TaskArn}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeTask(ctx, &datasync.DescribeTaskInput{TaskArn: tk.TaskArn})
	var ir *types.InvalidRequestException
	if !errors.As(err, &ir) {
		t.Fatalf("task not found: %T %v", err, err)
	}
	if _, err := c.DescribeTaskExecution(ctx, &datasync.DescribeTaskExecutionInput{TaskExecutionArn: ex.TaskExecutionArn}); !errors.As(err, &ir) {
		t.Fatalf("execution of a deleted task: %T %v", err, err)
	}
	if _, err := c.DeleteLocation(ctx, &datasync.DeleteLocationInput{LocationArn: dst.LocationArn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeLocationS3(ctx, &datasync.DescribeLocationS3Input{LocationArn: dst.LocationArn}); !errors.As(err, &ir) {
		t.Fatalf("location not found: %T %v", err, err)
	}
}
