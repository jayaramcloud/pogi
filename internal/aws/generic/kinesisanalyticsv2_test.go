package generic_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2"
	"github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/docker"
)

func TestManagedFlink(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent") // statuses only; TestFlinkCluster has the real cluster
	s := pogi.Start(t)
	ctx := context.Background()
	c := kinesisanalyticsv2.NewFromConfig(awstest.Config(t, s.URL))
	lifecycle(t, time.Hour)

	cr, err := c.CreateApplication(ctx, &kinesisanalyticsv2.CreateApplicationInput{ApplicationName: aws.String("flink1"), RuntimeEnvironment: types.RuntimeEnvironmentFlink120,
		ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/flink"), Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("data")}},
		ApplicationConfiguration: &types.ApplicationConfiguration{
			ApplicationCodeConfiguration: &types.ApplicationCodeConfiguration{CodeContentType: types.CodeContentTypeZipfile,
				CodeContent: &types.CodeContent{S3ContentLocation: &types.S3ContentLocation{BucketARN: aws.String("arn:aws:s3:::code"), FileKey: aws.String("app.jar")}}},
			EnvironmentProperties: &types.EnvironmentProperties{PropertyGroups: []types.PropertyGroup{{PropertyGroupId: aws.String("k"), PropertyMap: map[string]string{"a": "b"}}}},
			FlinkApplicationConfiguration: &types.FlinkApplicationConfiguration{
				ParallelismConfiguration: &types.ParallelismConfiguration{ConfigurationType: types.ConfigurationTypeCustom, Parallelism: aws.Int32(4)}}}})
	if err != nil {
		t.Fatal(err)
	}
	d := cr.ApplicationDetail
	if aws.ToString(d.ApplicationARN) != "arn:aws:kinesisanalytics:us-east-1:000000000000:application/flink1" || d.ApplicationStatus != types.ApplicationStatusReady || aws.ToInt64(d.ApplicationVersionId) != 1 {
		t.Fatalf("create: %+v", d)
	}
	if _, err := c.CreateApplication(ctx, &kinesisanalyticsv2.CreateApplicationInput{ApplicationName: aws.String("flink1"), RuntimeEnvironment: types.RuntimeEnvironmentFlink120, ServiceExecutionRole: aws.String("r")}); err == nil {
		t.Fatal("duplicate application")
	}
	get := func() *types.ApplicationDetail {
		g, err := c.DescribeApplication(ctx, &kinesisanalyticsv2.DescribeApplicationInput{ApplicationName: aws.String("flink1"), IncludeAdditionalDetails: aws.Bool(true)})
		if err != nil {
			t.Fatal(err)
		}
		return g.ApplicationDetail
	}
	g := get()
	cfg := g.ApplicationConfigurationDescription
	if cfg == nil || aws.ToString(cfg.ApplicationCodeConfigurationDescription.CodeContentDescription.S3ApplicationCodeLocationDescription.FileKey) != "app.jar" ||
		aws.ToInt32(cfg.FlinkApplicationConfigurationDescription.ParallelismConfigurationDescription.Parallelism) != 4 ||
		aws.ToBool(cfg.FlinkApplicationConfigurationDescription.CheckpointConfigurationDescription.CheckpointingEnabled) != true ||
		len(cfg.EnvironmentPropertyDescriptions.PropertyGroupDescriptions) != 1 {
		t.Fatalf("description: %+v", cfg)
	}

	// update bumps the version and changes the description
	up, err := c.UpdateApplication(ctx, &kinesisanalyticsv2.UpdateApplicationInput{ApplicationName: aws.String("flink1"), CurrentApplicationVersionId: aws.Int64(1),
		ApplicationConfigurationUpdate: &types.ApplicationConfigurationUpdate{FlinkApplicationConfigurationUpdate: &types.FlinkApplicationConfigurationUpdate{
			ParallelismConfigurationUpdate: &types.ParallelismConfigurationUpdate{ParallelismUpdate: aws.Int32(8), ConfigurationTypeUpdate: types.ConfigurationTypeCustom}}}})
	if err != nil || aws.ToInt64(up.ApplicationDetail.ApplicationVersionId) != 2 {
		t.Fatalf("update: %v %+v", err, up)
	}
	if p := get().ApplicationConfigurationDescription.FlinkApplicationConfigurationDescription.ParallelismConfigurationDescription; aws.ToInt32(p.Parallelism) != 8 {
		t.Fatalf("parallelism %+v", p)
	}
	if _, err := c.UpdateApplication(ctx, &kinesisanalyticsv2.UpdateApplicationInput{ApplicationName: aws.String("flink1"), CurrentApplicationVersionId: aws.Int64(1)}); err == nil {
		t.Fatal("update with a stale version")
	}
	if l, err := c.ListApplications(ctx, &kinesisanalyticsv2.ListApplicationsInput{}); err != nil || len(l.ApplicationSummaries) != 1 || l.ApplicationSummaries[0].ApplicationStatus != types.ApplicationStatusReady {
		t.Fatalf("list: %v %+v", err, l)
	}

	// start: STARTING, then RUNNING; stop: STOPPING, then READY
	if _, err := c.StartApplication(ctx, &kinesisanalyticsv2.StartApplicationInput{ApplicationName: aws.String("flink1")}); err != nil {
		t.Fatal(err)
	}
	if st := get().ApplicationStatus; st != types.ApplicationStatusStarting {
		t.Fatalf("starting: %s", st)
	}
	lifecycle(t, 0)
	if st := get().ApplicationStatus; st != types.ApplicationStatusRunning {
		t.Fatalf("running: %s", st)
	}
	if _, err := c.DeleteApplication(ctx, &kinesisanalyticsv2.DeleteApplicationInput{ApplicationName: aws.String("flink1"), CreateTimestamp: g.CreateTimestamp}); err == nil {
		t.Fatal("deleted a running application")
	}
	lifecycle(t, time.Hour)
	if _, err := c.StopApplication(ctx, &kinesisanalyticsv2.StopApplicationInput{ApplicationName: aws.String("flink1")}); err != nil {
		t.Fatal(err)
	}
	if st := get().ApplicationStatus; st != types.ApplicationStatusStopping {
		t.Fatalf("stopping: %s", st)
	}
	lifecycle(t, 0)
	if st := get().ApplicationStatus; st != types.ApplicationStatusReady {
		t.Fatalf("ready: %s", st)
	}

	// tags
	arn := aws.ToString(d.ApplicationARN)
	if _, err := c.TagResource(ctx, &kinesisanalyticsv2.TagResourceInput{ResourceARN: aws.String(arn), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &kinesisanalyticsv2.UntagResourceInput{ResourceARN: aws.String(arn), TagKeys: []string{"team"}}); err != nil {
		t.Fatal(err)
	}
	if l, err := c.ListTagsForResource(ctx, &kinesisanalyticsv2.ListTagsForResourceInput{ResourceARN: aws.String(arn)}); err != nil || len(l.Tags) != 1 || aws.ToString(l.Tags[0].Key) != "env" {
		t.Fatalf("tags: %v %+v", err, l)
	}

	// delete, then the typed not-found error
	if _, err := c.DeleteApplication(ctx, &kinesisanalyticsv2.DeleteApplicationInput{ApplicationName: aws.String("flink1"), CreateTimestamp: g.CreateTimestamp}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeApplication(ctx, &kinesisanalyticsv2.DescribeApplicationInput{ApplicationName: aws.String("flink1")})
	var nf *types.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("describe deleted: %v", err)
	}
}

// floci #4443: a snapshot is CREATING for a while, then READY.
func TestFlinkSnapshotStatus(t *testing.T) {
	s := pogi.Start(t)
	ctx := context.Background()
	c := kinesisanalyticsv2.NewFromConfig(awstest.Config(t, s.URL))
	lifecycle(t, time.Hour)
	if _, err := c.CreateApplication(ctx, &kinesisanalyticsv2.CreateApplicationInput{ApplicationName: aws.String("flink2"), RuntimeEnvironment: types.RuntimeEnvironmentFlink120,
		ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/flink")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateApplicationSnapshot(ctx, &kinesisanalyticsv2.CreateApplicationSnapshotInput{ApplicationName: aws.String("flink2"), SnapshotName: aws.String("snap1")}); err != nil {
		t.Fatal(err)
	}
	d, err := c.DescribeApplicationSnapshot(ctx, &kinesisanalyticsv2.DescribeApplicationSnapshotInput{ApplicationName: aws.String("flink2"), SnapshotName: aws.String("snap1")})
	if err != nil || d.SnapshotDetails.SnapshotStatus != types.SnapshotStatusCreating {
		t.Fatalf("new snapshot: %v %+v", err, d)
	}
	lifecycle(t, 0)
	d, err = c.DescribeApplicationSnapshot(ctx, &kinesisanalyticsv2.DescribeApplicationSnapshotInput{ApplicationName: aws.String("flink2"), SnapshotName: aws.String("snap1")})
	if err != nil || d.SnapshotDetails.SnapshotStatus != types.SnapshotStatusReady {
		t.Fatalf("settled snapshot: %v %+v", err, d)
	}
}

// floci #2404: StartApplication runs the JAR from S3 on a real Flink cluster that has AWS credentials and
// the endpoint of pogi and the runtime properties; stopping removes it. Needs Docker (and the Flink image).
func TestFlinkCluster(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a container engine")
	}
	ctx := context.Background()
	const image = "apache/flink:1.20.3"
	// the JAR is Flink's own streaming example, which never ends
	jarName := docker.Name("test", "flink-jar")
	if _, err := d.Create(ctx, &docker.Container{Name: jarName, Image: image, Service: "test", Resource: "flink-jar"}); err != nil {
		t.Fatal(err)
	}
	rc, err := d.CopyFrom(ctx, jarName, "/opt/flink/examples/streaming/TopSpeedWindowing.jar")
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(rc)
	var jar bytes.Buffer
	if _, err := tr.Next(); err != nil {
		t.Fatal(err)
	}
	jar.ReadFrom(tr)
	rc.Close()
	d.Remove(ctx, jarName)

	s := pogi.Start(t)
	cfg := awstest.Config(t, s.URL)
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	if _, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("code")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("code"), Key: aws.String("app.jar"), Body: bytes.NewReader(jar.Bytes())}); err != nil {
		t.Fatal(err)
	}
	c := kinesisanalyticsv2.NewFromConfig(cfg)
	if _, err := c.CreateApplication(ctx, &kinesisanalyticsv2.CreateApplicationInput{ApplicationName: aws.String("real"), RuntimeEnvironment: types.RuntimeEnvironmentFlink120,
		ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/flink"),
		ApplicationConfiguration: &types.ApplicationConfiguration{
			ApplicationCodeConfiguration: &types.ApplicationCodeConfiguration{CodeContentType: types.CodeContentTypeZipfile,
				CodeContent: &types.CodeContent{S3ContentLocation: &types.S3ContentLocation{BucketARN: aws.String("arn:aws:s3:::code"), FileKey: aws.String("app.jar")}}},
			EnvironmentProperties: &types.EnvironmentProperties{PropertyGroups: []types.PropertyGroup{{PropertyGroupId: aws.String("k"), PropertyMap: map[string]string{"a": "b"}}}}}}); err != nil {
		t.Fatal(err)
	}
	status := func() types.ApplicationStatus {
		g, err := c.DescribeApplication(ctx, &kinesisanalyticsv2.DescribeApplicationInput{ApplicationName: aws.String("real")})
		if err != nil {
			t.Fatal(err)
		}
		return g.ApplicationDetail.ApplicationStatus
	}
	wait := func(want types.ApplicationStatus) {
		t.Helper()
		for end := time.Now().Add(4 * time.Minute); status() != want; time.Sleep(time.Second) {
			if time.Now().After(end) {
				t.Fatalf("never became %s, is %s", want, status())
			}
		}
	}
	if _, err := c.StartApplication(ctx, &kinesisanalyticsv2.StartApplicationInput{ApplicationName: aws.String("real")}); err != nil {
		t.Fatal(err)
	}
	if st := status(); st != types.ApplicationStatusStarting {
		t.Fatalf("starting: %s", st)
	}
	wait(types.ApplicationStatusRunning)
	name := docker.Name("kinesisanalyticsv2", "real")
	run := func(cmd ...string) string {
		out, code, err := d.Exec(ctx, name, cmd...)
		if err != nil || code != 0 {
			t.Fatalf("%v: %v %d %s", cmd, err, code, out)
		}
		return string(out)
	}
	if out := run("printenv", "AWS_ENDPOINT_URL"); !strings.Contains(out, s.URL[strings.LastIndex(s.URL, ":"):]) || run("printenv", "AWS_ACCESS_KEY_ID") == "" {
		t.Fatalf("endpoint %q for %s", out, s.URL)
	}
	if out := run("cat", "/etc/flink/application_properties.json"); !strings.Contains(out, `"a":"b"`) {
		t.Fatalf("properties: %s", out)
	}
	if out := run("flink", "list", "-r"); !strings.Contains(out, "(RUNNING)") {
		t.Fatalf("no running job: %s", out)
	}

	if _, err := c.StopApplication(ctx, &kinesisanalyticsv2.StopApplicationInput{ApplicationName: aws.String("real")}); err != nil {
		t.Fatal(err)
	}
	wait(types.ApplicationStatusReady)
	if _, err := d.Inspect(ctx, name); err == nil {
		t.Fatal("the Flink container is still there after the stop")
	}
}
