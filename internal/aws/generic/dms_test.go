package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestDMS(t *testing.T) {
	s := pogi.Start(t)
	ctx := context.Background()
	c := databasemigrationservice.NewFromConfig(awstest.Config(t, s.URL))
	lifecycle(t, time.Hour)

	// subnet group
	sg, err := c.CreateReplicationSubnetGroup(ctx, &databasemigrationservice.CreateReplicationSubnetGroupInput{ReplicationSubnetGroupIdentifier: aws.String("sg1"),
		ReplicationSubnetGroupDescription: aws.String("subnets"), SubnetIds: []string{"subnet-a", "subnet-b"}})
	if err != nil || len(sg.ReplicationSubnetGroup.Subnets) != 2 || aws.ToString(sg.ReplicationSubnetGroup.SubnetGroupStatus) != "Complete" {
		t.Fatalf("subnet group: %v %+v", err, sg)
	}

	// replication instance: creating, then available
	ri, err := c.CreateReplicationInstance(ctx, &databasemigrationservice.CreateReplicationInstanceInput{ReplicationInstanceIdentifier: aws.String("ri1"), ReplicationInstanceClass: aws.String("dms.t3.micro"),
		ReplicationSubnetGroupIdentifier: aws.String("sg1"), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	riArn := aws.ToString(ri.ReplicationInstance.ReplicationInstanceArn)
	if !strings.Contains(riArn, ":rep:") || aws.ToString(ri.ReplicationInstance.ReplicationInstanceStatus) != "creating" || aws.ToString(ri.ReplicationInstance.ReplicationInstanceClass) != "dms.t3.micro" {
		t.Fatalf("create instance: %+v", ri.ReplicationInstance)
	}
	if _, err := c.CreateReplicationInstance(ctx, &databasemigrationservice.CreateReplicationInstanceInput{ReplicationInstanceIdentifier: aws.String("ri1"), ReplicationInstanceClass: aws.String("dms.t3.micro")}); err == nil {
		t.Fatal("duplicate instance")
	}
	descRI := func() *types.ReplicationInstance {
		d, err := c.DescribeReplicationInstances(ctx, &databasemigrationservice.DescribeReplicationInstancesInput{Filters: []types.Filter{{Name: aws.String("replication-instance-id"), Values: []string{"ri1"}}}})
		if err != nil || len(d.ReplicationInstances) != 1 {
			t.Fatalf("describe instance: %v %+v", err, d)
		}
		return &d.ReplicationInstances[0]
	}
	if st := aws.ToString(descRI().ReplicationInstanceStatus); st != "creating" {
		t.Fatalf("creating: %s", st)
	}
	lifecycle(t, 0)
	if st := aws.ToString(descRI().ReplicationInstanceStatus); st != "available" {
		t.Fatalf("available: %s", st)
	}
	if d, err := c.DescribeReplicationInstances(ctx, &databasemigrationservice.DescribeReplicationInstancesInput{Filters: []types.Filter{{Name: aws.String("replication-instance-id"), Values: []string{"nope"}}}}); err != nil || len(d.ReplicationInstances) != 0 {
		t.Fatalf("filter nothing: %v %+v", err, d)
	}
	lifecycle(t, time.Hour)
	if _, err := c.ModifyReplicationInstance(ctx, &databasemigrationservice.ModifyReplicationInstanceInput{ReplicationInstanceArn: aws.String(riArn), ReplicationInstanceClass: aws.String("dms.t3.large")}); err != nil {
		t.Fatal(err)
	}
	if got := descRI(); aws.ToString(got.ReplicationInstanceStatus) != "modifying" || aws.ToString(got.ReplicationInstanceClass) != "dms.t3.large" {
		t.Fatalf("modifying: %+v", got)
	}
	lifecycle(t, 0)

	// endpoints
	src, err := c.CreateEndpoint(ctx, &databasemigrationservice.CreateEndpointInput{EndpointIdentifier: aws.String("src"), EndpointType: types.ReplicationEndpointTypeValueSource, EngineName: aws.String("mysql"),
		ServerName: aws.String("db.example.com"), Port: aws.Int32(3306), Username: aws.String("u"), Password: aws.String("p"), DatabaseName: aws.String("app")})
	if err != nil || aws.ToString(src.Endpoint.Status) != "active" || !strings.Contains(aws.ToString(src.Endpoint.EndpointArn), ":endpoint:") || aws.ToString(src.Endpoint.ServerName) != "db.example.com" {
		t.Fatalf("source: %v %+v", err, src)
	}
	tgt, err := c.CreateEndpoint(ctx, &databasemigrationservice.CreateEndpointInput{EndpointIdentifier: aws.String("tgt"), EndpointType: types.ReplicationEndpointTypeValueTarget, EngineName: aws.String("s3"),
		S3Settings: &types.S3Settings{BucketName: aws.String("out"), ServiceAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/dms")}})
	if err != nil {
		t.Fatal(err)
	}
	if l, err := c.DescribeEndpoints(ctx, &databasemigrationservice.DescribeEndpointsInput{Filters: []types.Filter{{Name: aws.String("endpoint-type"), Values: []string{"source"}}}}); err != nil || len(l.Endpoints) != 1 {
		t.Fatalf("endpoints: %v %+v", err, l)
	}
	if _, err := c.ModifyEndpoint(ctx, &databasemigrationservice.ModifyEndpointInput{EndpointArn: src.Endpoint.EndpointArn, ServerName: aws.String("db2.example.com")}); err != nil {
		t.Fatal(err)
	}
	if l, _ := c.DescribeEndpoints(ctx, &databasemigrationservice.DescribeEndpointsInput{Filters: []types.Filter{{Name: aws.String("endpoint-arn"), Values: []string{aws.ToString(src.Endpoint.EndpointArn)}}}}); len(l.Endpoints) != 1 || aws.ToString(l.Endpoints[0].ServerName) != "db2.example.com" {
		t.Fatalf("modified endpoint: %+v", l)
	}

	// replication task: creating, ready, then running, stopped
	lifecycle(t, time.Hour)
	task, err := c.CreateReplicationTask(ctx, &databasemigrationservice.CreateReplicationTaskInput{ReplicationTaskIdentifier: aws.String("task1"), SourceEndpointArn: src.Endpoint.EndpointArn,
		TargetEndpointArn: tgt.Endpoint.EndpointArn, ReplicationInstanceArn: aws.String(riArn), MigrationType: types.MigrationTypeValueFullLoad,
		TableMappings: aws.String(`{"rules":[]}`)})
	if err != nil {
		t.Fatal(err)
	}
	tArn := aws.ToString(task.ReplicationTask.ReplicationTaskArn)
	if !strings.Contains(tArn, ":task:") {
		t.Fatalf("task arn %s", tArn)
	}
	if _, err := c.CreateReplicationTask(ctx, &databasemigrationservice.CreateReplicationTaskInput{ReplicationTaskIdentifier: aws.String("bad"), SourceEndpointArn: aws.String("arn:aws:dms:us-east-1:000000000000:endpoint:nope"),
		TargetEndpointArn: tgt.Endpoint.EndpointArn, ReplicationInstanceArn: aws.String(riArn), MigrationType: types.MigrationTypeValueFullLoad, TableMappings: aws.String("{}")}); err == nil {
		t.Fatal("task with a missing endpoint")
	}
	status := func() *types.ReplicationTask {
		d, err := c.DescribeReplicationTasks(ctx, &databasemigrationservice.DescribeReplicationTasksInput{Filters: []types.Filter{{Name: aws.String("replication-task-arn"), Values: []string{tArn}}}})
		if err != nil || len(d.ReplicationTasks) != 1 {
			t.Fatalf("describe task: %v %+v", err, d)
		}
		return &d.ReplicationTasks[0]
	}
	if st := aws.ToString(status().Status); st != "creating" {
		t.Fatalf("creating: %s", st)
	}
	lifecycle(t, 0)
	if got := status(); aws.ToString(got.Status) != "ready" || got.MigrationType != types.MigrationTypeValueFullLoad {
		t.Fatalf("ready: %+v", got)
	}
	lifecycle(t, time.Hour)
	if _, err := c.StartReplicationTask(ctx, &databasemigrationservice.StartReplicationTaskInput{ReplicationTaskArn: aws.String(tArn), StartReplicationTaskType: types.StartReplicationTaskTypeValueStartReplication}); err != nil {
		t.Fatal(err)
	}
	if st := aws.ToString(status().Status); st != "starting" {
		t.Fatalf("starting: %s", st)
	}
	if _, err := c.DeleteReplicationTask(ctx, &databasemigrationservice.DeleteReplicationTaskInput{ReplicationTaskArn: aws.String(tArn)}); err == nil {
		t.Fatal("deleted a starting task")
	}
	if _, err := c.DeleteReplicationInstance(ctx, &databasemigrationservice.DeleteReplicationInstanceInput{ReplicationInstanceArn: aws.String(riArn)}); err == nil {
		t.Fatal("deleted an instance a task uses")
	}
	lifecycle(t, 0) // a full-load task runs and stops by itself
	if got := status(); aws.ToString(got.Status) != "stopped" || got.ReplicationTaskStats == nil || got.ReplicationTaskStats.FullLoadProgressPercent != 100 {
		t.Fatalf("stopped: %+v", got)
	}
	if _, err := c.StopReplicationTask(ctx, &databasemigrationservice.StopReplicationTaskInput{ReplicationTaskArn: aws.String(tArn)}); err == nil {
		t.Fatal("stopped a stopped task")
	}

	// tags
	if _, err := c.AddTagsToResource(ctx, &databasemigrationservice.AddTagsToResourceInput{ResourceArn: aws.String(riArn), Tags: []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RemoveTagsFromResource(ctx, &databasemigrationservice.RemoveTagsFromResourceInput{ResourceArn: aws.String(riArn), TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if l, err := c.ListTagsForResource(ctx, &databasemigrationservice.ListTagsForResourceInput{ResourceArn: aws.String(riArn)}); err != nil || len(l.TagList) != 1 || aws.ToString(l.TagList[0].Key) != "a" {
		t.Fatalf("tags: %v %+v", err, l)
	}

	// deletes and the typed not-found error
	if _, err := c.DeleteReplicationTask(ctx, &databasemigrationservice.DeleteReplicationTaskInput{ReplicationTaskArn: aws.String(tArn)}); err != nil {
		t.Fatal(err)
	}
	for _, e := range []*string{src.Endpoint.EndpointArn, tgt.Endpoint.EndpointArn} {
		if _, err := c.DeleteEndpoint(ctx, &databasemigrationservice.DeleteEndpointInput{EndpointArn: e}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.DeleteReplicationInstance(ctx, &databasemigrationservice.DeleteReplicationInstanceInput{ReplicationInstanceArn: aws.String(riArn)}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DeleteReplicationInstance(ctx, &databasemigrationservice.DeleteReplicationInstanceInput{ReplicationInstanceArn: aws.String(riArn)})
	var nf *types.ResourceNotFoundFault
	if !errors.As(err, &nf) {
		t.Fatalf("delete twice: %v", err)
	}
	if _, err := c.DeleteReplicationSubnetGroup(ctx, &databasemigrationservice.DeleteReplicationSubnetGroupInput{ReplicationSubnetGroupIdentifier: aws.String("sg1")}); err != nil {
		t.Fatal(err)
	}
}
