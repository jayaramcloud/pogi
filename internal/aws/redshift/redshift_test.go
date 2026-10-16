package redshift_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/aws/smithy-go"
	"github.com/nrndev/pogi/internal/aws/rds"
	redshiftsvc "github.com/nrndev/pogi/internal/aws/redshift"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// fails checks that err is the API error code.
func fails(t *testing.T, err error, code string) {
	t.Helper()
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func TestClustersWithoutDocker(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	var svc *redshiftsvc.Service
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = redshiftsvc.New(h); return svc })
	c, ctx := redshift.NewFromConfig(awstest.Config(t, url)), context.Background()

	must(c.CreateClusterParameterGroup(ctx, &redshift.CreateClusterParameterGroupInput{ParameterGroupName: aws.String("Strict"),
		ParameterGroupFamily: aws.String("redshift-1.0"), Description: aws.String("strict"), Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("data")}}}))
	must(c.ModifyClusterParameterGroup(ctx, &redshift.ModifyClusterParameterGroupInput{ParameterGroupName: aws.String("strict"),
		Parameters: []types.Parameter{{ParameterName: aws.String("require_ssl"), ParameterValue: aws.String("true")},
			{ParameterName: aws.String("query_group"), ParameterValue: aws.String("etl")}}}))
	must(c.CreateClusterSubnetGroup(ctx, &redshift.CreateClusterSubnetGroupInput{ClusterSubnetGroupName: aws.String("private"),
		Description: aws.String("private"), SubnetIds: []string{"subnet-1", "subnet-2"}}))
	must(c.CreateClusterSecurityGroup(ctx, &redshift.CreateClusterSecurityGroupInput{ClusterSecurityGroupName: aws.String("legacy"), Description: aws.String("legacy")}))

	create := &redshift.CreateClusterInput{ClusterIdentifier: aws.String("Analytics"), NodeType: aws.String("ra3.xlplus"), NumberOfNodes: aws.Int32(2),
		MasterUsername: aws.String("awsuser"), MasterUserPassword: aws.String("Secret123"), ClusterParameterGroupName: aws.String("strict"),
		ClusterSubnetGroupName: aws.String("private"), ClusterSecurityGroups: []string{"legacy"}, VpcSecurityGroupIds: []string{"sg-1"},
		IamRoles: []string{"arn:aws:iam::000000000000:role/copy"}, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}
	cl := must(c.CreateCluster(ctx, create)).Cluster
	if *cl.ClusterIdentifier != "analytics" || *cl.ClusterStatus != "available" || *cl.ClusterAvailabilityStatus != "Available" || *cl.Endpoint.Address != "localhost" || *cl.Endpoint.Port != 5439 ||
		*cl.DBName != "dev" || *cl.NumberOfNodes != 2 || len(cl.ClusterNodes) != 3 || *cl.ClusterNodes[0].NodeRole != "LEADER" ||
		*cl.ClusterParameterGroups[0].ParameterGroupName != "strict" || *cl.ClusterSubnetGroupName != "private" ||
		*cl.ClusterSecurityGroups[0].ClusterSecurityGroupName != "legacy" || *cl.VpcSecurityGroups[0].VpcSecurityGroupId != "sg-1" ||
		*cl.IamRoles[0].ApplyStatus != "in-sync" || *cl.Tags[0].Key != "env" || *cl.ClusterVersion != "1.0" || *cl.ManualSnapshotRetentionPeriod != -1 ||
		*cl.AvailabilityZoneRelocationStatus != "disabled" || !strings.HasPrefix(*cl.ClusterNamespaceArn, "arn:aws:redshift:us-east-1:000000000000:namespace:") {
		t.Fatalf("created %+v", cl)
	}
	if v := svc.Inspect("Clusters", "analytics").(*redshiftsvc.Cluster); v.Pogi.Note != rds.NoDocker || v.Pogi.Password != "Secret123" {
		t.Fatalf("inspect %+v", v.Pogi)
	}
	if db := svc.DB("000000000000", "us-east-1", "Analytics"); db == nil || db.Addr != "" || db.Note != rds.NoDocker || db.User != "awsuser" {
		t.Fatalf("db %+v", db)
	}

	_, err := c.CreateCluster(ctx, create)
	fails(t, err, "ClusterAlreadyExists")
	_, err = c.DescribeClusters(ctx, &redshift.DescribeClustersInput{ClusterIdentifier: aws.String("nope")})
	fails(t, err, "ClusterNotFound")
	bad := *create
	bad.ClusterIdentifier, bad.MasterUserPassword = aws.String("other"), aws.String("weak")
	_, err = c.CreateCluster(ctx, &bad)
	fails(t, err, "InvalidParameterValue")
	bad.MasterUserPassword, bad.ClusterIdentifier = aws.String("Secret123"), aws.String("bad--name")
	_, err = c.CreateCluster(ctx, &bad)
	fails(t, err, "InvalidParameterValue")
	bad.ClusterIdentifier, bad.ClusterParameterGroupName = aws.String("other"), aws.String("missing")
	_, err = c.CreateCluster(ctx, &bad)
	fails(t, err, "ClusterParameterGroupNotFound")
	bad.ClusterParameterGroupName, bad.ClusterType = nil, aws.String("multi-node")
	bad.NumberOfNodes = aws.Int32(1)
	_, err = c.CreateCluster(ctx, &bad)
	fails(t, err, "InvalidParameterCombination")

	// the other cluster takes the defaults
	single := must(c.CreateCluster(ctx, &redshift.CreateClusterInput{ClusterIdentifier: aws.String("single"), NodeType: aws.String("dc2.large"),
		MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("Secret123"), DBName: aws.String("sales")})).Cluster
	if len(single.ClusterNodes) != 1 || *single.ClusterNodes[0].NodeRole != "SHARED" || *single.ClusterParameterGroups[0].ParameterGroupName != "default.redshift-1.0" {
		t.Fatalf("single %+v", single)
	}
	list := must(c.DescribeClusters(ctx, &redshift.DescribeClustersInput{})).Clusters
	if len(list) != 2 || *list[0].ClusterIdentifier != "analytics" || *list[1].ClusterIdentifier != "single" {
		t.Fatalf("clusters %+v", list)
	}
	var in struct {
		Clusters []struct{ ClusterIdentifier, ClusterStatus string }
	} // pogi's own in-process calls speak JSON
	if err := hub.Call("000000000000", "us-east-1", "RedshiftServiceVersion20121201", "DescribeClusters", map[string]string{"ClusterIdentifier": "single"}, &in); err != nil ||
		len(in.Clusters) != 1 || in.Clusters[0].ClusterStatus != "available" {
		t.Fatalf("in-process %+v %v", in, err)
	}

	mod := must(c.ModifyCluster(ctx, &redshift.ModifyClusterInput{ClusterIdentifier: aws.String("analytics"), NodeType: aws.String("ra3.4xlarge"),
		MasterUserPassword: aws.String("Changed123"), PreferredMaintenanceWindow: aws.String("Mon:01:00-Mon:01:30")})).Cluster
	if *mod.NodeType != "ra3.4xlarge" || *mod.PreferredMaintenanceWindow != "mon:01:00-mon:01:30" {
		t.Fatalf("modify %+v", mod)
	}
	if v := svc.Inspect("Clusters", "analytics").(*redshiftsvc.Cluster); v.Pogi.Password != "Changed123" {
		t.Fatalf("password %q", v.Pogi.Password)
	}
	if r := must(c.ResizeCluster(ctx, &redshift.ResizeClusterInput{ClusterIdentifier: aws.String("analytics"), NumberOfNodes: aws.Int32(4)})).Cluster; *r.NumberOfNodes != 4 || len(r.ClusterNodes) != 5 {
		t.Fatalf("resize %+v", r)
	}
	if p := must(c.PauseCluster(ctx, &redshift.PauseClusterInput{ClusterIdentifier: aws.String("analytics")})).Cluster; *p.ClusterStatus != "paused" || *p.ClusterAvailabilityStatus != "Unavailable" {
		t.Fatalf("pause %+v", p)
	}
	_, err = c.PauseCluster(ctx, &redshift.PauseClusterInput{ClusterIdentifier: aws.String("analytics")})
	fails(t, err, "InvalidClusterState")
	_, err = c.RebootCluster(ctx, &redshift.RebootClusterInput{ClusterIdentifier: aws.String("analytics")})
	fails(t, err, "InvalidClusterState")
	if r := must(c.ResumeCluster(ctx, &redshift.ResumeClusterInput{ClusterIdentifier: aws.String("analytics")})).Cluster; *r.ClusterStatus != "available" {
		t.Fatalf("resume %+v", r)
	}
	_, err = c.ResumeCluster(ctx, &redshift.ResumeClusterInput{ClusterIdentifier: aws.String("analytics")})
	fails(t, err, "InvalidClusterState")
	must(c.RebootCluster(ctx, &redshift.RebootClusterInput{ClusterIdentifier: aws.String("analytics")}))

	// temporary credentials, without a database to create them in
	cr := must(c.GetClusterCredentials(ctx, &redshift.GetClusterCredentialsInput{ClusterIdentifier: aws.String("analytics"), DbUser: aws.String("Alice")}))
	if *cr.DbUser != "IAM:alice" || len(*cr.DbPassword) != 40 || cr.Expiration.IsZero() {
		t.Fatalf("credentials %+v", cr)
	}
	if cr := must(c.GetClusterCredentials(ctx, &redshift.GetClusterCredentialsInput{ClusterIdentifier: aws.String("analytics"),
		DbUser: aws.String("bob"), AutoCreate: aws.Bool(true)})); *cr.DbUser != "IAMA:bob" {
		t.Fatalf("auto-create credentials %+v", cr)
	}
	if ci := must(c.GetClusterCredentialsWithIAM(ctx, &redshift.GetClusterCredentialsWithIAMInput{ClusterIdentifier: aws.String("analytics")})); *ci.DbUser != "IAM:root" || ci.NextRefreshTime == nil {
		t.Fatalf("IAM credentials %+v", ci)
	}
	_, err = c.GetClusterCredentials(ctx, &redshift.GetClusterCredentialsInput{ClusterIdentifier: aws.String("analytics"), DbUser: aws.String("a:b")})
	fails(t, err, "InvalidParameterValue")
	_, err = c.GetClusterCredentials(ctx, &redshift.GetClusterCredentialsInput{ClusterIdentifier: aws.String("analytics"), DbUser: aws.String("a"), DurationSeconds: aws.Int32(60)})
	fails(t, err, "InvalidParameterValue")
	_, err = c.GetClusterCredentials(ctx, &redshift.GetClusterCredentialsInput{ClusterIdentifier: aws.String("nope"), DbUser: aws.String("a")})
	fails(t, err, "ClusterNotFound")

	// logging
	if l := must(c.DescribeLoggingStatus(ctx, &redshift.DescribeLoggingStatusInput{ClusterIdentifier: aws.String("analytics")})); *l.LoggingEnabled {
		t.Fatalf("logging %+v", l)
	}
	_, err = c.EnableLogging(ctx, &redshift.EnableLoggingInput{ClusterIdentifier: aws.String("analytics")})
	fails(t, err, "InvalidParameterValue")
	must(c.EnableLogging(ctx, &redshift.EnableLoggingInput{ClusterIdentifier: aws.String("analytics"), LogDestinationType: types.LogDestinationTypeCloudwatch,
		LogExports: []string{"connectionlog", "userlog"}}))
	if l := must(c.DescribeLoggingStatus(ctx, &redshift.DescribeLoggingStatusInput{ClusterIdentifier: aws.String("analytics")})); !*l.LoggingEnabled ||
		l.LogDestinationType != types.LogDestinationTypeCloudwatch || !slices.Equal(l.LogExports, []string{"connectionlog", "userlog"}) {
		t.Fatalf("logging %+v", l)
	}
	if l := must(c.DisableLogging(ctx, &redshift.DisableLoggingInput{ClusterIdentifier: aws.String("analytics")})); *l.LoggingEnabled {
		t.Fatalf("disabled logging %+v", l)
	}

	// parameter groups
	ps := must(c.DescribeClusterParameters(ctx, &redshift.DescribeClusterParametersInput{ParameterGroupName: aws.String("strict"), Source: aws.String("user")})).Parameters
	if len(ps) != 2 || *ps[0].ParameterName != "query_group" || *ps[1].ParameterValue != "true" {
		t.Fatalf("parameters %+v", ps)
	}
	must(c.ResetClusterParameterGroup(ctx, &redshift.ResetClusterParameterGroupInput{ParameterGroupName: aws.String("strict"),
		Parameters: []types.Parameter{{ParameterName: aws.String("query_group")}}}))
	if ps := must(c.DescribeClusterParameters(ctx, &redshift.DescribeClusterParametersInput{ParameterGroupName: aws.String("strict")})).Parameters; len(ps) != 1 {
		t.Fatalf("after reset %+v", ps)
	}
	pgs := must(c.DescribeClusterParameterGroups(ctx, &redshift.DescribeClusterParameterGroupsInput{})).ParameterGroups
	if len(pgs) != 2 || *pgs[0].ParameterGroupName != "default.redshift-1.0" || *pgs[1].Tags[0].Value != "data" {
		t.Fatalf("parameter groups %+v", pgs)
	}
	_, err = c.ModifyClusterParameterGroup(ctx, &redshift.ModifyClusterParameterGroupInput{ParameterGroupName: aws.String("default.redshift-1.0"),
		Parameters: []types.Parameter{{ParameterName: aws.String("x"), ParameterValue: aws.String("y")}}})
	fails(t, err, "InvalidClusterParameterGroupState")
	_, err = c.DeleteClusterParameterGroup(ctx, &redshift.DeleteClusterParameterGroupInput{ParameterGroupName: aws.String("strict")})
	fails(t, err, "InvalidClusterParameterGroupState")
	_, err = c.CreateClusterParameterGroup(ctx, &redshift.CreateClusterParameterGroupInput{ParameterGroupName: aws.String("strict"),
		ParameterGroupFamily: aws.String("redshift-1.0"), Description: aws.String("again")})
	fails(t, err, "ClusterParameterGroupAlreadyExists")

	// subnet and security groups
	sgs := must(c.DescribeClusterSubnetGroups(ctx, &redshift.DescribeClusterSubnetGroupsInput{})).ClusterSubnetGroups
	if len(sgs) != 1 || len(sgs[0].Subnets) != 2 || *sgs[0].Subnets[1].SubnetAvailabilityZone.Name != "us-east-1b" {
		t.Fatalf("subnet groups %+v", sgs)
	}
	must(c.ModifyClusterSubnetGroup(ctx, &redshift.ModifyClusterSubnetGroupInput{ClusterSubnetGroupName: aws.String("private"), SubnetIds: []string{"subnet-3"}}))
	_, err = c.DeleteClusterSubnetGroup(ctx, &redshift.DeleteClusterSubnetGroupInput{ClusterSubnetGroupName: aws.String("private")})
	fails(t, err, "InvalidClusterSubnetGroupStateFault")
	_, err = c.DescribeClusterSubnetGroups(ctx, &redshift.DescribeClusterSubnetGroupsInput{ClusterSubnetGroupName: aws.String("nope")})
	fails(t, err, "ClusterSubnetGroupNotFoundFault")
	if g := must(c.DescribeClusterSecurityGroups(ctx, &redshift.DescribeClusterSecurityGroupsInput{})).ClusterSecurityGroups; len(g) != 1 || *g[0].Description != "legacy" {
		t.Fatalf("security groups %+v", g)
	}
	_, err = c.DeleteClusterSecurityGroup(ctx, &redshift.DeleteClusterSecurityGroupInput{ClusterSecurityGroupName: aws.String("legacy")})
	fails(t, err, "InvalidClusterSecurityGroupState")

	// tags
	arn := "arn:aws:redshift:us-east-1:000000000000:cluster:analytics"
	must(c.CreateTags(ctx, &redshift.CreateTagsInput{ResourceName: aws.String(arn), Tags: []types.Tag{{Key: aws.String("owner"), Value: aws.String("me")}}}))
	must(c.DeleteTags(ctx, &redshift.DeleteTagsInput{ResourceName: aws.String(arn), TagKeys: []string{"env"}}))
	tags := must(c.DescribeTags(ctx, &redshift.DescribeTagsInput{ResourceType: aws.String("cluster")})).TaggedResources
	if len(tags) != 1 || *tags[0].ResourceName != arn || *tags[0].Tag.Key != "owner" || *tags[0].ResourceType != "Cluster" {
		t.Fatalf("tags %+v", tags)
	}
	if all := must(c.DescribeTags(ctx, &redshift.DescribeTagsInput{})).TaggedResources; len(all) != 2 {
		t.Fatalf("all tags %+v", all)
	}
	_, err = c.CreateTags(ctx, &redshift.CreateTagsInput{ResourceName: aws.String(arn + "x"), Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}})
	fails(t, err, "ResourceNotFoundFault")

	// snapshots, and deleting
	sn := must(c.CreateClusterSnapshot(ctx, &redshift.CreateClusterSnapshotInput{ClusterIdentifier: aws.String("analytics"), SnapshotIdentifier: aws.String("before")})).Snapshot
	if *sn.Status != "available" || *sn.SnapshotType != "manual" || *sn.SnapshotArn != "arn:aws:redshift:us-east-1:000000000000:snapshot:analytics/before" {
		t.Fatalf("snapshot %+v", sn)
	}
	_, err = c.CreateClusterSnapshot(ctx, &redshift.CreateClusterSnapshotInput{ClusterIdentifier: aws.String("analytics"), SnapshotIdentifier: aws.String("before")})
	fails(t, err, "ClusterSnapshotAlreadyExists")
	_, err = c.DeleteCluster(ctx, &redshift.DeleteClusterInput{ClusterIdentifier: aws.String("analytics")})
	fails(t, err, "InvalidParameterCombination")
	del := must(c.DeleteCluster(ctx, &redshift.DeleteClusterInput{ClusterIdentifier: aws.String("analytics"), FinalClusterSnapshotIdentifier: aws.String("final")})).Cluster
	if *del.ClusterStatus != "deleting" {
		t.Fatalf("delete %+v", del)
	}
	_, err = c.DescribeClusters(ctx, &redshift.DescribeClustersInput{ClusterIdentifier: aws.String("analytics")})
	fails(t, err, "ClusterNotFound")
	snaps := must(c.DescribeClusterSnapshots(ctx, &redshift.DescribeClusterSnapshotsInput{ClusterIdentifier: aws.String("analytics")})).Snapshots
	if len(snaps) != 2 || *snaps[1].SnapshotIdentifier != "final" {
		t.Fatalf("snapshots %+v", snaps)
	}
	must(c.DeleteClusterSnapshot(ctx, &redshift.DeleteClusterSnapshotInput{SnapshotIdentifier: aws.String("before")}))
	_, err = c.DeleteClusterSnapshot(ctx, &redshift.DeleteClusterSnapshotInput{SnapshotIdentifier: aws.String("before")})
	fails(t, err, "ClusterSnapshotNotFound")
	must(c.DeleteClusterParameterGroup(ctx, &redshift.DeleteClusterParameterGroupInput{ParameterGroupName: aws.String("strict")}))
	must(c.DeleteClusterSubnetGroup(ctx, &redshift.DeleteClusterSubnetGroupInput{ClusterSubnetGroupName: aws.String("private")}))
	must(c.DeleteClusterSecurityGroup(ctx, &redshift.DeleteClusterSecurityGroupInput{ClusterSecurityGroupName: aws.String("legacy")}))

	// state survives a save and load
	b := must(svc.Save())
	again := redshiftsvc.New(core.NewHub(core.Config{}, nil))
	if err := again.Load(b); err != nil {
		t.Fatal(err)
	}
	if v, _ := again.Inspect("Clusters", "single").(*redshiftsvc.Cluster); v == nil || v.ClusterStatus != "available" || v.DBName != "sales" {
		t.Fatalf("loaded %+v", v)
	}
}

// floci #5245 and #5358: snapshot copy, copy grants, snapshot schedules, and a made-up Marker refused.
func TestSnapshotCopyAndSchedules(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return redshiftsvc.New(h) })
	c, ctx := redshift.NewFromConfig(awstest.Config(t, url)), context.Background()
	must(c.CreateCluster(ctx, &redshift.CreateClusterInput{ClusterIdentifier: aws.String("c1"), NodeType: aws.String("dc2.large"),
		MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("Secret123")}))
	id := aws.String("c1")

	_, err := c.DisableSnapshotCopy(ctx, &redshift.DisableSnapshotCopyInput{ClusterIdentifier: id})
	fails(t, err, "SnapshotCopyAlreadyDisabledFault")
	cl := must(c.EnableSnapshotCopy(ctx, &redshift.EnableSnapshotCopyInput{ClusterIdentifier: id, DestinationRegion: aws.String("us-west-2")})).Cluster
	if st := cl.ClusterSnapshotCopyStatus; st == nil || *st.DestinationRegion != "us-west-2" || *st.RetentionPeriod != 7 {
		t.Fatalf("copy status = %+v", st)
	}
	_, err = c.EnableSnapshotCopy(ctx, &redshift.EnableSnapshotCopyInput{ClusterIdentifier: id, DestinationRegion: aws.String("eu-west-1")})
	fails(t, err, "SnapshotCopyAlreadyEnabledFault")
	cl = must(c.ModifySnapshotCopyRetentionPeriod(ctx, &redshift.ModifySnapshotCopyRetentionPeriodInput{ClusterIdentifier: id, RetentionPeriod: aws.Int32(10)})).Cluster
	if *cl.ClusterSnapshotCopyStatus.RetentionPeriod != 10 {
		t.Fatalf("retention = %+v", cl.ClusterSnapshotCopyStatus)
	}
	d := must(c.DescribeClusters(ctx, &redshift.DescribeClustersInput{ClusterIdentifier: id})).Clusters[0]
	if d.ClusterSnapshotCopyStatus == nil {
		t.Fatal("DescribeClusters lacks the copy status")
	}
	must(c.DisableSnapshotCopy(ctx, &redshift.DisableSnapshotCopyInput{ClusterIdentifier: id}))

	sched := aws.String("s1")
	cr := must(c.CreateSnapshotSchedule(ctx, &redshift.CreateSnapshotScheduleInput{ScheduleIdentifier: sched, ScheduleDefinitions: []string{"rate(12 hours)"}}))
	if aws.ToString(cr.ScheduleIdentifier) != "s1" || len(cr.ScheduleDefinitions) != 1 {
		t.Fatalf("schedule = %+v", cr)
	}
	_, err = c.CreateSnapshotSchedule(ctx, &redshift.CreateSnapshotScheduleInput{ScheduleIdentifier: sched, ScheduleDefinitions: []string{"rate(1 hour)"}})
	fails(t, err, "SnapshotScheduleAlreadyExistsFault")
	_, err = c.CreateSnapshotSchedule(ctx, &redshift.CreateSnapshotScheduleInput{ScheduleIdentifier: aws.String("bad"), ScheduleDefinitions: []string{"every day"}})
	fails(t, err, "InvalidScheduleFault")
	must(c.ModifyClusterSnapshotSchedule(ctx, &redshift.ModifyClusterSnapshotScheduleInput{ClusterIdentifier: id, ScheduleIdentifier: sched}))
	l := must(c.DescribeSnapshotSchedules(ctx, &redshift.DescribeSnapshotSchedulesInput{ClusterIdentifier: id})).SnapshotSchedules
	if len(l) != 1 || *l[0].AssociatedClusterCount != 1 || *l[0].AssociatedClusters[0].ClusterIdentifier != "c1" {
		t.Fatalf("schedules of the cluster = %+v", l)
	}
	_, err = c.DeleteSnapshotSchedule(ctx, &redshift.DeleteSnapshotScheduleInput{ScheduleIdentifier: sched})
	fails(t, err, "SnapshotScheduleActiveFault")
	must(c.ModifySnapshotSchedule(ctx, &redshift.ModifySnapshotScheduleInput{ScheduleIdentifier: sched, ScheduleDefinitions: []string{"rate(6 hours)"}}))
	must(c.ModifyClusterSnapshotSchedule(ctx, &redshift.ModifyClusterSnapshotScheduleInput{ClusterIdentifier: id, DisassociateSchedule: aws.Bool(true)}))
	must(c.DeleteSnapshotSchedule(ctx, &redshift.DeleteSnapshotScheduleInput{ScheduleIdentifier: sched}))
	_, err = c.DescribeSnapshotSchedules(ctx, &redshift.DescribeSnapshotSchedulesInput{ScheduleIdentifier: sched})
	fails(t, err, "SnapshotScheduleNotFoundFault")

	must(c.CreateSnapshotCopyGrant(ctx, &redshift.CreateSnapshotCopyGrantInput{SnapshotCopyGrantName: aws.String("g1"), KmsKeyId: aws.String("alias/k")}))
	if g := must(c.DescribeSnapshotCopyGrants(ctx, &redshift.DescribeSnapshotCopyGrantsInput{})).SnapshotCopyGrants; len(g) != 1 || *g[0].KmsKeyId != "alias/k" {
		t.Fatalf("grants = %+v", g)
	}
	must(c.DeleteSnapshotCopyGrant(ctx, &redshift.DeleteSnapshotCopyGrantInput{SnapshotCopyGrantName: aws.String("g1")}))

	// A Marker the service did not make is refused, and the one it makes works.
	_, err = c.DescribeSnapshotCopyGrants(ctx, &redshift.DescribeSnapshotCopyGrantsInput{Marker: aws.String("!!bad!!")})
	fails(t, err, "InvalidParameterValue")
	if !strings.Contains(err.Error(), "Marker") || strings.Contains(err.Error(), "nextToken") {
		t.Fatalf("message = %v", err)
	}
	must(c.CreateCluster(ctx, &redshift.CreateClusterInput{ClusterIdentifier: aws.String("c2"), NodeType: aws.String("dc2.large"),
		MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("Secret123")}))
	n := 0
	for pages := redshift.NewDescribeClustersPaginator(c, &redshift.DescribeClustersInput{MaxRecords: aws.Int32(1)}); pages.HasMorePages(); {
		n += len(must(pages.NextPage(ctx)).Clusters)
	}
	if n != 2 {
		t.Fatalf("clusters over pages = %d", n)
	}
}
