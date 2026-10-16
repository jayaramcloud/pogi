package rds_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"

	"github.com/nrndev/pogi/internal/awstest"
)

// TestSnapshotAPI runs the snapshot and restore calls on the fake engine:
// dumps happen in the containers, restores copy them into fresh ones.
func TestSnapshotAPI(t *testing.T) {
	f, _, base := setup(t, false)
	c := awsrds.NewFromConfig(awstest.Config(t, base))
	create(t, base, "src")
	await(t, base, "src", "available")
	out, err := c.CreateDBSnapshot(ctx, &awsrds.CreateDBSnapshotInput{DBInstanceIdentifier: aws.String("src"), DBSnapshotIdentifier: aws.String("snap")})
	if err != nil || aws.ToString(out.DBSnapshot.Status) != "creating" {
		t.Fatalf("create snapshot: %v %+v", err, out)
	}
	eventually(t, "snapshot available", func() bool {
		d, err := c.DescribeDBSnapshots(ctx, &awsrds.DescribeDBSnapshotsInput{DBSnapshotIdentifier: aws.String("snap")})
		return err == nil && aws.ToString(d.DBSnapshots[0].Status) == "available"
	})
	if !strings.Contains(strings.Join(f.Called("exec pogi-rds-src env MYSQL_PWD=secret123 sh -c"), ""), "pg_dumpall") {
		t.Fatalf("no dump in %q", f.Called("exec"))
	}
	r, err := c.RestoreDBInstanceFromDBSnapshot(ctx, &awsrds.RestoreDBInstanceFromDBSnapshotInput{DBInstanceIdentifier: aws.String("copy"),
		DBSnapshotIdentifier: aws.String("snap")})
	if err != nil || aws.ToString(r.DBInstance.Engine) != "postgres" || aws.ToString(r.DBInstance.MasterUsername) != "admin" {
		t.Fatalf("restore: %v %+v", err, r)
	}
	p, err := c.RestoreDBInstanceToPointInTime(ctx, &awsrds.RestoreDBInstanceToPointInTimeInput{SourceDBInstanceIdentifier: aws.String("src"),
		TargetDBInstanceIdentifier: aws.String("pitr"), UseLatestRestorableTime: aws.Bool(true)})
	if err != nil || aws.ToString(p.DBInstance.DBInstanceIdentifier) != "pitr" {
		t.Fatalf("point in time: %v %+v", err, p)
	}
	for _, id := range []string{"copy", "pitr"} {
		await(t, base, id, "available")
		if len(f.Called("archive pogi-rds-"+id)) != 1 || !strings.Contains(f.Called("create --name pogi-rds-" + id)[0], "POSTGRES_PASSWORD=secret123") {
			t.Fatalf("%s: the dump goes in before the start, with the source's password: %q", id, f.Called(""))
		}
	}
	if _, err := c.RestoreDBInstanceFromDBSnapshot(ctx, &awsrds.RestoreDBInstanceFromDBSnapshotInput{DBInstanceIdentifier: aws.String("x"),
		DBSnapshotIdentifier: aws.String("nope")}); err == nil || !strings.Contains(err.Error(), "DBSnapshotNotFound") {
		t.Fatalf("missing snapshot: %v", err)
	}

	// clusters
	if _, err := c.CreateDBCluster(ctx, &awsrds.CreateDBClusterInput{DBClusterIdentifier: aws.String("cl"), Engine: aws.String("aurora-postgresql"),
		MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("secret123")}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "cluster available", func() bool {
		d, err := c.DescribeDBClusters(ctx, &awsrds.DescribeDBClustersInput{DBClusterIdentifier: aws.String("cl")})
		return err == nil && aws.ToString(d.DBClusters[0].Status) == "available"
	})
	cs, err := c.CreateDBClusterSnapshot(ctx, &awsrds.CreateDBClusterSnapshotInput{DBClusterIdentifier: aws.String("cl"),
		DBClusterSnapshotIdentifier: aws.String("cs"), Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}})
	if err != nil || aws.ToString(cs.DBClusterSnapshot.Engine) != "aurora-postgresql" {
		t.Fatalf("cluster snapshot: %v %+v", err, cs)
	}
	var list []types.DBClusterSnapshot
	eventually(t, "cluster snapshot available", func() bool {
		d, err := c.DescribeDBClusterSnapshots(ctx, &awsrds.DescribeDBClusterSnapshotsInput{DBClusterIdentifier: aws.String("cl")})
		list = d.DBClusterSnapshots
		return err == nil && len(list) == 1 && aws.ToString(list[0].Status) == "available"
	})
	tags, err := c.ListTagsForResource(ctx, &awsrds.ListTagsForResourceInput{ResourceName: list[0].DBClusterSnapshotArn})
	if err != nil || len(tags.TagList) != 1 {
		t.Fatalf("tags %v %+v", err, tags)
	}
	rc, err := c.RestoreDBClusterFromSnapshot(ctx, &awsrds.RestoreDBClusterFromSnapshotInput{DBClusterIdentifier: aws.String("cl2"),
		SnapshotIdentifier: aws.String("cs"), Engine: aws.String("aurora-postgresql")})
	if err != nil || aws.ToString(rc.DBCluster.MasterUsername) != "admin" {
		t.Fatalf("restore cluster: %v %+v", err, rc)
	}
	he, err := c.EnableHttpEndpoint(ctx, &awsrds.EnableHttpEndpointInput{ResourceArn: rc.DBCluster.DBClusterArn})
	if err != nil || !aws.ToBool(he.HttpEndpointEnabled) {
		t.Fatalf("enable http endpoint: %v %+v", err, he)
	}
	d, _ := c.DescribeDBClusters(ctx, &awsrds.DescribeDBClustersInput{DBClusterIdentifier: aws.String("cl2")})
	if !aws.ToBool(d.DBClusters[0].HttpEndpointEnabled) {
		t.Fatalf("http endpoint %+v", d.DBClusters[0])
	}
	if _, err := c.DeleteDBCluster(ctx, &awsrds.DeleteDBClusterInput{DBClusterIdentifier: aws.String("cl"), FinalDBSnapshotIdentifier: aws.String("last")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeDBClusterSnapshots(ctx, &awsrds.DescribeDBClusterSnapshotsInput{DBClusterSnapshotIdentifier: aws.String("last")}); err != nil {
		t.Fatalf("final cluster snapshot: %v", err)
	}
	if _, err := c.DeleteDBClusterSnapshot(ctx, &awsrds.DeleteDBClusterSnapshotInput{DBClusterSnapshotIdentifier: aws.String("cs")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteDBClusterSnapshot(ctx, &awsrds.DeleteDBClusterSnapshotInput{DBClusterSnapshotIdentifier: aws.String("cs")}); err == nil ||
		!strings.Contains(err.Error(), "DBClusterSnapshotNotFoundFault") {
		t.Fatalf("deleted twice: %v", err)
	}
}

// TestSnapshotRestore keeps rows through a snapshot and a point-in-time
// restore, on real PostgreSQL containers.
func TestSnapshotRestore(t *testing.T) {
	c := sdk(t)
	src, snap := fmt.Sprintf("snap-%d", os.Getpid()), fmt.Sprintf("snap-%d-1", os.Getpid())
	_, err := c.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{DBInstanceIdentifier: aws.String(src), Engine: aws.String("postgres"),
		EngineVersion: aws.String("16"), DBInstanceClass: aws.String("db.t3.micro"), MasterUsername: aws.String("admin"),
		MasterUserPassword: aws.String("secret123"), DBName: aws.String("shop")})
	if err != nil {
		t.Fatal(err)
	}
	port := int(aws.ToInt32(running(t, c, src).Endpoint.Port))
	conn, err := pgConnect(port, "admin", "secret123", "shop")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "CREATE TABLE items (n int); INSERT INTO items VALUES (1), (2), (3); CREATE USER clerk PASSWORD 'clerk-pw'"); err != nil {
		t.Fatal(err)
	}
	conn.Close(ctx)
	if _, err := c.CreateDBSnapshot(ctx, &awsrds.CreateDBSnapshotInput{DBInstanceIdentifier: aws.String(src), DBSnapshotIdentifier: aws.String(snap)}); err != nil {
		t.Fatal(err)
	}
	eventually5m(t, "snapshot available", func() bool {
		d, err := c.DescribeDBSnapshots(ctx, &awsrds.DescribeDBSnapshotsInput{DBSnapshotIdentifier: aws.String(snap)})
		if err != nil || aws.ToString(d.DBSnapshots[0].Status) == "failed" {
			t.Fatalf("snapshot %v %+v", err, d)
		}
		return aws.ToString(d.DBSnapshots[0].Status) == "available"
	})
	if _, err := c.RestoreDBInstanceFromDBSnapshot(ctx, &awsrds.RestoreDBInstanceFromDBSnapshotInput{DBInstanceIdentifier: aws.String(src + "-r"),
		DBSnapshotIdentifier: aws.String(snap)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RestoreDBInstanceToPointInTime(ctx, &awsrds.RestoreDBInstanceToPointInTimeInput{SourceDBInstanceIdentifier: aws.String(src),
		TargetDBInstanceIdentifier: aws.String(src + "-p"), UseLatestRestorableTime: aws.Bool(true)}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{src + "-r", src + "-p"} {
		i := running(t, c, id)
		p := int(aws.ToInt32(i.Endpoint.Port))
		if aws.ToString(i.DBName) != "shop" {
			t.Fatalf("%s: %+v", id, i)
		}
		if got := pgQuery(t, p, "clerk", "clerk-pw", "SELECT current_user::text"); got != "clerk" {
			t.Fatalf("%s: roles come back too, got %q", id, got)
		}
		if got := pgQuery(t, p, "admin", "secret123", "SELECT sum(n)::text FROM items", "shop"); got != "6" {
			t.Fatalf("%s: sum %s", id, got)
		}
	}
}

// floci #4083: CopyDBSnapshot (same and other region, with a dump) and ModifyDBSnapshot.
func TestCopyAndModifySnapshot(t *testing.T) {
	_, _, base := setup(t, false)
	cfg := awstest.Config(t, base)
	c := awsrds.NewFromConfig(cfg)
	eu := awsrds.NewFromConfig(cfg, func(o *awsrds.Options) { o.Region = "eu-west-1" })
	create(t, base, "src", "StorageEncrypted", "true", "Tags.Tag.1.Key", "env", "Tags.Tag.1.Value", "dev")
	await(t, base, "src", "available")
	s, err := c.CreateDBSnapshot(ctx, &awsrds.CreateDBSnapshotInput{DBInstanceIdentifier: aws.String("src"), DBSnapshotIdentifier: aws.String("s1"),
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil || !aws.ToBool(s.DBSnapshot.Encrypted) {
		t.Fatalf("snapshot of an encrypted instance: %v %+v", err, s)
	}
	eventually(t, "snapshot available", func() bool {
		d, _ := c.DescribeDBSnapshots(ctx, &awsrds.DescribeDBSnapshotsInput{DBSnapshotIdentifier: aws.String("s1")})
		return d != nil && aws.ToString(d.DBSnapshots[0].Status) == "available"
	})
	if _, err := c.ModifyDBSnapshot(ctx, &awsrds.ModifyDBSnapshotInput{DBSnapshotIdentifier: aws.String("s1")}); err != nil {
		t.Fatal(err)
	}
	m, err := c.ModifyDBSnapshot(ctx, &awsrds.ModifyDBSnapshotInput{DBSnapshotIdentifier: aws.String("s1"), EngineVersion: aws.String("16.4")})
	if err != nil || aws.ToString(m.DBSnapshot.EngineVersion) != "16.4" {
		t.Fatalf("ModifyDBSnapshot: %v %+v", err, m)
	}
	// Same region.
	cp, err := c.CopyDBSnapshot(ctx, &awsrds.CopyDBSnapshotInput{SourceDBSnapshotIdentifier: aws.String("s1"), TargetDBSnapshotIdentifier: aws.String("s2"), CopyTags: aws.Bool(false)})
	if err != nil || aws.ToString(cp.DBSnapshot.SourceDBSnapshotIdentifier) != aws.ToString(s.DBSnapshot.DBSnapshotArn) || !aws.ToBool(cp.DBSnapshot.Encrypted) {
		t.Fatalf("copy: %v %+v", err, cp)
	}
	if _, err := c.CopyDBSnapshot(ctx, &awsrds.CopyDBSnapshotInput{SourceDBSnapshotIdentifier: aws.String("s1"), TargetDBSnapshotIdentifier: aws.String("s2")}); err == nil || !strings.Contains(err.Error(), "DBSnapshotAlreadyExists") {
		t.Fatalf("copy over a snapshot: %v", err)
	}
	// Another region: the ARN needs SourceRegion; the SDK sends it as a pre-signed URL too.
	arn := s.DBSnapshot.DBSnapshotArn
	if _, err := eu.CopyDBSnapshot(ctx, &awsrds.CopyDBSnapshotInput{SourceDBSnapshotIdentifier: arn, TargetDBSnapshotIdentifier: aws.String("far")}); err == nil || !strings.Contains(err.Error(), "SourceRegion") {
		t.Fatalf("cross-region copy without SourceRegion: %v", err)
	}
	far, err := eu.CopyDBSnapshot(ctx, &awsrds.CopyDBSnapshotInput{SourceDBSnapshotIdentifier: arn, TargetDBSnapshotIdentifier: aws.String("far"), SourceRegion: aws.String("us-east-1"),
		KmsKeyId: aws.String("alias/eu"), CopyTags: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	if got := aws.ToString(far.DBSnapshot.DBSnapshotArn); !strings.Contains(got, ":eu-west-1:") || aws.ToString(far.DBSnapshot.KmsKeyId) != "alias/eu" || len(far.DBSnapshot.TagList) != 1 {
		t.Fatalf("far copy = %+v", far.DBSnapshot)
	}
	if d, err := eu.DescribeDBSnapshots(ctx, &awsrds.DescribeDBSnapshotsInput{DBSnapshotIdentifier: aws.String("far")}); err != nil || len(d.DBSnapshots) != 1 {
		t.Fatalf("describe in the other region: %v %+v", err, d)
	}
	if _, err := c.DescribeDBSnapshots(ctx, &awsrds.DescribeDBSnapshotsInput{DBSnapshotIdentifier: aws.String("far")}); err == nil {
		t.Fatal("the copy shows in the source region")
	}
}
