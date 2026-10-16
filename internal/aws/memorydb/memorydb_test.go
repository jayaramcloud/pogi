package memorydb_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/memorydb"
	"github.com/aws/aws-sdk-go-v2/service/memorydb/types"
	"github.com/aws/smithy-go"

	mdb "github.com/nrndev/pogi/internal/aws/memorydb"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/docker/dockertest"
)

const pw = "0123456789abcdef"

func code(err error) string {
	var api smithy.APIError
	if errors.As(err, &api) {
		return api.ErrorCode()
	}
	return fmt.Sprint(err)
}

func client(t *testing.T) (*memorydb.Client, *mdb.Service) {
	var svc *mdb.Service
	_, base := core.ServeTest(t, func(h *core.Hub) core.Service { svc = mdb.New(h); return svc })
	return memorydb.NewFromConfig(awstest.Config(t, base)), svc
}

// must returns v, or fails the test (by panicking) on an error.
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func errOf[T any](_ T, err error) error { return err }

func fails(t *testing.T, want string, err error) {
	t.Helper()
	if code(err) != want {
		t.Fatalf("want %s, got %v", want, err)
	}
}

func passwordMode(pws ...string) *types.AuthenticationMode {
	return &types.AuthenticationMode{Type: types.InputAuthenticationTypePassword, Passwords: pws}
}

// TestMetadata runs MemoryDB without a container engine.
func TestMetadata(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	db, svc := client(t)
	ctx := context.Background()

	acls := must(db.DescribeACLs(ctx, &memorydb.DescribeACLsInput{}))
	if len(acls.ACLs) != 1 || *acls.ACLs[0].Name != "open-access" || !slices.Equal(acls.ACLs[0].UserNames, []string{"default"}) {
		t.Fatalf("acls %+v", acls.ACLs)
	}
	u := must(db.CreateUser(ctx, &memorydb.CreateUserInput{UserName: aws.String("app"), AccessString: aws.String("on ~app:* +@all"),
		AuthenticationMode: passwordMode(pw, pw+"2"), Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("core")}}})).User
	if *u.Status != "active" || u.Authentication.Type != types.AuthenticationTypePassword || *u.Authentication.PasswordCount != 2 ||
		*u.ARN != "arn:aws:memorydb:us-east-1:000000000000:user/app" {
		t.Fatalf("user %+v", u)
	}
	must(db.CreateUser(ctx, &memorydb.CreateUserInput{UserName: aws.String("robot"), AccessString: aws.String("on ~* +@all"),
		AuthenticationMode: &types.AuthenticationMode{Type: types.InputAuthenticationTypeIam}}))
	fails(t, "UserAlreadyExistsFault", errOf(db.CreateUser(ctx, &memorydb.CreateUserInput{UserName: aws.String("app"), AccessString: aws.String("on"), AuthenticationMode: passwordMode(pw)})))
	fails(t, "UserAlreadyExistsFault", errOf(db.CreateUser(ctx, &memorydb.CreateUserInput{UserName: aws.String("default"), AccessString: aws.String("on"), AuthenticationMode: passwordMode(pw)})))
	fails(t, "InvalidParameterValueException", errOf(db.CreateUser(ctx, &memorydb.CreateUserInput{UserName: aws.String("short"), AccessString: aws.String("on"), AuthenticationMode: passwordMode("short")})))
	fails(t, "InvalidParameterCombinationException", errOf(db.CreateUser(ctx, &memorydb.CreateUserInput{UserName: aws.String("nopw"), AccessString: aws.String("on"), AuthenticationMode: passwordMode()})))

	fails(t, "UserNotFoundFault", errOf(db.CreateACL(ctx, &memorydb.CreateACLInput{ACLName: aws.String("team"), UserNames: []string{"ghost"}})))
	acl := must(db.CreateACL(ctx, &memorydb.CreateACLInput{ACLName: aws.String("Team"), UserNames: []string{"app", "robot"}})).ACL
	if *acl.Name != "team" || *acl.Status != "active" || !slices.Equal(acl.UserNames, []string{"app", "robot"}) {
		t.Fatalf("acl %+v", acl)
	}
	fails(t, "ACLAlreadyExistsFault", errOf(db.CreateACL(ctx, &memorydb.CreateACLInput{ACLName: aws.String("team")})))
	fails(t, "ACLNotFoundFault", errOf(db.CreateCluster(ctx, &memorydb.CreateClusterInput{ClusterName: aws.String("x"), NodeType: aws.String("db.t4g.small"), ACLName: aws.String("nope")})))

	cl := must(db.CreateCluster(ctx, &memorydb.CreateClusterInput{ClusterName: aws.String("Cache"), NodeType: aws.String("db.t4g.small"),
		ACLName: aws.String("team"), NumShards: aws.Int32(2)})).Cluster
	if *cl.Name != "cache" || *cl.Status != "available" || *cl.ClusterEndpoint.Address != "localhost" || cl.ClusterEndpoint.Port != 6379 ||
		*cl.Engine != "valkey" || *cl.EngineVersion != "7.3" || *cl.ParameterGroupName != "default.memorydb-valkey7" || *cl.SubnetGroupName != "default" ||
		!*cl.TLSEnabled || *cl.ACLName != "team" || *cl.NumberOfShards != 2 || cl.DataTiering != types.DataTieringStatusFalse {
		t.Fatalf("cluster %+v", cl)
	}
	fails(t, "ClusterAlreadyExistsFault", errOf(db.CreateCluster(ctx, &memorydb.CreateClusterInput{ClusterName: aws.String("cache"), NodeType: aws.String("db.t4g.small"), ACLName: aws.String("team")})))
	fails(t, "InvalidParameterValueException", errOf(db.CreateCluster(ctx, &memorydb.CreateClusterInput{ClusterName: aws.String("bad--name"), NodeType: aws.String("db.t4g.small"), ACLName: aws.String("team")})))
	fails(t, "ClusterNotFoundFault", errOf(db.DescribeClusters(ctx, &memorydb.DescribeClustersInput{ClusterName: aws.String("nope")})))
	shards := must(db.DescribeClusters(ctx, &memorydb.DescribeClustersInput{ClusterName: aws.String("cache"), ShowShardDetails: aws.Bool(true)})).Clusters[0].Shards
	if len(shards) != 2 || *shards[1].Slots != "8192-16383" || *shards[1].NumberOfNodes != 2 || *shards[1].Nodes[1].Name != "cache-0002-002" ||
		*shards[0].Status != "available" || shards[0].Nodes[0].Endpoint.Port != 6379 {
		t.Fatalf("shards %+v", shards)
	}
	if v := svc.Inspect("Clusters", "cache"); v == nil {
		t.Fatal("inspect")
	}
	users := must(db.DescribeUsers(ctx, &memorydb.DescribeUsersInput{UserName: aws.String("app")})).Users
	if len(users) != 1 || !slices.Equal(users[0].ACLNames, []string{"team"}) {
		t.Fatalf("users %+v", users)
	}
	fails(t, "UserNotFoundFault", errOf(db.DescribeUsers(ctx, &memorydb.DescribeUsersInput{UserName: aws.String("ghost")})))
	fails(t, "InvalidACLStateFault", errOf(db.DeleteACL(ctx, &memorydb.DeleteACLInput{ACLName: aws.String("team")})))
	acl = must(db.UpdateACL(ctx, &memorydb.UpdateACLInput{ACLName: aws.String("team"), UserNamesToRemove: []string{"robot"}})).ACL
	if !slices.Equal(acl.UserNames, []string{"app"}) || !slices.Equal(acl.Clusters, []string{"cache"}) {
		t.Fatalf("updated acl %+v", acl)
	}
	fails(t, "InvalidParameterValueException", errOf(db.UpdateACL(ctx, &memorydb.UpdateACLInput{ACLName: aws.String("open-access"), UserNamesToAdd: []string{"app"}})))

	must(db.CreateSubnetGroup(ctx, &memorydb.CreateSubnetGroupInput{SubnetGroupName: aws.String("net"), SubnetIds: []string{"subnet-1", "subnet-2"}}))
	fails(t, "SubnetGroupAlreadyExistsFault", errOf(db.CreateSubnetGroup(ctx, &memorydb.CreateSubnetGroupInput{SubnetGroupName: aws.String("net"), SubnetIds: []string{"subnet-1"}})))
	sg := must(db.UpdateSubnetGroup(ctx, &memorydb.UpdateSubnetGroupInput{SubnetGroupName: aws.String("net"), Description: aws.String("d")})).SubnetGroup
	if *sg.Description != "d" || len(sg.Subnets) != 2 || *sg.Subnets[1].AvailabilityZone.Name != "us-east-1b" {
		t.Fatalf("subnet group %+v", sg)
	}
	must(db.CreateParameterGroup(ctx, &memorydb.CreateParameterGroupInput{ParameterGroupName: aws.String("tuned"), Family: aws.String("memorydb_valkey7")}))
	fails(t, "ParameterGroupAlreadyExistsFault", errOf(db.CreateParameterGroup(ctx, &memorydb.CreateParameterGroupInput{ParameterGroupName: aws.String("tuned"), Family: aws.String("memorydb_valkey7")})))
	must(db.UpdateParameterGroup(ctx, &memorydb.UpdateParameterGroupInput{ParameterGroupName: aws.String("tuned"),
		ParameterNameValues: []types.ParameterNameValue{{ParameterName: aws.String("maxmemory-policy"), ParameterValue: aws.String("allkeys-lru")}}}))
	ps := must(db.DescribeParameters(ctx, &memorydb.DescribeParametersInput{ParameterGroupName: aws.String("tuned")})).Parameters
	if len(ps) != 1 || *ps[0].Name != "maxmemory-policy" || *ps[0].Value != "allkeys-lru" {
		t.Fatalf("parameters %+v", ps)
	}
	fails(t, "InvalidParameterGroupStateFault", errOf(db.UpdateParameterGroup(ctx, &memorydb.UpdateParameterGroupInput{ParameterGroupName: aws.String("default.memorydb-valkey7"),
		ParameterNameValues: []types.ParameterNameValue{{ParameterName: aws.String("x"), ParameterValue: aws.String("y")}}})))
	cl = must(db.UpdateCluster(ctx, &memorydb.UpdateClusterInput{ClusterName: aws.String("cache"), ParameterGroupName: aws.String("tuned"),
		ACLName: aws.String("open-access"), ShardConfiguration: &types.ShardConfigurationRequest{ShardCount: 1}})).Cluster
	if *cl.ParameterGroupName != "tuned" || *cl.ACLName != "open-access" || *cl.NumberOfShards != 1 {
		t.Fatalf("updated cluster %+v", cl)
	}
	fails(t, "InvalidParameterGroupStateFault", errOf(db.DeleteParameterGroup(ctx, &memorydb.DeleteParameterGroupInput{ParameterGroupName: aws.String("tuned")})))
	must(db.CreateCluster(ctx, &memorydb.CreateClusterInput{ClusterName: aws.String("netted"), NodeType: aws.String("db.t4g.small"),
		ACLName: aws.String("open-access"), SubnetGroupName: aws.String("net"), Engine: aws.String("redis")}))
	fails(t, "SubnetGroupInUseFault", errOf(db.DeleteSubnetGroup(ctx, &memorydb.DeleteSubnetGroupInput{SubnetGroupName: aws.String("net")})))

	arn := aws.String("arn:aws:memorydb:us-east-1:000000000000:cluster/cache")
	must(db.TagResource(ctx, &memorydb.TagResourceInput{ResourceArn: arn, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}))
	tags := must(db.ListTags(ctx, &memorydb.ListTagsInput{ResourceArn: arn})).TagList
	if len(tags) != 1 || *tags[0].Key != "env" {
		t.Fatalf("tags %+v", tags)
	}
	must(db.UntagResource(ctx, &memorydb.UntagResourceInput{ResourceArn: arn, TagKeys: []string{"env"}}))
	fails(t, "TagNotFoundFault", errOf(db.UntagResource(ctx, &memorydb.UntagResourceInput{ResourceArn: arn, TagKeys: []string{"env"}})))
	fails(t, "InvalidARNFault", errOf(db.ListTags(ctx, &memorydb.ListTagsInput{ResourceArn: aws.String("nope")})))
	fails(t, "ClusterNotFoundFault", errOf(db.ListTags(ctx, &memorydb.ListTagsInput{ResourceArn: aws.String("arn:aws:memorydb:us-east-1:000000000000:cluster/gone")})))
	if tags := must(db.ListTags(ctx, &memorydb.ListTagsInput{ResourceArn: u.ARN})).TagList; len(tags) != 1 || *tags[0].Value != "core" {
		t.Fatalf("user tags %+v", tags)
	}

	must(db.CreateSnapshot(ctx, &memorydb.CreateSnapshotInput{ClusterName: aws.String("cache"), SnapshotName: aws.String("snap")}))
	fails(t, "SnapshotAlreadyExistsFault", errOf(db.CreateSnapshot(ctx, &memorydb.CreateSnapshotInput{ClusterName: aws.String("cache"), SnapshotName: aws.String("snap")})))
	must(db.DeleteCluster(ctx, &memorydb.DeleteClusterInput{ClusterName: aws.String("cache"), FinalSnapshotName: aws.String("final")}))
	snaps := must(db.DescribeSnapshots(ctx, &memorydb.DescribeSnapshotsInput{ClusterName: aws.String("cache")})).Snapshots
	if len(snaps) != 2 || *snaps[0].Name != "final" || *snaps[1].ClusterConfiguration.NumShards != 1 || *snaps[1].Status != "available" {
		t.Fatalf("snapshots %+v", snaps)
	}
	must(db.DeleteSnapshot(ctx, &memorydb.DeleteSnapshotInput{SnapshotName: aws.String("snap")}))
	fails(t, "SnapshotNotFoundFault", errOf(db.DescribeSnapshots(ctx, &memorydb.DescribeSnapshotsInput{SnapshotName: aws.String("snap")})))
	fails(t, "ClusterNotFoundFault", errOf(db.DeleteCluster(ctx, &memorydb.DeleteClusterInput{ClusterName: aws.String("cache")})))

	must(db.DeleteUser(ctx, &memorydb.DeleteUserInput{UserName: aws.String("app")}))
	if acl := must(db.DescribeACLs(ctx, &memorydb.DescribeACLsInput{ACLName: aws.String("team")})).ACLs[0]; len(acl.UserNames) != 0 {
		t.Fatalf("a deleted user leaves its ACLs: %+v", acl)
	}
	must(db.DeleteACL(ctx, &memorydb.DeleteACLInput{ACLName: aws.String("team")}))
	fails(t, "ACLNotFoundFault", errOf(db.DescribeACLs(ctx, &memorydb.DescribeACLsInput{ACLName: aws.String("team")})))
	vs := must(db.DescribeEngineVersions(ctx, &memorydb.DescribeEngineVersionsInput{Engine: aws.String("redis"), DefaultOnly: true})).EngineVersions
	if len(vs) != 1 || *vs[0].EngineVersion != "7.1" || *vs[0].ParameterGroupFamily != "memorydb_redis7" {
		t.Fatalf("versions %+v", vs)
	}
}

// TestUsersInServer checks the users script and container on a fake engine.
func TestUsersInServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Write([]byte("+PONG\r\n"))
			c.Close()
		}
	}()
	f := dockertest.Start(t, ln.Addr().(*net.TCPAddr).Port, false)
	db, _ := client(t)
	ctx := context.Background()
	must(db.CreateUser(ctx, &memorydb.CreateUserInput{UserName: aws.String("app"), AccessString: aws.String("on ~* +@all"), AuthenticationMode: passwordMode(pw)}))
	must(db.CreateACL(ctx, &memorydb.CreateACLInput{ACLName: aws.String("team"), UserNames: []string{"app"}}))
	must(db.CreateCluster(ctx, &memorydb.CreateClusterInput{ClusterName: aws.String("cache"), NodeType: aws.String("db.t4g.small"), ACLName: aws.String("team")}))
	eventually(t, func() bool {
		return *must(db.DescribeClusters(ctx, &memorydb.DescribeClustersInput{ClusterName: aws.String("cache")})).Clusters[0].Status == "available" &&
			len(f.Called("exec pogi-memorydb-cache sh -c")) == 1
	})
	if run := f.Called("create --name pogi-memorydb-cache "); len(run) != 1 || !strings.Contains(run[0], ":6379 valkey/valkey:8-alpine --cluster-enabled yes") {
		t.Fatalf("docker create = %q", run)
	}
	script := strings.Join(f.Called("exec pogi-memorydb-cache sh -c"), "")
	for _, want := range []string{"cluster addslotsrange 0 16383", "ACL SETUSER 'default' reset 'off' nopass", "ACL SETUSER 'app' reset 'on' '~*' '+@all' '>" + pw + "'"} {
		if !strings.Contains(script, want) {
			t.Fatalf("missing %q in %q", want, script)
		}
	}
	must(db.UpdateCluster(ctx, &memorydb.UpdateClusterInput{ClusterName: aws.String("cache"), ACLName: aws.String("open-access")}))
	eventually(t, func() bool { return len(f.Called("exec pogi-memorydb-cache sh -c")) == 2 })
	if all := f.Called("exec pogi-memorydb-cache sh -c"); !strings.Contains(all[1], "ACL SETUSER 'default' reset 'on' '~*' '&*' '+@all' nopass") {
		t.Fatalf("open access: %q", all)
	}
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(10 * time.Second); !cond(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("timed out")
		}
	}
}

// resp sends a command as a RESP array and returns the first line of the reply
// (the value of a bulk string).
func resp(t *testing.T, c net.Conn, r *bufio.Reader, args ...string) string {
	t.Helper()
	cmd := fmt.Sprintf("*%d\r\n", len(args))
	for _, a := range args {
		cmd += fmt.Sprintf("$%d\r\n%s\r\n", len(a), a)
	}
	c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.WriteString(c, cmd); err != nil {
		t.Fatal(err)
	}
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("%s: %v", args[0], err)
	}
	if strings.HasPrefix(line, "$") && line != "$-1\r\n" {
		line, _ = r.ReadString('\n')
	}
	return strings.TrimSpace(line)
}

// TestValkeyInDocker checks an ACL's users in a real Valkey.
func TestValkeyInDocker(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	db, _ := client(t)
	ctx := context.Background()
	name := fmt.Sprintf("it-%d", os.Getpid())
	must(db.CreateUser(ctx, &memorydb.CreateUserInput{UserName: aws.String("app"), AccessString: aws.String("on ~* &* +@all"), AuthenticationMode: passwordMode(pw)}))
	must(db.CreateACL(ctx, &memorydb.CreateACLInput{ACLName: aws.String("team"), UserNames: []string{"app"}}))
	must(db.CreateCluster(ctx, &memorydb.CreateClusterInput{ClusterName: aws.String(name), NodeType: aws.String("db.t4g.small"), ACLName: aws.String("team")}))
	var cl types.Cluster
	for end := time.Now().Add(5 * time.Minute); ; time.Sleep(200 * time.Millisecond) {
		cl = must(db.DescribeClusters(ctx, &memorydb.DescribeClustersInput{ClusterName: aws.String(name)})).Clusters[0]
		if *cl.Status == "available" {
			break
		}
		if *cl.Status == "create-failed" || time.Now().After(end) {
			t.Fatalf("cluster %s", *cl.Status)
		}
	}
	dial := func() (net.Conn, *bufio.Reader) {
		c, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(int(cl.ClusterEndpoint.Port)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c, bufio.NewReader(c)
	}
	c, r := dial()
	for end := time.Now().Add(30 * time.Second); resp(t, c, r, "AUTH", "app", pw) != "+OK"; time.Sleep(200 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("the ACL's user never came")
		}
	}
	for end := time.Now().Add(30 * time.Second); resp(t, c, r, "SET", "k", "v") != "+OK"; time.Sleep(200 * time.Millisecond) {
		if time.Now().After(end) { // the slots come with the users
			t.Fatal("SET as app failed")
		}
	}
	if got := resp(t, c, r, "GET", "k"); got != "v" {
		t.Fatalf("GET = %q", got)
	}
	if got := resp(t, c, r, "AUTH", "app", "wrong-password-123"); !strings.HasPrefix(got, "-WRONGPASS") {
		t.Fatalf("a wrong password: %s", got)
	}
	c, r = dial()
	if got := resp(t, c, r, "GET", "k"); !strings.HasPrefix(got, "-NOAUTH") {
		t.Fatalf("the default user is off under a restricted ACL: %s", got)
	}
	must(db.UpdateCluster(ctx, &memorydb.UpdateClusterInput{ClusterName: aws.String(name), ACLName: aws.String("open-access")}))
	for end := time.Now().Add(30 * time.Second); resp(t, c, r, "GET", "k") != "v"; time.Sleep(200 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("open-access lets the default user in")
		}
	}
	must(db.DeleteCluster(ctx, &memorydb.DeleteClusterInput{ClusterName: aws.String(name)}))
}
