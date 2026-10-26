package elasticache_test

import (
	"bufio"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/aws/elasticache"
	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/docker/dockertest"
)

// setup serves ElastiCache with a fake engine whose containers all publish
// a listener that answers like a cache server.
func setup(t *testing.T, down bool) (*dockertest.Engine, *elasticache.Service, string) {
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
			go func() {
				defer c.Close()
				c.Write([]byte("+PONG\r\n"))
				c.SetReadDeadline(time.Now().Add(time.Second))
				io.Copy(io.Discard, c)
			}()
		}
	}()
	f := dockertest.Start(t, ln.Addr().(*net.TCPAddr).Port, down)
	var svc *elasticache.Service
	_, base := core.ServeTest(t, func(h *core.Hub) core.Service { svc = elasticache.New(h); return svc })
	return f, svc, base
}

func freePort(t *testing.T) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// call sends a Query request the way the SDKs and the CLI do.
func call(t *testing.T, base string, params ...string) (int, string) {
	t.Helper()
	form := url.Values{"Version": {"2015-02-02"}}
	for i := 0; i+1 < len(params); i += 2 {
		form.Set(params[i], params[i+1])
	}
	req, _ := http.NewRequest(http.MethodPost, base, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	awstest.Sign(t, req, "elasticache")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func ok(t *testing.T, base string, v any, params ...string) string {
	t.Helper()
	st, body := call(t, base, params...)
	if st != 200 {
		t.Fatalf("%s: %d %s", params[1], st, body)
	}
	if v != nil {
		reflect.ValueOf(v).Elem().SetZero()
		if err := xml.Unmarshal([]byte(body), v); err != nil {
			t.Fatalf("%s: %v in %s", params[1], err, body)
		}
	}
	return body
}

func fails(t *testing.T, base string, status int, code string, params ...string) {
	t.Helper()
	st, body := call(t, base, params...)
	var e struct {
		Code string `xml:"Error>Code"`
	}
	xml.Unmarshal([]byte(body), &e)
	if st != status || e.Code != code {
		t.Fatalf("%s: want %d %s, got %d %s", params[1], status, code, st, body)
	}
}

func eventually(t *testing.T, what string, wait time.Duration, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(wait); !cond(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

type tag struct{ Key, Value string }

type endpoint struct {
	Address string
	Port    int
}

type cacheCluster struct {
	CacheClusterId, CacheClusterStatus, Engine, EngineVersion, CacheNodeType, ReplicationGroupId, ARN string
	NumCacheNodes                                                                                     int
	ConfigurationEndpoint                                                                             endpoint
	Nodes                                                                                             []struct {
		CacheNodeId string
		Endpoint    endpoint
	} `xml:"CacheNodes>CacheNode"`
	SecurityGroups []string `xml:"SecurityGroups>member>SecurityGroupId"`
	ParameterGroup string   `xml:"CacheParameterGroup>CacheParameterGroupName"`
}

type group struct {
	ReplicationGroupId, Status, ClusterMode, AutomaticFailover string
	ClusterEnabled                                             bool
	Members                                                    []string `xml:"MemberClusters>ClusterId"`
	ConfigurationEndpoint                                      endpoint
	NodeGroups                                                 []struct {
		NodeGroupId, Slots string
		Primary            endpoint                                       `xml:"PrimaryEndpoint"`
		Members            []struct{ CacheClusterId, CurrentRole string } `xml:"NodeGroupMembers>NodeGroupMember"`
	} `xml:"NodeGroups>NodeGroup"`
}

type serverless struct {
	ServerlessCacheName, Status, MajorEngineVersion, FullEngineVersion string
	Endpoint, ReaderEndpoint                                           endpoint
	SecurityGroupIds                                                   []string `xml:"SecurityGroupIds>SecurityGroupId"`
}

// res is any ElastiCache response: <OpResponse><OpResult>…</OpResult>…</OpResponse>.
type res struct {
	R struct {
		Cluster        cacheCluster   `xml:"CacheCluster"`
		Clusters       []cacheCluster `xml:"CacheClusters>CacheCluster"`
		Group          group          `xml:"ReplicationGroup"`
		Groups         []group        `xml:"ReplicationGroups>ReplicationGroup"`
		Serverless     serverless     `xml:"ServerlessCache"`
		ServerlessList []serverless   `xml:"ServerlessCaches>member"`
		SubnetGroups   []struct {
			CacheSubnetGroupName string
			Subnets              []string `xml:"Subnets>Subnet>SubnetIdentifier"`
		} `xml:"CacheSubnetGroups>CacheSubnetGroup"`
		ParamGroups []struct{ CacheParameterGroupName, CacheParameterGroupFamily string } `xml:"CacheParameterGroups>CacheParameterGroup"`
		ParamGroup  string                                                                `xml:"CacheParameterGroupName"`
		Parameters  []struct{ ParameterName, ParameterValue string }                      `xml:"Parameters>Parameter"`
		Tags        []tag                                                                 `xml:"TagList>Tag"`
		Versions    []struct{ Engine, EngineVersion, CacheParameterGroupFamily string }   `xml:"CacheEngineVersions>CacheEngineVersion"`
	} `xml:",any"`
}

func describe(t *testing.T, base, id string) cacheCluster {
	t.Helper()
	var r res
	ok(t, base, &r, "Action", "DescribeCacheClusters", "CacheClusterId", id, "ShowCacheNodeInfo", "true")
	return r.R.Clusters[0]
}

func await(t *testing.T, base, id, status string) cacheCluster {
	t.Helper()
	var c cacheCluster
	eventually(t, id+" "+status, 10*time.Second, func() bool { c = describe(t, base, id); return c.CacheClusterStatus == status })
	return c
}

func describeGroup(t *testing.T, base, id string) group {
	t.Helper()
	var r res
	ok(t, base, &r, "Action", "DescribeReplicationGroups", "ReplicationGroupId", id)
	return r.R.Groups[0]
}

func TestCacheClusterLifecycle(t *testing.T) {
	f, _, base := setup(t, false)
	want := freePort(t)
	var r res
	create := []string{"Action", "CreateCacheCluster", "CacheClusterId", "Sessions", "Engine", "redis", "CacheNodeType", "cache.t3.micro",
		"NumCacheNodes", "1", "Port", strconv.Itoa(want), "SecurityGroupIds.SecurityGroupId.1", "sg-1", "Tags.Tag.1.Key", "app", "Tags.Tag.1.Value", "web"}
	ok(t, base, &r, create...)
	if c := r.R.Cluster; c.CacheClusterId != "sessions" || c.CacheClusterStatus != "creating" || c.EngineVersion != "7.1.0" ||
		c.ParameterGroup != "default.redis7" || !slices.Equal(c.SecurityGroups, []string{"sg-1"}) || len(c.Nodes) != 0 {
		t.Fatalf("created %+v", c)
	}
	c := await(t, base, "sessions", "available")
	if len(c.Nodes) != 1 || c.Nodes[0].CacheNodeId != "0001" || c.Nodes[0].Endpoint != (endpoint{"localhost", f.Port}) {
		t.Fatalf("nodes %+v", c.Nodes)
	}
	if run := f.Called("create"); len(run) != 1 ||
		!strings.HasSuffix(run[0], "--name pogi-elasticache-sessions -p 127.0.0.1:"+strconv.Itoa(want)+":6379 redis:7-alpine") {
		t.Fatalf("docker create = %q", run)
	}
	fails(t, base, 400, "CacheClusterAlreadyExists", create...)
	fails(t, base, 400, "InvalidParameterValue", "Action", "CreateCacheCluster", "CacheClusterId", "big", "Engine", "redis", "NumCacheNodes", "2")
	fails(t, base, 400, "InvalidParameterValue", "Action", "CreateCacheCluster", "CacheClusterId", "odd", "Engine", "etcd")
	fails(t, base, 400, "CacheSubnetGroupNotFoundFault", "Action", "CreateCacheCluster", "CacheClusterId", "lost", "CacheSubnetGroupName", "nope")

	ok(t, base, &r, "Action", "CreateCacheCluster", "CacheClusterId", "mc", "Engine", "memcached", "NumCacheNodes", "3")
	mc := await(t, base, "mc", "available")
	if mc.ConfigurationEndpoint.Port != f.Port || len(mc.Nodes) != 3 || mc.Nodes[2].CacheNodeId != "0003" || mc.ParameterGroup != "default.memcached1.6" {
		t.Fatalf("memcached %+v", mc)
	}
	if run := f.Called("create --name pogi-elasticache-mc "); len(run) != 1 || !strings.HasSuffix(run[0], ":11211 memcached:1-alpine") {
		t.Fatalf("docker create = %q", run)
	}

	ok(t, base, &r, "Action", "ModifyCacheCluster", "CacheClusterId", "sessions", "CacheNodeType", "cache.t3.small", "ApplyImmediately", "true")
	if r.R.Cluster.CacheNodeType != "cache.t3.small" {
		t.Fatalf("modify %+v", r.R.Cluster)
	}
	ok(t, base, &r, "Action", "RebootCacheCluster", "CacheClusterId", "sessions", "CacheNodeIdsToReboot.CacheNodeId.1", "0001")
	if r.R.Cluster.CacheClusterStatus != "rebooting cache cluster nodes" {
		t.Fatalf("reboot %+v", r.R.Cluster)
	}
	await(t, base, "sessions", "available")
	if len(f.Called("restart pogi-elasticache-sessions")) != 1 {
		t.Fatalf("calls %q", f.Called(""))
	}
	ok(t, base, &r, "Action", "DeleteCacheCluster", "CacheClusterId", "sessions")
	if r.R.Cluster.CacheClusterStatus != "deleting" {
		t.Fatalf("delete %+v", r.R.Cluster)
	}
	fails(t, base, 404, "CacheClusterNotFound", "Action", "DescribeCacheClusters", "CacheClusterId", "sessions")
	eventually(t, "docker rm", 10*time.Second, func() bool { return len(f.Called("rm -f -v pogi-elasticache-sessions")) == 1 }) // no leftover before run, so only the delete
}

func TestReplicationGroups(t *testing.T) {
	f, _, base := setup(t, false)
	var r res
	ok(t, base, &r, "Action", "CreateReplicationGroup", "ReplicationGroupId", "Cart", "ReplicationGroupDescription", "carts",
		"Engine", "valkey", "NumCacheClusters", "2", "AutomaticFailoverEnabled", "true")
	if g := r.R.Group; g.ReplicationGroupId != "cart" || g.Status != "creating" || !slices.Equal(g.Members, []string{"cart-001", "cart-002"}) ||
		g.AutomaticFailover != "enabled" || g.ClusterMode != "disabled" {
		t.Fatalf("created %+v", g)
	}
	var g group
	eventually(t, "group available", 10*time.Second, func() bool { g = describeGroup(t, base, "cart"); return g.Status == "available" })
	if ng := g.NodeGroups; len(ng) != 1 || ng[0].Primary != (endpoint{"localhost", f.Port}) || len(ng[0].Members) != 2 ||
		ng[0].Members[0].CurrentRole != "primary" || ng[0].Members[1].CurrentRole != "replica" {
		t.Fatalf("node groups %+v", ng)
	}
	if c := describe(t, base, "cart-002"); c.ReplicationGroupId != "cart" || c.CacheClusterStatus != "available" || c.EngineVersion != "8.1.0" ||
		c.Nodes[0].Endpoint.Port != f.Port {
		t.Fatalf("member %+v", c)
	}
	if run := f.Called("create"); len(run) != 1 || !strings.HasSuffix(run[0], ":6379 valkey/valkey:8-alpine") {
		t.Fatalf("one container per group: %q", run)
	}
	ok(t, base, &r, "Action", "CreateCacheCluster", "CacheClusterId", "cart-003", "ReplicationGroupId", "cart")
	if c := r.R.Cluster; c.ReplicationGroupId != "cart" || c.Engine != "valkey" {
		t.Fatalf("replica %+v", c)
	}
	if g := describeGroup(t, base, "cart"); len(g.Members) != 3 {
		t.Fatalf("members %v", g.Members)
	}
	ok(t, base, nil, "Action", "DeleteCacheCluster", "CacheClusterId", "cart-003")
	ok(t, base, &r, "Action", "ModifyReplicationGroup", "ReplicationGroupId", "cart", "CacheNodeType", "cache.r7g.large", "ApplyImmediately", "true")
	if c := describe(t, base, "cart-001"); c.CacheNodeType != "cache.r7g.large" {
		t.Fatalf("members follow the group: %+v", c)
	}

	ok(t, base, &r, "Action", "CreateReplicationGroup", "ReplicationGroupId", "big", "ReplicationGroupDescription", "sharded",
		"NumNodeGroups", "2", "ReplicasPerNodeGroup", "1", "CacheParameterGroupName", "default.redis7.cluster.on")
	if g := r.R.Group; !g.ClusterEnabled || g.ClusterMode != "enabled" ||
		!slices.Equal(g.Members, []string{"big-0001-001", "big-0001-002", "big-0002-001", "big-0002-002"}) {
		t.Fatalf("cluster mode %+v", g)
	}
	eventually(t, "sharded group available", 10*time.Second, func() bool { g = describeGroup(t, base, "big"); return g.Status == "available" })
	if g.ConfigurationEndpoint.Port != f.Port || len(g.NodeGroups) != 2 || g.NodeGroups[1].Slots != "8192-16383" || len(g.NodeGroups[1].Members) != 2 {
		t.Fatalf("sharded %+v", g)
	}
	run := f.Called("create --name pogi-elasticache-big ")
	m := regexp.MustCompile(`-p 127\.0\.0\.1:(\d+):6379 redis:7-alpine --cluster-enabled yes --cluster-announce-ip 127\.0\.0\.1 --cluster-announce-port (\d+)$`).
		FindStringSubmatch(strings.Join(run, ""))
	if len(run) != 1 || m == nil || m[1] != m[2] {
		t.Fatalf("cluster mode announces the published port: %q", run)
	}
	if len(f.Called("exec pogi-elasticache-big redis-cli cluster addslotsrange 0 16383")) != 1 {
		t.Fatalf("slots: %q", f.Called(""))
	}

	fails(t, base, 400, "ReplicationGroupAlreadyExists", "Action", "CreateReplicationGroup", "ReplicationGroupId", "cart", "ReplicationGroupDescription", "x")
	fails(t, base, 400, "InvalidParameterValue", "Action", "CreateReplicationGroup", "ReplicationGroupId", "mc", "ReplicationGroupDescription", "x", "Engine", "memcached")
	fails(t, base, 400, "InvalidParameterValue", "Action", "CreateReplicationGroup", "ReplicationGroupId", "nodesc")
	fails(t, base, 400, "InvalidParameterCombination", "Action", "CreateReplicationGroup", "ReplicationGroupId", "single",
		"ReplicationGroupDescription", "x", "AutomaticFailoverEnabled", "true")
	fails(t, base, 400, "InvalidParameterCombination", "Action", "CreateCacheCluster", "CacheClusterId", "big-extra", "ReplicationGroupId", "big")
	fails(t, base, 404, "ReplicationGroupNotFoundFault", "Action", "CreateCacheCluster", "CacheClusterId", "orphan", "ReplicationGroupId", "nope")
	ok(t, base, nil, "Action", "CreateReplicationGroup", "ReplicationGroupId", "solo", "ReplicationGroupDescription", "x")
	fails(t, base, 400, "InvalidCacheClusterState", "Action", "DeleteCacheCluster", "CacheClusterId", "solo-001")

	ok(t, base, &r, "Action", "DeleteReplicationGroup", "ReplicationGroupId", "cart")
	if r.R.Group.Status != "deleting" {
		t.Fatalf("delete %+v", r.R.Group)
	}
	fails(t, base, 404, "ReplicationGroupNotFoundFault", "Action", "DescribeReplicationGroups", "ReplicationGroupId", "cart")
	fails(t, base, 404, "CacheClusterNotFound", "Action", "DescribeCacheClusters", "CacheClusterId", "cart-001")
	eventually(t, "group container removed", 10*time.Second, func() bool { return len(f.Called("rm -f -v pogi-elasticache-cart")) == 1 })
}

func TestQueryShape(t *testing.T) {
	_, _, base := setup(t, true)
	ok(t, base, nil, "Action", "CreateReplicationGroup", "ReplicationGroupId", "rg", "ReplicationGroupDescription", "x",
		"SecurityGroupIds.SecurityGroupId.1", "sg-1")
	for body, wants := range map[string][]string{
		ok(t, base, nil, "Action", "DescribeReplicationGroups"): {
			`<DescribeReplicationGroupsResponse xmlns="http://elasticache.amazonaws.com/doc/2015-02-02/"><DescribeReplicationGroupsResult><ReplicationGroups><ReplicationGroup>`,
			`<MemberClusters><ClusterId>rg-001</ClusterId></MemberClusters>`,
			`<NodeGroups><NodeGroup><NodeGroupId>0001</NodeGroupId><Status>available</Status><PrimaryEndpoint><Address>localhost</Address><Port>6379</Port></PrimaryEndpoint>`,
			`<NodeGroupMembers><NodeGroupMember><CacheClusterId>rg-001</CacheClusterId>`,
		},
		ok(t, base, nil, "Action", "DescribeCacheClusters", "ShowCacheNodeInfo", "true"): {
			`<CacheClusters><CacheCluster><CacheClusterId>rg-001</CacheClusterId>`,
			`<CacheNodes><CacheNode><CacheNodeId>0001</CacheNodeId>`,
			`<SecurityGroups><member><SecurityGroupId>sg-1</SecurityGroupId><Status>active</Status></member></SecurityGroups>`,
		},
	} {
		for _, want := range wants {
			if !strings.Contains(body, want) {
				t.Fatalf("missing %s in\n%s", want, body)
			}
		}
		if strings.Contains(body, "<pogi") || strings.Contains(body, "Docker is not available") {
			t.Fatalf("pogi's fields leaked into\n%s", body)
		}
	}
}

func TestNoDocker(t *testing.T) {
	f, svc, base := setup(t, true)
	ok(t, base, nil, "Action", "CreateCacheCluster", "CacheClusterId", "plain", "Engine", "redis")
	if c := describe(t, base, "plain"); c.CacheClusterStatus != "available" || c.Nodes[0].Endpoint != (endpoint{"localhost", 6379}) {
		t.Fatalf("no Docker: %+v", c)
	}
	if v := svc.Inspect("Cache clusters", "plain").(*elasticache.CacheCluster); v.Pogi.Note != rds.NoDocker || v.Pogi.Container != "" {
		t.Fatalf("inspect %+v", v.Pogi)
	}
	var r res
	ok(t, base, &r, "Action", "CreateServerlessCache", "ServerlessCacheName", "api", "Engine", "memcached")
	if s := r.R.Serverless; s.Status != "available" || s.Endpoint.Port != 11211 || s.MajorEngineVersion != "1.6" {
		t.Fatalf("serverless %+v", s)
	}
	if len(f.Called("create")) != 0 {
		t.Fatalf("no containers without Docker: %q", f.Called(""))
	}
}

func TestServerlessGroupsTagsCatalog(t *testing.T) {
	f, _, base := setup(t, false)
	var r res
	ok(t, base, &r, "Action", "CreateServerlessCache", "ServerlessCacheName", "API", "Engine", "valkey", "MajorEngineVersion", "8",
		"SecurityGroupIds.SecurityGroupId.1", "sg-9", "Tags.Tag.1.Key", "k", "Tags.Tag.1.Value", "v")
	if s := r.R.Serverless; s.ServerlessCacheName != "api" || s.Status != "creating" || s.FullEngineVersion != "8.1.0" {
		t.Fatalf("created %+v", s)
	}
	eventually(t, "serverless available", 10*time.Second, func() bool {
		ok(t, base, &r, "Action", "DescribeServerlessCaches")
		return len(r.R.ServerlessList) == 1 && r.R.ServerlessList[0].Status == "available"
	})
	if s := r.R.ServerlessList[0]; s.Endpoint.Port != f.Port || s.ReaderEndpoint.Port != f.Port || !slices.Equal(s.SecurityGroupIds, []string{"sg-9"}) {
		t.Fatalf("serverless %+v", s)
	}
	fails(t, base, 400, "ServerlessCacheAlreadyExistsFault", "Action", "CreateServerlessCache", "ServerlessCacheName", "api", "Engine", "valkey")
	ok(t, base, &r, "Action", "DeleteServerlessCache", "ServerlessCacheName", "api")
	if r.R.Serverless.Status != "deleting" {
		t.Fatalf("delete %+v", r.R.Serverless)
	}
	fails(t, base, 404, "ServerlessCacheNotFoundFault", "Action", "DescribeServerlessCaches", "ServerlessCacheName", "api")

	subnets := []string{"Action", "CreateCacheSubnetGroup", "CacheSubnetGroupName", "Cache-Net", "CacheSubnetGroupDescription", "net",
		"SubnetIds.SubnetIdentifier.1", "subnet-1", "SubnetIds.SubnetIdentifier.2", "subnet-2"}
	ok(t, base, nil, subnets...)
	fails(t, base, 400, "CacheSubnetGroupAlreadyExists", subnets...)
	ok(t, base, &r, "Action", "DescribeCacheSubnetGroups", "CacheSubnetGroupName", "cache-net")
	if g := r.R.SubnetGroups; len(g) != 1 || !slices.Equal(g[0].Subnets, []string{"subnet-1", "subnet-2"}) {
		t.Fatalf("subnet groups %+v", g)
	}
	fails(t, base, 400, "CacheSubnetGroupNotFoundFault", "Action", "DescribeCacheSubnetGroups", "CacheSubnetGroupName", "nope")

	params := []string{"Action", "CreateCacheParameterGroup", "CacheParameterGroupName", "tuned", "CacheParameterGroupFamily", "redis7", "Description", "x"}
	ok(t, base, nil, params...)
	fails(t, base, 400, "CacheParameterGroupAlreadyExists", params...)
	ok(t, base, &r, "Action", "ModifyCacheParameterGroup", "CacheParameterGroupName", "tuned",
		"ParameterNameValues.ParameterNameValue.1.ParameterName", "maxmemory-policy", "ParameterNameValues.ParameterNameValue.1.ParameterValue", "allkeys-lru")
	if r.R.ParamGroup != "tuned" {
		t.Fatalf("modify %+v", r.R)
	}
	ok(t, base, &r, "Action", "DescribeCacheParameters", "CacheParameterGroupName", "tuned")
	if p := r.R.Parameters; len(p) != 1 || p[0].ParameterName != "maxmemory-policy" || p[0].ParameterValue != "allkeys-lru" {
		t.Fatalf("parameters %+v", p)
	}
	ok(t, base, nil, "Action", "ResetCacheParameterGroup", "CacheParameterGroupName", "tuned", "ResetAllParameters", "true")
	if ok(t, base, &r, "Action", "DescribeCacheParameters", "CacheParameterGroupName", "tuned"); len(r.R.Parameters) != 0 {
		t.Fatalf("reset left %+v", r.R.Parameters)
	}
	ok(t, base, &r, "Action", "DescribeCacheParameterGroups", "CacheParameterGroupName", "default.valkey8")
	if g := r.R.ParamGroups; len(g) != 1 || g[0].CacheParameterGroupFamily != "valkey8" {
		t.Fatalf("default group %+v", g)
	}
	fails(t, base, 400, "InvalidCacheParameterGroupState", "Action", "DeleteCacheParameterGroup", "CacheParameterGroupName", "default.redis7")
	fails(t, base, 404, "CacheParameterGroupNotFound", "Action", "DeleteCacheParameterGroup", "CacheParameterGroupName", "nope")

	ok(t, base, &r, "Action", "CreateCacheCluster", "CacheClusterId", "tagged", "CacheSubnetGroupName", "cache-net", "CacheParameterGroupName", "tuned")
	arn := r.R.Cluster.ARN
	fails(t, base, 400, "CacheSubnetGroupInUse", "Action", "DeleteCacheSubnetGroup", "CacheSubnetGroupName", "cache-net")
	fails(t, base, 400, "InvalidCacheParameterGroupState", "Action", "DeleteCacheParameterGroup", "CacheParameterGroupName", "tuned")
	ok(t, base, &r, "Action", "AddTagsToResource", "ResourceName", arn, "Tags.Tag.1.Key", "team", "Tags.Tag.1.Value", "core",
		"Tags.Tag.2.Key", "env", "Tags.Tag.2.Value", "dev")
	if len(r.R.Tags) != 2 {
		t.Fatalf("add tags %+v", r.R.Tags)
	}
	ok(t, base, &r, "Action", "RemoveTagsFromResource", "ResourceName", arn, "TagKeys.member.1", "env")
	ok(t, base, &r, "Action", "ListTagsForResource", "ResourceName", arn)
	if !slices.Equal(r.R.Tags, []tag{{"team", "core"}}) {
		t.Fatalf("tags %+v", r.R.Tags)
	}
	fails(t, base, 404, "TagNotFound", "Action", "RemoveTagsFromResource", "ResourceName", arn, "TagKeys.member.1", "env")
	fails(t, base, 400, "InvalidARN", "Action", "ListTagsForResource", "ResourceName", "nope")
	fails(t, base, 404, "CacheClusterNotFound", "Action", "ListTagsForResource", "ResourceName", "arn:aws:elasticache:us-east-1:000000000000:cluster:gone")
	ok(t, base, nil, "Action", "DeleteCacheCluster", "CacheClusterId", "tagged")
	ok(t, base, nil, "Action", "DeleteCacheSubnetGroup", "CacheSubnetGroupName", "cache-net")
	ok(t, base, nil, "Action", "DeleteCacheParameterGroup", "CacheParameterGroupName", "tuned")

	ok(t, base, &r, "Action", "DescribeCacheEngineVersions", "Engine", "redis", "DefaultOnly", "true")
	if v := r.R.Versions; len(v) != 1 || v[0].EngineVersion != "7.1.0" || v[0].CacheParameterGroupFamily != "redis7" {
		t.Fatalf("versions %+v", v)
	}
	ok(t, base, &r, "Action", "DescribeCacheEngineVersions", "CacheParameterGroupFamily", "redis6.x")
	if v := r.R.Versions; len(v) != 1 || v[0].EngineVersion != "6.2.6" {
		t.Fatalf("versions %+v", v)
	}
}

// TestRedisInDocker runs a real Redis when a container engine answers.
func TestRedisInDocker(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	_, base := core.ServeTest(t, func(h *core.Hub) core.Service { return elasticache.New(h) })
	id := fmt.Sprintf("it-%d", os.Getpid())
	ok(t, base, nil, "Action", "CreateCacheCluster", "CacheClusterId", id, "Engine", "redis", "CacheNodeType", "cache.t3.micro")
	var c cacheCluster
	eventually(t, "redis to start (the first run pulls the image)", 5*time.Minute, func() bool {
		if c = describe(t, base, id); c.CacheClusterStatus == "create-failed" {
			t.Fatalf("container failed: %+v", c)
		}
		return c.CacheClusterStatus == "available"
	})
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(c.Nodes[0].Endpoint.Address, strconv.Itoa(c.Nodes[0].Endpoint.Port)), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	io.WriteString(conn, "PING\r\n")
	line, err := bufio.NewReader(conn).ReadString('\n')
	conn.Close()
	if line != "+PONG\r\n" {
		t.Fatalf("PING = %q, %v", line, err)
	}
	ok(t, base, nil, "Action", "DeleteCacheCluster", "CacheClusterId", id)
	eventually(t, "container removal", 30*time.Second, func() bool {
		list, err := d.List(context.Background(), docker.LabelService+"=elasticache", docker.LabelResource+"="+id)
		return err == nil && len(list) == 0
	})
}
