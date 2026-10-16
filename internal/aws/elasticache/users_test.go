package elasticache_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/nrndev/pogi/internal/aws/elasticache"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/docker/dockertest"
)

type user struct {
	UserId, UserName, AccessString, ARN string
	Type                                string   `xml:"Authentication>Type"`
	PasswordCount                       int      `xml:"Authentication>PasswordCount"`
	Groups                              []string `xml:"UserGroupIds>member"`
}

type userGroup struct {
	UserGroupId, Status string
	UserIds             []string `xml:"UserIds>member"`
	ReplicationGroups   []string `xml:"ReplicationGroups>ReplicationGroup"` // rds.Lists renames them all
}

// ures is any users or user groups response.
type ures struct {
	R struct {
		user
		Users      []user      `xml:"Users>member"`
		UserGroups []userGroup `xml:"UserGroups>member"`
		Group      struct {
			ReplicationGroupId string
			UserGroupIds       []string `xml:"UserGroupIds>member"`
			Endpoint           endpoint `xml:"NodeGroups>NodeGroup>PrimaryEndpoint"`
		} `xml:"ReplicationGroup"`
	} `xml:",any"`
}

var pw16 = "0123456789abcdef"

func TestUsersAndUserGroups(t *testing.T) {
	_, _, base := setup(t, true)
	var r ures
	ok(t, base, &r, "Action", "CreateUser", "UserId", "app", "UserName", "app", "Engine", "valkey", "AccessString", "on ~app:* +@all",
		"Passwords.member.1", pw16, "Tags.Tag.1.Key", "team", "Tags.Tag.1.Value", "core")
	if u := r.R.user; u.UserId != "app" || u.Type != "password" || u.PasswordCount != 1 || u.ARN != "arn:aws:elasticache:us-east-1:000000000000:user:app" {
		t.Fatalf("created %+v", u)
	}
	ok(t, base, &r, "Action", "CreateUser", "UserId", "robot", "UserName", "robot", "Engine", "redis", "AccessString", "on ~* +@all", "AuthenticationMode.Type", "iam")
	if u := r.R.user; u.Type != "iam" || u.PasswordCount != 0 {
		t.Fatalf("iam user %+v", u)
	}
	for _, c := range []struct {
		status int
		code   string
		args   []string
	}{
		{400, "UserAlreadyExists", []string{"UserId", "app", "UserName", "x", "Engine", "redis", "AccessString", "on", "NoPasswordRequired", "true"}},
		{400, "UserAlreadyExists", []string{"UserId", "default", "UserName", "x", "Engine", "redis", "AccessString", "on", "NoPasswordRequired", "true"}},
		{400, "InvalidParameterValue", []string{"UserId", "bot", "UserName", "robot", "Engine", "redis", "AccessString", "on", "AuthenticationMode.Type", "iam"}},
		{400, "InvalidParameterValue", []string{"UserId", "short", "UserName", "short", "Engine", "redis", "AccessString", "on", "Passwords.member.1", "short"}},
		{400, "InvalidParameterValue", []string{"UserId", "mc", "UserName", "mc", "Engine", "memcached", "AccessString", "on", "NoPasswordRequired", "true"}},
		{400, "InvalidParameterCombination", []string{"UserId", "nopw", "UserName", "nopw", "Engine", "redis", "AccessString", "on"}},
	} {
		fails(t, base, c.status, c.code, append([]string{"Action", "CreateUser"}, c.args...)...)
	}
	ok(t, base, &r, "Action", "DescribeUsers")
	if ids := []string{r.R.Users[0].UserId, r.R.Users[1].UserId, r.R.Users[2].UserId}; len(r.R.Users) != 3 || !slices.Equal(ids, []string{"app", "default", "robot"}) {
		t.Fatalf("users %+v", r.R.Users)
	}
	fails(t, base, 404, "UserNotFound", "Action", "DescribeUsers", "UserId", "nope")

	fails(t, base, 400, "DefaultUserRequired", "Action", "CreateUserGroup", "UserGroupId", "team", "Engine", "valkey", "UserIds.member.1", "app")
	fails(t, base, 404, "UserNotFound", "Action", "CreateUserGroup", "UserGroupId", "team", "Engine", "valkey", "UserIds.member.1", "default", "UserIds.member.2", "ghost")
	ok(t, base, nil, "Action", "CreateUser", "UserId", "closed", "UserName", "default", "Engine", "valkey", "AccessString", "off", "NoPasswordRequired", "true")
	fails(t, base, 400, "DuplicateUserName", "Action", "CreateUserGroup", "UserGroupId", "team", "Engine", "valkey", "UserIds.member.1", "default", "UserIds.member.2", "closed")
	group := []string{"Action", "CreateUserGroup", "UserGroupId", "Team", "Engine", "valkey", "UserIds.member.1", "default", "UserIds.member.2", "app"}
	ok(t, base, nil, group...)
	fails(t, base, 400, "UserGroupAlreadyExists", group...)
	fails(t, base, 404, "UserGroupNotFound", "Action", "CreateReplicationGroup", "ReplicationGroupId", "rg", "ReplicationGroupDescription", "x",
		"UserGroupIds.member.1", "nope")
	ok(t, base, &r, "Action", "CreateReplicationGroup", "ReplicationGroupId", "rg", "ReplicationGroupDescription", "x", "Engine", "valkey",
		"UserGroupIds.member.1", "team")
	if !slices.Equal(r.R.Group.UserGroupIds, []string{"team"}) {
		t.Fatalf("replication group %+v", r.R.Group)
	}
	ok(t, base, &r, "Action", "ModifyUserGroup", "UserGroupId", "team", "UserIdsToAdd.member.1", "robot")
	ok(t, base, &r, "Action", "DescribeUserGroups", "UserGroupId", "team")
	if g := r.R.UserGroups; len(g) != 1 || !slices.Equal(g[0].UserIds, []string{"app", "default", "robot"}) || !slices.Equal(g[0].ReplicationGroups, []string{"rg"}) {
		t.Fatalf("user groups %+v", g)
	}
	fails(t, base, 400, "DefaultUserRequired", "Action", "ModifyUserGroup", "UserGroupId", "team", "UserIdsToRemove.member.1", "default")
	fails(t, base, 400, "InvalidUserGroupState", "Action", "DeleteUserGroup", "UserGroupId", "team")
	ok(t, base, &r, "Action", "DescribeUsers", "UserId", "app")
	if u := r.R.Users; len(u) != 1 || !slices.Equal(u[0].Groups, []string{"team"}) {
		t.Fatalf("user groups of app %+v", u)
	}
	ok(t, base, &r, "Action", "ModifyUser", "UserId", "app", "AppendAccessString", "-flushall", "Passwords.member.1", pw16, "Passwords.member.2", pw16+"2")
	if u := r.R.user; u.AccessString != "on ~app:* +@all -flushall" || u.PasswordCount != 2 {
		t.Fatalf("modified %+v", u)
	}
	fails(t, base, 400, "InvalidParameterValue", "Action", "ModifyUser", "UserId", "default", "AccessString", "off")
	ok(t, base, &r, "Action", "ListTagsForResource", "ResourceName", "arn:aws:elasticache:us-east-1:000000000000:user:app")

	ok(t, base, nil, "Action", "DeleteUser", "UserId", "app")
	ok(t, base, &r, "Action", "DescribeUserGroups", "UserGroupId", "team")
	if !slices.Equal(r.R.UserGroups[0].UserIds, []string{"default", "robot"}) {
		t.Fatalf("a deleted user leaves its groups: %+v", r.R.UserGroups)
	}
	ok(t, base, &r, "Action", "ModifyReplicationGroup", "ReplicationGroupId", "rg", "RemoveUserGroups", "true")
	if len(r.R.Group.UserGroupIds) != 0 {
		t.Fatalf("removed groups %+v", r.R.Group)
	}
	ok(t, base, nil, "Action", "DeleteUserGroup", "UserGroupId", "team")
	fails(t, base, 404, "UserGroupNotFound", "Action", "DescribeUserGroups", "UserGroupId", "team")
}

// fakeRedis answers +OK to every command and records the AUTH and HELLO ones.
type fakeRedis struct {
	mu   sync.Mutex
	auth []string
}

func (f *fakeRedis) serve(t *testing.T) int {
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
				r := bufio.NewReader(c)
				for {
					args, err := readArray(r)
					if err != nil {
						return
					}
					if len(args) > 0 && (args[0] == "AUTH" || args[0] == "HELLO") {
						f.mu.Lock()
						f.auth = append(f.auth, strings.Join(args, " "))
						f.mu.Unlock()
					}
					io.WriteString(c, "+OK\r\n")
				}
			}()
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}

func (f *fakeRedis) last() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.auth) == 0 {
		return ""
	}
	return f.auth[len(f.auth)-1]
}

// readArray reads a RESP array of bulk strings (or an inline command).
func readArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return strings.Fields(line), nil
	}
	n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
	args := make([]string, n)
	for i := range args {
		h, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, _ := strconv.Atoi(strings.TrimSpace(h[1:]))
		b := make([]byte, size+2)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		args[i] = string(b[:size])
	}
	return args, nil
}

// send writes a command as a RESP array and reads one reply line.
func send(t *testing.T, c net.Conn, r *bufio.Reader, args ...string) string {
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
	if strings.HasPrefix(line, "$") { // a bulk string
		line, _ = r.ReadString('\n')
	}
	return strings.TrimSpace(line)
}

// token is an ElastiCache IAM auth token, made the way AWS's samples make it.
func token(t *testing.T, cache, user, secret string, at time.Time) string {
	req, _ := http.NewRequest(http.MethodGet, "http://"+cache+"/", nil)
	req.URL.RawQuery = url.Values{"Action": {"connect"}, "User": {user}, "X-Amz-Expires": {"900"}}.Encode()
	empty := sha256.Sum256(nil)
	u, _, err := v4.NewSigner().PresignHTTP(context.Background(), aws.Credentials{AccessKeyID: "test", SecretAccessKey: secret},
		req, hex.EncodeToString(empty[:]), "elasticache", "us-east-1", at)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimPrefix(u, "http://")
}

// TestIAMProxy runs the IAM proxy in front of a fake server that records
// what it is asked to AUTH with: a valid token becomes the user's password
// in the server, anything else passes as is.
func TestIAMProxy(t *testing.T) {
	var redis fakeRedis
	f := dockertest.Start(t, redis.serve(t), false)
	h := core.NewHub(core.Config{ValidateSigs: true}, nil)
	h.Register(elasticache.New(h))
	base := core.Listen(t, h)
	t.Cleanup(h.Close)
	ok(t, base, nil, "Action", "CreateUser", "UserId", "robot", "UserName", "robot", "Engine", "valkey", "AccessString", "on ~* +@all", "AuthenticationMode.Type", "iam")
	ok(t, base, nil, "Action", "CreateUser", "UserId", "app", "UserName", "app", "Engine", "valkey", "AccessString", "on ~* +@all", "Passwords.member.1", pw16)
	ok(t, base, nil, "Action", "CreateUserGroup", "UserGroupId", "team", "Engine", "valkey", "UserIds.member.1", "default", "UserIds.member.2", "robot",
		"UserIds.member.3", "app")
	ok(t, base, nil, "Action", "CreateReplicationGroup", "ReplicationGroupId", "cart", "ReplicationGroupDescription", "x", "Engine", "valkey", "UserGroupIds.member.1", "team")
	var g group
	eventually(t, "group available", 10*time.Second, func() bool { g = describeGroup(t, base, "cart"); return g.Status == "available" })
	port := g.NodeGroups[0].Primary.Port
	if port == f.Port || port == 0 {
		t.Fatalf("IAM users need the proxy, got port %d (server %d)", port, f.Port)
	}
	script := strings.Join(f.Called("exec pogi-elasticache-cart sh -c"), "")
	for _, want := range []string{"valkey-cli ACL SETUSER pogi-admin on '>", "ACL SETUSER 'app' reset 'on' '~*' '+@all' '>" + pw16 + "'",
		"ACL SETUSER 'default' reset 'on' '~*' '+@all' nopass", "ACL SETUSER 'robot' reset 'on' '~*' '+@all' '>"} {
		if !strings.Contains(script, want) {
			t.Fatalf("missing %q in the users script %q", want, script)
		}
	}

	c, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := bufio.NewReader(c)
	now := time.Now()
	good := token(t, "cart", "robot", "test", now)
	for _, tc := range []struct {
		user, cred string
		hello      bool
		swapped    bool
	}{
		{"robot", good, false, true},
		{"robot", good, true, true},
		{"app", pw16, false, false},                                                          // a password user
		{"robot", token(t, "cart", "robot", "wrong", now), false, false},                     // a bad signature
		{"robot", token(t, "other", "robot", "test", now), false, false},                     // another cache
		{"app", token(t, "cart", "app", "test", now), false, false},                          // not an IAM user
		{"robot", token(t, "cart", "robot", "test", now.Add(-16*time.Minute)), false, false}, // expired
		{"robot", "not-a-token", false, false},
	} {
		args := []string{"AUTH", tc.user, tc.cred}
		if tc.hello {
			args = []string{"HELLO", "3", "AUTH", tc.user, tc.cred, "SETNAME", "x"}
		}
		if got := send(t, c, r, args...); got != "+OK" {
			t.Fatalf("%v: %s", args, got)
		}
		if sent := redis.last(); strings.Contains(sent, tc.cred) == tc.swapped || !strings.HasPrefix(sent, args[0]) {
			t.Fatalf("%v reached the server as %q", args, sent)
		}
	}
	if got := send(t, c, r, "GET", "k"); got != "+OK" { // everything else passes
		t.Fatalf("GET: %s", got)
	}

	ok(t, base, nil, "Action", "ModifyUserGroup", "UserGroupId", "team", "UserIdsToRemove.member.1", "robot")
	if g = describeGroup(t, base, "cart"); g.NodeGroups[0].Primary.Port != f.Port {
		t.Fatalf("without IAM users the proxy goes: %+v", g.NodeGroups[0])
	}
}

// TestIAMAuthInDocker signs in to a real Valkey through the proxy.
func TestIAMAuthInDocker(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	_, base := core.ServeTest(t, func(h *core.Hub) core.Service { return elasticache.New(h) })
	id := fmt.Sprintf("iam-%d", os.Getpid())
	ok(t, base, nil, "Action", "CreateUser", "UserId", "robot", "UserName", "robot", "Engine", "valkey", "AccessString", "on ~* +@all", "AuthenticationMode.Type", "iam")
	ok(t, base, nil, "Action", "CreateUser", "UserId", "app", "UserName", "app", "Engine", "valkey", "AccessString", "on ~app:* +@all", "Passwords.member.1", pw16)
	ok(t, base, nil, "Action", "CreateUser", "UserId", "closed", "UserName", "default", "Engine", "valkey", "AccessString", "off", "NoPasswordRequired", "true")
	ok(t, base, nil, "Action", "CreateUserGroup", "UserGroupId", "team", "Engine", "valkey", "UserIds.member.1", "closed", "UserIds.member.2", "robot", "UserIds.member.3", "app")
	ok(t, base, nil, "Action", "CreateReplicationGroup", "ReplicationGroupId", id, "ReplicationGroupDescription", "x", "Engine", "valkey", "UserGroupIds.member.1", "team")
	var g group
	eventually(t, "valkey to start", 5*time.Minute, func() bool {
		if g = describeGroup(t, base, id); g.Status == "create-failed" {
			t.Fatalf("container failed: %+v", g)
		}
		return g.Status == "available"
	})
	dial := func() (net.Conn, *bufio.Reader) {
		c, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(g.NodeGroups[0].Primary.Port))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c, bufio.NewReader(c)
	}
	c, r := dial()
	eventually(t, "the users", 30*time.Second, func() bool { return send(t, c, r, "AUTH", "robot", token(t, id, "robot", "test", time.Now())) == "+OK" })
	if got := send(t, c, r, "SET", "k", "v"); got != "+OK" {
		t.Fatalf("SET as robot: %s", got)
	}
	if got := send(t, c, r, "AUTH", "robot", token(t, id, "robot", "test", time.Now().Add(-20*time.Minute))); !strings.HasPrefix(got, "-WRONGPASS") {
		t.Fatalf("an expired token: %s", got)
	}
	c, r = dial()
	if got := send(t, c, r, "GET", "k"); !strings.HasPrefix(got, "-NOAUTH") {
		t.Fatalf("the default user is off: %s", got)
	}
	if got := send(t, c, r, "HELLO", "2", "AUTH", "robot", token(t, id, "robot", "test", time.Now())); !strings.HasPrefix(got, "*") {
		t.Fatalf("HELLO with a token: %s", got)
	}
	c, r = dial()
	if got := send(t, c, r, "AUTH", "app", pw16); got != "+OK" {
		t.Fatalf("a password user: %s", got)
	}
	if got := send(t, c, r, "SET", "app:k", "v"); got != "+OK" {
		t.Fatalf("SET as app: %s", got)
	}
	if got := send(t, c, r, "GET", "k"); !strings.HasPrefix(got, "-NOPERM") {
		t.Fatalf("app's keys only: %s", got)
	}
	ok(t, base, nil, "Action", "DeleteReplicationGroup", "ReplicationGroupId", id)
}

// floci #4502: an IAM user needs Redis 7 or newer.
func TestIAMUserNeedsRedis7(t *testing.T) {
	_, _, base := setup(t, true)
	ok(t, base, nil, "Action", "CreateUser", "UserId", "iamu", "UserName", "iamu", "Engine", "redis", "AccessString", "on ~* +@all", "AuthenticationMode.Type", "iam")
	ok(t, base, nil, "Action", "CreateUserGroup", "UserGroupId", "ug", "Engine", "redis", "UserIds.member.1", "default", "UserIds.member.2", "iamu")
	rg := []string{"Action", "CreateReplicationGroup", "ReplicationGroupDescription", "d", "Engine", "redis", "NumCacheClusters", "1", "UserGroupIds.member.1", "ug"}
	fails(t, base, 400, "InvalidParameterCombination", append(rg, "ReplicationGroupId", "g62", "EngineVersion", "6.2")...)
	ok(t, base, nil, append(rg, "ReplicationGroupId", "g7", "EngineVersion", "7.1")...)
}
