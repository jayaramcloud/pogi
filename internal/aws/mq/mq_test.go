package mq_test

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/mq"
	"github.com/aws/aws-sdk-go-v2/service/mq/types"
	"github.com/aws/smithy-go"
	pogimq "github.com/nrndev/pogi/internal/aws/mq"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	amqp "github.com/rabbitmq/amqp091-go"
)

var ctx = context.Background()

const pw = "local-pass-123"

func start(t *testing.T) (*core.Hub, *pogimq.Service, *mq.Client) {
	var svc *pogimq.Service
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = pogimq.New(h); return svc })
	return h, svc, mq.NewFromConfig(awstest.Config(t, url))
}

func wantCode(t *testing.T, err error, code string) {
	t.Helper()
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func user(name string, console bool, groups ...string) types.User {
	return types.User{Username: aws.String(name), Password: aws.String(pw), ConsoleAccess: aws.Bool(console), Groups: groups}
}

func brokerInput(name, engine, version string, users ...types.User) *mq.CreateBrokerInput {
	return &mq.CreateBrokerInput{BrokerName: aws.String(name), EngineType: types.EngineType(engine), EngineVersion: aws.String(version),
		HostInstanceType: aws.String("mq.t3.micro"), DeploymentMode: types.DeploymentModeSingleInstance, PubliclyAccessible: aws.Bool(false),
		Users: users, Tags: map[string]string{"team": "a"}}
}

func describe(t *testing.T, c *mq.Client, id string) *mq.DescribeBrokerOutput {
	t.Helper()
	d, err := c.DescribeBroker(ctx, &mq.DescribeBrokerInput{BrokerId: aws.String(id)})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestBrokers(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent") // metadata only
	_, svc, c := start(t)

	et, err := c.DescribeBrokerEngineTypes(ctx, &mq.DescribeBrokerEngineTypesInput{EngineType: aws.String("rabbitmq")})
	if err != nil || len(et.BrokerEngineTypes) != 1 || et.BrokerEngineTypes[0].EngineType != types.EngineTypeRabbitmq ||
		*et.BrokerEngineTypes[0].EngineVersions[1].Name != "3.13" {
		t.Fatalf("engine types %+v, %v", et, err)
	}
	opts, err := c.DescribeBrokerInstanceOptions(ctx, &mq.DescribeBrokerInstanceOptionsInput{EngineType: aws.String("ACTIVEMQ"), HostInstanceType: aws.String("mq.m5.large")})
	if err != nil || len(opts.BrokerInstanceOptions) != 1 || len(opts.BrokerInstanceOptions[0].SupportedDeploymentModes) != 2 {
		t.Fatalf("instance options %+v, %v", opts, err)
	}

	out, err := c.CreateBroker(ctx, brokerInput("orders", "RABBITMQ", "3.13", user("admin", false)))
	if err != nil || !strings.HasPrefix(*out.BrokerArn, "arn:aws:mq:us-east-1:000000000000:broker:orders:b-") {
		t.Fatalf("create %+v, %v", out, err)
	}
	id := *out.BrokerId
	d := describe(t, c, id)
	in := d.BrokerInstances
	if d.BrokerState != types.BrokerStateRunning || d.EngineType != "RabbitMQ" || d.AuthenticationStrategy != "simple" || d.StorageType != "ebs" ||
		!*d.AutoMinorVersionUpgrade || d.Configurations.Current == nil || *d.Configurations.Current.Revision != 1 ||
		len(in) != 1 || *in[0].ConsoleURL != "http://127.0.0.1:15672" || !slices.Equal(in[0].Endpoints, []string{"amqp://127.0.0.1:5672"}) ||
		len(d.Users) != 1 || *d.Users[0].Username != "admin" || d.Tags["team"] != "a" || *d.Logs.General || d.Logs.Audit != nil {
		t.Fatalf("describe %+v", d)
	}
	if b, _ := json.Marshal(svc.Inspect("Brokers", "orders")); !strings.Contains(string(b), "Docker is not available") || strings.Contains(string(b), pw) {
		t.Fatalf("inspect %s", b)
	}
	if l, err := c.ListBrokers(ctx, &mq.ListBrokersInput{}); err != nil || len(l.BrokerSummaries) != 1 || *l.BrokerSummaries[0].BrokerId != id {
		t.Fatalf("list %+v, %v", l, err)
	}

	// RabbitMQ keeps the user it was made with; Amazon MQ manages no others
	u, err := c.DescribeUser(ctx, &mq.DescribeUserInput{BrokerId: &id, Username: aws.String("admin")})
	if err != nil || *u.ConsoleAccess || u.Pending != nil {
		t.Fatalf("describe user %+v, %v", u, err)
	}
	_, err = c.CreateUser(ctx, &mq.CreateUserInput{BrokerId: &id, Username: aws.String("other"), Password: aws.String(pw)})
	wantCode(t, err, "BadRequestException")

	for _, bad := range []*mq.CreateBrokerInput{
		brokerInput("x", "RabbitMQ", "3.13", types.User{Username: aws.String("admin"), Password: aws.String("short")}),
		brokerInput("x", "RabbitMQ", "3.13", user("guest", false)),
		brokerInput("x", "RabbitMQ", "3.13", user("first", false), user("second", false)),
		brokerInput("x", "RabbitMQ", "3.8.6", user("admin", false)),
		brokerInput("x", "Kafka", "3.13", user("admin", false)),
		brokerInput("x y", "RabbitMQ", "3.13", user("admin", false)),
		brokerInput("x", "RabbitMQ", "3.13"),
	} {
		_, err = c.CreateBroker(ctx, bad)
		wantCode(t, err, "BadRequestException")
	}
	_, err = c.CreateBroker(ctx, brokerInput("orders", "RabbitMQ", "3.13", user("admin", false)))
	wantCode(t, err, "ConflictException")

	// state survives a restart
	state, err := svc.Save()
	if err != nil {
		t.Fatal(err)
	}
	svc.Reset()
	_, err = c.DescribeBroker(ctx, &mq.DescribeBrokerInput{BrokerId: &id})
	wantCode(t, err, "NotFoundException")
	if err := svc.Load(state); err != nil {
		t.Fatal(err)
	}
	if d := describe(t, c, id); d.BrokerState != types.BrokerStateRunning || *d.BrokerName != "orders" {
		t.Fatalf("after load %+v", d)
	}

	if _, err := c.DeleteBroker(ctx, &mq.DeleteBrokerInput{BrokerId: &id}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeBroker(ctx, &mq.DescribeBrokerInput{BrokerId: &id})
	wantCode(t, err, "NotFoundException")
}

// TestActiveMQUsers checks that user and broker changes wait for a reboot.
func TestActiveMQUsers(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, _, c := start(t)
	out, err := c.CreateBroker(ctx, brokerInput("events", "ACTIVEMQ", "5.18", user("admin", true, "admins"), user("gone", false)))
	if err != nil {
		t.Fatal(err)
	}
	id := out.BrokerId
	d := describe(t, c, *id)
	if d.EngineType != "ActiveMQ" || d.StorageType != "efs" || d.Logs.Audit == nil || len(d.BrokerInstances[0].Endpoints) != 5 ||
		d.BrokerInstances[0].Endpoints[2] != "stomp://127.0.0.1:61613" || *d.BrokerInstances[0].ConsoleURL != "http://127.0.0.1:8161" {
		t.Fatalf("describe %+v", d)
	}

	if _, err := c.CreateUser(ctx, &mq.CreateUserInput{BrokerId: id, Username: aws.String("app"), Password: aws.String(pw), Groups: []string{"apps"}}); err != nil {
		t.Fatal(err)
	}
	_, err = c.CreateUser(ctx, &mq.CreateUserInput{BrokerId: id, Username: aws.String("app"), Password: aws.String(pw)})
	wantCode(t, err, "ConflictException")
	if _, err := c.UpdateUser(ctx, &mq.UpdateUserInput{BrokerId: id, Username: aws.String("admin"), Groups: []string{"ops"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteUser(ctx, &mq.DeleteUserInput{BrokerId: id, Username: aws.String("gone")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.UpdateUser(ctx, &mq.UpdateUserInput{BrokerId: id, Username: aws.String("nobody"), ConsoleAccess: aws.Bool(true)})
	wantCode(t, err, "NotFoundException")
	_, err = c.UpdateUser(ctx, &mq.UpdateUserInput{BrokerId: id, Username: aws.String("admin"), Password: aws.String("a:b:c:d:e:f:g")})
	wantCode(t, err, "BadRequestException")

	l, err := c.ListUsers(ctx, &mq.ListUsersInput{BrokerId: id})
	pending := map[string]types.ChangeType{}
	for _, u := range l.Users {
		pending[*u.Username] = u.PendingChange
	}
	if err != nil || len(pending) != 3 || pending["admin"] != types.ChangeTypeUpdate || pending["app"] != types.ChangeTypeCreate || pending["gone"] != types.ChangeTypeDelete {
		t.Fatalf("list users %+v, %v", pending, err)
	}
	u, err := c.DescribeUser(ctx, &mq.DescribeUserInput{BrokerId: id, Username: aws.String("admin")})
	if err != nil || !*u.ConsoleAccess || !slices.Equal(u.Groups, []string{"admins"}) || u.Pending.PendingChange != types.ChangeTypeUpdate ||
		!slices.Equal(u.Pending.Groups, []string{"ops"}) {
		t.Fatalf("describe user %+v, %v", u, err)
	}

	cf, err := c.CreateConfiguration(ctx, &mq.CreateConfigurationInput{Name: aws.String("events"), EngineType: types.EngineTypeActivemq, EngineVersion: aws.String("5.18")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.UpdateBroker(ctx, &mq.UpdateBrokerInput{BrokerId: id, EngineVersion: aws.String("5.17.6"), HostInstanceType: aws.String("mq.m5.large"),
		Logs: &types.Logs{General: aws.Bool(true)}, SecurityGroups: []string{"sg-1"}, Configuration: &types.ConfigurationId{Id: cf.Id}})
	if err != nil {
		t.Fatal(err)
	}
	d = describe(t, c, *id)
	if *d.EngineVersion != "5.18" || *d.PendingEngineVersion != "5.17.6" || *d.PendingHostInstanceType != "mq.m5.large" || !*d.Logs.Pending.General ||
		*d.Configurations.Pending.Id != *cf.Id || !slices.Equal(d.SecurityGroups, []string{"sg-1"}) {
		t.Fatalf("pending %+v", d)
	}
	_, err = c.UpdateBroker(ctx, &mq.UpdateBrokerInput{BrokerId: id, EngineVersion: aws.String("5.10")})
	wantCode(t, err, "BadRequestException")

	if _, err := c.RebootBroker(ctx, &mq.RebootBrokerInput{BrokerId: id}); err != nil {
		t.Fatal(err)
	}
	d = describe(t, c, *id)
	if d.BrokerState != types.BrokerStateRunning || *d.EngineVersion != "5.17.6" || d.PendingEngineVersion != nil || *d.HostInstanceType != "mq.m5.large" ||
		!*d.Logs.General || d.Logs.Pending != nil || *d.Configurations.Current.Id != *cf.Id || len(d.Configurations.History) != 1 || len(d.Users) != 2 {
		t.Fatalf("after reboot %+v", d)
	}
	for _, u := range d.Users {
		if u.PendingChange != "" {
			t.Fatalf("still pending: %+v", d.Users)
		}
	}
	u, err = c.DescribeUser(ctx, &mq.DescribeUserInput{BrokerId: id, Username: aws.String("admin")})
	if err != nil || !slices.Equal(u.Groups, []string{"ops"}) || u.Pending != nil {
		t.Fatalf("describe user after reboot %+v, %v", u, err)
	}
}

func TestConfigurations(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, _, c := start(t)
	cf, err := c.CreateConfiguration(ctx, &mq.CreateConfigurationInput{Name: aws.String("rabbit"), EngineType: "RabbitMQ", EngineVersion: aws.String("3.13"),
		Tags: map[string]string{"env": "dev"}})
	if err != nil || !strings.HasPrefix(*cf.Arn, "arn:aws:mq:us-east-1:000000000000:configuration:c-") || *cf.LatestRevision.Revision != 1 ||
		cf.AuthenticationStrategy != "simple" {
		t.Fatalf("create %+v, %v", cf, err)
	}
	data := "consumer_timeout = 3600000\n"
	up, err := c.UpdateConfiguration(ctx, &mq.UpdateConfigurationInput{ConfigurationId: cf.Id, Data: aws.String(base64.StdEncoding.EncodeToString([]byte(data))),
		Description: aws.String("longer timeout")})
	if err != nil || *up.LatestRevision.Revision != 2 {
		t.Fatalf("update %+v, %v", up, err)
	}
	d, err := c.DescribeConfiguration(ctx, &mq.DescribeConfigurationInput{ConfigurationId: cf.Id})
	if err != nil || d.EngineType != "RabbitMQ" || *d.EngineVersion != "3.13" || *d.LatestRevision.Revision != 2 || *d.Description != "longer timeout" {
		t.Fatalf("describe %+v, %v", d, err)
	}
	rv, err := c.DescribeConfigurationRevision(ctx, &mq.DescribeConfigurationRevisionInput{ConfigurationId: cf.Id, ConfigurationRevision: aws.String("2")})
	if b, _ := base64.StdEncoding.DecodeString(aws.ToString(rv.Data)); err != nil || string(b) != data {
		t.Fatalf("revision %+v, %v", rv, err)
	}
	rv, err = c.DescribeConfigurationRevision(ctx, &mq.DescribeConfigurationRevisionInput{ConfigurationId: cf.Id, ConfigurationRevision: aws.String("1")})
	if b, _ := base64.StdEncoding.DecodeString(aws.ToString(rv.Data)); err != nil || !strings.Contains(string(b), "consumer_timeout = 1800000") {
		t.Fatalf("default revision %+v, %v", rv, err)
	}
	if l, err := c.ListConfigurationRevisions(ctx, &mq.ListConfigurationRevisionsInput{ConfigurationId: cf.Id}); err != nil || len(l.Revisions) != 2 {
		t.Fatalf("revisions %+v, %v", l, err)
	}
	if l, err := c.ListConfigurations(ctx, &mq.ListConfigurationsInput{}); err != nil || len(l.Configurations) != 1 {
		t.Fatalf("list %+v, %v", l, err)
	}
	_, err = c.DescribeConfigurationRevision(ctx, &mq.DescribeConfigurationRevisionInput{ConfigurationId: cf.Id, ConfigurationRevision: aws.String("3")})
	wantCode(t, err, "NotFoundException")
	_, err = c.UpdateConfiguration(ctx, &mq.UpdateConfigurationInput{ConfigurationId: cf.Id, Data: aws.String("not base64!")})
	wantCode(t, err, "BadRequestException")
	_, err = c.DescribeConfiguration(ctx, &mq.DescribeConfigurationInput{ConfigurationId: aws.String("c-nope")})
	wantCode(t, err, "NotFoundException")

	// tags
	if _, err := c.CreateTags(ctx, &mq.CreateTagsInput{ResourceArn: cf.Arn, Tags: map[string]string{"x": "y"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteTags(ctx, &mq.DeleteTagsInput{ResourceArn: cf.Arn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tags, err := c.ListTags(ctx, &mq.ListTagsInput{ResourceArn: cf.Arn}); err != nil || len(tags.Tags) != 1 || tags.Tags["x"] != "y" {
		t.Fatalf("tags %+v, %v", tags, err)
	}
	_, err = c.ListTags(ctx, &mq.ListTagsInput{ResourceArn: aws.String("arn:aws:mq:us-east-1:000000000000:broker:x:b-nope")})
	wantCode(t, err, "NotFoundException")

	// a broker runs a configuration of its engine, which cannot go while in use
	in := brokerInput("rabbit", "RabbitMQ", "3.13", user("admin", false))
	in.Configuration = &types.ConfigurationId{Id: cf.Id, Revision: aws.Int32(2)}
	out, err := c.CreateBroker(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if d := describe(t, c, *out.BrokerId); *d.Configurations.Current.Id != *cf.Id || *d.Configurations.Current.Revision != 2 {
		t.Fatalf("broker configuration %+v", d.Configurations)
	}
	in.BrokerName, in.EngineType, in.EngineVersion = aws.String("active"), "ActiveMQ", aws.String("5.18")
	_, err = c.CreateBroker(ctx, in)
	wantCode(t, err, "BadRequestException")
	_, err = c.DeleteConfiguration(ctx, &mq.DeleteConfigurationInput{ConfigurationId: cf.Id})
	wantCode(t, err, "ConflictException")
	if _, err := c.DeleteBroker(ctx, &mq.DeleteBrokerInput{BrokerId: out.BrokerId}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteConfiguration(ctx, &mq.DeleteConfigurationInput{ConfigurationId: cf.Id}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeConfiguration(ctx, &mq.DescribeConfigurationInput{ConfigurationId: cf.Id})
	wantCode(t, err, "NotFoundException")
}

// ---- real brokers, when a container engine answers ----

// running waits for a broker to run; the first run pulls its image.
func running(t *testing.T, c *mq.Client, svc *pogimq.Service, id string) *mq.DescribeBrokerOutput {
	t.Helper()
	for end := time.Now().Add(5 * time.Minute); ; time.Sleep(500 * time.Millisecond) {
		d := describe(t, c, id)
		switch d.BrokerState {
		case types.BrokerStateRunning:
			return d
		case types.BrokerStateCreationInProgress, types.BrokerStateRebootInProgress:
		default:
			b, _ := json.Marshal(svc.Inspect("Brokers", id))
			t.Fatalf("broker %s: %s", d.BrokerState, b)
		}
		if time.Now().After(end) {
			b, _ := json.Marshal(svc.Inspect("Brokers", id))
			t.Fatalf("broker %s is still %s: %s", id, d.BrokerState, b)
		}
	}
}

func endpoint(t *testing.T, d *mq.DescribeBrokerOutput, scheme string) string {
	t.Helper()
	for _, e := range d.BrokerInstances[0].Endpoints {
		if a, ok := strings.CutPrefix(e, scheme+"://"); ok {
			return a
		}
	}
	t.Fatalf("no %s endpoint in %v", scheme, d.BrokerInstances[0].Endpoints)
	return ""
}

func gone(t *testing.T, h *core.Hub, c *mq.Client, id string) {
	t.Helper()
	if _, err := c.DeleteBroker(ctx, &mq.DeleteBrokerInput{BrokerId: &id}); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		if _, err := h.Docker().Inspect(ctx, "pogi-mq-"+id); err != nil {
			return
		}
		if time.Now().After(end) {
			t.Fatal("DeleteBroker leaves its container")
		}
	}
}

// get fetches a URL with basic auth and returns the status and body.
func get(t *testing.T, url, user, pass string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	req.SetBasicAuth(user, pass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestRabbitMQInDocker(t *testing.T) {
	h, svc, c := start(t)
	if testing.Short() || !h.Docker().Available() {
		t.Skip("needs a running container engine")
	}
	out, err := c.CreateBroker(ctx, brokerInput("rabbit-"+core.RandHex(3), "RabbitMQ", "3.13", user("admin", false)))
	if err != nil {
		t.Fatal(err)
	}
	id := *out.BrokerId
	d := running(t, c, svc, id)
	addr := endpoint(t, d, "amqp")
	roundTrip := func() {
		t.Helper()
		conn, err := amqp.Dial("amqp://admin:" + pw + "@" + addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		ch, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		q, err := ch.QueueDeclare("", false, true, true, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("hello")}); err != nil {
			t.Fatal(err)
		}
		select {
		case m := <-msgs:
			if string(m.Body) != "hello" {
				t.Fatalf("got %q", m.Body)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("no message")
		}
	}
	roundTrip()
	if _, err := amqp.Dial("amqp://admin:wrong-password-1@" + addr + "/"); err == nil {
		t.Fatal("a wrong password connects")
	}

	// the console is RabbitMQ's management UI and API
	console := *d.BrokerInstances[0].ConsoleURL
	if code, body := get(t, console+"/api/whoami", "admin", pw); code != 200 || !strings.Contains(body, `"name":"admin"`) {
		t.Fatalf("whoami %d %s", code, body)
	}
	if code, _ := get(t, console+"/api/whoami", "admin", "wrong-password-1"); code != 401 {
		t.Fatalf("wrong password: %d", code)
	}

	// persisted state picks the running container up again
	state, err := svc.Save()
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Load(state); err != nil {
		t.Fatal(err)
	}
	if d := running(t, c, svc, id); endpoint(t, d, "amqp") != addr {
		t.Fatalf("endpoint moved: %v", d.BrokerInstances)
	}
	roundTrip()
	gone(t, h, c, id)
}

// stomp connects to a STOMP endpoint and returns the broker's first frame.
func stomp(t *testing.T, addr, login, passcode string) (net.Conn, *bufio.Reader, string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	fmt.Fprintf(conn, "CONNECT\naccept-version:1.2\nhost:/\nlogin:%s\npasscode:%s\n\n\x00", login, passcode)
	r := bufio.NewReader(conn)
	return conn, r, frame(t, r)
}

// frame reads one STOMP frame, skipping heart-beat newlines.
func frame(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	f, err := r.ReadString(0)
	if err != nil {
		t.Fatalf("read frame: %v (%q)", err, f)
	}
	return strings.TrimLeft(f, "\r\n")
}

func TestActiveMQInDocker(t *testing.T) {
	h, svc, c := start(t)
	if testing.Short() || !h.Docker().Available() {
		t.Skip("needs a running container engine")
	}
	out, err := c.CreateBroker(ctx, brokerInput("active-"+core.RandHex(3), "ActiveMQ", "5.18", user("admin", true, "admins")))
	if err != nil {
		t.Fatal(err)
	}
	id := *out.BrokerId
	name := docker.Name("mq", id)
	t.Cleanup(func() { // a failure explains itself: the container's state and last lines (a reboot once hung 5 minutes)
		if !t.Failed() {
			return
		}
		if in, err := h.Docker().Inspect(ctx, name); err == nil {
			t.Logf("container %s: %s, running=%v, exit=%d, out of memory=%v", name, in.State.Status, in.State.Running, in.State.ExitCode, in.State.OOMKilled)
		}
		var lines []string
		h.Docker().Logs(ctx, name, false, func(l string) { lines = append(lines, l) })
		if len(lines) > 40 {
			lines = lines[len(lines)-40:]
		}
		t.Logf("last log lines:\n%s", strings.Join(lines, "\n"))
	})
	d := running(t, c, svc, id)
	addr := endpoint(t, d, "stomp")

	conn, r, f := stomp(t, addr, "admin", pw)
	if !strings.HasPrefix(f, "CONNECTED") {
		t.Fatalf("connect: %q", f)
	}
	fmt.Fprint(conn, "SUBSCRIBE\nid:0\ndestination:/queue/pogi\nack:auto\n\n\x00")
	fmt.Fprint(conn, "SEND\ndestination:/queue/pogi\ncontent-type:text/plain\n\nhello\x00")
	if f := frame(t, r); !strings.HasPrefix(f, "MESSAGE") || !strings.HasSuffix(f, "\n\nhello\x00") {
		t.Fatalf("message: %q", f)
	}
	if _, _, f := stomp(t, addr, "admin", "wrong-password-1"); !strings.HasPrefix(f, "ERROR") {
		t.Fatalf("a wrong password: %q", f)
	}

	// users with console access sign in to the web console
	console := *d.BrokerInstances[0].ConsoleURL
	if code, _ := get(t, console+"/admin/", "admin", pw); code != 200 {
		t.Fatalf("console: %d", code)
	}
	if code, _ := get(t, console+"/admin/", "admin", "wrong-password-1"); code != 401 {
		t.Fatalf("console, wrong password: %d", code)
	}

	// a new user signs in once a reboot applies it, on the same endpoint
	if _, err := c.CreateUser(ctx, &mq.CreateUserInput{BrokerId: &id, Username: aws.String("app"), Password: aws.String(pw)}); err != nil {
		t.Fatal(err)
	}
	if _, _, f := stomp(t, addr, "app", pw); !strings.HasPrefix(f, "ERROR") {
		t.Fatalf("a pending user connects: %q", f)
	}
	if _, err := c.RebootBroker(ctx, &mq.RebootBrokerInput{BrokerId: &id}); err != nil {
		t.Fatal(err)
	}
	if d := running(t, c, svc, id); endpoint(t, d, "stomp") != addr {
		t.Fatalf("endpoint moved: %v", d.BrokerInstances)
	}
	if _, _, f := stomp(t, addr, "app", pw); !strings.HasPrefix(f, "CONNECTED") {
		t.Fatalf("connect as app after reboot: %q", f)
	}

	// a container that stops on its own (out of memory, a crash) makes the broker CRITICAL_ACTION_REQUIRED, saying why
	if err := h.Docker().Stop(ctx, name); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(30 * time.Second); ; time.Sleep(250 * time.Millisecond) {
		if d := describe(t, c, id); d.BrokerState == types.BrokerStateCriticalActionRequired {
			b, _ := json.Marshal(svc.Inspect("Brokers", id))
			if !strings.Contains(string(b), "stopped: exit") {
				t.Fatalf("a stopped broker should say why: %s", b)
			}
			break
		}
		if time.Now().After(end) {
			t.Fatalf("broker %s is still %s after its container stopped", id, describe(t, c, id).BrokerState)
		}
	}
	gone(t, h, c, id)
}
