package mq

// Brokers in containers: one per broker, pogi-mq-<broker id>, its listeners
// published on fixed 127.0.0.1 ports so that a reboot keeps its endpoints.

import (
	"archive/tar"
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// noDocker is the note of a broker created while Docker was not available.
const noDocker = "Docker is not available: the API works, but no broker runs behind these endpoints"

// engine is how pogi runs one broker engine.
type engine struct {
	versions  [][2]string    // version and image, newest first; the first is the default
	modes     []string       // deployment modes
	storage   string         // default storage type
	listeners []listener     // published on 127.0.0.1: the endpoints, then the web console (http)
	hello     map[int]string // ports that reply once the broker is up, and what they are sent
}

// listener is a port the broker listens on and the scheme of its URL.
type listener struct {
	scheme string
	port   int
}

const httpGet = "GET / HTTP/1.0\r\n\r\n"

// engines are the broker engines on offer, by the names DescribeBroker uses.
var engines = map[string]*engine{
	"ActiveMQ": {
		versions:  [][2]string{{"5.18", "apache/activemq-classic:5.18.7"}, {"5.17.6", "apache/activemq-classic:5.17.6"}},
		modes:     []string{"SINGLE_INSTANCE", "ACTIVE_STANDBY_MULTI_AZ"},
		storage:   "efs",
		listeners: []listener{{"tcp", 61616}, {"amqp", 5672}, {"stomp", 61613}, {"mqtt", 1883}, {"ws", 61614}, {"http", 8161}},
		hello:     map[int]string{61616: "", 8161: httpGet}, // OpenWire speaks first; the console starts last
	},
	"RabbitMQ": {
		versions:  [][2]string{{"4.2", "rabbitmq:4.2-management-alpine"}, {"3.13", "rabbitmq:3-management-alpine"}},
		modes:     []string{"SINGLE_INSTANCE", "CLUSTER_MULTI_AZ"},
		storage:   "ebs",
		listeners: []listener{{"amqp", 5672}, {"http", 15672}},
		hello:     map[int]string{5672: "AMQP\x00\x00\x09\x01", 15672: httpGet},
	},
}

// engineName is AWS's name of an engine type given in any case, or "".
func engineName(s string) string {
	for n := range engines {
		if strings.EqualFold(n, s) {
			return n
		}
	}
	return ""
}

// image is the image of a listed version or of a patch release of one, or "".
func (e *engine) image(v string) string {
	for _, x := range e.versions {
		if v == x[0] || strings.HasPrefix(v, x[0]+".") {
			return x[1]
		}
	}
	return ""
}

// image is what a broker runs: docker.images "mq.rabbitmq" or "mq.activemq"
// in pogi.yaml, else the image of its engine version.
func (s *Service) image(b *Broker) string {
	eng := str(b.Doc["engineType"])
	return cmp.Or(s.hub.Cfg.DockerImages["mq."+strings.ToLower(eng)], engines[eng].image(str(b.Doc["engineVersion"])))
}

// amqConf is where the ActiveMQ image keeps its configuration.
const amqConf = "/opt/apache-activemq/conf"

// jaas runs ActiveMQ with logins checked against users.properties and
// groups.properties, adding the JAAS plugin to the image's activemq.xml once.
const jaas = `grep -q jaasAuthenticationPlugin conf/activemq.xml || ` +
	`sed -i 's|</broker>|<plugins><jaasAuthenticationPlugin configuration="activemq"/></plugins></broker>|' conf/activemq.xml; ` +
	`exec activemq console`

// boot runs the container of a broker in the background and makes the
// broker RUNNING once its ports reply. Verb "run" makes a fresh container,
// "start" and "restart" reuse the one it has (or make one when it is gone).
// Lock held.
func (s *Service) boot(k string, b *Broker, verb string) {
	if b.cancel != nil {
		b.cancel()
	}
	ctx, cancel := context.WithCancel(s.ctx)
	b.cancel = cancel
	e := engines[str(b.Doc["engineType"])]
	b.Image = s.image(b)
	ct := &docker.Container{Name: b.Container, Image: b.Image, Service: "mq", Resource: str(b.Doc["brokerId"]), Ports: map[int]int{}}
	for _, l := range e.listeners {
		ct.Ports[l.port] = b.Ports[l.port] // the endpoints it had, if they are still free
	}
	var files []byte
	if b.Doc["engineType"] == "ActiveMQ" {
		ct.WorkDir, ct.Cmd, files = "/opt/apache-activemq", []string{"sh", "-c", jaas}, b.logins()
	}
	for n, u := range b.Users { // RabbitMQ's one user, made at its first start
		if b.Doc["engineType"] == "RabbitMQ" && u.Live != nil {
			ct.Env = []string{"RABBITMQ_DEFAULT_USER=" + n, "RABBITMQ_DEFAULT_PASS=" + u.Live.Password}
		}
	}
	d := s.hub.Docker()
	core.Go("mq.boot", func() {
		defer cancel()
		ports, err := run(ctx, d, ct, files, verb)
		if err == nil {
			err = answer(ctx, d, ports, e.hello)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		switch {
		case s.st.Brokers[k] != b: // deleted meanwhile
			s.drop(b)
		case ctx.Err() != nil: // pogi closes, or a newer boot took over
		case err != nil:
			failed := "CRITICAL_ACTION_REQUIRED"
			if b.State == "CREATION_IN_PROGRESS" {
				failed = "CREATION_FAILED"
			}
			b.State, b.Note = failed, err.Error()
			s.hub.Log.Warn("mq broker failed", "broker", k, "err", err)
		default:
			b.State, b.Ports, b.Note = "RUNNING", ports, ""
		}
	})
}

// run starts a broker's container and returns the host ports it publishes.
func run(ctx context.Context, d *docker.Client, ct *docker.Container, files []byte, verb string) (map[int]int, error) {
	if verb == "run" || !reuse(ctx, d, ct.Name, files, verb) {
		if err := fresh(ctx, d, ct, files); err != nil {
			return nil, err
		}
	}
	info, err := d.Inspect(ctx, ct.Name)
	if err != nil {
		return nil, err
	}
	ports := map[int]int{}
	for p := range ct.Ports {
		ports[p] = info.HostPort(p)
	}
	return ports, nil
}

// reuse starts or restarts the container there is, with fresh login files.
func reuse(ctx context.Context, d *docker.Client, name string, files []byte, verb string) bool {
	if files != nil && d.CopyTo(ctx, name, amqConf, bytes.NewReader(files)) != nil {
		return false
	}
	start := d.Start
	if verb == "restart" {
		start = d.Restart
	}
	return start(ctx, name) == nil
}

// fresh makes the container anew on the ports it asks for, or on any free
// ones when those are taken.
func fresh(ctx context.Context, d *docker.Client, ct *docker.Container, files []byte) error {
	var err error
	for try := range 2 {
		d.Remove(ctx, ct.Name) // a leftover, or the container being replaced
		for p, h := range ct.Ports {
			if try > 0 {
				h = 0 // a port picked free was taken by the time the engine bound it
			}
			ct.Ports[p] = freePort(h)
		}
		if _, err = d.Create(ctx, ct); err == nil && files != nil {
			err = d.CopyTo(ctx, ct.Name, amqConf, bytes.NewReader(files))
		}
		if err == nil {
			if err = d.Start(ctx, ct.Name); err == nil {
				return nil
			}
		}
	}
	return err
}

// answer waits until every port in hello replies to what it is sent. Docker
// Desktop accepts connections before the server listens, then drops them, so
// a reply is required.
func answer(ctx context.Context, d *docker.Client, ports map[int]int, hello map[int]string) error {
	end := time.Now().Add(5 * time.Minute)
	for p, h := range hello {
		addr := d.Addr(ports[p])
		for !replies(addr, h) {
			if time.Now().After(end) {
				return fmt.Errorf("nothing answered on %s within 5 minutes (docker logs tells why)", addr)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
	}
	return nil
}

func replies(addr, hello string) bool {
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	io.WriteString(c, hello)
	n, _ := c.Read(make([]byte, 1))
	return n > 0
}

// logins is a tar of the files the ActiveMQ image checks logins against: the
// broker's (users.properties, groups.properties) and its web console's
// (jetty-realm.properties, the users with console access). Pending changes
// are left out until a reboot applies them.
// ponytail: passwords are written as ASCII; escape \uXXXX if someone needs others.
func (b *Broker) logins() []byte {
	var users, realm, groups strings.Builder
	members := map[string][]string{}
	for _, n := range core.SortedKeys(b.Users) {
		l := b.Users[n].Live
		if l == nil {
			continue
		}
		pw := strings.ReplaceAll(l.Password, `\`, `\\`)
		fmt.Fprintf(&users, "%s=%s\n", n, pw)
		if l.ConsoleAccess {
			fmt.Fprintf(&realm, "%s: %s, admin\n", n, pw)
		}
		for _, g := range l.Groups {
			members[g] = append(members[g], n)
		}
	}
	for _, g := range core.SortedKeys(members) {
		fmt.Fprintf(&groups, "%s=%s\n", g, strings.Join(members[g], ","))
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, f := range [][2]string{{"users.properties", users.String()}, {"groups.properties", groups.String()}, {"jetty-realm.properties", realm.String()}} {
		tw.WriteHeader(&tar.Header{Name: f[0], Mode: 0o644, Size: int64(len(f[1]))})
		io.WriteString(tw, f[1])
	}
	tw.Close()
	return buf.Bytes()
}

// freePort returns want when it is free on 127.0.0.1, else any free port.
func freePort(want int) int {
	ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(want)))
	if err != nil {
		ln, err = net.Listen("tcp", "127.0.0.1:0")
	}
	if err != nil {
		return 0 // the engine picks one
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// drop stops the container work of a broker that is gone and removes its
// container, unless a current broker runs in it. Lock held.
func (s *Service) drop(b *Broker) {
	if b.cancel != nil {
		b.cancel()
	}
	for _, x := range s.st.Brokers {
		if x.Container == b.Container {
			return
		}
	}
	if b.Container != "" {
		go s.hub.Docker().Remove(context.Background(), b.Container)
	}
}

// Close stops container work and removes the brokers' containers, unless
// state is persisted: the next run starts them again.
func (s *Service) Close() error {
	s.stop()
	if cfg := s.hub.Cfg; cfg.Persist || cfg.Storage != "" && cfg.Storage != "memory" {
		return nil
	}
	s.mu.Lock()
	var names []string
	for _, b := range s.st.Brokers {
		if b.Container != "" {
			names = append(names, b.Container)
		}
	}
	s.mu.Unlock()
	for _, n := range names {
		s.hub.Docker().Remove(context.Background(), n)
	}
	return nil
}
