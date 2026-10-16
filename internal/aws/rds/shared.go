package rds

// Helpers shared with internal/aws/elasticache: database containers run by
// the hub's container engine client, and the Query dialect both APIs speak.

import (
	"archive/tar"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// NoDocker is the note of a resource created while Docker was not available.
const NoDocker = "Docker is not available: the API works, but no database runs behind this endpoint"

// Spec is a database container.
type Spec struct {
	Name  string // container name
	Image string
	Port  int      // port the server listens on inside the container
	Host  int      // preferred host port; 0 means any free one
	Env   []string // KEY=value
	Args  []string // after the image; "{port}" becomes the host port
	Entry []string // replaces the image's entrypoint when set
	Hello string   // what a client sends first ("" when the server speaks first); any reply means ready
	Init  []string // command run in the container once the server answers (errors ignored)
}

// Server is pogi's side of a database resource: its container and how to
// reach it. Resources keep it in a Pogi field tagged json:"pogi" query:"-"
// (not embedded: core flattens embedded structs into API responses), so it
// shows in Save and Inspect only.
type Server struct {
	Container string `json:"container,omitempty"` // empty: created without Docker, nothing runs
	State     string `json:"state"`
	HostPort  int    `json:"hostPort,omitempty"`  // preferred until the container runs, then the real one
	Proxy     int    `json:"proxyPort,omitempty"` // pogi's proxy in front of HostPort (RDS IAM auth): clients connect here
	Seed      string `json:"seed,omitempty"`      // SQL file the next fresh container loads at init, once (restores)
	Password  string `json:"masterPassword,omitempty"`
	Note      string `json:"pogiNote,omitempty"`
}

// Addr is where clients connect, or nil while the server is being created.
func (s *Server) Addr() *Endpoint {
	if s.HostPort == 0 || s.State == "creating" {
		return nil
	}
	return &Endpoint{Address: "localhost", Port: cmp.Or(s.Proxy, s.HostPort)}
}

// Box runs the containers of one service. Engine calls for a container run
// in the background, one at a time and in API order (create, stop, delete);
// the service lock is never held while the engine works.
type Box struct {
	hub    *core.Hub
	d      *docker.Client
	label  string             // pogi.service label and name prefix
	fail   string             // status of a resource whose container failed
	lock   *sync.Mutex        // the service lock: it guards every Server
	live   func(*Server) bool // lock held: the Server still belongs to a resource
	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	tail  map[string]chan struct{} // last queued call per container
	names map[string]bool          // every container touched, removed on Close
}

// NewBox creates the container runner of a service.
func NewBox(hub *core.Hub, label, fail string, lock *sync.Mutex, live func(*Server) bool) *Box {
	ctx, cancel := context.WithCancel(context.Background())
	return &Box{hub: hub, d: hub.Docker(), label: label, fail: fail, lock: lock, live: live, ctx: ctx, cancel: cancel,
		tail: map[string]chan struct{}{}, names: map[string]bool{}}
}

// Available reports whether a container engine answers (one that starts
// after pogi is found later).
func (b *Box) Available() bool { return b.d.Available() }

// Name is the container name of a resource, pogi-<label>-<id>, qualified
// outside the default account and region so they never collide.
func (b *Box) Name(account, region, id string) string {
	n := docker.Name(b.label, id)
	if account != core.DefaultAccount || region != b.hub.Cfg.Region {
		n += "-" + account + "-" + region
	}
	return n
}

// Up starts the container of srv in the background: verb "run" makes a fresh
// one, "start" and "restart" reuse it (or run a fresh one when it is gone).
// srv becomes "available" once the server answers. Call with the lock held.
func (b *Box) Up(srv *Server, sp Spec, verb string) {
	sp.Name = srv.Container
	b.serial(sp.Name, func() {
		b.lock.Lock()
		seed := srv.Seed // read here: a restore sets it right after Up, in the same lock hold
		b.lock.Unlock()
		port, err := b.up(sp, verb, seed)
		core.Go("rds.Up", func() { // waiting for the server must not hold up a delete
			if err == nil {
				err = b.wait(srv, port, sp.Hello)
			}
			if err == nil && len(sp.Init) > 0 {
				b.exec(sp.Name, sp.Init...)
			}
			if err != nil && b.ctx.Err() == nil {
				b.hub.Log.Warn("database container failed", "container", sp.Name, "err", err)
			}
			b.done(srv, func() {
				if err != nil {
					srv.State, srv.Note = b.fail, err.Error()
					return
				}
				srv.State, srv.HostPort, srv.Note, srv.Seed = "available", port, "", ""
			})
		})
	})
}

// Stop stops the container of srv in the background; srv becomes "stopped".
func (b *Box) Stop(srv *Server) {
	name := srv.Container
	b.serial(name, func() {
		err := b.d.Stop(b.ctx, name)
		b.done(srv, func() {
			srv.State = "stopped"
			if err != nil {
				srv.Note = err.Error()
			}
		})
	})
}

// Exec runs a command in the container of srv in the background; a failure
// lands in the note.
func (b *Box) Exec(srv *Server, cmd ...string) {
	name := srv.Container
	b.serial(name, func() {
		if err := b.exec(name, cmd...); err != nil {
			b.done(srv, func() { srv.Note = err.Error() })
		}
	})
}

// exec runs cmd in a container; a non-zero exit is an error with its output.
func (b *Box) exec(name string, cmd ...string) error {
	out, code, err := b.d.Exec(b.ctx, name, cmd...)
	if err == nil && code != 0 {
		err = fmt.Errorf("%s exited with %d: %s", cmd[0], code, strings.TrimSpace(string(out)))
	}
	return err
}

// Out runs cmd in a running container and returns its output; a non-zero exit is an error.
func (b *Box) Out(name string, cmd ...string) ([]byte, error) {
	out, code, err := b.d.Exec(b.ctx, name, cmd...)
	if err == nil && code != 0 {
		err = fmt.Errorf("%s exited with %d: %s", cmd[0], code, strings.TrimSpace(string(out)))
	}
	return out, err
}

// Put writes a file into a running container.
func (b *Box) Put(name, path string, data []byte) error {
	var tb bytes.Buffer
	tw := tar.NewWriter(&tb)
	tw.WriteHeader(&tar.Header{Name: strings.TrimPrefix(path, "/"), Mode: 0o644, Size: int64(len(data))})
	tw.Write(data)
	tw.Close()
	return b.d.CopyTo(b.ctx, name, "/", &tb)
}

// Remove deletes a container and its anonymous volumes in the background.
func (b *Box) Remove(name string) {
	b.serial(name, func() { b.d.Remove(b.ctx, name) })
}

// Serve gives a new resource its server: a fresh container, or without
// Docker only an address (want, else def) and a note. Lock held.
func (b *Box) Serve(srv *Server, account, region, id string, want, def int, spec func() Spec, docker bool) {
	if !docker {
		srv.State, srv.HostPort, srv.Note = "available", cmp.Or(want, def), NoDocker
		b.hub.Log.Warn(NoDocker, "service", b.label, "id", id)
		return
	}
	srv.Container, srv.State, srv.HostPort = b.Name(account, region, id), "creating", want
	b.Up(srv, spec(), "run")
}

// Resume reconnects a loaded resource to its container by name, starting it
// again (or afresh when it is gone). Call with the lock held.
func (b *Box) Resume(srv *Server, sp Spec, docker bool) {
	switch {
	case srv.Container == "" || srv.State == "stopped":
	case !docker:
		srv.State, srv.Note = "available", NoDocker
	default:
		b.Up(srv, sp, "start")
	}
}

// Close stops background work and removes every container this box touched,
// except, when state is persisted, those in keep: the next run resumes them.
// A container still being created now is left to the hub's docker.Client
// (owner label), or after a crash to its Sweep.
func (b *Box) Close(keep map[string]bool) {
	b.cancel()
	if cfg := b.hub.Cfg; !cfg.Persist && (cfg.Storage == "" || cfg.Storage == "memory") {
		keep = nil
	}
	b.mu.Lock()
	var rm []string
	for n := range b.names {
		if !keep[n] {
			rm = append(rm, n)
		}
	}
	b.mu.Unlock()
	sort.Strings(rm)
	for _, n := range rm {
		b.d.Remove(context.Background(), n)
	}
}

// serial runs fn in the background after every earlier command for the same container.
func (b *Box) serial(name string, fn func()) {
	b.mu.Lock()
	b.names[name] = true
	prev, done := b.tail[name], make(chan struct{})
	b.tail[name] = done
	b.mu.Unlock()
	core.Go("rds.serial", func() {
		if prev != nil {
			<-prev
		}
		defer func() { // runs on a panic too, so later calls are not stuck
			close(done)
			b.mu.Lock()
			if b.tail[name] == done {
				delete(b.tail, name)
			}
			b.mu.Unlock()
		}()
		fn()
	})
}

// done applies a background result to srv unless its resource is gone.
func (b *Box) done(srv *Server, apply func()) {
	b.lock.Lock()
	if b.live(srv) {
		apply()
	}
	b.lock.Unlock()
	b.hub.MarkDirty()
}

// up starts the container and returns its host port; a fresh one gets seed.
func (b *Box) up(sp Spec, verb, seed string) (int, error) {
	switch verb {
	case "start":
		if b.d.Start(b.ctx, sp.Name) == nil {
			return b.port(sp)
		}
	case "restart":
		if b.d.Restart(b.ctx, sp.Name) == nil {
			return b.port(sp)
		}
	}
	var err error
	for _, want := range []int{sp.Host, 0} { // the preferred port may be taken by the time the engine binds it
		host := freePort(want)
		ct := &docker.Container{Name: sp.Name, Image: sp.Image, Service: b.label, Resource: strings.TrimPrefix(sp.Name, "pogi-"+b.label+"-"),
			Env: sp.Env, Entrypoint: sp.Entry, Ports: map[int]int{sp.Port: host}}
		for _, a := range sp.Args {
			ct.Cmd = append(ct.Cmd, strings.ReplaceAll(a, "{port}", strconv.Itoa(host)))
		}
		b.d.Remove(b.ctx, sp.Name) // a leftover of a crashed run
		if err = b.run(ct, seed); err == nil {
			return b.port(sp)
		}
		if sp.Host == 0 {
			break
		}
	}
	return 0, err
}

// run creates and starts a container. A seed file goes in first, as
// /docker-entrypoint-initdb.d/pogi-seed.sql, which the database images load
// on their first start, before they listen; a seed that is gone is skipped.
func (b *Box) run(ct *docker.Container, seed string) error {
	data, err := os.ReadFile(seed)
	if seed == "" || errors.Is(err, fs.ErrNotExist) {
		_, err := b.d.Run(b.ctx, ct)
		return err
	}
	if err != nil {
		return err
	}
	var tb bytes.Buffer
	tw := tar.NewWriter(&tb)
	tw.WriteHeader(&tar.Header{Name: "docker-entrypoint-initdb.d/pogi-seed.sql", Mode: 0o644, Size: int64(len(data))})
	tw.Write(data)
	tw.Close()
	id, err := b.d.Create(b.ctx, ct)
	if err == nil {
		err = b.d.CopyTo(b.ctx, id, "/", &tb)
	}
	if err == nil {
		err = b.d.Start(b.ctx, id)
	}
	return err
}

// port asks the engine which host port publishes the server.
func (b *Box) port(sp Spec) (int, error) {
	info, err := b.d.Inspect(b.ctx, sp.Name)
	if err != nil {
		return 0, err
	}
	if p := info.HostPort(sp.Port); p != 0 {
		return p, nil
	}
	return 0, fmt.Errorf("%s publishes no port %d", sp.Name, sp.Port)
}

// wait polls until the server answers hello. Docker Desktop accepts
// connections before the server listens and then drops them, so a reply is
// required, not just a connection.
func (b *Box) wait(srv *Server, port int, hello string) error {
	addr := b.d.Addr(port)
	for end := time.Now().Add(5 * time.Minute); time.Now().Before(end); {
		if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
			c.SetDeadline(time.Now().Add(2 * time.Second))
			io.WriteString(c, hello)
			n, _ := c.Read(make([]byte, 1))
			c.Close()
			if n > 0 {
				return nil
			}
		}
		b.lock.Lock()
		live := b.live(srv)
		b.lock.Unlock()
		if !live {
			return errors.New("deleted")
		}
		select {
		case <-b.ctx.Done():
			return b.ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return fmt.Errorf("nothing answered on %s within 5 minutes (docker logs tells why)", addr)
}

// freePort returns want when it is free on 127.0.0.1, else any free port.
func freePort(want int) int {
	ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(want)))
	if err != nil && want != 0 {
		ln, err = net.Listen("tcp", "127.0.0.1:0")
	}
	if err != nil {
		return 0
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// Lists serves an RDS-style Query API with core.RPC. These APIs name list
// items after their type instead of "member": Tags.Tag.1 in requests,
// <TagList><Tag> in responses. items maps a list element to its item name.
// ponytail: core's Query codec could learn a member= tag; then drop this.
func Lists(rpc *core.RPC, items map[string]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Amz-Target") == "" {
			// core.RPC adds the URL parameters its form lacks: the renamed
			// keys go there, on a copy so the request log keeps the original.
			form, _ := url.ParseQuery(string(core.Body(r)))
			maps.Copy(form, r.URL.Query())
			u := *r.URL
			u.RawQuery = members(form).Encode()
			r = r.WithContext(r.Context())
			r.URL = &u
		}
		res := &response{header: http.Header{}}
		rpc.ServeHTTP(res, r)
		maps.Copy(w.Header(), res.header)
		w.WriteHeader(cmp.Or(res.code, http.StatusOK))
		w.Write(rename(res.Bytes(), items))
	}
}

// members renames list items to what core.DecodeQuery reads: Tags.Tag.1.Key
// becomes Tags.member.1.Key. Top-level flattened lists (Name.1) stay.
func members(form url.Values) url.Values {
	out := url.Values{}
	for k, v := range form {
		p := strings.Split(k, ".")
		for i := 1; i+1 < len(p); i++ {
			if _, err := strconv.Atoi(p[i+1]); err == nil {
				p[i] = "member"
			}
		}
		out[strings.Join(p, ".")] = v
	}
	return out
}

// rename turns <List><member> into <List><Item> for the lists in items.
func rename(body []byte, items map[string]string) []byte {
	if !bytes.Contains(body, []byte("<member>")) {
		return body
	}
	var out bytes.Buffer
	d, e := xml.NewDecoder(bytes.NewReader(body)), xml.NewEncoder(&out)
	var open []string
	for {
		tok, err := d.RawToken()
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if n := len(open); n > 0 && t.Name.Local == "member" && items[open[n-1]] != "" {
				t.Name.Local = items[open[n-1]]
			}
			open = append(open, t.Name.Local)
			tok = t
		case xml.EndElement:
			if len(open) == 0 {
				return body
			}
			t.Name.Local, open = open[len(open)-1], open[:len(open)-1]
			tok = t
		}
		e.EncodeToken(tok)
	}
	e.Flush()
	return out.Bytes()
}

// response buffers what core.RPC writes.
type response struct {
	header http.Header
	code   int
	bytes.Buffer
}

func (r *response) Header() http.Header { return r.header }
func (r *response) WriteHeader(code int) {
	if r.code == 0 {
		r.code = code
	}
}

// Page returns the items after marker in key order, at most max of them
// (default and cap 100), and the marker of the next page.
func Page[T any](items []T, key func(T) string, marker string, max int) ([]T, string) {
	if max <= 0 || max > 100 {
		max = 100
	}
	slices.SortFunc(items, func(a, b T) int { return strings.Compare(key(a), key(b)) })
	i := sort.Search(len(items), func(i int) bool { return key(items[i]) > marker })
	if items = items[i:]; len(items) > max {
		return items[:max], key(items[max-1])
	}
	return items, ""
}

var idRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-]*$`)

// ValidID checks an RDS or ElastiCache identifier.
func ValidID(param, id string, max int) error {
	if len(id) > max || !idRe.MatchString(id) || strings.HasSuffix(id, "-") || strings.Contains(id, "--") {
		return core.Errorf(400, "InvalidParameterValue", "The parameter %s is not a valid identifier. Identifiers must begin with a letter; "+
			"must contain only ASCII letters, digits, and hyphens; and must not end with a hyphen or contain two consecutive hyphens.", param)
	}
	return nil
}

// Clone deep-copies a record into an API view that is safe to encode after unlocking.
func Clone[T any](v *T) *T {
	b, _ := json.Marshal(v)
	out := new(T)
	json.Unmarshal(b, out)
	return out
}

// Tag is a resource tag.
type Tag struct {
	Key   string
	Value string
}

// AddTags sets tags, replacing the values of existing keys.
func AddTags(list, add []Tag) []Tag {
	for _, t := range add {
		if i := slices.IndexFunc(list, func(x Tag) bool { return x.Key == t.Key }); i >= 0 {
			list[i].Value = t.Value
		} else {
			list = append(list, t)
		}
	}
	return list
}

// RemoveTags drops tags by key.
func RemoveTags(list []Tag, keys []string) []Tag {
	return slices.DeleteFunc(list, func(t Tag) bool { return slices.Contains(keys, t.Key) })
}

// Endpoint is where a database listens.
type Endpoint struct {
	Address      string
	Port         int
	HostedZoneId string `json:",omitempty"`
}

// DefaultVPC is the VPC of every subnet group (pogi has no VPCs).
const DefaultVPC = "vpc-1a2b3c4d"

// Subnet is a subnet of a subnet group.
type Subnet struct {
	SubnetIdentifier       string
	SubnetAvailabilityZone struct{ Name string }
	SubnetStatus           string
}

// Subnets describes subnet ids, spreading them over the zones a, b and c.
func Subnets(region string, ids []string) []Subnet {
	out := make([]Subnet, len(ids))
	for i, id := range ids {
		out[i].SubnetIdentifier, out[i].SubnetStatus = id, "Active"
		out[i].SubnetAvailabilityZone.Name = region + string(rune('a'+i%3))
	}
	return out
}

// DBSpec, SAPassword and Alter expose an engine's container, SQL Server's sa
// password and the master-password command to Azure's database servers and Cloud SQL.
// Engines are RDS's: postgres, mysql, mariadb and sqlserver-ex.
func DBSpec(engine, version, user, password, db string, host int) Spec {
	return dbSpec(engine, version, user, password, db, host)
}

func SAPassword(pw string) string { return saPassword(pw) }

func Alter(engine, user, old, pw string) []string { return alter(engine, user, old, pw) }

// TSQL runs a T-SQL batch as sa in a SQL Server container, retrying while it starts.
func TSQL(sa, batch string) []string { return tsql(sa, batch) }
