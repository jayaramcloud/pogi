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

// Removed is the note of a resource whose container was gone when pogi started it again.
const Removed = "its container was removed while pogi was stopped, so it was recreated empty"

// Spec is a database container.
type Spec struct {
	Name  string // container name
	Image string
	Port  int      // port the server listens on inside the container
	Host  int      // preferred host port; 0 means any free one
	Env   []string // KEY=value
	Args  []string // after the image; "{port}" becomes the host port
	Data  string   // where the server keeps its data in the container; Box mounts the volume <name>-data there ("" keeps none)
	Entry []string // replaces the image's entrypoint when set
	Hello string   // what a client sends first ("" when the server speaks first); any reply means ready
	Init  []string // command run in the container once the server answers (errors ignored)
}

// Server is pogi's side of a database resource: its container and how to
// reach it. Resources keep it in a Pogi field tagged json:"pogi" query:"-"
// (not embedded: core flattens embedded structs into API responses), so it
// shows in Save and Inspect only.
type Server struct {
	ID        string `json:"id,omitempty"`        // the resource's identifier, which the console's row shows (the container name may carry an account and region)
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
		port, note, err := b.up(sp, verb, seed, b.resource(srv, sp.Name))
		exit := make(chan string, 1) // the container stopped on its own
		if err == nil {
			b.d.Watch(b.ctx, sp.Name, func(n string) {
				exit <- n
				b.done(srv, func() {
					if srv.State == "available" { // not mid-reboot, stop or delete
						srv.State, srv.Note = b.fail, n
						b.hub.Log.Warn("database container stopped", "container", sp.Name, "note", n)
					}
				})
			})
		}
		start := b.d.Track(b.label, b.resource(srv, sp.Name), "start", sp.Image) // until the server answers
		core.Go("rds.Up", func() {                                               // waiting for the server must not hold up a delete
			if err == nil {
				err = b.wait(srv, port, sp.Hello, exit)
			}
			if err == nil && len(sp.Init) > 0 {
				b.exec(sp.Name, sp.Init...)
			}
			start.End(err)
			if err != nil && b.ctx.Err() == nil {
				b.hub.Log.Warn("database container failed", "container", sp.Name, "err", err)
			}
			b.done(srv, func() {
				if err != nil {
					srv.State, srv.Note = b.fail, docker.Plain(err)
					return
				}
				srv.State, srv.HostPort, srv.Note, srv.Seed = "available", port, note, ""
				select {
				case n := <-exit: // it stopped just as it became ready
					srv.State, srv.Note = b.fail, n
				default:
				}
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

// Remove deletes a container and its data volume in the background.
func (b *Box) Remove(name string) {
	b.serial(name, func() { b.rm(b.ctx, name) })
}

// rm deletes a container with its data volume (a missing one is no error).
func (b *Box) rm(ctx context.Context, name string) error {
	err := b.d.Remove(ctx, name)
	if err == nil {
		err = b.d.RemoveVolume(ctx, volume(name))
	}
	return err
}

// volume is the name of the data volume of a container.
func volume(container string) string { return container + "-data" }

// Serve gives a new resource its server: a fresh container, or without
// Docker only an address (want, else def) and a note. Lock held.
func (b *Box) Serve(srv *Server, account, region, id string, want, def int, spec func() Spec, docker bool) {
	srv.ID = id
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // a dead engine must not hold up shutdown
	defer cancel()
	for _, n := range rm {
		b.rm(ctx, n)
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

// up starts the container and returns its host port; a fresh one gets seed. A container that is gone or won't
// start is made again on the same data volume, so its data stays; the note says the data is gone only when
// the volume is gone too. Verb "run" is a fresh start: the old volume goes first, as the images load their
// init scripts only into an empty data folder.
func (b *Box) up(sp Spec, verb, seed, resource string) (port int, note string, err error) {
	var gone error
	switch verb {
	case "start":
		gone = b.d.Start(b.ctx, sp.Name)
	case "restart":
		gone = b.d.Restart(b.ctx, sp.Name)
	}
	if verb != "run" {
		if gone == nil {
			port, err = b.port(sp)
			return port, "", err
		}
		if docker.EngineDown(gone) || b.ctx.Err() != nil {
			return 0, "", gone // the engine is the problem, not the container: don't remove what may be there
		}
		kept := sp.Data != "" && b.d.HasVolume(b.ctx, volume(sp.Name))
		switch {
		case kept && docker.NotFound(gone): // nothing is lost: no note
		case kept:
			note = "its container would not start again (" + docker.Plain(gone) + "), so it was recreated with its data"
		case docker.NotFound(gone):
			note = Removed
		default:
			note = "its container would not start again (" + docker.Plain(gone) + "), so it was recreated empty"
		}
	}
	for _, want := range []int{sp.Host, 0} { // the preferred port may be taken by the time the engine binds it
		host := freePort(want)
		ct := &docker.Container{Name: sp.Name, Image: sp.Image, Service: b.label, Resource: resource,
			Env: sp.Env, Entrypoint: sp.Entry, Ports: map[int]int{sp.Port: host}}
		if sp.Data != "" {
			ct.Binds = []string{volume(sp.Name) + ":" + sp.Data}
		}
		for _, a := range sp.Args {
			ct.Cmd = append(ct.Cmd, strings.ReplaceAll(a, "{port}", strconv.Itoa(host)))
		}
		if err = b.d.RemoveLeftover(b.ctx, sp.Name); err != nil { // a leftover of a crashed run
			return 0, "", err
		}
		if verb == "run" && sp.Data != "" {
			if err = b.d.RemoveVolume(b.ctx, volume(sp.Name)); docker.EngineDown(err) {
				return 0, "", err
			}
		}
		if err = b.run(ct, seed); err == nil {
			port, err = b.port(sp)
			return port, note, err
		}
		if sp.Host == 0 {
			break
		}
	}
	return 0, "", err
}

// resource is the pogi.resource label of a container: the identifier Serve saved, else (a state saved by an
// older pogi) the container name without its prefix.
func (b *Box) resource(srv *Server, name string) string {
	if srv.ID != "" {
		return srv.ID
	}
	return strings.TrimPrefix(name, "pogi-"+b.label+"-")
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
func (b *Box) wait(srv *Server, port int, hello string, exit <-chan string) error {
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
		case n := <-exit: // the container is gone: no point in waiting for the rest of the five minutes
			return errors.New(n)
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

// MongoSpec is the MongoDB container of DocumentDB, for Cosmos DB's MongoDB API
// accounts: the root user is user, with password pw.
func MongoSpec(user, pw string, host int) Spec {
	sp, _ := otherSpec("docdb", user, pw, host)
	return sp
}

// StatusCell is the Status cell of a console table for a resource a Box runs: its status, then a short form of
// pogi's note when there is one ("available · no database: Docker isn't running", "failed · killed: out of memory…").
// Every Box-based service's table uses it. A status that is still changing stays bare, so the table keeps refreshing.
func StatusCell(status string, srv *Server) string {
	if srv == nil || srv.Note == "" {
		return status
	}
	switch strings.ToLower(status) { // the in-between statuses of feedback.js
	case "creating", "modifying", "deleting", "backing-up", "provisioning", "pending_create", "updating":
		return status
	}
	note := srv.Note
	switch note {
	case NoDocker:
		note = "no database: Docker isn't running"
	case Removed:
		note = "recreated empty: its container was removed"
	}
	if r := []rune(note); len(r) > 70 {
		note = string(r[:69]) + "…"
	}
	return status + " · " + note
}
