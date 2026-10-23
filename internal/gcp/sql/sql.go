// Package sql emulates the Cloud SQL Admin API (sqladmin v1beta4 and v1, REST
// JSON): instances, databases, users, SSL certificates, operations, flags and
// tiers, with connectSettings and generateEphemeralCert for the connectors.
// Each instance is a real PostgreSQL, MySQL or SQL Server container, run by
// the same code as RDS (internal/aws/rds); users and databases are created in
// the engine. Without Docker the API still answers and nothing runs behind the
// address.
//
// The instance's PRIMARY ipAddress is "127.0.0.1:<port>": a published
// container has no fixed port 5432 or 3306, so the address carries it.
// ponytail: no Cloud SQL Auth Proxy: connect straight to that address (plain
// TCP, no TLS, no IAM database authentication); the ephemeral certificates
// are real, signed by the instance's CA, but nothing checks them.
package sql

import (
	"cmp"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/core"
	sqladmin "google.golang.org/api/sqladmin/v1beta4"
)

// Service is the Cloud SQL Admin emulator.
type Service struct {
	hub *core.Hub
	box *rds.Box
	mu  sync.Mutex
	st  state
	dir string // dumps of clones, made on first use
}

type state struct {
	Instances map[string]*instance `json:"instances"` // project/name
	Ops       map[string]*op       `json:"operations"`
}

func newState() state { return state{Instances: map[string]*instance{}, Ops: map[string]*op{}} }

// instance is what the API shows plus pogi's side: the container, the CA of
// its certificates and what lives in the engine.
type instance struct {
	Spec      *sqladmin.DatabaseInstance `json:"instance"`
	Databases []*sqladmin.Database       `json:"databases"`
	Users     []*sqladmin.User           `json:"users"`
	Certs     []*sqladmin.SslCert        `json:"sslCerts"`
	CA        *ca                        `json:"ca"`
	Pogi      rds.Server                 `json:"pogi"` // Password is the root password
}

// op is an Operation: it is RUNNING while its instance is in state Wait (a
// container working: creating, starting, rebooting, stopping), else DONE.
type op struct {
	Op   *sqladmin.Operation `json:"op"`
	Wait string              `json:"wait,omitempty"`
}

// engine is what a databaseVersion runs.
type engine struct {
	name, version, admin string // rds engine, version, the built-in user
	port                 int
}

var versions = []struct {
	re *regexp.Regexp
	f  func(m []string) engine
}{
	{regexp.MustCompile(`^POSTGRES_(1\d)$`), func(m []string) engine { return engine{"postgres", m[1], "postgres", 5432} }},
	{regexp.MustCompile(`^MYSQL_(5_7|8_0|8_4)$`), func(m []string) engine {
		return engine{"mysql", strings.ReplaceAll(m[1], "_", "."), "root", 3306}
	}},
	{regexp.MustCompile(`^SQLSERVER_(20\d\d)_(STANDARD|ENTERPRISE|EXPRESS|WEB)$`), func(m []string) engine {
		return engine{"sqlserver-ee", "16.00", "sqlserver", 1433} // ponytail: every SQL Server version runs the 2022 image
	}},
}

func engineOf(databaseVersion string) (engine, bool) {
	for _, v := range versions {
		if m := v.re.FindStringSubmatch(databaseVersion); m != nil {
			return v.f(m), true
		}
	}
	return engine{}, false
}

// Image is the image a databaseVersion (MYSQL_8_0, POSTGRES_16…) runs, from the same spec the service uses;
// ok is false for a version pogi does not run.
func Image(databaseVersion string) (image string, ok bool) {
	e, ok := engineOf(databaseVersion)
	if !ok {
		return "", false
	}
	return rds.DBSpec(e.name, e.version, e.admin, "p", "", 0).Image, true
}

func (i *instance) engine() engine { e, _ := engineOf(i.Spec.DatabaseVersion); return e }

// New creates the Cloud SQL service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: newState()}
	s.box = rds.NewBox(hub, "cloudsql", "failed", &s.mu, s.owns)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "sqladmin", Cloud: core.GCP, Title: "Cloud SQL", Match: s.match}
}

// owns reports whether srv belongs to an instance; lock held.
func (s *Service) owns(srv *rds.Server) bool {
	for _, i := range s.st.Instances {
		if &i.Pogi == srv {
			return true
		}
	}
	return false
}

func (s *Service) spec(i *instance) func() rds.Spec {
	e := i.engine()
	return func() rds.Spec { return rds.DBSpec(e.name, e.version, e.admin, i.Pogi.Password, "", i.Pogi.HostPort) }
}

// ---- persistence, console, shutdown ----

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (st *state) containers() map[string]bool {
	out := map[string]bool{}
	for _, i := range st.Instances {
		if i.Pogi.Container != "" {
			out[i.Pogi.Container] = true
		}
	}
	return out
}

// Load restores state and reconnects to the containers by name (--persist
// leaves them behind on shutdown); containers of dropped instances go.
func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	docker := len(st.containers()) > 0 && s.box.Available()
	s.mu.Lock()
	old := s.st
	s.st = st
	for _, i := range st.Instances {
		s.box.Resume(&i.Pogi, s.spec(i)(), docker)
	}
	keep := st.containers()
	s.mu.Unlock()
	for n := range old.containers() {
		if !keep[n] {
			s.box.Remove(n)
		}
	}
	return nil
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

// Close removes the containers, except those persisted state resumes next run.
func (s *Service) Close() error {
	s.mu.Lock()
	keep := s.st.containers()
	dir := s.dir
	s.mu.Unlock()
	s.box.Close(keep)
	if dir != "" {
		os.RemoveAll(dir)
	}
	return nil
}

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Instances", Columns: []string{"Name", "Project", "Version", "State", "Address", "Tier", "Region"}}
	for _, k := range core.SortedKeys(s.st.Instances) {
		v := s.view("", s.st.Instances[k])
		addr := ""
		if len(v.IpAddresses) > 0 {
			addr = v.IpAddresses[0].IpAddress
		}
		t.Rows = append(t.Rows, []string{v.Name, v.Project, v.DatabaseVersion, rds.StatusCell(v.State, &s.st.Instances[k].Pogi), addr, v.Settings.Tier, v.Region})
	}
	return []core.Table{t}
}

// Inspect returns an instance (id project/name) with its databases, users
// and pogi's side: the container, the root password and the note.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.st.Instances[id]
	if table != "Instances" || i == nil {
		return nil
	}
	return map[string]any{"instance": s.view("", i), "databases": i.Databases, "users": i.Users, "sslCerts": i.Certs,
		"pogi": map[string]any{"container": i.Pogi.Container, "state": i.Pogi.State, "hostPort": i.Pogi.HostPort,
			"rootPassword": i.Pogi.Password, "note": i.Pogi.Note}}
}

// ---- errors ----

// apiError is a Google API error: status, reason and message.
type apiError struct {
	code   int
	reason string
	msg    string
}

func (e *apiError) Error() string { return e.msg }

func fail(code int, reason, format string, a ...any) *apiError {
	return &apiError{code, reason, fmt.Sprintf(format, a...)}
}

var statusNames = map[int]string{400: "INVALID_ARGUMENT", 401: "UNAUTHENTICATED", 403: "PERMISSION_DENIED", 404: "NOT_FOUND",
	409: "ABORTED", 412: "FAILED_PRECONDITION", 501: "UNIMPLEMENTED"}

func writeErr(w http.ResponseWriter, c *core.Ctx, e *apiError) {
	name := cmp.Or(statusNames[e.code], core.GoogleStatus(e.code))
	if e.code == 409 && e.reason == "instanceAlreadyExists" {
		name = "ALREADY_EXISTS"
	}
	c.ErrCode = e.reason
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(e.code)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": e.code, "message": e.msg, "status": name,
		"errors": []map[string]string{{"message": e.msg, "domain": "global", "reason": e.reason}}}})
}

func notFound(what string) *apiError {
	switch what {
	case "instance":
		return fail(404, "instanceDoesNotExist", "The Cloud SQL instance does not exist.")
	case "operation":
		return fail(404, "notFound", "The operation does not exist.")
	}
	return fail(404, "notFound", "Not found: Cloud SQL %s does not exist.", what)
}

var busy = fail(409, "operationInProgress", "Operation failed because another operation was already in progress.")

// ---- time ----

func stamp(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

func clone[T any](v *T) *T {
	b, _ := json.Marshal(v)
	out := new(T)
	json.Unmarshal(b, out)
	return out
}
