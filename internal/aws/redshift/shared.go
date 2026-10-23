package redshift

// Helpers shared with internal/aws/redshiftserverless and
// internal/aws/redshiftdata: the PostgreSQL container that stands in for a
// cluster or workgroup, logins to it, and how the Data API reaches it.

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// Port is where Redshift listens, inside the container too.
const Port = 5439

// Spec is the container of a cluster or workgroup: PostgreSQL on Redshift's
// port, where PUBLIC may create tables in schema public as on Redshift.
func Spec(user, password, db string, host int) rds.Spec {
	return rds.Spec{Image: "postgres:16-alpine", Port: Port, Host: host, Data: "/var/lib/postgresql/data", Hello: "\x00\x00\x00\x08\x04\xd2\x16\x2f", // SSLRequest
		Env:  []string{"PGPORT=" + strconv.Itoa(Port), "POSTGRES_USER=" + user, "POSTGRES_PASSWORD=" + password, "POSTGRES_DB=" + db},
		Init: []string{"psql", "-U", user, "-d", db, "-c", "GRANT ALL ON SCHEMA public TO PUBLIC"}}
}

// Alter is the command that changes a password inside a container, where
// local connections are trusted. Check the name and password first.
func Alter(user, password string) []string {
	return []string{"psql", "-U", user, "-d", "postgres", "-c", `ALTER USER "` + user + `" WITH PASSWORD '` + password + `'`}
}

var (
	userRe = regexp.MustCompile(`^[a-z][a-z0-9_+.@-]{0,127}$`)      // master and admin users
	roleRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_+.@-]{0,63}$`) // DbUser and DbGroups
	dbRe   = regexp.MustCompile(`^[a-z_][a-z0-9_$]{0,63}$`)
)

// ValidUser checks a master or admin user name.
func ValidUser(u string) bool { return userRe.MatchString(u) && u != "public" }

// ValidRole checks a DbUser or DbGroups name.
func ValidRole(r string) bool { return roleRe.MatchString(r) }

// ValidDB checks a database name.
func ValidDB(db string) bool { return dbRe.MatchString(db) }

// ValidPassword checks a master or admin password: 8 to 64 printable ASCII
// characters with an uppercase letter, a lowercase letter and a digit, and
// none of ' " \ / @ or space.
func ValidPassword(pw string) bool {
	for _, r := range pw {
		if r <= ' ' || r > '~' || strings.ContainsRune(`'"\/@`, r) {
			return false
		}
	}
	return len(pw) >= 8 && len(pw) <= 64 && strings.IndexFunc(pw, unicode.IsUpper) >= 0 &&
		strings.IndexFunc(pw, unicode.IsLower) >= 0 && strings.IndexFunc(pw, unicode.IsDigit) >= 0
}

// IAMUser is the database user of an IAM identity, as GetClusterCredentialsWithIAM
// and Redshift Serverless name it: IAM:<user> or IAMR:<role>.
func IAMUser(principal string) string {
	_, _, _, res, _ := core.ParseARN(principal)
	kind, name, _ := strings.Cut(res, "/")
	switch kind {
	case "assumed-role":
		name, _, _ = strings.Cut(name, "/")
		return "IAMR:" + name
	case "role":
		return "IAMR:" + name[strings.LastIndex(name, "/")+1:]
	case "user":
		return "IAM:" + name[strings.LastIndex(name, "/")+1:]
	}
	return "IAM:" + kind // root
}

// DB is where the database of a cluster or workgroup answers.
type DB struct {
	Addr     string // 127.0.0.1:port; empty while nothing answers, and Note says why
	User     string // the master or admin user
	Password string
	Name     string // the default database
	Note     string
}

// NewDB describes the database srv runs; what names its resource in notes.
func NewDB(d *docker.Client, srv *rds.Server, user, password, db, what string) *DB {
	out := &DB{User: user, Password: password, Name: db}
	switch {
	case srv.Container == "":
		out.Note = rds.NoDocker
	case srv.State != "available":
		out.Note = what + " is " + status(srv.State) + ", not available"
	default:
		out.Addr = d.Addr(srv.HostPort)
	}
	return out
}

// status is a cluster status: the server's, in Redshift's words.
func status(state string) string {
	if state == "stopped" {
		return "paused"
	}
	return state
}

// Connect logs in to database db (default: the default database) as user
// with password (default: the master user).
func (d *DB) Connect(ctx context.Context, user, password, db string) (*pgx.Conn, error) {
	if d.Addr == "" {
		return nil, fmt.Errorf("pogi: %s", d.Note)
	}
	cfg, err := pgx.ParseConfig("sslmode=disable connect_timeout=10")
	if err != nil {
		return nil, err
	}
	host, port, _ := net.SplitHostPort(d.Addr)
	p, _ := strconv.Atoi(port)
	cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database = host, uint16(p), d.User, d.Password, d.Name
	if user != "" {
		cfg.User, cfg.Password = user, password
	}
	if db != "" {
		cfg.Database = db
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol // text results; parameters become literals the server casts, as Redshift's do
	return pgx.ConnectConfig(ctx, cfg)
}

// Login lets login sign in with password until expiry, the way temporary
// credentials do: a role that joins the database user it stands for (and acts
// as that user once signed in) and the groups that exist. create first makes
// the user when it is missing. Check the names first.
func (d *DB) Login(ctx context.Context, login, user string, groups []string, create bool, password string, expiry time.Time) error {
	conn, err := d.Connect(ctx, "", "", "")
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	id := pgx.Identifier{login}.Sanitize()
	sql := fmt.Sprintf("DO $$BEGIN CREATE ROLE %s; EXCEPTION WHEN duplicate_object THEN NULL; END$$;\n"+
		"ALTER ROLE %s LOGIN PASSWORD '%s' VALID UNTIL '%s';\n", id, id, password, expiry.UTC().Format(time.RFC3339))
	if create && user != "" {
		sql += fmt.Sprintf("DO $$BEGIN CREATE ROLE %s; EXCEPTION WHEN duplicate_object THEN NULL; END$$;\n", pgx.Identifier{user}.Sanitize())
	}
	for i, g := range append([]string{user}, groups...) {
		if g == "" {
			continue
		}
		then := fmt.Sprintf("GRANT %s TO %s;", pgx.Identifier{g}.Sanitize(), id)
		if i == 0 {
			then += fmt.Sprintf(" ALTER ROLE %s SET role = '%s';", id, g)
		}
		sql += fmt.Sprintf("DO $$BEGIN IF EXISTS (SELECT FROM pg_roles WHERE rolname = '%s') THEN %s END IF; END$$;\n", g, then)
	}
	_, err = conn.Exec(ctx, sql)
	return err
}
