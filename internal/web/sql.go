package web

// SQL editor for the container databases: RDS, Cloud SQL and the Azure
// servers. It runs the statement with psql or mysql inside the database's
// container (docker exec), so it needs no driver and no host port.
// ponytail: PostgreSQL, MySQL and MariaDB only (no SQL Server), one statement
// result per run; the upgrade is a pgx/mysql connection to the host port.

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
)

type sqlData struct {
	Targets []choice
	Target  string
	DB      string
	SQL     string
	Res     *result
	Err     string
}

// sqlConn is how to reach one database server.
type sqlConn struct {
	Container, Engine, User, Password, DB string
}

// sqlTargets lists the servers the editor can open: value is "kind|id".
func (ui *UI) sqlTargets(r *http.Request) []choice {
	var out []choice
	for _, r := range ui.rows(r, "rds", "DB instances") {
		out = append(out, choice{"rds|" + r[0], "RDS · " + r[0] + " (" + r[1] + ")"})
	}
	for _, r := range ui.rows(r, "rds", "DB clusters") {
		out = append(out, choice{"rdscluster|" + r[0], "RDS cluster · " + r[0] + " (" + r[1] + ")"})
	}
	for _, r := range ui.rows(r, "sqladmin", "Instances") {
		out = append(out, choice{"cloudsql|" + r[1] + "/" + r[0], "Cloud SQL · " + r[0] + " (" + r[2] + ")"})
	}
	for _, r := range ui.rows(r, "azdatabases", "Servers") {
		if !strings.Contains(strings.ToLower(r[1]), "redis") {
			out = append(out, choice{"azure|" + r[0], "Azure · " + r[0] + " (" + r[1] + ")"})
		}
	}
	return out
}

// sqlResolve finds the container, engine and master credentials of a target.
func (ui *UI) sqlResolve(target string) (sqlConn, error) {
	kind, id, _ := strings.Cut(target, "|")
	var c sqlConn
	switch kind {
	case "rds", "rdscluster":
		table := map[string]string{"rds": "DB instances", "rdscluster": "DB clusters"}[kind]
		m := ui.inspectMap("rds", table, id)
		c = sqlConn{dig(m, "pogi", "container"), dig(m, "Engine"), dig(m, "MasterUsername"), dig(m, "pogi", "masterPassword"), dig(m, "DBName") + dig(m, "DatabaseName")}
	case "cloudsql":
		m := ui.inspectMap("sqladmin", "Instances", id)
		c = sqlConn{Container: dig(m, "pogi", "container"), Engine: dig(m, "instance", "databaseVersion"), Password: dig(m, "pogi", "rootPassword")}
		c.User = map[bool]string{true: "postgres", false: "root"}[strings.HasPrefix(c.Engine, "POSTGRES")]
	case "azure":
		m := ui.inspectMap("azdatabases", "Servers", id)
		c = sqlConn{dig(m, "pogi", "container"), dig(m, "resource", "type"), dig(m, "resource", "properties", "administratorLogin"), dig(m, "pogi", "masterPassword"), ""}
	default:
		return c, errors.New("choose a database server")
	}
	if c.Container == "" {
		return c, fmt.Errorf("%s has no running container (is Docker available?)", id)
	}
	e := strings.ToLower(c.Engine)
	switch {
	case strings.Contains(e, "postgres"), e == "pg":
		c.Engine = "postgres"
	case strings.Contains(e, "mysql"), strings.Contains(e, "mariadb"):
		c.Engine = "mysql"
	default:
		return c, fmt.Errorf("the SQL editor speaks PostgreSQL, MySQL and MariaDB, not %q", c.Engine)
	}
	return c, nil
}

func (ui *UI) sqlPage(w http.ResponseWriter, r *http.Request) {
	ui.render(w, r, "x-sql", "SQL editor", "explore", &sqlData{Targets: ui.sqlTargets(r), Target: r.URL.Query().Get("target")})
}

func (ui *UI) sqlRun(w http.ResponseWriter, r *http.Request) {
	d := &sqlData{Targets: ui.sqlTargets(r), Target: r.FormValue("target"), DB: strings.TrimSpace(r.FormValue("db")), SQL: r.FormValue("sql")}
	c, err := ui.sqlResolve(d.Target)
	if err == nil {
		if d.DB != "" {
			c.DB = d.DB
		}
		d.Res, err = ui.sqlExec(c, d.SQL)
	}
	if err != nil {
		d.Err = err.Error()
	}
	ui.render(w, r, "x-sql", "SQL editor", "explore", d)
}

func (ui *UI) sqlExec(c sqlConn, sql string) (*result, error) {
	d := ui.hub.Docker()
	if d == nil || !d.Available() {
		return nil, errors.New("Docker is not available")
	}
	if strings.TrimSpace(sql) == "" {
		return nil, errors.New("type a statement")
	}
	var cmd []string
	switch c.Engine {
	case "postgres":
		db := c.DB
		if db == "" {
			db = "postgres"
		}
		cmd = []string{"env", "PGPASSWORD=" + c.Password, "psql", "-X", "-v", "ON_ERROR_STOP=1", "--csv", "-U", c.User, "-d", db, "-c", sql}
	default: // mysql and mariadb (whose client may only be called mariadb)
		cmd = []string{"env", "MYSQL_PWD=" + c.Password, "sh", "-c", `exec "$(command -v mysql || command -v mariadb)" "$@"`, "sh", "-B", "-u", c.User}
		if c.DB != "" {
			cmd = append(cmd, "-D", c.DB)
		}
		cmd = append(cmd, "-e", sql)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, code, err := d.Exec(ctx, c.Container, cmd...)
	if err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, errors.New(strings.TrimSpace(string(out)))
	}
	return parseSQLOut(c.Engine, string(out)), nil
}

var cmdTag = regexp.MustCompile(`^[A-Z][A-Z ]+( \d+)*$`)

// parseSQLOut reads psql --csv or mysql -B output as a table; a lone command
// tag ("INSERT 0 1") or nothing becomes a note.
func parseSQLOut(engine, out string) *result {
	out = strings.TrimRight(out, "\n")
	if out == "" {
		return &result{Note: "OK, no rows returned"}
	}
	var recs [][]string
	if engine == "postgres" {
		cr := csv.NewReader(strings.NewReader(out))
		cr.FieldsPerRecord, cr.LazyQuotes = -1, true
		recs, _ = cr.ReadAll()
		if len(recs) == 1 && len(recs[0]) == 1 && cmdTag.MatchString(recs[0][0]) {
			return &result{Note: recs[0][0]}
		}
	} else {
		unescape := strings.NewReplacer(`\t`, "\t", `\n`, "\n", `\\`, `\`)
		for _, l := range strings.Split(out, "\n") {
			var rec []string
			for _, f := range strings.Split(l, "\t") {
				rec = append(rec, unescape.Replace(f))
			}
			recs = append(recs, rec)
		}
	}
	if len(recs) == 0 {
		return &result{Note: "OK, no rows returned"}
	}
	return &result{Cols: recs[0], Rows: recs[1:], Note: fmt.Sprintf("%d rows", len(recs)-1)}
}
