// Package redshiftdata emulates the Amazon Redshift Data API over JSON 1.1.
// Statements run in the background on the PostgreSQL container of a Redshift
// cluster or Redshift Serverless workgroup, as its admin user or the user of
// a Secrets Manager secret. :name parameters become literals the server
// casts, as Redshift's do, and the Redshift-only DDL clauses PostgreSQL lacks
// are dropped.
package redshiftdata

import (
	"bytes"
	"cmp"
	"context"
	"encoding/csv"
	"encoding/json"
	"math"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nrndev/pogi/internal/aws/redshift"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Redshift Data API emulator.
type Service struct {
	hub *core.Hub
	rpc *core.RPC
	mu  sync.Mutex
	st  map[string]*stmt // by id
	qid int64            // the last RedshiftQueryId
}

// stmt is a statement, or a batch whose SubStatements are its statements.
// Exported fields are DescribeStatement's; results are kept until it expires.
type stmt struct {
	Id                string
	Status            string
	QueryString       string `json:",omitempty"`
	Error             string `json:",omitempty"`
	CreatedAt         core.Time
	UpdatedAt         core.Time
	Duration          int64 // nanoseconds
	HasResultSet      bool
	ResultRows        int64
	ResultSize        int64
	RedshiftQueryId   int64
	RedshiftPid       int64   `json:",omitempty"`
	ClusterIdentifier string  `json:",omitempty"`
	WorkgroupName     string  `json:",omitempty"`
	Database          string  `json:",omitempty"`
	DbUser            string  `json:",omitempty"`
	SecretArn         string  `json:",omitempty"`
	ResultFormat      string  `json:",omitempty"`
	QueryParameters   []param `json:",omitempty"`
	SubStatements     []*stmt `json:",omitempty"`

	name, account, region, principal string
	event                            bool
	created                          time.Time
	cols                             []column
	recs                             [][]*string // text values; nil is NULL
	cancel                           context.CancelFunc
	done                             chan struct{} // closed when the statement (or its batch) ends
}

type param struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// column is a ColumnMetadata.
type column struct {
	ColumnDefault   string `json:"columnDefault,omitempty"`
	IsCaseSensitive bool   `json:"isCaseSensitive"`
	IsCurrency      bool   `json:"isCurrency"`
	IsSigned        bool   `json:"isSigned"`
	Label           string `json:"label"`
	Length          int    `json:"length"`
	Name            string `json:"name"`
	Nullable        int    `json:"nullable"`
	Precision       int    `json:"precision"`
	Scale           int    `json:"scale"`
	SchemaName      string `json:"schemaName,omitempty"`
	TableName       string `json:"tableName,omitempty"`
	TypeName        string `json:"typeName"`
}

// New creates the Redshift Data API service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: map[string]*stmt{}}
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"ExecuteStatement":      core.H(s.execute),
		"BatchExecuteStatement": core.H(s.execute),
		"DescribeStatement":     core.H(s.describeStatement),
		"GetStatementResult":    core.H(s.getResult),
		"GetStatementResultV2":  core.H(s.getResult),
		"ListStatements":        core.H(s.listStatements),
		"CancelStatement":       core.H(s.cancelStatement),
		"ListDatabases":         core.H(s.listDatabases),
		"ListSchemas":           core.H(s.listSchemas),
		"ListTables":            core.H(s.listTables),
		"DescribeTable":         core.H(s.describeTable),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "redshift-data", Cloud: core.AWS, Title: "Redshift Data API", Targets: []string{"RedshiftData"},
		Signing: []string{"redshift-data"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func invalid(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}

// ---- where statements run ----

// target is a database and the login to use there.
type target struct {
	db             *redshift.DB
	user, password string // "": the admin's
}

// connect finds the database a request names, a cluster's or a workgroup's,
// and the login of its secret, if any.
func (s *Service) connect(c *core.Ctx, cluster, workgroup, secret string) (*target, error) {
	if (cluster == "") == (workgroup == "") {
		return nil, invalid("Either ClusterIdentifier or WorkgroupName must be given, but not both.")
	}
	name, id := "redshift", cluster
	if workgroup != "" {
		name, id = "redshift-serverless", workgroup
	}
	src, _ := s.hub.Service(name).(interface {
		DB(account, region, id string) *redshift.DB
	})
	if src == nil {
		return nil, invalid("%s is not enabled in pogi.", name)
	}
	t := &target{db: src.DB(c.Account, c.Region, id)}
	switch {
	case t.db == nil && workgroup != "":
		return nil, invalid("Workgroup %s not found.", workgroup)
	case t.db == nil:
		return nil, invalid("Redshift endpoint doesn't exist in this region.")
	case secret == "":
		return t, nil
	}
	var out struct{ SecretString string }
	if err := s.hub.Call(c.Account, c.Region, "secretsmanager", "GetSecretValue", map[string]string{"SecretId": secret}, &out); err != nil {
		return nil, invalid("Could not read secret %s: %s", secret, core.AsError(err).Msg)
	}
	var login struct{ Username, Password string }
	if json.Unmarshal([]byte(out.SecretString), &login) != nil || login.Username == "" {
		return nil, invalid("Secret %s holds no username and password.", secret)
	}
	t.user, t.password = login.Username, login.Password
	return t, nil
}

// ---- statements ----

type executeInput struct {
	Sql                                                                string
	Sqls                                                               []string
	Parameters                                                         []param
	ClusterIdentifier, WorkgroupName, Database, DbUser, SecretArn      string
	StatementName, ResultFormat, ExecutionMode, SessionId, ClientToken string
	WithEvent                                                          bool
	WaitTimeSeconds, SessionKeepAliveSeconds                           int
}

// maxActive is how many statements of an account may run at once.
const maxActive = 500

// execute serves ExecuteStatement and BatchExecuteStatement.
func (s *Service) execute(c *core.Ctx, in *executeInput) (any, error) {
	batch, sqls := c.Op == "BatchExecuteStatement", in.Sqls
	if !batch {
		sqls = []string{in.Sql}
	}
	format := cmp.Or(in.ResultFormat, "JSON")
	switch {
	case len(sqls) == 0 || len(sqls) > 40 || slices.Contains(sqls, ""):
		return nil, invalid("Give 1 to 40 non-empty SQL statements.")
	case in.Database == "":
		return nil, invalid("Database must be given.")
	case format != "JSON" && format != "CSV":
		return nil, invalid("ResultFormat must be JSON or CSV.")
	case in.ExecutionMode != "" && in.ExecutionMode != "TRANSACTION" && in.ExecutionMode != "AUTO_COMMIT":
		return nil, invalid("ExecutionMode must be TRANSACTION or AUTO_COMMIT.")
	case in.SessionId != "" || in.SessionKeepAliveSeconds > 0: // ponytail: each statement opens its own connection
		return nil, invalid("pogi does not keep Data API sessions yet.")
	}
	queries, args, err := bind(sqls, in.Parameters)
	if err != nil {
		return nil, err
	}
	t, err := s.connect(c, in.ClusterIdentifier, in.WorkgroupName, in.SecretArn)
	if err != nil {
		return nil, err
	}
	if t.db.Addr == "" {
		code := map[bool]string{false: "ExecuteStatementException", true: "BatchExecuteStatementException"}[batch]
		return nil, core.Errorf(400, code, "%s", t.db.Note)
	}
	now := time.Now().UTC()
	st := &stmt{Id: core.UUID(), Status: "SUBMITTED", CreatedAt: core.Time(now), UpdatedAt: core.Time(now), ResultRows: -1, ResultSize: -1,
		ClusterIdentifier: in.ClusterIdentifier, WorkgroupName: in.WorkgroupName, Database: in.Database, DbUser: in.DbUser,
		SecretArn: in.SecretArn, ResultFormat: format, QueryParameters: in.Parameters, name: in.StatementName, account: c.Account,
		region: c.Region, principal: c.Principal, event: in.WithEvent, created: now, done: make(chan struct{})}
	if batch {
		for i, sql := range sqls {
			st.SubStatements = append(st.SubStatements, &stmt{Id: st.Id + ":" + strconv.Itoa(i+1), Status: "SUBMITTED", QueryString: sql,
				CreatedAt: st.CreatedAt, UpdatedAt: st.CreatedAt, ResultRows: -1, ResultSize: -1, ResultFormat: format, done: st.done})
		}
	} else {
		st.QueryString = in.Sql
	}
	var ctx context.Context
	ctx, st.cancel = context.WithCancel(context.Background())
	s.mu.Lock()
	active := 0
	for id, old := range s.st {
		switch {
		case now.Sub(old.created) > 24*time.Hour && !running(old): // results expire after a day
			delete(s.st, id)
		case old.account == c.Account && running(old):
			active++
		}
	}
	if active >= maxActive {
		s.mu.Unlock()
		st.cancel()
		return nil, core.Errorf(400, "ActiveStatementsExceededException", "The account has %d running statements, the most allowed.", maxActive)
	}
	s.st[st.Id] = st
	for _, sub := range append([]*stmt{st}, st.SubStatements...) {
		s.qid++
		sub.RedshiftQueryId = s.qid
	}
	s.mu.Unlock()
	core.Go("redshiftdata.run", func() { s.run(ctx, st, t, queries, args, in.ExecutionMode == "AUTO_COMMIT") })
	s.wait(st, in.WaitTimeSeconds)
	s.mu.Lock()
	defer s.mu.Unlock()
	return &executed{Id: st.Id, Status: st.Status, CreatedAt: st.CreatedAt, Database: st.Database, ClusterIdentifier: st.ClusterIdentifier,
		WorkgroupName: st.WorkgroupName, DbUser: st.DbUser, SecretArn: st.SecretArn, HasResultSet: st.HasResultSet, RedshiftPid: st.RedshiftPid}, nil
}

// executed is the output of ExecuteStatement and BatchExecuteStatement.
type executed struct {
	Id                string
	Status            string
	CreatedAt         core.Time
	Database          string
	ClusterIdentifier string `json:",omitempty"`
	WorkgroupName     string `json:",omitempty"`
	DbUser            string `json:",omitempty"`
	SecretArn         string `json:",omitempty"`
	HasResultSet      bool
	RedshiftPid       int64 `json:",omitempty"`
}

func running(st *stmt) bool {
	return st.Status == "SUBMITTED" || st.Status == "PICKED" || st.Status == "STARTED"
}

// wait waits up to secs seconds (at most 30) for a statement to end.
func (s *Service) wait(st *stmt, secs int) {
	if secs > 0 {
		select {
		case <-st.done:
		case <-time.After(time.Duration(min(secs, 30)) * time.Second):
		}
	}
}

// run runs a statement, or the statements of a batch in one transaction
// (or each on its own with autocommit).
func (s *Service) run(ctx context.Context, st *stmt, t *target, queries []string, args [][]any, autocommit bool) {
	defer close(st.done)
	defer st.cancel()
	s.update(st, func() { st.Status = "PICKED" })
	conn, err := t.db.Connect(ctx, t.user, t.password, st.Database)
	if err != nil {
		s.end(st, err)
		return
	}
	defer conn.Close(context.Background())
	start := time.Now()
	s.update(st, func() { st.Status, st.RedshiftPid = "STARTED", int64(conn.PgConn().PID()) })
	todo, tx := st.SubStatements, len(st.SubStatements) > 0 && !autocommit
	if todo == nil {
		todo = []*stmt{st}
	}
	if tx {
		_, err = conn.Exec(ctx, "BEGIN")
	}
	for i, sub := range todo {
		if ctx.Err() != nil || err != nil && tx { // cancelled, or the transaction is lost: the rest never run
			break
		}
		s.update(sub, func() {
			if running(sub) {
				sub.Status = "STARTED"
			}
		})
		began := time.Now()
		r, qerr := query(ctx, conn, queries[i], args[i])
		s.update(sub, func() {
			if sub.Status == "ABORTED" {
				return
			}
			sub.Duration, sub.Status = time.Since(began).Nanoseconds(), "FINISHED"
			if qerr != nil {
				sub.Status, sub.Error = "FAILED", qerr.Error()
				return
			}
			sub.HasResultSet, sub.ResultRows, sub.ResultSize, sub.cols, sub.recs = r.has, r.rows, r.size, r.cols, r.recs
		})
		err = cmp.Or(err, qerr)
	}
	if tx {
		if err == nil {
			_, err = conn.Exec(ctx, "COMMIT")
		} else {
			conn.Exec(ctx, "ROLLBACK")
		}
	}
	s.update(st, func() { st.Duration = time.Since(start).Nanoseconds() })
	s.end(st, err)
}

// update changes a statement under the lock.
func (s *Service) update(st *stmt, f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f()
	st.UpdatedAt = core.Time(time.Now().UTC())
}

// end records how a statement ended, unless it was cancelled, and announces
// it when it asked to.
func (s *Service) end(st *stmt, err error) {
	s.mu.Lock()
	if st.Status != "ABORTED" {
		st.Status = "FINISHED"
		if err != nil {
			st.Status, st.Error = "FAILED", err.Error()
		}
	}
	for _, sub := range st.SubStatements {
		if running(sub) {
			sub.Status = "ABORTED"
		}
		st.HasResultSet = st.HasResultSet || sub.HasResultSet
	}
	st.UpdatedAt = core.Time(time.Now().UTC())
	detail, _ := json.Marshal(map[string]any{"principal": st.principal, "statementName": st.name, "statementId": st.Id,
		"redshiftQueryId": st.RedshiftQueryId, "state": st.Status, "rows": st.ResultRows, "expireAt": st.created.Add(24 * time.Hour).Unix()})
	resource := core.ARN("redshift", st.region, st.account, "cluster:"+st.ClusterIdentifier)
	if st.WorkgroupName != "" {
		resource = core.ARN("redshift-serverless", st.region, st.account, "workgroup/"+st.WorkgroupName)
	}
	event, bus := st.event, core.ARN("events", st.region, st.account, "event-bus/default")
	s.mu.Unlock()
	if event {
		s.hub.Deliver(bus, &core.Message{Body: string(detail), Source: "aws.redshift-data",
			DetailType: "Redshift Data Statement Status Change", Resources: []string{resource}})
	}
}

// result is what a statement returned.
type result struct {
	has        bool
	rows, size int64
	cols       []column
	recs       [][]*string
}

// query runs one SQL statement and reads its rows as text.
func query(ctx context.Context, conn *pgx.Conn, sql string, args []any) (*result, error) {
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	r := &result{}
	for _, f := range rows.FieldDescriptions() {
		typ := "unknown"
		if t, ok := conn.TypeMap().TypeForOID(f.DataTypeOID); ok {
			typ = t.Name
		}
		r.cols = append(r.cols, meta(f.Name, typ))
	}
	for rows.Next() {
		rec := make([]*string, len(r.cols))
		for i, v := range rows.RawValues() {
			if v != nil {
				s := string(v)
				rec[i], r.size = &s, r.size+int64(len(v))
			}
		}
		r.recs = append(r.recs, rec)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	r.has, r.rows = len(r.cols) > 0, rows.CommandTag().RowsAffected()
	return r, nil
}

// meta is the metadata of a column of a type.
func meta(name, typ string) column {
	return column{Name: name, Label: name, TypeName: typ, Nullable: 1,
		IsSigned:        slices.Contains([]string{"int2", "int4", "int8", "float4", "float8", "numeric"}, typ),
		IsCaseSensitive: slices.Contains([]string{"text", "varchar", "bpchar", "char", "name"}, typ)}
}

// field is the Data API field of a text value.
func field(v *string, typ string) map[string]any {
	if v == nil {
		return map[string]any{"isNull": true}
	}
	switch typ {
	case "int2", "int4", "int8", "oid":
		if n, err := strconv.ParseInt(*v, 10, 64); err == nil {
			return map[string]any{"longValue": n}
		}
	case "float4", "float8":
		if f, err := strconv.ParseFloat(*v, 64); err == nil && !math.IsInf(f, 0) && !math.IsNaN(f) {
			return map[string]any{"doubleValue": f}
		}
		return map[string]any{"doubleValue": *v} // NaN, Infinity and -Infinity, as the SDKs spell them
	case "bool":
		return map[string]any{"booleanValue": *v == "t"}
	}
	return map[string]any{"stringValue": *v}
}

// ---- SQL ----

// ponytail: regexes, not a SQL parser: a :name or a Redshift clause inside a
// comment or a dollar-quoted string is rewritten too.
var (
	paramRe    = regexp.MustCompile(`'(?:[^']|'')*'|"(?:[^"]|"")*"|::|:[A-Za-z_][A-Za-z0-9_]*`)
	ddlRe      = regexp.MustCompile(`(?is)^\s*(create|alter)\b`)
	identityRe = regexp.MustCompile(`(?i)^identity\s*\(\s*(-?\d+)\s*,\s*(-?\d+)\s*\)$`)
	clauseRe   = regexp.MustCompile(`(?i)'(?:[^']|'')*'|"(?:[^"]|"")*"|\bdiststyle\s+(?:auto|even|key|all)\b|` +
		`\b(?:compound\s+|interleaved\s+)?sortkey\b(?:\s+auto\b|\s*\([^)]*\))?|\bdistkey\b(?:\s*\([^)]*\))?|\bencode\s+\w+|` +
		`\bbackup\s+(?:yes|no)\b|\bidentity\s*\(\s*-?\d+\s*,\s*-?\d+\s*\)|\(\s*max\s*\)`)
)

// postgres rewrites Redshift-only DDL for PostgreSQL: DISTSTYLE, DISTKEY,
// SORTKEY, ENCODE and BACKUP go, IDENTITY(seed, step) becomes an identity
// column and (MAX) lengths the longest Redshift allows.
func postgres(sql string) string {
	if !ddlRe.MatchString(sql) {
		return sql
	}
	return clauseRe.ReplaceAllStringFunc(sql, func(m string) string {
		switch {
		case m[0] == '\'' || m[0] == '"':
			return m
		case m[0] == '(':
			return "(65535)"
		case identityRe.MatchString(m):
			return identityRe.ReplaceAllString(m, "GENERATED BY DEFAULT AS IDENTITY (START WITH $1 INCREMENT BY $2)")
		}
		return ""
	})
}

// bind rewrites statements for PostgreSQL and turns their :name parameters
// into $n with the values in order. Every parameter must be used.
func bind(sqls []string, params []param) ([]string, [][]any, error) {
	values, used := map[string]string{}, map[string]bool{}
	for _, p := range params {
		if p.Name == "" || p.Value == "" {
			return nil, nil, invalid("Parameters need a name and a value.")
		}
		values[p.Name] = p.Value
	}
	queries, args := make([]string, len(sqls)), make([][]any, len(sqls))
	var missing string
	for i, sql := range sqls {
		at := map[string]int{}
		queries[i] = paramRe.ReplaceAllStringFunc(postgres(sql), func(m string) string {
			if m[0] != ':' || m == "::" {
				return m
			}
			name := m[1:]
			if _, ok := at[name]; !ok {
				v, ok := values[name]
				if !ok {
					missing = name
					return m
				}
				args[i], at[name], used[name] = append(args[i], v), len(args[i])+1, true
			}
			return "$" + strconv.Itoa(at[name])
		})
	}
	if missing != "" {
		return nil, nil, invalid("The SQL uses parameter :%s, which Parameters lack.", missing)
	}
	for name := range values {
		if !used[name] {
			return nil, nil, invalid("Parameter %s is not used in the SQL.", name)
		}
	}
	return queries, args, nil
}

// ---- describing statements and their results ----

var idRe = regexp.MustCompile(`^[a-z0-9]{8}(-[a-z0-9]{4}){3}-[a-z0-9]{12}(:\d+)?$`)

type idInput struct {
	Id, NextToken   string
	WaitTimeSeconds int
}

// find returns the statement an Id names, or with a :n suffix the nth of a batch.
func (s *Service) find(c *core.Ctx, id string) (*stmt, error) {
	if !idRe.MatchString(id) {
		return nil, invalid("Id must be a statement id, with an optional :n suffix: %q", id)
	}
	base, n, sub := strings.Cut(id, ":")
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.st[base]
	if st == nil || st.account != c.Account || st.region != c.Region {
		return nil, core.Errorf(400, "ResourceNotFoundException", "Query does not exist.")
	}
	if !sub {
		return st, nil
	}
	i, _ := strconv.Atoi(n)
	if i < 1 || i > len(st.SubStatements) {
		return nil, core.Errorf(400, "ResourceNotFoundException", "Query does not exist.")
	}
	return st.SubStatements[i-1], nil
}

// view copies a statement to encode it after unlocking; lock held.
func (st *stmt) view() *stmt {
	v := *st
	v.SubStatements = nil
	for _, sub := range st.SubStatements {
		cp := *sub
		v.SubStatements = append(v.SubStatements, &cp)
	}
	return &v
}

func (s *Service) describeStatement(c *core.Ctx, in *idInput) (any, error) {
	st, err := s.find(c, in.Id)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return st.view(), nil
}

// rowsPerPage is how many records a page of results has.
// ponytail: a fixed row count; AWS pages by size.
const rowsPerPage = 1000

// getResult serves GetStatementResult (JSON) and GetStatementResultV2 (CSV).
func (s *Service) getResult(c *core.Ctx, in *idInput) (any, error) {
	st, err := s.find(c, in.Id)
	if err != nil {
		return nil, err
	}
	s.wait(st, in.WaitTimeSeconds)
	s.mu.Lock()
	defer s.mu.Unlock()
	v2 := c.Op == "GetStatementResultV2"
	switch {
	case st.Status != "FINISHED" || !st.HasResultSet || len(st.SubStatements) > 0:
		return nil, core.Errorf(400, "ResourceNotFoundException", "Query does not have result. Please check query status with DescribeStatement.")
	case v2 != (st.ResultFormat == "CSV"):
		return nil, invalid("The statement's results are in %s format: use %s.", st.ResultFormat,
			map[bool]string{true: "GetStatementResultV2", false: "GetStatementResult"}[st.ResultFormat == "CSV"])
	}
	recs, next, err := page(st.recs, in.NextToken, rowsPerPage)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"ColumnMetadata": st.cols, "TotalNumRows": len(st.recs)}
	if next != "" {
		out["NextToken"] = next
	}
	if v2 {
		var b bytes.Buffer
		w := csv.NewWriter(&b)
		if in.NextToken == "" {
			w.Write(each(st.cols, func(c column) string { return c.Name }))
		}
		for _, r := range recs {
			w.Write(each(r, text))
		}
		w.Flush()
		out["Records"], out["ResultFormat"] = []map[string]string{{"CSVRecords": b.String()}}, "CSV"
		return out, nil
	}
	out["Records"] = each(recs, func(r []*string) []map[string]any {
		row := make([]map[string]any, len(r))
		for i, v := range r {
			row[i] = field(v, st.cols[i].TypeName)
		}
		return row
	})
	return out, nil
}

type listInput struct {
	ClusterIdentifier, WorkgroupName, Database, StatementName, Status, NextToken string
	MaxResults                                                                   int
	RoleLevel                                                                    *bool
}

// statementData is an item of ListStatements.
type statementData struct {
	Id               string
	Status           string
	StatementName    string   `json:",omitempty"`
	QueryString      string   `json:",omitempty"`
	QueryStrings     []string `json:",omitempty"`
	IsBatchStatement bool
	CreatedAt        core.Time
	UpdatedAt        core.Time
	SecretArn        string  `json:",omitempty"`
	QueryParameters  []param `json:",omitempty"`
	ResultFormat     string
}

// listStatements lists the statements of the account, newest first.
// ponytail: RoleLevel is ignored; every caller sees every statement.
func (s *Service) listStatements(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []statementData
	for _, st := range s.st {
		switch {
		case st.account != c.Account || st.region != c.Region,
			in.Status != "" && in.Status != "ALL" && st.Status != in.Status,
			!strings.HasPrefix(st.name, in.StatementName),
			in.ClusterIdentifier != "" && st.ClusterIdentifier != in.ClusterIdentifier,
			in.WorkgroupName != "" && st.WorkgroupName != in.WorkgroupName,
			in.Database != "" && st.Database != in.Database:
			continue
		}
		list = append(list, statementData{Id: st.Id, Status: st.Status, StatementName: st.name, QueryString: st.QueryString,
			QueryStrings: each(st.SubStatements, func(sub *stmt) string { return sub.QueryString }), IsBatchStatement: len(st.SubStatements) > 0,
			CreatedAt: st.CreatedAt, UpdatedAt: st.UpdatedAt, SecretArn: st.SecretArn, QueryParameters: st.QueryParameters, ResultFormat: st.ResultFormat})
	}
	slices.SortFunc(list, func(a, b statementData) int {
		return cmp.Or(time.Time(b.CreatedAt).Compare(time.Time(a.CreatedAt)), strings.Compare(a.Id, b.Id))
	})
	list, next, err := page(list, in.NextToken, min(cmp.Or(in.MaxResults, 100), 100))
	if err != nil {
		return nil, err
	}
	out := map[string]any{"Statements": list}
	if next != "" {
		out["NextToken"] = next
	}
	return out, nil
}

func (s *Service) cancelStatement(c *core.Ctx, in *idInput) (any, error) {
	st, err := s.find(c, in.Id)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !running(st) || st.cancel == nil {
		return nil, invalid("Could not cancel a query that is already in %s state.", st.Status)
	}
	st.Status, st.UpdatedAt = "ABORTED", core.Time(time.Now().UTC())
	for _, sub := range st.SubStatements {
		if running(sub) {
			sub.Status = "ABORTED"
		}
	}
	st.cancel()
	return map[string]bool{"Status": true}, nil
}

// ---- catalog ----

type catalogInput struct {
	ClusterIdentifier, WorkgroupName, Database, ConnectedDatabase, DbUser, SecretArn string
	SchemaPattern, TablePattern, Schema, Table, NextToken                            string
	MaxResults                                                                       int
}

// catalog runs a query on the database a request names and returns its rows.
// ponytail: ConnectedDatabase is ignored, PostgreSQL has no cross-database queries.
func (s *Service) catalog(c *core.Ctx, in *catalogInput, db, sql string, args ...any) ([][]string, error) {
	t, err := s.connect(c, in.ClusterIdentifier, in.WorkgroupName, in.SecretArn)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := t.db.Connect(ctx, t.user, t.password, db)
	if err != nil {
		return nil, core.Errorf(400, "DatabaseConnectionException", "%v", err)
	}
	defer conn.Close(ctx)
	r, err := query(ctx, conn, sql, args)
	if err != nil {
		return nil, err
	}
	return each(r.recs, func(rec []*string) []string { return each(rec, text) }), nil
}

func (s *Service) listDatabases(c *core.Ctx, in *catalogInput) (any, error) {
	rows, err := s.catalog(c, in, in.Database, "SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY 1")
	if err != nil {
		return nil, err
	}
	return listOut("Databases", each(rows, func(r []string) string { return r[0] }), in.NextToken, in.MaxResults)
}

func (s *Service) listSchemas(c *core.Ctx, in *catalogInput) (any, error) {
	if in.Database == "" {
		return nil, invalid("Database must be given.")
	}
	rows, err := s.catalog(c, in, in.Database, `SELECT nspname FROM pg_namespace WHERE nspname LIKE $1 AND nspname !~ '^pg_(toast|temp)' ORDER BY 1`,
		cmp.Or(in.SchemaPattern, "%"))
	if err != nil {
		return nil, err
	}
	return listOut("Schemas", each(rows, func(r []string) string { return r[0] }), in.NextToken, in.MaxResults)
}

// table is a TableMember.
type table struct {
	Name   string `json:"name"`
	Schema string `json:"schema"`
	Type   string `json:"type"`
}

func (s *Service) listTables(c *core.Ctx, in *catalogInput) (any, error) {
	if in.Database == "" {
		return nil, invalid("Database must be given.")
	}
	rows, err := s.catalog(c, in, in.Database, `SELECT table_name, table_schema, CASE table_type WHEN 'BASE TABLE' THEN 'TABLE' ELSE table_type END
FROM information_schema.tables WHERE table_schema LIKE $1 AND table_name LIKE $2 ORDER BY 2, 1`,
		cmp.Or(in.SchemaPattern, "%"), cmp.Or(in.TablePattern, "%"))
	if err != nil {
		return nil, err
	}
	return listOut("Tables", each(rows, func(r []string) table { return table{r[0], r[1], r[2]} }), in.NextToken, in.MaxResults)
}

func (s *Service) describeTable(c *core.Ctx, in *catalogInput) (any, error) {
	if in.Database == "" {
		return nil, invalid("Database must be given.")
	}
	rows, err := s.catalog(c, in, in.Database, `SELECT column_name, udt_name, coalesce(character_maximum_length, numeric_precision, 0),
  coalesce(numeric_scale, 0), is_nullable, coalesce(column_default, ''), table_schema, table_name
FROM information_schema.columns WHERE ($1 = '' OR table_schema = $1) AND ($2 = '' OR table_name = $2)
ORDER BY table_schema, table_name, ordinal_position`, in.Schema, in.Table)
	if err != nil {
		return nil, err
	}
	out, err := listOut("ColumnList", each(rows, func(r []string) column {
		col := meta(r[0], r[1])
		col.Precision, _ = strconv.Atoi(r[2])
		col.Scale, _ = strconv.Atoi(r[3])
		if p, ok := map[string]int{"int2": 5, "int4": 10, "int8": 19, "float4": 8, "float8": 17}[r[1]]; ok { // decimal digits, not bits
			col.Precision = p
		}
		col.Nullable = map[string]int{"NO": 0, "YES": 1}[r[4]]
		col.ColumnDefault, col.SchemaName, col.TableName = r[5], r[6], r[7]
		return col
	}), in.NextToken, in.MaxResults)
	if err == nil && in.Table != "" {
		out["TableName"] = in.Table
	}
	return out, err
}

// ---- helpers ----

// text is a text value, "" for NULL.
func text(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func each[T, U any](in []T, f func(T) U) []U {
	out := make([]U, len(in))
	for i, v := range in {
		out[i] = f(v)
	}
	return out
}

// page returns at most max items from the offset in token, and the token of the next page.
func page[T any](items []T, token string, max int) ([]T, string, error) {
	from := 0
	if token != "" {
		n, err := strconv.Atoi(token)
		if err != nil || n < 0 || n > len(items) {
			return nil, "", invalid("Invalid NextToken.")
		}
		from = n
	}
	to, next := min(from+max, len(items)), ""
	if to < len(items) {
		next = strconv.Itoa(to)
	}
	return items[from:to], next, nil
}

// listOut is a catalog listing: name holds a page of at most MaxResults (default and cap 1000) items.
func listOut[T any](name string, items []T, token string, max int) (map[string]any, error) {
	if max <= 0 || max > 1000 {
		max = 1000
	}
	items, next, err := page(items, token, max)
	if err != nil {
		return nil, err
	}
	out := map[string]any{name: items}
	if next != "" {
		out["NextToken"] = next
	}
	return out, nil
}

// ---- console ----

// Save and Load keep nothing: statements and their results live in memory.
// ponytail: AWS keeps them a day; persist them if restarts lose ones you need.
func (s *Service) Save() ([]byte, error) { return []byte("{}"), nil }
func (s *Service) Load([]byte) error     { s.Reset(); return nil }

func (s *Service) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.st {
		st.cancel()
	}
	s.st = map[string]*stmt{}
}

// Close cancels the running statements.
func (s *Service) Close() error {
	s.Reset()
	return nil
}

// Resources lists statements for the console, newest first.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Statements", Columns: []string{"Id", "Status", "SQL", "Cluster or workgroup", "Database", "Created", "Region", "Account"}}
	for _, st := range s.st {
		sql := st.QueryString
		if len(st.SubStatements) > 0 {
			sql = strings.Join(each(st.SubStatements, func(sub *stmt) string { return sub.QueryString }), "; ")
		}
		t.Rows = append(t.Rows, []string{st.Id, st.Status, sql, cmp.Or(st.ClusterIdentifier, st.WorkgroupName), st.Database,
			st.created.Format(time.RFC3339), st.region, st.account})
	}
	slices.SortFunc(t.Rows, func(a, b []string) int { return strings.Compare(b[5], a[5]) })
	return []core.Table{t}
}

// Inspect returns a statement as DescribeStatement shows it.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st := s.st[id]; st != nil {
		return st.view()
	}
	return nil
}
