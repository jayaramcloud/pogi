// Package rdsdata emulates the RDS Data API (REST-JSON): SQL statements,
// batches and transactions on the databases of pogi's Aurora clusters, with
// credentials from Secrets Manager. PostgreSQL goes through pgx, MySQL
// through its database/sql driver.
package rdsdata

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the RDS Data API emulator.
type Service struct {
	hub  *core.Hub
	mux  *http.ServeMux
	stop func()
	mu   sync.Mutex
	dbs  map[string]*sql.DB // pools by cluster, credentials, database and schema
	txs  map[string]*tx     // open transactions by id
}

// tx is an open transaction; it runs one statement at a time.
type tx struct {
	*sql.Tx
	mysql    bool
	resource string
	used     time.Time
	mu       sync.Mutex
}

// timeout is the Data API's limit on a call.
const timeout = 45 * time.Second

// The Data API's size limits: one row, and all rows of one call.
const (
	maxRow    = 64 << 10
	maxResult = 1 << 20
)

// size is the number of bytes a row holds, as the Data API counts them.
func size(vals []any) (n int) {
	for _, v := range vals {
		switch v := v.(type) {
		case string:
			n += len(v)
		case []byte:
			n += len(v)
		default:
			n += 8
		}
	}
	return n
}

// New creates the Data API service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux(), dbs: map[string]*sql.DB{}, txs: map[string]*tx{}}
	for pattern, h := range map[string]http.HandlerFunc{
		"POST /Execute":             handle("ExecuteStatement", s.execute),
		"POST /BatchExecute":        handle("BatchExecuteStatement", s.batch),
		"POST /BeginTransaction":    handle("BeginTransaction", s.begin),
		"POST /CommitTransaction":   handle("CommitTransaction", s.end),
		"POST /RollbackTransaction": handle("RollbackTransaction", s.end),
		"POST /ExecuteSql":          handle("ExecuteSql", s.executeSQL),
		"/":                         handle("Unknown", s.unknown),
	} {
		s.mux.HandleFunc(pattern, h)
	}
	s.stop = core.Every(10*time.Second, s.Tick)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "rds-data", Cloud: core.AWS, Title: "RDS Data API", Signing: []string{"rds-data"}, Protocol: "rest-json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Close rolls back open transactions and closes the pools.
func (s *Service) Close() error {
	s.stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, t := range s.txs {
		t.Rollback()
		delete(s.txs, id)
	}
	for k, db := range s.dbs {
		db.Close()
		delete(s.dbs, k)
	}
	return nil
}

// Tick rolls back transactions idle for three minutes, as the Data API does.
func (s *Service) Tick(now time.Time) {
	s.mu.Lock()
	var idle []*tx
	for id, t := range s.txs {
		if now.Sub(t.used) > 3*time.Minute {
			idle = append(idle, t)
			delete(s.txs, id)
		}
	}
	s.mu.Unlock()
	for _, t := range idle {
		t.Rollback()
	}
}

// input is the request of every operation.
type input struct {
	ResourceArn, SecretArn, Sql, Database, Schema, TransactionId, FormatRecordsAs string
	IncludeResultMetadata, ContinueAfterTimeout                                   bool
	Parameters                                                                    []param
	ParameterSets                                                                 [][]param
	DbClusterOrInstanceArn, AwsSecretStoreArn, SqlStatements                      string // ExecuteSql
}

type param struct {
	Name, TypeHint string
	Value          value
}

// value is a Field union.
type value struct {
	IsNull       *bool
	BooleanValue *bool
	LongValue    *int64
	DoubleValue  *float64
	StringValue  *string
	BlobValue    []byte
	ArrayValue   *array
}

type array struct {
	BooleanValues []bool
	LongValues    []int64
	DoubleValues  []float64
	StringValues  []string
	ArrayValues   []array
}

func (v *value) arg() any {
	switch {
	case v.BooleanValue != nil:
		return *v.BooleanValue
	case v.LongValue != nil:
		return *v.LongValue
	case v.DoubleValue != nil:
		return *v.DoubleValue
	case v.StringValue != nil:
		return *v.StringValue
	case v.BlobValue != nil:
		return v.BlobValue
	case v.ArrayValue != nil:
		return v.ArrayValue.slice()
	}
	return nil // isNull
}

func (a *array) slice() any {
	switch {
	case a.BooleanValues != nil:
		return a.BooleanValues
	case a.LongValues != nil:
		return a.LongValues
	case a.DoubleValues != nil:
		return a.DoubleValues
	case a.StringValues != nil:
		return a.StringValues
	}
	out := make([]any, len(a.ArrayValues))
	for i := range a.ArrayValues {
		out[i] = a.ArrayValues[i].slice()
	}
	return out
}

// result is an ExecuteStatement response.
type result struct {
	Records                [][]map[string]any `json:"records,omitempty"`
	ColumnMetadata         []column           `json:"columnMetadata,omitempty"`
	NumberOfRecordsUpdated int64              `json:"numberOfRecordsUpdated"`
	GeneratedFields        []map[string]any   `json:"generatedFields,omitempty"`
	FormattedRecords       string             `json:"formattedRecords,omitempty"`
}

type column struct {
	Name                string `json:"name"`
	Label               string `json:"label"`
	TypeName            string `json:"typeName"`
	Type                int    `json:"type"` // java.sql.Types
	Nullable            int    `json:"nullable"`
	Precision           int    `json:"precision"`
	Scale               int    `json:"scale"`
	IsSigned            bool   `json:"isSigned"`
	IsAutoIncrement     bool   `json:"isAutoIncrement"`
	IsCaseSensitive     bool   `json:"isCaseSensitive"`
	IsCurrency          bool   `json:"isCurrency"`
	SchemaName          string `json:"schemaName"`
	TableName           string `json:"tableName"`
	ArrayBaseColumnType int    `json:"arrayBaseColumnType"`
}

func badRequest(format string, a ...any) error {
	return core.Errorf(400, "BadRequestException", format, a...)
}

// handle runs one operation and writes its REST-JSON result or error.
func handle(op string, fn func(*core.Ctx, *input) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := core.C(r)
		c.Op = op
		var in input
		err := c.Intercept()
		if b := core.Body(r); err == nil && len(b) > 0 && json.Unmarshal(b, &in) != nil {
			err = badRequest("The request body is not valid JSON.")
		}
		var out any
		if err == nil {
			out, err = fn(c, &in)
		}
		if err != nil {
			core.RESTJSONError(w, c, err)
			return
		}
		core.RESTJSON(w, c, http.StatusOK, out)
	}
}

func (s *Service) unknown(c *core.Ctx, in *input) (any, error) {
	return nil, core.Errorf(404, "UnknownOperationException", "pogi does not implement this RDS Data API request")
}

// open returns the pool of a call's cluster, secret and database, and whether it speaks MySQL.
func (s *Service) open(c *core.Ctx, in *input) (*sql.DB, bool, error) {
	svc, _ := s.hub.Service("rds").(*rds.Service)
	if svc == nil {
		return nil, false, badRequest("RDS is not enabled in pogi.")
	}
	t, ok := svc.DataTarget(in.ResourceArn)
	switch {
	case !ok:
		return nil, false, core.Errorf(404, "NotFoundException", "DB cluster %s not found.", in.ResourceArn)
	case !t.HTTPEndpoint:
		return nil, false, badRequest("HttpEndpoint is not enabled for cluster %s. Please refer to "+
			"https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.html#data-api.troubleshooting", t.ID)
	}
	user, pw, err := s.secret(c, in.SecretArn)
	switch {
	case err != nil:
		return nil, false, err
	case t.Status != "available":
		return nil, false, badRequest("DB cluster %s is %s, not available.", t.ID, t.Status)
	case t.Port == 0:
		return nil, false, badRequest("%s.", rds.NoDocker)
	}
	my := strings.Contains(t.Engine, "mysql")
	if !my && !strings.Contains(t.Engine, "postgres") {
		return nil, false, badRequest("The RDS Data API does not support the %s engine.", t.Engine)
	}
	db := cmp.Or(in.Database, t.Database)
	if !my {
		db = cmp.Or(db, "postgres")
	}
	k := strings.Join([]string{t.Engine, strconv.Itoa(t.Port), user, pw, db, in.Schema}, "\x00")
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.dbs[k]; p != nil {
		return p, my, nil
	}
	var p *sql.DB
	if my {
		cfg := mysql.NewConfig()
		cfg.Net, cfg.Addr, cfg.User, cfg.Passwd, cfg.DBName, cfg.Timeout = "tcp", s.hub.Docker().Addr(t.Port), user, pw, db, 10*time.Second
		conn, err := mysql.NewConnector(cfg)
		if err != nil {
			return nil, false, err
		}
		p = sql.OpenDB(conn)
	} else {
		cfg, err := pgx.ParseConfig("sslmode=disable")
		if err != nil {
			return nil, false, err
		}
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database = "127.0.0.1", uint16(t.Port), user, pw, db
		cfg.Fallbacks, cfg.ValidateConnect, cfg.ConnectTimeout, cfg.RuntimeParams = nil, nil, 10*time.Second, map[string]string{}
		cfg.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec // no statement cache to go stale after DDL
		if in.Schema != "" {
			cfg.RuntimeParams["search_path"] = in.Schema
		}
		p = stdlib.OpenDB(*cfg)
	}
	p.SetConnMaxIdleTime(time.Minute)
	s.dbs[k] = p
	return p, my, nil
}

// secret reads the username and password a Secrets Manager secret holds.
func (s *Service) secret(c *core.Ctx, arn string) (string, string, error) {
	var out struct{ SecretString string }
	if err := s.hub.Call(c.Account, c.Region, "secretsmanager", "GetSecretValue", map[string]string{"SecretId": arn}, &out); err != nil {
		return "", "", core.Errorf(404, "NotFoundException", "Error fetching secret %s : %s", arn, core.AsError(err).Msg)
	}
	var v struct{ Username, Password string }
	if json.Unmarshal([]byte(out.SecretString), &v) != nil || v.Username == "" {
		return "", "", badRequest("The secret %s must hold a JSON username and password.", arn)
	}
	return v.Username, v.Password, nil
}

// tx finds an open transaction of the call's cluster and marks it used;
// take also ends its tracking.
func (s *Service) tx(in *input, take bool) (*tx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.txs[in.TransactionId]
	if t == nil || t.resource != in.ResourceArn {
		return nil, core.Errorf(404, "TransactionNotFoundException", "Transaction %s is not found", in.TransactionId)
	}
	if take {
		delete(s.txs, in.TransactionId)
	}
	t.used = time.Now()
	return t, nil
}

// runner is a pool or a transaction.
type runner interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// run calls fn with the call's transaction or pool within the 45-second
// limit; past it the statement is cancelled unless continueAfterTimeout.
func (s *Service) run(c *core.Ctx, in *input, fn func(context.Context, runner, bool) (any, error)) (any, error) {
	var q runner
	var my bool
	if in.TransactionId != "" {
		t, err := s.tx(in, false)
		if err != nil {
			return nil, err
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		q, my = t.Tx, t.mysql
	} else {
		p, m, err := s.open(c, in)
		if err != nil {
			return nil, err
		}
		q, my = p, m
	}
	ctx, cancel := context.WithCancel(context.Background())
	type res struct {
		out any
		err error
	}
	done := make(chan res, 1)
	core.Go("rdsdata.run", func() {
		defer cancel()
		var out any
		err := errors.New("internal error") // sent if fn panics
		defer func() { done <- res{out, err} }()
		out, err = fn(ctx, q, my)
	})
	select {
	case r := <-done:
		return r.out, dbError(r.err)
	case <-time.After(timeout):
		if !in.ContinueAfterTimeout {
			cancel()
		}
		return nil, core.Errorf(400, "StatementTimeoutException", "The statement did not finish within %s.", timeout)
	}
}

// dbError gives a database error the Data API's code: refused credentials
// are ForbiddenException, other server errors DatabaseErrorException.
func dbError(err error) error {
	var ce *core.Error
	var pe *pgconn.PgError
	var me *mysql.MySQLError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &ce):
		return ce
	case errors.As(err, &pe) && strings.HasPrefix(pe.Code, "28"), errors.As(err, &me) && me.Number == 1045:
		return core.Errorf(403, "ForbiddenException", "%v", err)
	case errors.As(err, &pe):
		return core.Errorf(400, "DatabaseErrorException", "ERROR: %s; SQLState: %s", pe.Message, pe.Code)
	case errors.As(err, &me):
		return core.Errorf(400, "DatabaseErrorException", "%s; SQLState: %s", me.Message, me.SQLState[:])
	}
	return badRequest("%v", err)
}

func (s *Service) execute(c *core.Ctx, in *input) (any, error) {
	if strings.TrimSpace(in.Sql) == "" {
		return nil, badRequest("The sql parameter is required.")
	}
	return s.run(c, in, func(ctx context.Context, q runner, my bool) (any, error) {
		return statement(ctx, q, my, in, in.Parameters)
	})
}

// batch runs the statement once per parameter set, all in one transaction:
// the call's, or one of its own.
func (s *Service) batch(c *core.Ctx, in *input) (any, error) {
	return s.run(c, in, func(ctx context.Context, q runner, my bool) (any, error) {
		var own *sql.Tx
		if p, ok := q.(*sql.DB); ok {
			t, err := p.BeginTx(ctx, nil)
			if err != nil {
				return nil, err
			}
			defer t.Rollback() // after a commit, a no-op
			q, own = t, t
		}
		results := []map[string]any{}
		for _, set := range in.ParameterSets {
			r, err := statement(ctx, q, my, in, set)
			if err != nil {
				return nil, err
			}
			gf := r.GeneratedFields
			if gf == nil {
				gf = []map[string]any{}
			}
			results = append(results, map[string]any{"generatedFields": gf})
		}
		if own != nil {
			if err := own.Commit(); err != nil {
				return nil, err
			}
		}
		return map[string]any{"updateResults": results}, nil
	})
}

func (s *Service) begin(c *core.Ctx, in *input) (any, error) {
	p, my, err := s.open(c, in)
	if err != nil {
		return nil, err
	}
	t, err := p.BeginTx(context.Background(), nil) // it outlives this call
	if err != nil {
		return nil, dbError(err)
	}
	id := core.RandString(64, core.AlphaNum)
	s.mu.Lock()
	s.txs[id] = &tx{Tx: t, mysql: my, resource: in.ResourceArn, used: time.Now()}
	s.mu.Unlock()
	return map[string]string{"transactionId": id}, nil
}

// end serves CommitTransaction and RollbackTransaction.
func (s *Service) end(c *core.Ctx, in *input) (any, error) {
	t, err := s.tx(in, true)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if c.Op == "RollbackTransaction" {
		t.Rollback()
		return map[string]string{"transactionStatus": "Rollback Complete"}, nil
	}
	if err := t.Commit(); err != nil {
		return nil, dbError(err)
	}
	return map[string]string{"transactionStatus": "Transaction Committed"}, nil
}

// executeSQL is the deprecated ExecuteSql.
// ponytail: sqlStatements runs as one statement; split it if a client sends several.
func (s *Service) executeSQL(c *core.Ctx, in *input) (any, error) {
	in.ResourceArn, in.SecretArn, in.Sql, in.IncludeResultMetadata = in.DbClusterOrInstanceArn, in.AwsSecretStoreArn, in.SqlStatements, true
	return s.run(c, in, func(ctx context.Context, q runner, my bool) (any, error) {
		r, err := statement(ctx, q, my, in, nil)
		if err != nil {
			return nil, err
		}
		out := map[string]any{"numberOfRecordsUpdated": r.NumberOfRecordsUpdated}
		if r.ColumnMetadata != nil {
			legacy := map[string]string{"longValue": "bigIntValue", "booleanValue": "bitValue"}
			recs := make([]map[string]any, len(r.Records))
			for i, rec := range r.Records {
				vals := make([]map[string]any, len(rec))
				for j, f := range rec {
					for k, v := range f {
						vals[j] = map[string]any{cmp.Or(legacy[k], k): v}
					}
				}
				recs[i] = map[string]any{"values": vals}
			}
			out["resultFrame"] = map[string]any{"records": recs,
				"resultSetMetadata": map[string]any{"columnCount": len(r.ColumnMetadata), "columnMetadata": r.ColumnMetadata}}
		}
		return map[string]any{"sqlStatementResults": []any{out}}, nil
	})
}

var (
	// rowsRe tells statements that return rows (run as queries) from the
	// others (execs, which tell the rows they changed and MySQL's insert id).
	rowsRe      = regexp.MustCompile(`(?is)^(\s+|--[^\n]*|#[^\n]*|/\*.*?\*/|\()*(select|with|show|values|table|explain|describe|desc)\b|\breturning\b`)
	returningRe = regexp.MustCompile(`(?i)\breturning\b`)
)

// statement runs one SQL statement with a set of Data API parameters.
func statement(ctx context.Context, q runner, my bool, in *input, params []param) (*result, error) {
	text, args := bind(in.Sql, params, my)
	out := &result{}
	if !rowsRe.MatchString(text) {
		res, err := q.ExecContext(ctx, text, args...)
		if err != nil {
			return nil, err
		}
		out.NumberOfRecordsUpdated, _ = res.RowsAffected()
		if id, err := res.LastInsertId(); err == nil && id > 0 { // MySQL's AUTO_INCREMENT
			out.GeneratedFields = []map[string]any{{"longValue": id}}
		}
		return out, nil
	}
	rows, err := q.QueryContext(ctx, text, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	vals, ptrs := make([]any, len(cols)), make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	records := [][]map[string]any{}
	total := 0
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		n := size(vals)
		if total += n; n > maxRow || total > maxResult {
			return nil, badRequest("Database response exceeded size limit")
		}
		rec := make([]map[string]any, len(cols))
		for i, v := range vals {
			rec[i] = field(v, typeOf(cols[i]))
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if in.IncludeResultMetadata {
		out.ColumnMetadata = metadata(cols, my)
	}
	if strings.EqualFold(in.FormatRecordsAs, "JSON") {
		out.FormattedRecords = formatted(cols, records)
	} else {
		out.Records = records
	}
	if returningRe.MatchString(text) {
		out.NumberOfRecordsUpdated = int64(len(records))
	}
	return out, nil
}

// casts carry a Postgres parameter's typeHint.
var casts = map[string]string{"DATE": "::date", "TIME": "::time", "TIMESTAMP": "::timestamp", "DECIMAL": "::numeric", "JSON": "::json", "UUID": "::uuid"}

var dollar = regexp.MustCompile(`^\$([A-Za-z_][A-Za-z0-9_]*)?\$`)

// bind turns :name placeholders outside quotes and comments into the
// driver's ($n for Postgres, cast by typeHint; ? for MySQL) and returns the
// arguments in order. A name without a parameter stays as it is.
func bind(text string, params []param, my bool) (string, []any) {
	byName := map[string]*param{}
	for i := range params {
		byName[params[i].Name] = &params[i]
	}
	var b strings.Builder
	var args []any
	pos := map[string]int{}
	for i := 0; i < len(text); {
		c, j := text[i], i+1
		switch {
		case c == '\'' || c == '"' || c == '`':
			for ; j < len(text) && text[j] != c; j++ {
				if text[j] == '\\' && my {
					j++
				}
			}
			j = min(j+1, len(text))
		case strings.HasPrefix(text[i:], "--") || c == '#' && my:
			if j = strings.IndexByte(text[i:], '\n'); j < 0 {
				j = len(text)
			} else {
				j += i
			}
		case strings.HasPrefix(text[i:], "/*"):
			if j = strings.Index(text[i+2:], "*/"); j < 0 {
				j = len(text)
			} else {
				j += i + 4
			}
		case c == '$' && !my: // $tag$ … $tag$
			if tag := dollar.FindString(text[i:]); tag != "" {
				if k := strings.Index(text[i+len(tag):], tag); k >= 0 {
					j = i + 2*len(tag) + k
				} else {
					j = len(text)
				}
			}
		case c == ':' && j < len(text) && text[j] == ':': // a Postgres cast
			j++
		case c == ':' && j < len(text) && (text[j] == '_' || isLetter(text[j]) || isDigit(text[j])): // :1 too, as drizzle writes them
			for j < len(text) && (text[j] == '_' || isLetter(text[j]) || isDigit(text[j])) {
				j++
			}
			p := byName[text[i+1:j]]
			if p == nil {
				break
			}
			if my {
				args = append(args, p.Value.arg())
				b.WriteByte('?')
			} else {
				n, ok := pos[p.Name]
				if !ok {
					args = append(args, p.Value.arg())
					n, pos[p.Name] = len(args), len(args)
				}
				b.WriteString("$" + strconv.Itoa(n) + casts[strings.ToUpper(p.TypeHint)])
			}
			i = j
			continue
		}
		b.WriteString(text[i:j])
		i = j
	}
	return b.String(), args
}

func isLetter(c byte) bool { return 'a' <= c|0x20 && c|0x20 <= 'z' }
func isDigit(c byte) bool  { return '0' <= c && c <= '9' }

// jdbc maps database type names to java.sql.Types, as the Data API reports them.
var jdbc = map[string]int{
	"bit": -7, "bool": -7, "tinyint": -6, "smallint": 5, "int2": 5, "mediumint": 4, "int": 4, "int4": 4, "serial": 4,
	"bigint": -5, "int8": -5, "float": 7, "float4": 7, "double": 8, "float8": 8, "decimal": 3, "numeric": 2,
	"char": 1, "bpchar": 1, "varchar": 12, "text": 12, "name": 12, "enum": 12, "set": 12, "tinytext": 12, "mediumtext": -1, "longtext": -1,
	"date": 91, "year": 91, "time": 92, "timetz": 92, "datetime": 93, "timestamp": 93, "timestamptz": 93,
	"bytea": -2, "binary": -2, "varbinary": -3, "tinyblob": -3, "blob": -4, "mediumblob": -4, "longblob": -4,
}

// typeOf is a column's database type name in lower case, without UNSIGNED.
func typeOf(c *sql.ColumnType) string {
	return strings.ToLower(strings.TrimPrefix(c.DatabaseTypeName(), "UNSIGNED "))
}

func metadata(cols []*sql.ColumnType, my bool) []column {
	out := make([]column, len(cols))
	for i, c := range cols {
		typ, name := typeOf(c), c.DatabaseTypeName()
		jt, ok := jdbc[typ]
		switch {
		case !ok && strings.HasPrefix(typ, "_"):
			jt = 2003 // ARRAY
		case !ok:
			jt = 1111 // OTHER
		}
		if !my {
			name = typ
		}
		m := column{Name: c.Name(), Label: c.Name(), TypeName: name, Type: jt, Nullable: 2} // nullable unknown
		switch jt {
		case -6, -5, 2, 3, 4, 5, 7, 8:
			m.IsSigned = !strings.HasPrefix(c.DatabaseTypeName(), "UNSIGNED")
		}
		if n, ok := c.Nullable(); ok {
			m.Nullable = map[bool]int{false: 0, true: 1}[n]
		}
		if p, s, ok := c.DecimalSize(); ok {
			m.Precision, m.Scale = int(p), int(s)
		} else if l, ok := c.Length(); ok {
			m.Precision = int(min(l, math.MaxInt32))
		}
		out[i] = m
	}
	return out
}

// field is the Data API form of a column value of type typ.
func field(v any, typ string) map[string]any {
	if elem, ok := strings.CutPrefix(typ, "_"); ok { // a Postgres array arrives as its text form
		var text string
		switch v := v.(type) {
		case string:
			text = v
		case []byte:
			text = string(v)
		}
		if elems, ok := pgArray(text); ok {
			if a, ok := arrayField(elems, elem); ok {
				return map[string]any{"arrayValue": a}
			}
		}
	}
	switch v := v.(type) {
	case nil:
		return map[string]any{"isNull": true}
	case bool:
		return map[string]any{"booleanValue": v}
	case int64:
		return map[string]any{"longValue": v}
	case uint64:
		return map[string]any{"longValue": int64(v)}
	case float64:
		return map[string]any{"doubleValue": v}
	case float32:
		return map[string]any{"doubleValue": float64(v)}
	case time.Time:
		layout := "2006-01-02 15:04:05.999999"
		if typ == "date" {
			layout = "2006-01-02"
		}
		return map[string]any{"stringValue": v.UTC().Format(layout)}
	case []byte:
		if j := jdbc[typ]; j == -7 || j >= -4 && j <= -2 {
			return map[string]any{"blobValue": v}
		}
		return map[string]any{"stringValue": string(v)}
	}
	return map[string]any{"stringValue": fmt.Sprint(v)}
}

// formatted is formattedRecords: the rows as a JSON array of objects, in column order.
func formatted(cols []*sql.ColumnType, records [][]map[string]any) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, rec := range records {
		b.WriteString(map[bool]string{true: ",{", false: "{"}[i > 0])
		for j, f := range rec {
			var v any
			for k, x := range f {
				if k != "isNull" {
					v = x
				}
			}
			name, _ := json.Marshal(cols[j].Name())
			val, _ := json.Marshal(v)
			if j > 0 {
				b.WriteByte(',')
			}
			b.Write(name)
			b.WriteByte(':')
			b.Write(val)
		}
		b.WriteByte('}')
	}
	b.WriteByte(']')
	return b.String()
}

// pgArray parses a Postgres array literal such as {1,"a b",{2,3}} into nested
// []any holding strings (nil for NULL). ok is false when s is not one.
func pgArray(s string) (out []any, ok bool) {
	var parse func(i int) ([]any, int, bool)
	parse = func(i int) ([]any, int, bool) {
		if i >= len(s) || s[i] != '{' {
			return nil, 0, false
		}
		out := []any{}
		for i++; i < len(s); i++ {
			switch s[i] {
			case '}':
				return out, i + 1, true
			case ',':
				continue
			case '{':
				sub, n, ok := parse(i)
				if !ok {
					return nil, 0, false
				}
				out, i = append(out, sub), n-1
			case '"':
				var b strings.Builder
				for i++; i < len(s) && s[i] != '"'; i++ {
					if s[i] == '\\' && i+1 < len(s) {
						i++
					}
					b.WriteByte(s[i])
				}
				out = append(out, b.String())
			default:
				n := strings.IndexAny(s[i:], ",}")
				if n < 0 {
					return nil, 0, false
				}
				if w := s[i : i+n]; strings.EqualFold(w, "null") {
					out = append(out, nil)
				} else {
					out = append(out, w)
				}
				i += n - 1
			}
		}
		return nil, 0, false
	}
	out, n, ok := parse(0)
	return out, ok && n == len(s)
}

// arrayField is the Data API arrayValue of parsed elements of database type
// elem. ok is false for what it cannot show (a NULL element).
// ponytail: such an array stays a string; numeric and other types are strings.
func arrayField(elems []any, elem string) (map[string]any, bool) {
	if len(elems) > 0 {
		if _, nested := elems[0].([]any); nested {
			subs := []any{}
			for _, e := range elems {
				sub, isArr := e.([]any)
				if !isArr {
					return nil, false
				}
				f, ok := arrayField(sub, elem)
				if !ok {
					return nil, false
				}
				subs = append(subs, f)
			}
			return map[string]any{"arrayValues": subs}, true
		}
	}
	strs := make([]string, len(elems))
	for i, e := range elems {
		s, isStr := e.(string)
		if !isStr {
			return nil, false
		}
		strs[i] = s
	}
	switch elem {
	case "int2", "int4", "int8":
		ns := make([]int64, len(strs))
		for i, s := range strs {
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, false
			}
			ns[i] = n
		}
		return map[string]any{"longValues": ns}, true
	case "float4", "float8":
		fs := make([]float64, len(strs))
		for i, s := range strs {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return nil, false
			}
			fs[i] = f
		}
		return map[string]any{"doubleValues": fs}, true
	case "bool":
		bs := make([]bool, len(strs))
		for i, s := range strs {
			bs[i] = s == "t"
		}
		return map[string]any{"booleanValues": bs}, true
	}
	return map[string]any{"stringValues": strs}, true
}
