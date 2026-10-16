package athena

import (
	"bytes"
	"cmp"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// The query engine is DuckDB in a container (see internal/duckdb); this is
// the server it runs for Athena.
// server runs in the engine container. POST / with {"setup": [...], "sql":
// "..."} runs the statements on a fresh in-memory DuckDB and answers
// {"columns": [[name, type], ...], "rows": [[...], ...]}, values rendered as
// Athena does, or {"error": "..."}.
const server = `
import datetime, json, duckdb
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

def cell(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return 'true' if v else 'false'
    if isinstance(v, datetime.datetime):
        s = v.replace(tzinfo=None).isoformat(' ', 'milliseconds')
        return s + ' ' + v.tzname() if v.tzinfo else s
    if isinstance(v, (list, tuple)):
        return '[' + ', '.join('null' if x is None else cell(x) for x in v) + ']'
    if isinstance(v, dict):
        return '{' + ', '.join(f'{k}=' + ('null' if x is None else cell(x)) for k, x in v.items()) + '}'
    return str(v)

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.reply(200, {'duckdb': duckdb.__version__})

    def do_POST(self):
        req = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        con = duckdb.connect()
        try:
            for stmt in req['setup']:
                con.execute(stmt)
            rel = con.sql(req['sql'])
            out = {'columns': [], 'rows': []}
            if rel is not None:
                out['columns'] = [[n, str(t)] for n, t in zip(rel.columns, rel.types)]
                out['rows'] = [[cell(v) for v in row] for row in rel.fetchall()]
            self.reply(200, out)
        except Exception as e:
            self.reply(400, {'error': str(e)})
        finally:
            con.close()

    def reply(self, code, body):
        b = json.dumps(body).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def log_message(self, *args):
        pass

ThreadingHTTPServer(('', 8080), Handler).serve_forever()
`

// query runs a SELECT on DuckDB: the Glue tables it names become views over
// copies of their S3 files, in schemas of an awsdatacatalog database.
func (j *job) query(sql string) (*result, error) {
	url, root, err := j.s.eng.Start(j.ctx)
	if err != nil {
		return nil, err
	}
	tables, err := j.referenced(sql)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(root, j.id)
	defer os.RemoveAll(dir)
	setup := []string{"ATTACH ':memory:' AS awsdatacatalog", "USE awsdatacatalog", "CREATE SCHEMA " + ident(j.db)}
	schemas := map[string]bool{j.db: true}
	res := &result{}
	for i, t := range tables {
		if !schemas[t.DatabaseName] {
			schemas[t.DatabaseName] = true
			setup = append(setup, "CREATE SCHEMA "+ident(t.DatabaseName))
		}
		v, n, err := j.view(t, filepath.Join(dir, strconv.Itoa(i)), "/data/"+j.id+"/"+strconv.Itoa(i))
		if err != nil {
			return nil, err
		}
		res.scanned += n
		setup = append(setup, "CREATE VIEW "+ident(t.DatabaseName)+"."+ident(t.Name)+" AS "+v)
	}
	setup = append(setup, "USE awsdatacatalog."+ident(j.db))
	body, _ := json.Marshal(obj{"setup": setup, "sql": rewrite(sql)})
	// ponytail: StopQueryExecution drops this request; DuckDB finishes the query in the container anyway.
	req, _ := http.NewRequestWithContext(j.ctx, http.MethodPost, url, bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if j.ctx.Err() == nil {
			j.s.eng.Forget()
		}
		return nil, fmt.Errorf("pogi: the DuckDB engine did not answer: %w", err)
	}
	defer resp.Body.Close()
	var out struct {
		Columns [][2]string
		Rows    [][]*string
		Error   string
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if out.Error != "" {
		return nil, errors.New(out.Error)
	}
	header := make([]*string, len(out.Columns))
	for i, c := range out.Columns {
		header[i] = &c[0]
		res.cols = append(res.cols, columnInfo(c[0], c[1]))
	}
	res.rows = append([][]*string{header}, out.Rows...)
	return res, nil
}

// referenced returns the Glue tables a query may read: every identifier,
// alone (in the query's database) or qualified by a database, that names a
// table. Aliases and columns that happen to match only cost a copy.
func (j *job) referenced(sql string) ([]gTable, error) {
	toks, err := lex(sql, false)
	if err != nil {
		return nil, err
	}
	type ref struct{ db, name string }
	var refs []ref
	for i := 0; i < len(toks); i++ {
		if !toks[i].ident() || i > 0 && toks[i-1].is(".") {
			continue
		}
		chain := []string{toks[i].s}
		for i+2 < len(toks) && toks[i+1].is(".") && toks[i+2].ident() {
			chain, i = append(chain, toks[i+2].s), i+2
		}
		r := ref{j.db, chain[0]}
		if n := len(chain); n > 1 {
			r = ref{chain[n-2], chain[n-1]}
		}
		r = ref{strings.ToLower(r.db), strings.ToLower(r.name)}
		if !slices.Contains(refs, r) {
			refs = append(refs, r)
		}
	}
	byDB := map[string]map[string]gTable{}
	var out []gTable
	for _, r := range refs {
		if _, ok := byDB[r.db]; !ok {
			byDB[r.db], _ = j.tables(r.db, "") // not a database: no tables
		}
		if t, ok := byDB[r.db][r.name]; ok {
			out = append(out, t)
		}
	}
	return out, nil
}

// view copies a table's files into dir (only its registered partitions when
// it has partition keys) and returns the DuckDB query reading them where the
// container sees dir (mnt), with the partition values as columns.
// ponytail: the data is copied for every query; cache copies by ETag if big tables make that slow.
func (j *job) view(t gTable, dir, mnt string) (string, int64, error) {
	type part struct {
		values []string
		loc    string
	}
	parts := []part{{loc: t.StorageDescriptor.Location}}
	if len(t.PartitionKeys) > 0 {
		ps, err := j.partitions(t)
		if err != nil {
			return "", 0, err
		}
		parts = parts[:0]
		for _, p := range ps {
			if len(p.Values) == len(t.PartitionKeys) && p.StorageDescriptor.Location != "" {
				parts = append(parts, part{p.Values, p.StorageDescriptor.Location})
			}
		}
	}
	var selects []string
	var scanned int64
	for i, p := range parts {
		files, n, err := j.download(p.loc, filepath.Join(dir, strconv.Itoa(i)), mnt+"/"+strconv.Itoa(i))
		if err != nil {
			return "", 0, err
		}
		scanned += n
		if len(files) == 0 {
			continue
		}
		from, err := reader(t, files)
		if err != nil {
			return "", 0, err
		}
		sel := "SELECT *"
		for k, key := range t.PartitionKeys {
			sel += ", CAST(" + literal(p.values[k]) + " AS " + duckType(key.Type) + ") AS " + ident(key.Name)
		}
		selects = append(selects, sel+" FROM "+from)
	}
	if len(selects) == 0 { // no data: no rows, with the table's columns
		var cols []string
		for _, c := range append(slices.Clone(t.StorageDescriptor.Columns), t.PartitionKeys...) {
			cols = append(cols, "CAST(NULL AS "+duckType(c.Type)+") AS "+ident(c.Name))
		}
		if len(cols) == 0 {
			cols = []string{"NULL AS " + ident("_")}
		}
		return "SELECT " + strings.Join(cols, ", ") + " WHERE false", 0, nil
	}
	return strings.Join(selects, " UNION ALL BY NAME "), scanned, nil
}

// download copies the objects under an S3 location into dir and returns
// their paths under mnt, and their size.
func (j *job) download(loc, dir, mnt string) ([]string, int64, error) {
	bucket, prefix := splitS3(loc)
	keys, err := objects(j.s.hub, j.account, j.region, bucket, prefix)
	if err != nil {
		return nil, 0, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, 0, err
	}
	var files []string
	var size int64
	for i, k := range keys {
		st, _, data := j.s.hub.Do(j.account, j.region, "s3", http.MethodGet, (&url.URL{Path: "/" + bucket + "/" + k}).EscapedPath(), nil, nil)
		if st != http.StatusOK {
			return nil, 0, fmt.Errorf("cannot read s3://%s/%s (HTTP %d)", bucket, k, st)
		}
		if len(data) == 0 {
			continue
		}
		name := strconv.Itoa(i) + path.Ext(k) // the extension tells DuckDB the compression
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			return nil, 0, err
		}
		files, size = append(files, mnt+"/"+name), size+int64(len(data))
	}
	return files, size, nil
}

// format is how a table's files are read: csv, json or parquet.
func format(t gTable) string {
	sd := t.StorageDescriptor
	all := strings.ToLower(sd.SerdeInfo.SerializationLibrary + " " + sd.InputFormat + " " + t.Parameters["classification"])
	for _, f := range []string{"parquet", "json", "orc", "avro"} {
		if strings.Contains(all, f) {
			return f
		}
	}
	return "csv"
}

// reader is the DuckDB table function that reads a table's files: CSV with
// the SerDe's delimiter and the header lines skipped, JSON or Parquet.
func reader(t gTable, files []string) (string, error) {
	sd := t.StorageDescriptor
	var list, cols []string
	for _, f := range files {
		list = append(list, literal(f))
	}
	for _, c := range sd.Columns {
		cols = append(cols, literal(c.Name)+": "+literal(duckType(c.Type)))
	}
	paths, schema := "["+strings.Join(list, ", ")+"]", "{"+strings.Join(cols, ", ")+"}"
	switch f := format(t); f {
	case "parquet":
		return "read_parquet(" + paths + ", union_by_name = true)", nil
	case "json":
		if len(cols) == 0 {
			return "read_json_auto(" + paths + ")", nil
		}
		// ponytail: keys match column names exactly, where Hive's JSON SerDes ignore case.
		return "read_json(" + paths + ", columns = " + schema + ")", nil
	case "orc", "avro":
		return "", fmt.Errorf("pogi: Athena cannot read %s tables yet", strings.ToUpper(f))
	}
	p := sd.SerdeInfo.Parameters
	delim := ","
	switch lib := sd.SerdeInfo.SerializationLibrary; {
	case strings.Contains(lib, "OpenCSVSerde"):
		delim = cmp.Or(p["separatorChar"], ",")
	case p["field.delim"] != "" || p["serialization.format"] != "":
		delim = hiveByte(cmp.Or(p["field.delim"], p["serialization.format"]))
	case strings.Contains(lib, "LazySimpleSerDe"):
		delim = "\x01"
	}
	skip, _ := strconv.Atoi(t.Parameters["skip.header.line.count"])
	opts := ", delim = " + literal(delim) + ", header = false, skip = " + strconv.Itoa(skip)
	if len(cols) == 0 {
		return "read_csv(" + paths + opts + ")", nil
	}
	return "read_csv(" + paths + ", columns = " + schema + opts + ", auto_detect = false, null_padding = true)", nil
}

// hiveByte reads a LazySimpleSerDe delimiter: a number is a byte value ("1" is \001).
func hiveByte(s string) string {
	if n, err := strconv.ParseUint(s, 10, 7); err == nil {
		return string(rune(n))
	}
	return s[:1]
}

// duckType converts a Hive type (string, decimal(10,2), array<struct<a:int>>…) to DuckDB's.
func duckType(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	if i := strings.IndexByte(t, '<'); i > 0 && strings.HasSuffix(t, ">") {
		args := splitType(t[i+1 : len(t)-1])
		switch t[:i] {
		case "array":
			return duckType(args[0]) + "[]"
		case "map":
			if len(args) == 2 {
				return "MAP(" + duckType(args[0]) + ", " + duckType(args[1]) + ")"
			}
		case "struct":
			for k, f := range args {
				n, ft, _ := strings.Cut(f, ":")
				args[k] = ident(strings.TrimSpace(n)) + " " + duckType(ft)
			}
			return "STRUCT(" + strings.Join(args, ", ") + ")"
		}
	}
	base, _, _ := strings.Cut(t, "(")
	switch base {
	case "int", "integer":
		return "INTEGER"
	case "bigint", "smallint", "tinyint", "double", "float", "boolean", "date", "timestamp":
		return strings.ToUpper(base)
	case "decimal":
		return strings.ToUpper(t)
	case "binary":
		return "BLOB"
	}
	return "VARCHAR"
}

// splitType splits a type's arguments at the commas outside brackets.
func splitType(s string) []string {
	var out []string
	depth, start := 0, 0
	for i, c := range s {
		switch c {
		case '<', '(':
			depth++
		case '>', ')':
			depth--
		case ',':
			if depth == 0 {
				out, start = append(out, s[start:i]), i+1
			}
		}
	}
	return append(out, s[start:])
}

// columnInfo describes a result column with Athena's name for its DuckDB type.
func columnInfo(name, duck string) column {
	c := column{CatalogName: "hive", Name: name, Label: name, Nullable: "UNKNOWN"}
	switch {
	case strings.HasSuffix(duck, "]"):
		c.Type = "array"
	case strings.HasPrefix(duck, "STRUCT"):
		c.Type = "row"
	case strings.HasPrefix(duck, "MAP"):
		c.Type = "map"
	case strings.HasPrefix(duck, "DECIMAL"):
		c.Type = "decimal"
		fmt.Sscanf(duck, "DECIMAL(%d,%d)", &c.Precision, &c.Scale)
	case duck == "TIMESTAMP WITH TIME ZONE":
		c.Type, c.Precision = "timestamp with time zone", 3
	case strings.HasPrefix(duck, "TIMESTAMP"):
		c.Type, c.Precision = "timestamp", 3
	default:
		t, ok := athenaTypes[duck]
		if !ok {
			t, c.CaseSensitive = athenaType{"varchar", 2147483647}, true
		}
		c.Type, c.Precision = t.name, t.prec
	}
	return c
}

type athenaType struct {
	name string
	prec int
}

// athenaTypes are Athena's names and precisions of DuckDB's simple types; the rest are varchar.
var athenaTypes = map[string]athenaType{"BIGINT": {"bigint", 19}, "HUGEINT": {"bigint", 19}, "UBIGINT": {"bigint", 19},
	"INTEGER": {"integer", 10}, "UINTEGER": {"integer", 10}, "SMALLINT": {"smallint", 5}, "TINYINT": {"tinyint", 3},
	"DOUBLE": {"double", 17}, "FLOAT": {"float", 7}, "BOOLEAN": {"boolean", 0}, "DATE": {"date", 0}, "TIME": {"time", 3},
	"BLOB": {"varbinary", 0}, "JSON": {"json", 0}, "INTERVAL": {"interval day to second", 0}}

// rewrites translate the Trino functions DuckDB lacks. Each gets the call's
// arguments, already rewritten; calls with another argument count stay.
var rewrites = map[string]struct {
	n int
	f func(a []string) string
}{
	"from_iso8601_timestamp": {1, func(a []string) string { return "CAST(" + a[0] + " AS TIMESTAMPTZ)" }},
	"from_iso8601_date":      {1, func(a []string) string { return "CAST(" + a[0] + " AS DATE)" }},
	"date_parse":             {2, func(a []string) string { return "strptime(" + a[0] + ", " + mysqlFormat(a[1]) + ")" }},
	"date_format":            {2, func(a []string) string { return "strftime(" + a[0] + ", " + mysqlFormat(a[1]) + ")" }},
	"approx_distinct":        {1, func(a []string) string { return "approx_count_distinct(" + a[0] + ")" }},
	"cardinality":            {1, func(a []string) string { return "len(" + a[0] + ")" }},
	"json_extract_scalar":    {2, func(a []string) string { return "json_extract_string(" + a[0] + ", " + a[1] + ")" }},
	"regexp_like":            {2, func(a []string) string { return "regexp_matches(" + a[0] + ", " + a[1] + ")" }},
}

// mysqlSpecs turns the MySQL-style formats of Trino's date_parse and
// date_format into DuckDB's strptime/strftime ones.
var mysqlSpecs = strings.NewReplacer("%%", "%%", "%i", "%M", "%s", "%S", "%T", "%H:%M:%S", "%r", "%I:%M:%S %p", "%c", "%m",
	"%e", "%d", "%k", "%H", "%l", "%I", "%h", "%I", "%M", "%B", "%W", "%A", "%v", "%V", "%x", "%G")

func mysqlFormat(a string) string {
	if len(a) >= 2 && a[0] == '\'' && a[len(a)-1] == '\'' {
		return mysqlSpecs.Replace(a)
	}
	return a
}

// rewrite translates a Trino query to DuckDB; strings, quoted names and
// comments stay as they are.
func rewrite(sql string) string {
	var b strings.Builder
	for i := 0; i < len(sql); {
		switch c := sql[i]; {
		case c == '\'' || c == '"':
			j := quoted(sql, i)
			b.WriteString(sql[i:j])
			i = j
		case strings.HasPrefix(sql[i:], "--") || strings.HasPrefix(sql[i:], "/*"):
			end, j := "\n", len(sql)
			if c == '/' {
				end = "*/"
			}
			if k := strings.Index(sql[i+2:], end); k >= 0 {
				j = i + 2 + k + len(end)
			}
			b.WriteString(sql[i:j])
			i = j
		case isWord(c):
			j := i
			for j < len(sql) && isWord(sql[j]) {
				j++
			}
			k := j
			for k < len(sql) && (sql[k] == ' ' || sql[k] == '\t' || sql[k] == '\n' || sql[k] == '\r') {
				k++
			}
			if r, ok := rewrites[strings.ToLower(sql[i:j])]; ok && k < len(sql) && sql[k] == '(' {
				if end := closing(sql, k); end > 0 {
					if args := splitArgs(sql[k+1 : end]); len(args) == r.n {
						for a := range args {
							args[a] = rewrite(strings.TrimSpace(args[a]))
						}
						b.WriteString(r.f(args))
						i = end + 1
						continue
					}
				}
			}
			b.WriteString(sql[i:j])
			i = j
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

// quoted returns the index after the quoted text that starts at i (doubled quotes are escapes).
func quoted(s string, i int) int {
	for j := i + 1; j < len(s); j++ {
		if s[j] == s[i] {
			if j+1 < len(s) && s[j+1] == s[i] {
				j++
				continue
			}
			return j + 1
		}
	}
	return len(s)
}

// closing returns the index of the parenthesis that closes the one at k, or -1.
func closing(s string, k int) int {
	depth := 0
	for i := k; i < len(s); i++ {
		switch s[i] {
		case '\'', '"':
			i = quoted(s, i) - 1
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 {
				return i
			}
		}
	}
	return -1
}

// splitArgs splits a list of arguments at the commas outside brackets and quotes.
func splitArgs(s string) []string {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'', '"':
			i = quoted(s, i) - 1
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case ',':
			if depth == 0 {
				out, start = append(out, s[start:i]), i+1
			}
		}
	}
	return append(out, s[start:])
}

// bind replaces the ? placeholders outside quotes with the parameters, in order.
func bind(sql string, params []string) (string, error) {
	var b strings.Builder
	n := 0
	for i := 0; i < len(sql); i++ {
		switch c := sql[i]; {
		case c == '\'' || c == '"':
			j := quoted(sql, i)
			b.WriteString(sql[i:j])
			i = j - 1
		case c == '?' && n < len(params):
			b.WriteString(params[n])
			n++
		case c == '?':
			n++
		default:
			b.WriteByte(c)
		}
	}
	if n != len(params) {
		return "", fmt.Errorf("Incorrect number of parameters: expected %d but found %d", n, len(params))
	}
	return b.String(), nil
}

func ident(s string) string   { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// write puts a query's results in S3: a SELECT's as <id>.csv (every value
// quoted, NULL empty) plus <id>.csv.metadata, the rest as <id>.txt.
func (j *job) write(loc string, res *result) error {
	var b bytes.Buffer
	csv := strings.HasSuffix(loc, ".csv")
	for _, row := range res.rows {
		for i, v := range row {
			switch {
			case i > 0 && csv:
				b.WriteByte(',')
			case i > 0:
				b.WriteByte('\t')
			}
			if v != nil && csv {
				b.WriteString(`"` + strings.ReplaceAll(*v, `"`, `""`) + `"`)
			} else if v != nil {
				b.WriteString(*v)
			}
		}
		b.WriteByte('\n')
	}
	bucket, key := splitS3(loc)
	key = strings.TrimSuffix(key, "/")
	put := func(key string, data []byte) error {
		st, _, _ := j.s.hub.Do(j.account, j.region, "s3", http.MethodPut, (&url.URL{Path: "/" + bucket + "/" + key}).EscapedPath(), nil, data)
		switch {
		case st == http.StatusNotFound:
			return fmt.Errorf("Unable to verify/create output bucket %s", bucket)
		case st != http.StatusOK:
			return fmt.Errorf("Unable to write results to s3://%s/%s (HTTP %d)", bucket, key, st)
		}
		return nil
	}
	if err := put(key, b.Bytes()); err != nil || !csv {
		return err
	}
	meta, _ := json.Marshal(obj{"ColumnInfo": res.cols})
	return put(key+".metadata", meta)
}

// splitS3 splits s3://bucket/prefix into the bucket and a prefix that is
// empty or ends in /.
func splitS3(loc string) (string, string) {
	_, rest, _ := strings.Cut(loc, "://")
	bucket, prefix, _ := strings.Cut(rest, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return bucket, prefix
}

// objects lists the keys under prefix in pogi's S3, skipping folder markers
// and the files Athena skips (names starting with _ or .).
func objects(hub *core.Hub, account, region, bucket, prefix string) ([]string, error) {
	var keys []string
	for token := ""; ; {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		st, _, body := hub.Do(account, region, "s3", http.MethodGet, "/"+bucket+"?"+q.Encode(), nil, nil)
		if st != http.StatusOK {
			return nil, fmt.Errorf("cannot list s3://%s/%s (HTTP %d)", bucket, prefix, st)
		}
		var out struct {
			Contents              []struct{ Key string }
			IsTruncated           bool
			NextContinuationToken string
		}
		if err := xml.Unmarshal(body, &out); err != nil {
			return nil, err
		}
		for _, o := range out.Contents {
			if b := path.Base(o.Key); !strings.HasSuffix(o.Key, "/") && b[0] != '_' && b[0] != '.' {
				keys = append(keys, o.Key)
			}
		}
		if !out.IsTruncated {
			return keys, nil
		}
		token = out.NextContinuationToken
	}
}
