package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// server runs in the engine container. POST / with
//
//	{"setup": [sql...], "stmts": [{"sql": ..., "query": bool}], "dump": [table...]}
//
// runs the setup and the statements, in order, on a fresh in-memory DuckDB, and answers
//
//	{"results": [{"columns": [[name, type]...], "rows": [[...]...]} | {"count": n}...],
//	 "dump": {table: {"columns": ..., "rows": ...} | null}}
//
// with values as JSON (decimals, dates and times as text, bytes as base64), or
// {"error": "..."} with status 400.
const server = `
import base64, datetime, decimal, json, math, duckdb
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

def cell(v):
    if v is None or isinstance(v, (bool, int)):
        return v
    if isinstance(v, float):
        if math.isfinite(v):
            return v
        return 'NaN' if v != v else ('Infinity' if v > 0 else '-Infinity')
    if isinstance(v, (bytes, bytearray)):
        return base64.b64encode(v).decode()
    if isinstance(v, (list, tuple)):
        return [cell(x) for x in v]
    if isinstance(v, dict):
        return {str(k): cell(x) for k, x in v.items()}
    if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
        return v.isoformat()
    return str(v)

def table(rel):
    return {'columns': [[n, str(t)] for n, t in zip(rel.columns, rel.types)],
            'rows': [[cell(v) for v in row] for row in rel.fetchall()]}

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.reply(200, {'duckdb': duckdb.__version__})

    def do_POST(self):
        req = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        con = duckdb.connect()
        try:
            for s in req['setup']:
                con.execute(s)
            results = []
            for st in req['stmts']:
                if st['query']:
                    results.append(table(con.sql(st['sql'])))
                else:
                    cur = con.execute(st['sql'])
                    n = 0
                    if cur.description:
                        row = cur.fetchone()
                        if row and isinstance(row[0], int):
                            n = row[0]
                    results.append({'count': n})
            dump = {}
            for name in req.get('dump', []):
                try:
                    dump[name] = table(con.sql('SELECT * FROM ' + name))
                except Exception:
                    dump[name] = None
            self.reply(200, {'results': results, 'dump': dump})
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

type dumped struct {
	Columns [][2]string `json:"columns"`
	Rows    [][]any     `json:"rows"`
}

type engineResp struct {
	Results []struct {
		dumped
		Count int64 `json:"count"`
	} `json:"results"`
	Dump  map[string]*dumped `json:"dump"`
	Error string             `json:"error"`
}

// result is what a query yields: rows with a schema, or the rows a DML statement changed.
type result struct {
	fields    []*Field
	rows      []row
	verb      string
	affected  int64
	processed int64
	target    *tableRef // the table a DDL or DML statement changed
}

// runSQL runs GoogleSQL: each statement of the script on DuckDB (or in Go for
// the ones that only change metadata) and returns the last one's result.
func (s *Service) runSQL(ctx context.Context, project, defDataset, sql string, ps *params, dryRun bool) (*result, error) {
	tr, err := translate(sql, tctx{project, defDataset}, ps)
	if err != nil {
		return nil, err
	}
	var res *result
	for _, st := range tr.stmts {
		if res, err = s.runStmt(ctx, st, tr, dryRun); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (s *Service) datasetExists(r tref) bool {
	_, ok := s.st.datasets[r.project+"/"+r.dataset]
	return ok
}

// runStmt runs one statement.
func (s *Service) runStmt(ctx context.Context, st *stmt, tr *translation, dryRun bool) (*result, error) {
	if st.verb == "CREATE_VIEW" || strings.HasPrefix(st.verb, "DROP_") || st.verb == "CREATE_SCHEMA" {
		if dryRun {
			return &result{verb: st.verb}, nil
		}
		res, err := s.native(st)
		if err == nil && st.verb == "CREATE_VIEW" {
			s.fillViewSchema(&http.Request{}, st.target.ref())
		}
		return res, err
	}
	// What the statement reads must exist; what it creates needs its dataset.
	s.mu.Lock()
	for i, r := range st.refs {
		creating := (st.verb == "CREATE_TABLE" || st.verb == "CREATE_TABLE_AS_SELECT") && st.target != nil && *st.target == r && i == 0
		if !s.datasetExists(r) {
			s.mu.Unlock()
			return nil, notFound("Not found: Dataset %s:%s was not found in location US", r.project, r.dataset)
		}
		if _, ok := s.st.datasets[r.project+"/"+r.dataset].tables[r.table]; !ok && !creating {
			s.mu.Unlock()
			return nil, notFound("Not found: Table %s:%s.%s was not found in location US", r.project, r.dataset, r.table)
		}
	}
	setup, files, err := s.snapshot(st.refs)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	sql, query := st.sql, st.query
	if dryRun {
		if !query {
			return &result{verb: st.verb}, nil
		}
		sql = "SELECT * FROM (" + sql + ") LIMIT 0"
	}
	req := map[string]any{"setup": setup, "stmts": []map[string]any{{"sql": sql, "query": query}}}
	var dumpName string
	if st.target != nil && !dryRun && !query {
		dumpName = st.target.duck()
		req["dump"] = []string{dumpName}
	}
	out, err := s.engine(ctx, req, files)
	if err != nil {
		return nil, err
	}
	res := &result{verb: st.verb}
	for _, r := range st.refs {
		res.processed += s.sizeOf(r)
	}
	r0 := out.Results[0]
	if query {
		for i, c := range r0.Columns {
			res.fields = append(res.fields, fieldOf(colName(c[0], i), parseDType(c[1])))
		}
		for _, raw := range r0.Rows {
			rw := row{}
			for i, f := range res.fields {
				v, err := norm(f, raw[i], true)
				if err != nil {
					return nil, errf(500, "backendError", "cannot read a result value: %v", err)
				}
				if v != nil {
					rw[f.Name] = v
				}
			}
			res.rows = append(res.rows, rw)
		}
		return res, nil
	}
	res.affected = r0.Count
	if d := out.Dump[dumpName]; d != nil {
		ref := st.target.ref()
		if err := s.writeBack(ref, st, d); err != nil {
			return nil, err
		}
		res.target = &ref
	}
	return res, nil
}

func (s *Service) sizeOf(r tref) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d := s.st.datasets[r.project+"/"+r.dataset]; d != nil && d.tables[r.table] != nil {
		return int64(len(d.tables[r.table].Rows)) * int64(max(len(d.tables[r.table].fields()), 1)) * 8
	}
	return 0
}

// snapshot makes the setup statements that put the tables (and the views
// over them) into a fresh DuckDB, and the files with their rows. Lock held.
func (s *Service) snapshot(refs []tref) (setup []string, files map[string][]byte, err error) {
	setup = []string{"SET TimeZone='UTC'", "SET default_null_order='nulls_first'"}
	files = map[string][]byte{}
	schemas := map[string]bool{}
	done := map[tref]bool{}
	attached := map[string]bool{}
	token := core.RandHex(6)
	var add func(r tref) error
	add = func(r tref) error {
		if done[r] {
			return nil
		}
		done[r] = true
		d := s.st.datasets[r.project+"/"+r.dataset]
		if d == nil {
			return nil
		}
		if !attached[r.project] {
			attached[r.project] = true
			setup = append(setup, "ATTACH ':memory:' AS "+ident(r.project))
		}
		if k := r.project + "/" + r.dataset; !schemas[k] {
			schemas[k] = true
			setup = append(setup, "CREATE SCHEMA "+ident(r.project)+"."+ident(r.dataset))
		}
		t := d.tables[r.table]
		if t == nil {
			return nil
		}
		if t.Type == "VIEW" {
			vt, err := translate(t.View.Query, tctx{r.project, ""}, nil)
			if err != nil || len(vt.stmts) != 1 {
				return invalid("The view %s cannot be queried: %v", t.Ref, err)
			}
			for _, dep := range vt.refs {
				if err := add(dep); err != nil {
					return err
				}
			}
			setup = append(setup, "CREATE VIEW "+r.duck()+" AS "+vt.stmts[0].sql)
			return nil
		}
		var cols, reads, exprs []string
		for _, f := range t.fields() {
			cols = append(cols, ident(f.Name)+" "+duckType(f, false))
			rt := duckType(f, false)
			expr := ident(f.Name)
			switch {
			case f.Type == "BYTES" && !f.repeated():
				rt, expr = "VARCHAR", "from_base64("+expr+")"
			case f.Type == "JSON" && !f.repeated():
				rt, expr = "VARCHAR", "CAST("+expr+" AS JSON)"
			}
			reads = append(reads, literal(f.Name)+": "+literal(rt))
			exprs = append(exprs, expr)
		}
		if len(cols) == 0 {
			return nil
		}
		setup = append(setup, "CREATE TABLE "+r.duck()+" ("+strings.Join(cols, ", ")+")")
		if len(t.Rows) > 0 {
			var b bytes.Buffer
			enc := json.NewEncoder(&b)
			for _, rw := range t.Rows {
				m := map[string]any{}
				for _, f := range t.fields() {
					if v := rw[f.Name]; v != nil {
						m[f.Name] = forDuck(f, v)
					}
				}
				enc.Encode(m)
			}
			name := token + "/" + strconv.Itoa(len(files)) + ".json"
			files[name] = b.Bytes()
			setup = append(setup, "INSERT INTO "+r.duck()+" SELECT "+strings.Join(exprs, ", ")+" FROM read_json("+literal("/data/"+name)+
				", format = 'newline_delimited', columns = {"+strings.Join(reads, ", ")+"})")
		}
		return nil
	}
	for _, r := range refs {
		if err := add(r); err != nil {
			return nil, nil, err
		}
	}
	return setup, files, nil
}

// engine sends a request to DuckDB: the row files go into the engine's data
// directory first.
func (s *Service) engine(ctx context.Context, req map[string]any, files map[string][]byte) (*engineResp, error) {
	url, dir, err := s.eng.Start(ctx)
	if err != nil {
		return nil, errf(400, "invalidQuery", "%v", err)
	}
	for name, data := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(p, data, 0o644); err != nil {
			return nil, err
		}
		defer os.RemoveAll(filepath.Dir(p))
	}
	body, _ := json.Marshal(req)
	hr, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(hr)
	if err != nil {
		if ctx.Err() == nil {
			s.eng.Forget()
		}
		return nil, errf(500, "backendError", "pogi: the DuckDB engine did not answer: %v", err)
	}
	defer resp.Body.Close()
	var out engineResp
	d := json.NewDecoder(resp.Body)
	d.UseNumber()
	if err := d.Decode(&out); err != nil {
		return nil, errf(500, "backendError", "pogi: the DuckDB engine sent a bad answer: %v", err)
	}
	if out.Error != "" {
		msg := duckMessage(out.Error)
		if strings.Contains(msg, "already exists") {
			return nil, errf(409, "duplicate", "Already Exists: %s", msg)
		}
		return nil, errf(400, "invalidQuery", "%s", msg)
	}
	return &out, nil
}

// duckMessage trims DuckDB's error class and hints.
func duckMessage(e string) string {
	e, _, _ = strings.Cut(e, "\nLINE ")
	e, _, _ = strings.Cut(e, "\n\nCandidate bindings")
	return strings.TrimSpace(e)
}

// writeBack stores what DuckDB made of a table a statement changed.
func (s *Service) writeBack(ref tableRef, st *stmt, d *dumped) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ds := s.st.datasets[ref.ProjectID+"/"+ref.DatasetID]
	if ds == nil {
		return notFound("Not found: Dataset %s:%s", ref.ProjectID, ref.DatasetID)
	}
	t := ds.tables[ref.TableID]
	fresh := t == nil || st.orReplace
	if t == nil {
		t = &table{Ref: ref, Type: "TABLE", Created: ms(time.Now())}
		ds.tables[ref.TableID] = t
		if st.partition != "" {
			t.TimePartitioning = &timePartitioning{Type: "DAY", Field: st.partition}
		}
		if len(st.clusterBy) > 0 {
			t.Clustering = &clustering{Fields: st.clusterBy}
		}
	}
	var fs []*Field
	for i, c := range d.Columns {
		name := colName(c[0], i)
		var old *Field
		if !fresh {
			for _, f := range t.fields() {
				if f.Name == name {
					old = f
				}
			}
		}
		if old != nil {
			fs = append(fs, old)
		} else {
			fs = append(fs, fieldOf(name, parseDType(c[1])))
		}
	}
	rows := make([]row, 0, len(d.Rows))
	for _, raw := range d.Rows {
		rw := row{}
		for i, f := range fs {
			v, err := norm(f, raw[i], true)
			if err != nil {
				return errf(500, "backendError", "cannot store a value: %v", err)
			}
			if v != nil {
				rw[f.Name] = v
			}
		}
		rows = append(rows, rw)
	}
	t.Schema, t.Rows, t.Modified = &schema{Fields: fs}, rows, ms(time.Now())
	return nil
}
