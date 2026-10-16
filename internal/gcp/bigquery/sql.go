package bigquery

import (
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

// This file translates GoogleSQL to DuckDB's dialect. It works on tokens: it
// resolves table names to "project"."dataset"."table", maps the type names
// and the functions that differ, and leaves the rest to DuckDB.

type token struct {
	kind       byte // w word, q backtick name, s string (text is the DuckDB literal), n number, p punctuation, l ready SQL
	text       string
	start, end int // in the source
}

func (t token) is(w string) bool  { return t.kind == 'w' && strings.EqualFold(t.text, w) }
func (t token) isP(p string) bool { return t.kind == 'p' && t.text == p }

// params are the values of query parameters, as SQL literals.
type params struct {
	named      map[string]string
	positional []string
	next       int
}

func lex(sql string, ps *params) ([]token, error) {
	var toks []token
	for i := 0; i < len(sql); {
		c := sql[i]
		start := i
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '#' || strings.HasPrefix(sql[i:], "--"):
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case strings.HasPrefix(sql[i:], "/*"):
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				return nil, invalid("Syntax error: unclosed comment")
			}
			i += end + 4
		case c == '`':
			end := strings.IndexByte(sql[i+1:], '`')
			if end < 0 {
				return nil, invalid("Syntax error: unclosed identifier literal at %d", i)
			}
			toks = append(toks, token{'q', sql[i+1 : i+1+end], start, i + end + 2})
			i += end + 2
		case c == '\'' || c == '"':
			s, n, err := lexString(sql, i, false, false)
			if err != nil {
				return nil, err
			}
			toks = append(toks, token{'s', s, start, n})
			i = n
		case unicode.IsLetter(rune(c)) || c == '_':
			j := i
			for j < len(sql) && (sql[j] == '_' || unicode.IsLetter(rune(sql[j])) || unicode.IsDigit(rune(sql[j]))) {
				j++
			}
			w := sql[i:j]
			if lw := strings.ToLower(w); j < len(sql) && (sql[j] == '\'' || sql[j] == '"') && (lw == "r" || lw == "b" || lw == "rb" || lw == "br") {
				s, n, err := lexString(sql, j, strings.Contains(lw, "r"), strings.Contains(lw, "b"))
				if err != nil {
					return nil, err
				}
				toks = append(toks, token{'s', s, start, n})
				i = n
				break
			}
			toks = append(toks, token{'w', w, start, j})
			i = j
		case c >= '0' && c <= '9' || c == '.' && i+1 < len(sql) && sql[i+1] >= '0' && sql[i+1] <= '9':
			j := i
			if strings.HasPrefix(sql[i:], "0x") || strings.HasPrefix(sql[i:], "0X") {
				for j += 2; j < len(sql) && strings.IndexByte("0123456789abcdefABCDEF", sql[j]) >= 0; j++ {
				}
			} else {
				for j < len(sql) && (sql[j] >= '0' && sql[j] <= '9' || sql[j] == '.') {
					j++
				}
				if j < len(sql) && (sql[j] == 'e' || sql[j] == 'E') {
					k := j + 1
					if k < len(sql) && (sql[k] == '+' || sql[k] == '-') {
						k++
					}
					if k < len(sql) && sql[k] >= '0' && sql[k] <= '9' {
						for j = k; j < len(sql) && sql[j] >= '0' && sql[j] <= '9'; j++ {
						}
					}
				}
			}
			toks = append(toks, token{'n', sql[i:j], start, j})
			i = j
		case c == '@':
			j := i + 1
			for j < len(sql) && (sql[j] == '_' || unicode.IsLetter(rune(sql[j])) || unicode.IsDigit(rune(sql[j]))) {
				j++
			}
			name := sql[i+1 : j]
			lit, ok := "", false
			if ps != nil {
				lit, ok = ps.named[name]
			}
			if !ok || name == "" {
				return nil, invalid("Query parameter '%s' not found at [1:%d]", name, i+1)
			}
			toks = append(toks, token{'l', lit, start, j})
			i = j
		case c == '?':
			if ps == nil || ps.next >= len(ps.positional) {
				return nil, invalid("Syntax error: unexpected ? (no positional query parameter for it)")
			}
			toks = append(toks, token{'l', ps.positional[ps.next], start, i + 1})
			ps.next++
			i++
		default:
			n := 1
			if two := sql[i:min(i+2, len(sql))]; two == "<=" || two == ">=" || two == "<>" || two == "!=" || two == "||" {
				n = 2
			}
			toks = append(toks, token{'p', sql[i : i+n], start, i + n})
			i += n
		}
	}
	return toks, nil
}

// lexString reads a quoted string (one or three quotes, with backslash
// escapes unless raw) at sql[i] and returns it as a DuckDB literal.
func lexString(sql string, i int, raw, bytes bool) (string, int, error) {
	q := sql[i]
	triple := strings.HasPrefix(sql[i:], strings.Repeat(string(q), 3))
	j := i + 1
	if triple {
		j = i + 3
	}
	var b strings.Builder
	for ; j < len(sql); j++ {
		c := sql[j]
		switch {
		case triple && strings.HasPrefix(sql[j:], strings.Repeat(string(q), 3)):
			return wrapLiteral(b.String(), bytes), j + 3, nil
		case !triple && c == q:
			return wrapLiteral(b.String(), bytes), j + 1, nil
		case c == '\\' && !raw && j+1 < len(sql):
			j++
			switch e := sql[j]; e {
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case 'b':
				b.WriteByte('\b')
			case 'f':
				b.WriteByte('\f')
			case 'v':
				b.WriteByte('\v')
			case 'a':
				b.WriteByte('\a')
			case '0':
				b.WriteByte(0)
			case 'x', 'u', 'U':
				n := map[byte]int{'x': 2, 'u': 4, 'U': 8}[e]
				if j+n < len(sql) {
					if r, err := strconv.ParseUint(sql[j+1:j+1+n], 16, 32); err == nil {
						if e == 'x' && bytes {
							b.WriteByte(byte(r))
						} else {
							b.WriteRune(rune(r))
						}
						j += n
						continue
					}
				}
				b.WriteByte(e)
			default:
				b.WriteByte(e)
			}
		case c == '\n' && !triple:
			return "", 0, invalid("Syntax error: unclosed string literal at %d", i)
		default:
			b.WriteByte(c)
		}
	}
	return "", 0, invalid("Syntax error: unclosed string literal at %d", i)
}

func wrapLiteral(s string, bytes bool) string {
	if bytes {
		return "CAST(" + literal(s) + " AS BLOB)"
	}
	return literal(s)
}

// ---- statements ----

type tref struct{ project, dataset, table string }

func (r tref) ref() tableRef { return tableRef{r.project, r.dataset, r.table} }

// duck is the DuckDB name of the table.
func (r tref) duck() string { return ident(r.project) + "." + ident(r.dataset) + "." + ident(r.table) }

type stmt struct {
	sql    string // for DuckDB
	refs   []tref // the tables it names
	query  bool   // it returns rows
	verb   string // SELECT, INSERT, UPDATE, DELETE, MERGE, CREATE_TABLE, CREATE_TABLE_AS_SELECT, ALTER_TABLE, TRUNCATE_TABLE, CREATE_VIEW, DROP_TABLE, DROP_VIEW, CREATE_SCHEMA, DROP_SCHEMA
	target *tref  // the table that DML or DDL changes
	// native statements
	viewSQL     string
	ifExists    bool
	orReplace   bool
	cascade     bool
	location    string
	partition   string
	clusterBy   []string
	description string
}

type translation struct {
	stmts []*stmt
	refs  []tref
}

// tctx is where names resolve: the project of the job and its default dataset.
type tctx struct {
	project, dataset string
}

type translator struct {
	ctx       tctx
	sql       string
	refs      []tref
	err       error
	cur       *stmt
	nextTable bool // the next token starts a table name (the statement's first token, when the keyword was left out)
	ddl       bool // in CREATE TABLE or ALTER TABLE: words after a column name are types
	ddlCreate bool // in CREATE TABLE: OPTIONS, PARTITION BY and CLUSTER BY are BigQuery's
}

func translate(sql string, ctx tctx, ps *params) (*translation, error) {
	toks, err := lex(sql, ps)
	if err != nil {
		return nil, err
	}
	tr := &translator{ctx: ctx, sql: sql}
	out := &translation{}
	start := 0
	for i := 0; i <= len(toks); i++ {
		if i < len(toks) && !toks[i].isP(";") {
			continue
		}
		if i > start {
			st, err := tr.statement(toks[start:i])
			if err != nil {
				return nil, err
			}
			out.stmts = append(out.stmts, st)
		}
		start = i + 1
	}
	if len(out.stmts) == 0 {
		return nil, invalid("Syntax error: Unexpected end of script")
	}
	for _, r := range tr.refs {
		dup := false
		for _, o := range out.refs {
			dup = dup || o == r
		}
		if !dup {
			out.refs = append(out.refs, r)
		}
	}
	return out, nil
}

var ctasRE = regexp.MustCompile(`(?i)\bas\s*\(?\s*(select|with)\b`)

func syntax(format string, a ...any) error { return invalid("Syntax error: "+format, a...) }

// resolve names a table from the parts of its path.
func (tr *translator) resolve(parts []string) (tref, error) {
	switch len(parts) {
	case 1:
		if tr.ctx.dataset == "" {
			return tref{}, invalid("Table name %q missing dataset while no default dataset is set in the request.", parts[0])
		}
		return tref{tr.ctx.project, tr.ctx.dataset, parts[0]}, nil
	case 2:
		return tref{tr.ctx.project, parts[0], parts[1]}, nil
	case 3:
		return tref{parts[0], parts[1], parts[2]}, nil
	}
	return tref{}, invalid("Invalid table name %q", strings.Join(parts, "."))
}

// chain reads a possibly dotted name at toks[i] ("a.b.c", `a.b`.c, ...) and
// returns its parts and the index after it.
func chain(toks []token, i int) ([]string, int) {
	var parts []string
	for i < len(toks) {
		switch toks[i].kind {
		case 'q':
			parts = append(parts, strings.Split(toks[i].text, ".")...)
		case 'w':
			parts = append(parts, toks[i].text)
		default:
			return parts, i
		}
		i++
		if i+1 < len(toks) && toks[i].isP(".") && (toks[i+1].kind == 'w' || toks[i+1].kind == 'q') {
			i++
			continue
		}
		break
	}
	return parts, i
}

// matching returns the index of the closer of the opener at toks[i].
func matching(toks []token, i int) int {
	open, closer := toks[i].text, map[string]string{"(": ")", "[": "]"}[toks[i].text]
	depth := 0
	for j := i; j < len(toks); j++ {
		if toks[j].kind == 'p' && toks[j].text == open {
			depth++
		} else if toks[j].kind == 'p' && toks[j].text == closer {
			if depth--; depth == 0 {
				return j
			}
		}
	}
	return -1
}

func splitComma(toks []token) [][]token {
	var out [][]token
	depth, start := 0, 0
	for i, t := range toks {
		switch {
		case t.isP("(") || t.isP("["):
			depth++
		case t.isP(")") || t.isP("]"):
			depth--
		case t.isP(",") && depth == 0:
			out, start = append(out, toks[start:i]), i+1
		}
	}
	if len(toks) > 0 || start > 0 {
		out = append(out, toks[start:])
	}
	return out
}

// statement translates one statement.
func (tr *translator) statement(toks []token) (*stmt, error) {
	st := &stmt{}
	tr.cur = st
	before := len(tr.refs)
	defer func() { st.refs = append([]tref(nil), tr.refs[before:]...) }()
	first := ""
	if toks[0].kind == 'w' {
		first = lower(toks[0].text)
	}
	word := func(i int) string {
		if i < len(toks) && toks[i].kind == 'w' {
			return lower(toks[i].text)
		}
		return ""
	}
	switch first {
	case "create":
		i := 1
		if word(i) == "or" && word(i+1) == "replace" {
			st.orReplace, i = true, i+2
		}
		if w := word(i); w == "temp" || w == "temporary" {
			return nil, syntax("temporary tables are not supported by pogi")
		}
		kind := word(i)
		i++
		switch kind {
		case "view", "materialized":
			if kind == "materialized" {
				return nil, syntax("materialized views are not supported by pogi")
			}
			return tr.createView(st, toks, i)
		case "schema", "dataset":
			return tr.schemaStmt(st, toks, i, "CREATE_SCHEMA")
		case "table":
			st.verb = "CREATE_TABLE"
			if err := tr.targetAfter(st, toks, i, word); err != nil {
				return nil, err
			}
		default:
			return nil, syntax("pogi does not support CREATE %s", strings.ToUpper(kind))
		}
	case "drop":
		switch kind := word(1); kind {
		case "table", "view", "schema":
			return tr.dropStmt(st, toks, kind)
		default:
			return nil, syntax("pogi does not support DROP %s", strings.ToUpper(kind))
		}
	case "alter":
		if word(1) != "table" {
			return nil, syntax("pogi does not support ALTER %s", strings.ToUpper(word(1)))
		}
		st.verb = "ALTER_TABLE"
	case "insert", "update", "delete", "merge", "truncate":
		st.verb = strings.ToUpper(first)
		if first == "truncate" {
			st.verb = "TRUNCATE_TABLE"
		}
	case "select", "with", "(", "values":
		st.verb, st.query = "SELECT", true
	case "":
		if toks[0].isP("(") {
			st.verb, st.query = "SELECT", true
			break
		}
		fallthrough
	default:
		return nil, syntax("unexpected %q (pogi supports SELECT, DML and basic DDL)", first)
	}
	var b strings.Builder
	switch first {
	case "insert":
		if word(1) != "into" {
			b.WriteString("INSERT INTO ")
			toks, tr.nextTable = toks[1:], true
		}
	case "delete":
		if word(1) != "from" {
			b.WriteString("DELETE FROM ")
			toks, tr.nextTable = toks[1:], true
		}
	case "merge":
		if word(1) != "into" {
			b.WriteString("MERGE INTO ")
			toks, tr.nextTable = toks[1:], true
		}
	case "truncate":
		b.WriteString("DELETE FROM ")
		if len(toks) < 3 || !toks[1].is("table") {
			return nil, syntax("TRUNCATE TABLE <table> expected")
		}
		toks, tr.nextTable = toks[2:], true
	}
	tr.ddl = st.verb == "CREATE_TABLE" || st.verb == "ALTER_TABLE"
	tr.ddlCreate = st.verb == "CREATE_TABLE"
	body := tr.run(toks, "", st)
	if tr.err != nil {
		return nil, tr.err
	}
	b.WriteString(body)
	st.sql = b.String()
	if st.verb == "CREATE_TABLE" && ctasRE.MatchString(st.sql) {
		st.verb = "CREATE_TABLE_AS_SELECT"
	}
	return st, nil
}

// targetAfter reads the table a CREATE names at toks[i] (after IF [NOT] EXISTS).
func (tr *translator) targetAfter(st *stmt, toks []token, i int, word func(int) string) error {
	if word(i) == "if" && word(i+1) == "not" && word(i+2) == "exists" {
		i += 3
	}
	parts, _ := chain(toks, i)
	if len(parts) == 0 {
		return syntax("a table name is expected")
	}
	r, err := tr.resolve(parts)
	if err != nil {
		return err
	}
	st.target = &r
	return nil
}

func (tr *translator) createView(st *stmt, toks []token, i int) (*stmt, error) {
	st.verb = "CREATE_VIEW"
	if i+2 < len(toks) && toks[i].is("if") && toks[i+1].is("not") && toks[i+2].is("exists") {
		st.ifExists, i = true, i+3
	}
	parts, j := chain(toks, i)
	if len(parts) == 0 {
		return nil, syntax("a view name is expected")
	}
	r, err := tr.resolve(parts)
	if err != nil {
		return nil, err
	}
	st.target = &r
	for ; j < len(toks) && !toks[j].is("as"); j++ { // OPTIONS(...) and column names are skipped
	}
	if j+1 >= len(toks) {
		return nil, syntax("CREATE VIEW needs AS <query>")
	}
	st.viewSQL = strings.TrimSpace(tr.sql[toks[j+1].start:toks[len(toks)-1].end])
	return st, nil
}

func (tr *translator) schemaStmt(st *stmt, toks []token, i int, verb string) (*stmt, error) {
	st.verb = verb
	if i+2 < len(toks) && toks[i].is("if") && toks[i+1].is("not") && toks[i+2].is("exists") {
		st.ifExists, i = true, i+3
	}
	parts, j := chain(toks, i)
	switch len(parts) {
	case 1:
		st.target = &tref{project: tr.ctx.project, dataset: parts[0]}
	case 2:
		st.target = &tref{project: parts[0], dataset: parts[1]}
	default:
		return nil, syntax("a dataset name is expected")
	}
	for ; j+2 < len(toks); j++ { // OPTIONS(location = "EU", description = "...")
		if toks[j].isP("(") || toks[j].isP(",") {
			if k := toks[j+1]; k.kind == 'w' && toks[j+2].isP("=") && j+3 < len(toks) && toks[j+3].kind == 's' {
				switch lower(k.text) {
				case "location":
					st.location = unliteral(toks[j+3].text)
				case "description":
					st.description = unliteral(toks[j+3].text)
				}
			}
		}
	}
	return st, nil
}

// unliteral reverses literal for what lexString made.
func unliteral(s string) string {
	s = strings.TrimSuffix(strings.TrimPrefix(s, "'"), "'")
	return strings.ReplaceAll(s, "''", "'")
}

func (tr *translator) dropStmt(st *stmt, toks []token, kind string) (*stmt, error) {
	st.verb = "DROP_" + strings.ToUpper(kind)
	i := 2
	if i+1 < len(toks) && toks[i].is("if") && toks[i+1].is("exists") {
		st.ifExists, i = true, i+2
	}
	parts, j := chain(toks, i)
	if len(parts) == 0 {
		return nil, syntax("a name is expected")
	}
	if kind == "schema" {
		if len(parts) == 1 {
			st.target = &tref{project: tr.ctx.project, dataset: parts[0]}
		} else {
			st.target = &tref{project: parts[0], dataset: parts[1]}
		}
		st.cascade = j < len(toks) && toks[j].is("cascade")
		return st, nil
	}
	r, err := tr.resolve(parts)
	if err != nil {
		return nil, err
	}
	st.target = &r
	return st, nil
}

// ---- expressions ----

var typeWords = map[string]string{"int64": "BIGINT", "float64": "DOUBLE", "bool": "BOOLEAN", "bytes": "BLOB", "string": "VARCHAR",
	"numeric": "DECIMAL(38,9)", "bignumeric": "DECIMAL(38,9)", "timestamp": "TIMESTAMPTZ", "datetime": "TIMESTAMP",
	"int": "BIGINT", "smallint": "BIGINT", "integer": "BIGINT", "bigint": "BIGINT", "tinyint": "BIGINT", "byteint": "BIGINT", "float": "DOUBLE"}

var tableKeywords = map[string]bool{"from": true, "join": true, "into": true, "update": true, "table": true, "using": true}

// clauseWords end a FROM list.
var clauseWords = map[string]bool{"where": true, "group": true, "order": true, "having": true, "limit": true, "union": true, "intersect": true,
	"except": true, "window": true, "qualify": true, "on": true, "select": true, "set": true, "values": true, "offset": true, "when": true}

// aliasStops are the words after UNNEST(...) that are not its alias.
var aliasStops = map[string]bool{"with": true, "on": true, "where": true, "cross": true, "left": true, "right": true, "inner": true, "full": true, "join": true,
	"group": true, "order": true, "limit": true, "union": true, "having": true, "qualify": true, "window": true, "using": true, "except": true, "intersect": true, "natural": true}

func (tr *translator) fail(err error) {
	if tr.err == nil {
		tr.err = err
	}
}

// typeAt reads a type at toks[i] (INT64, ARRAY<STRING>, STRUCT<a INT64>) and
// returns DuckDB's name for it.
func (tr *translator) typeAt(toks []token, i int) (string, int) {
	w := lower(toks[i].text)
	if (w == "array" || w == "struct") && i+1 < len(toks) && toks[i+1].isP("<") {
		// find the matching >
		depth, j := 0, i+1
		for ; j < len(toks); j++ {
			if toks[j].isP("<") {
				depth++
			} else if toks[j].isP(">") {
				if depth--; depth == 0 {
					break
				}
			}
		}
		if j >= len(toks) {
			tr.fail(syntax("unclosed < in a type"))
			return "", len(toks)
		}
		inner := toks[i+2 : j]
		if w == "array" {
			t, _ := tr.typeAt(inner, 0)
			return t + "[]", j + 1
		}
		// split at commas outside <>
		var fields []string
		depth, start := 0, 0
		for k := 0; k <= len(inner); k++ {
			if k < len(inner) && inner[k].isP("<") {
				depth++
			} else if k < len(inner) && inner[k].isP(">") {
				depth--
			}
			if k == len(inner) || inner[k].isP(",") && depth == 0 {
				part := inner[start:k]
				start = k + 1
				if len(part) == 0 {
					continue
				}
				name := "_field_" + strconv.Itoa(len(fields)+1)
				if len(part) >= 2 && (part[0].kind == 'w' || part[0].kind == 'q') && !(part[1].isP("<") || part[1].isP("(")) {
					name, part = part[0].text, part[1:]
				}
				t, _ := tr.typeAt(part, 0)
				fields = append(fields, ident(name)+" "+t)
			}
		}
		return "STRUCT(" + strings.Join(fields, ", ") + ")", j + 1
	}
	if d, ok := typeWords[w]; ok {
		n := i + 1
		if n < len(toks) && toks[n].isP("(") { // STRING(10), NUMERIC(10, 2): lengths are ignored
			if j := matching(toks, n); j > 0 {
				n = j + 1
			}
		}
		return d, n
	}
	return toks[i].text, i + 1
}

// run translates a token range; opener is the function whose arguments it is.
func (tr *translator) run(toks []token, opener string, st *stmt) string {
	var out []string
	prev, prevTok := "", "" // the previous word (lower-case), and the previous token's text
	fromList := false       // in a FROM list: a comma is followed by another table
	tableNext := tr.nextTable
	tr.nextTable = false
	for i := 0; i < len(toks) && tr.err == nil; {
		t := toks[i]
		lw := ""
		if t.kind == 'w' {
			lw = lower(t.text)
		}
		wasTable := tableNext
		tableNext = false
		advance := func(n int, s string, word string) {
			out = append(out, s)
			prevTok, prev, i = s, word, i+n
		}
		next := token{}
		if i+1 < len(toks) {
			next = toks[i+1]
		}
		// A table name.
		if wasTable && (t.kind == 'w' || t.kind == 'q') {
			if lw == "if" && next.is("not") && i+2 < len(toks) && toks[i+2].is("exists") {
				advance(3, "IF NOT EXISTS", "")
				tableNext = true
				continue
			}
			if lw == "if" && next.is("exists") {
				advance(2, "IF EXISTS", "")
				tableNext = true
				continue
			}
			parts, j := chain(toks, i)
			if !(lw == "unnest" && j < len(toks) && toks[j].isP("(")) { // not a table function
				r, err := tr.resolve(parts)
				if err != nil {
					tr.fail(err)
					break
				}
				tr.refs = append(tr.refs, r)
				if tr.cur != nil && tr.cur.target == nil && tr.cur.verb != "SELECT" && tr.cur.verb != "" {
					rr := r
					tr.cur.target = &rr
				}
				advance(j-i, r.duck(), "")
				continue
			}
		}
		switch {
		case t.kind == 'l' || t.kind == 's' || t.kind == 'n':
			advance(1, t.text, "")
		case t.kind == 'q':
			advance(1, ident(t.text), "")
		case t.isP(",") && fromList:
			tableNext = true
			advance(1, ",", "")
		case t.isP("("):
			j := matching(toks, i)
			if j < 0 {
				tr.fail(syntax("missing )"))
				return ""
			}
			advance(j-i+1, "("+tr.run(toks[i+1:j], prev, nil)+")", "")
		case t.isP("["):
			j := matching(toks, i)
			if j < 0 {
				tr.fail(syntax("missing ]"))
				return ""
			}
			inner := toks[i+1 : j]
			// arr[OFFSET(n)] is 0-based and arr[ORDINAL(n)] 1-based; DuckDB's index is 1-based and NULL out of range.
			if len(inner) >= 3 && inner[0].kind == 'w' && inner[1].isP("(") {
				idx := tr.run(inner[2:len(inner)-1], "", nil)
				switch lower(inner[0].text) {
				case "offset", "safe_offset":
					advance(j-i+1, "[("+idx+")+1]", "")
					continue
				case "ordinal", "safe_ordinal":
					advance(j-i+1, "["+idx+"]", "")
					continue
				}
			}
			advance(j-i+1, "["+tr.run(inner, "", nil)+"]", "")
		case t.kind == 'p':
			advance(1, t.text, "")
		default: // a word
			if clauseWords[lw] {
				fromList = false
			}
			switch {
			case lw == "except" && next.isP("(") && prevTok == "*":
				advance(1, "EXCLUDE", lw)
			case tr.ddlCreate && lw == "options" && next.isP("("):
				if j := matching(toks, i+1); j > 0 {
					i = j + 1
				}
			case tr.ddlCreate && (lw == "partition" || lw == "cluster") && next.is("by"):
				j := i + 2
				for ; j < len(toks) && !toks[j].is("as") && !toks[j].is("options") && !toks[j].is("cluster") && !toks[j].is("partition"); j++ {
				}
				tr.partitionOrCluster(lw, toks[i+2:j])
				i = j
			case (lw == "array" || lw == "struct") && next.isP("<") || tr.isType(lw, prev, prevTok, next):
				ty, n := tr.typeAt(toks, i)
				if n < len(toks) && toks[n].isP("[") && strings.HasSuffix(ty, "[]") { // ARRAY<INT64>[1, 2]
					if j := matching(toks, n); j > 0 {
						advance(j+1-i, "CAST(["+tr.run(toks[n+1:j], "", nil)+"] AS "+ty+")", "")
						break
					}
				}
				advance(n-i, ty, lw)
			case next.isP("(") && prevTok != ".":
				j := matching(toks, i+1)
				if j < 0 {
					tr.fail(syntax("missing )"))
					return ""
				}
				if s, n, ok := tr.call(lw, toks, i, j, prev); ok {
					advance(n-i, s, "")
				} else if r, ok := renames[lw]; ok {
					advance(1, r, lw)
				} else {
					advance(1, t.text, lw)
				}
			case tableKeywords[lw] && prevTok != ".":
				if (lw != "from" || tr.fromIsTable(prev, opener)) && !(lw == "update" && next.is("set")) { // MERGE ... UPDATE SET
					tableNext = true
					fromList = lw == "from"
				}
				advance(1, strings.ToUpper(t.text), lw)
			default:
				advance(1, t.text, lw)
			}
		}
	}
	return strings.Join(out, " ")
}

// isType is whether a word names a type here: after AS, before a string
// literal (TIMESTAMP '2020-01-01'), or as a column's type in DDL.
func (tr *translator) isType(w, prev, prevTok string, next token) bool {
	if typeWords[w] == "" || prevTok == "." {
		return false
	}
	if next.isP("(") && prev != "as" && !tr.ddl {
		return false // a function: TIMESTAMP(x)
	}
	switch {
	case prev == "as" || next.kind == 's':
		return true
	case tr.ddl && prev != "":
		return !exprWords[prev]
	}
	return false
}

// exprWords precede an expression, so a word after them is a column or function.
var exprWords = map[string]bool{"select": true, "distinct": true, "by": true, "where": true, "and": true, "or": true, "not": true, "on": true,
	"when": true, "then": true, "else": true, "set": true, "having": true, "qualify": true, "in": true, "is": true, "from": true, "join": true}

// fromIsTable is whether a FROM is followed by a table name.
func (tr *translator) fromIsTable(prev, opener string) bool {
	return prev != "distinct" && opener != "extract" && opener != "trim" && opener != "substring" && opener != "substr" && opener != "overlay"
}

func (tr *translator) partitionOrCluster(kind string, toks []token) {
	st := tr.cur
	if st == nil {
		return
	}
	var names []string
	for _, t := range toks {
		if t.kind == 'w' || t.kind == 'q' {
			names = append(names, t.text)
		}
	}
	if kind == "cluster" {
		st.clusterBy = names
		return
	}
	for _, n := range names {
		if !strings.EqualFold(n, "date") && !strings.EqualFold(n, "timestamp_trunc") && !strings.EqualFold(n, "date_trunc") && !strings.EqualFold(n, "datetime_trunc") {
			st.partition = n
			return
		}
	}
}

var renames = map[string]string{"safe_cast": "TRY_CAST", "countif": "count_if", "regexp_contains": "regexp_matches", "starts_with": "starts_with",
	"ends_with": "ends_with", "split": "string_split", "array_length": "len", "logical_or": "bool_or", "logical_and": "bool_and",
	"json_extract_scalar": "json_extract_string", "json_value": "json_extract_string", "json_query": "json_extract", "array_concat": "list_concat",
	"array_reverse": "list_reverse", "format": "printf", "timestamp_seconds": "to_timestamp", "char_length": "length", "character_length": "length",
	"byte_length": "octet_length", "approx_count_distinct": "approx_count_distinct", "any_value": "any_value", "array_agg": "list"}

// units are the date parts BigQuery takes as bare words.
func unit(s string) string { return literal(strings.ToLower(strings.TrimSpace(s))) }

// call translates the call of function name, whose "(" is toks[i+1] and ")"
// toks[j]. It returns the SQL and the index after the call.
func (tr *translator) call(name string, toks []token, i, j int, prev string) (string, int, bool) {
	inner := toks[i+2 : j]
	args := func() []string {
		var out []string
		for _, a := range splitComma(inner) {
			out = append(out, tr.run(a, name, nil))
		}
		return out
	}
	if name == "unnest" {
		arg := tr.run(inner, name, nil)
		n := j + 1
		if prev == "in" {
			return "(SELECT UNNEST(" + arg + "))", n, true
		}
		alias := ""
		switch {
		case n+1 < len(toks) && toks[n].is("as") && (toks[n+1].kind == 'w' || toks[n+1].kind == 'q'):
			alias, n = toks[n+1].text, n+2
		case n < len(toks) && toks[n].kind == 'w' && !aliasStops[lower(toks[n].text)]:
			alias, n = toks[n].text, n+1
		}
		if n+1 < len(toks) && toks[n].is("with") && toks[n+1].is("offset") {
			tr.fail(syntax("pogi does not support UNNEST ... WITH OFFSET"))
		}
		if alias == "" {
			return "UNNEST(" + arg + ")", n, true
		}
		return "UNNEST(" + arg + ") AS " + ident("_u_"+alias) + "(" + ident(alias) + ")", n, true
	}
	if name == "struct" {
		var fields []string
		for k, a := range splitComma(inner) {
			fname := "_field_" + strconv.Itoa(k+1)
			if len(a) >= 3 && a[len(a)-2].is("as") && (a[len(a)-1].kind == 'w' || a[len(a)-1].kind == 'q') {
				fname, a = a[len(a)-1].text, a[:len(a)-2]
			}
			fields = append(fields, ident(fname)+" := "+tr.run(a, name, nil))
		}
		return "struct_pack(" + strings.Join(fields, ", ") + ")", j + 1, true
	}
	f, ok := callRewrites[name]
	if !ok {
		return "", 0, false
	}
	a := args()
	if len(inner) == 0 {
		a = nil
	}
	s, ok := f(a)
	return s, j + 1, ok
}

var callRewrites = map[string]func(a []string) (string, bool){
	"current_timestamp": func(a []string) (string, bool) { return "now()", len(a) == 0 },
	"current_date":      func(a []string) (string, bool) { return "current_date", len(a) == 0 },
	"current_datetime":  func(a []string) (string, bool) { return "CAST(now() AS TIMESTAMP)", len(a) == 0 },
	"current_time":      func(a []string) (string, bool) { return "CAST(now() AS TIME)", len(a) == 0 },
	"generate_uuid":     func(a []string) (string, bool) { return "CAST(uuid() AS VARCHAR)", len(a) == 0 },
	"safe_divide": func(a []string) (string, bool) {
		return "(CASE WHEN (" + a[1] + ") = 0 THEN NULL ELSE CAST(" + a[0] + " AS DOUBLE) / (" + a[1] + ") END)", len(a) == 2
	},
	"div":              func(a []string) (string, bool) { return "((" + a[0] + ") // (" + a[1] + "))", len(a) == 2 },
	"date_add":         func(a []string) (string, bool) { return "CAST(" + a[0] + " + " + a[1] + " AS DATE)", len(a) == 2 },
	"date_sub":         func(a []string) (string, bool) { return "CAST(" + a[0] + " - " + a[1] + " AS DATE)", len(a) == 2 },
	"timestamp_add":    func(a []string) (string, bool) { return "(" + a[0] + " + " + a[1] + ")", len(a) == 2 },
	"timestamp_sub":    func(a []string) (string, bool) { return "(" + a[0] + " - " + a[1] + ")", len(a) == 2 },
	"datetime_add":     func(a []string) (string, bool) { return "(" + a[0] + " + " + a[1] + ")", len(a) == 2 },
	"datetime_sub":     func(a []string) (string, bool) { return "(" + a[0] + " - " + a[1] + ")", len(a) == 2 },
	"date_diff":        diff,
	"timestamp_diff":   diff,
	"datetime_diff":    diff,
	"date_trunc":       trunc,
	"timestamp_trunc":  trunc,
	"datetime_trunc":   trunc,
	"format_timestamp": func(a []string) (string, bool) { return "strftime(" + a[1] + ", " + a[0] + ")", len(a) == 2 },
	"format_date":      func(a []string) (string, bool) { return "strftime(" + a[1] + ", " + a[0] + ")", len(a) == 2 },
	"format_datetime":  func(a []string) (string, bool) { return "strftime(" + a[1] + ", " + a[0] + ")", len(a) == 2 },
	"parse_timestamp": func(a []string) (string, bool) {
		return "CAST(strptime(" + a[1] + ", " + a[0] + ") AS TIMESTAMPTZ)", len(a) == 2
	},
	"parse_date": func(a []string) (string, bool) {
		return "CAST(strptime(" + a[1] + ", " + a[0] + ") AS DATE)", len(a) == 2
	},
	"parse_datetime":   func(a []string) (string, bool) { return "strptime(" + a[1] + ", " + a[0] + ")", len(a) == 2 },
	"timestamp":        func(a []string) (string, bool) { return "CAST(" + a[0] + " AS TIMESTAMPTZ)", len(a) == 1 },
	"datetime":         func(a []string) (string, bool) { return "CAST(" + a[0] + " AS TIMESTAMP)", len(a) == 1 },
	"date":             func(a []string) (string, bool) { return "CAST(" + a[0] + " AS DATE)", len(a) == 1 },
	"unix_seconds":     func(a []string) (string, bool) { return "CAST(epoch(" + a[0] + ") AS BIGINT)", len(a) == 1 },
	"unix_millis":      func(a []string) (string, bool) { return "epoch_ms(" + a[0] + ")", len(a) == 1 },
	"timestamp_millis": func(a []string) (string, bool) { return "CAST(epoch_ms(" + a[0] + ") AS TIMESTAMPTZ)", len(a) == 1 },
	"to_json_string":   func(a []string) (string, bool) { return "CAST(to_json(" + a[0] + ") AS VARCHAR)", len(a) >= 1 },
	"regexp_replace": func(a []string) (string, bool) {
		return "regexp_replace(" + strings.Join(a, ", ") + ", 'g')", len(a) == 3
	},
	"regexp_extract": func(a []string) (string, bool) {
		group := ""
		if strings.Contains(a[1], "(") {
			group = ", 1"
		}
		return "NULLIF(regexp_extract(" + a[0] + ", " + a[1] + group + "), '')", len(a) == 2
	},
	"generate_array": func(a []string) (string, bool) {
		return "generate_series(" + strings.Join(a, ", ") + ")", len(a) == 2 || len(a) == 3
	},
}

func diff(a []string) (string, bool) {
	if len(a) != 3 {
		return "", false
	}
	return "date_diff(" + unit(a[2]) + ", " + a[1] + ", " + a[0] + ")", true
}

func trunc(a []string) (string, bool) {
	if len(a) < 2 {
		return "", false
	}
	return "date_trunc(" + unit(a[1]) + ", " + a[0] + ")", true
}
