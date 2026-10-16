package athena

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strings"
)

// job is one query execution being run.
type job struct {
	s                   *Service
	ctx                 context.Context
	account, region, id string
	db                  string // the query's database
}

// result is what a statement returns.
type result struct {
	cols    []column
	rows    [][]*string
	scanned int64 // bytes read
}

// The Glue shapes Athena reads and writes.
type (
	gColumn struct {
		Name    string
		Type    string `json:",omitempty"`
		Comment string `json:",omitempty"`
	}
	gSerde struct {
		SerializationLibrary string            `json:",omitempty"`
		Parameters           map[string]string `json:",omitempty"`
	}
	gSD struct {
		Columns      []gColumn
		Location     string `json:",omitempty"`
		InputFormat  string `json:",omitempty"`
		OutputFormat string `json:",omitempty"`
		SerdeInfo    gSerde
	}
	gTable struct {
		Name              string
		DatabaseName      string `json:",omitempty"`
		Description       string `json:",omitempty"`
		TableType         string `json:",omitempty"`
		CreateTime        any    `json:",omitempty"`
		LastAccessTime    any    `json:",omitempty"`
		Parameters        map[string]string
		PartitionKeys     []gColumn
		StorageDescriptor gSD
	}
	gPartition struct {
		Values            []string
		StorageDescriptor gSD
	}
	gDatabase struct {
		Name, Description string
		Parameters        map[string]string
	}
)

func (j *job) glue(op string, in, out any) error {
	return j.s.hub.Call(j.account, j.region, "AWSGlue", op, in, out)
}

func (j *job) table(db, name string) (gTable, error) {
	var out struct{ Table gTable }
	err := j.glue("GetTable", obj{"DatabaseName": db, "Name": name}, &out)
	if code(err) == "EntityNotFoundException" {
		err = fmt.Errorf("FAILED: SemanticException [Error 10001]: Table not found %s.%s", db, name)
	}
	return out.Table, err
}

// tables returns the tables of a database whose names match a pattern, by name.
func (j *job) tables(db, pattern string) (map[string]gTable, error) {
	out := map[string]gTable{}
	for token := ""; ; {
		var page struct {
			TableList []gTable
			NextToken string
		}
		if err := j.glue("GetTables", obj{"DatabaseName": db, "Expression": pattern, "NextToken": token, "MaxResults": 1000}, &page); err != nil {
			if code(err) == "EntityNotFoundException" {
				err = fmt.Errorf("FAILED: SemanticException [Error 10072]: Database does not exist: %s", db)
			}
			return out, err
		}
		for _, t := range page.TableList {
			out[t.Name] = t
		}
		if token = page.NextToken; token == "" {
			return out, nil
		}
	}
}

func (j *job) partitions(t gTable) ([]gPartition, error) {
	var all []gPartition
	for token := ""; ; {
		var page struct {
			Partitions []gPartition
			NextToken  string
		}
		if err := j.glue("GetPartitions", obj{"DatabaseName": t.DatabaseName, "TableName": t.Name, "NextToken": token, "MaxResults": 1000}, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Partitions...)
		if token = page.NextToken; token == "" {
			return all, nil
		}
	}
}

// tok is a token: a word, a quoted identifier ("x" or `x`), a string
// literal or one punctuation character.
type tok struct {
	s   string
	k   byte // 'w', 'i', 's' or 'p'
	pos int
}

func (t tok) is(w string) bool { return (t.k == 'w' || t.k == 'p') && strings.EqualFold(t.s, w) }
func (t tok) ident() bool      { return t.k == 'w' || t.k == 'i' }

func isWord(c byte) bool {
	return c == '_' || c == '$' || '0' <= c && c <= '9' || 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || c >= 0x80
}

// lex splits a statement into tokens, without comments. With hive, string
// literals decode Hive's backslash escapes (\t, \n, \\, \', \001); without,
// only a doubled quote is an escape, as in Trino.
func lex(sql string, hive bool) ([]tok, error) {
	var out []tok
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case strings.HasPrefix(sql[i:], "--"):
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case strings.HasPrefix(sql[i:], "/*"):
			j := strings.Index(sql[i+2:], "*/")
			if j < 0 {
				return nil, errors.New("line 1:1: unterminated comment")
			}
			i += j + 4
		case c == '\'':
			var b strings.Builder
			j := i + 1
			for ; j < len(sql) && (sql[j] != '\'' || j+1 < len(sql) && sql[j+1] == '\''); j++ {
				switch {
				case sql[j] == '\'':
					j++
					b.WriteByte('\'')
				case hive && sql[j] == '\\' && j+1 < len(sql):
					j++
					switch e := sql[j]; {
					case j+2 < len(sql) && octal(sql[j]) && octal(sql[j+1]) && octal(sql[j+2]): // \001
						b.WriteByte((e-'0')<<6 | (sql[j+1]-'0')<<3 | (sql[j+2] - '0'))
						j += 2
					case e == 't':
						b.WriteByte('\t')
					case e == 'n':
						b.WriteByte('\n')
					case e == 'r':
						b.WriteByte('\r')
					default: // \\, \' and the rest stand for themselves
						b.WriteByte(e)
					}
				default:
					b.WriteByte(sql[j])
				}
			}
			if j >= len(sql) {
				return nil, fmt.Errorf("line %s: unterminated string literal", where(sql, i))
			}
			out, i = append(out, tok{b.String(), 's', i}), j+1
		case c == '"' || c == '`':
			j := strings.IndexByte(sql[i+1:], c)
			if j < 0 {
				return nil, fmt.Errorf("line %s: unterminated quoted identifier", where(sql, i))
			}
			out, i = append(out, tok{sql[i+1 : i+1+j], 'i', i}), i+j+2
		case isWord(c):
			j := i
			for j < len(sql) && isWord(sql[j]) {
				j++
			}
			out, i = append(out, tok{sql[i:j], 'w', i}), j
		default:
			out, i = append(out, tok{string(c), 'p', i}), i+1
		}
	}
	return out, nil
}

func octal(c byte) bool { return '0' <= c && c <= '7' }

// where is a position as Athena reports it: line:column.
func where(sql string, pos int) string {
	return fmt.Sprintf("%d:%d", strings.Count(sql[:pos], "\n")+1, pos-strings.LastIndexByte(sql[:pos], '\n'))
}

type parser struct {
	sql string
	t   []tok
	i   int
}

func (p *parser) peek(ws ...string) bool {
	for k, w := range ws {
		if p.i+k >= len(p.t) || !p.t[p.i+k].is(w) {
			return false
		}
	}
	return true
}

func (p *parser) accept(ws ...string) bool {
	if p.peek(ws...) {
		p.i += len(ws)
		return true
	}
	return false
}

func (p *parser) fail() error {
	if p.i >= len(p.t) {
		return fmt.Errorf("line %s: mismatched input '<EOF>'", where(p.sql, len(p.sql)))
	}
	return fmt.Errorf("line %s: mismatched input '%s'", where(p.sql, p.t[p.i].pos), p.t[p.i].s)
}

func (p *parser) expect(ws ...string) error {
	if !p.accept(ws...) {
		return p.fail()
	}
	return nil
}

func (p *parser) ident() (string, error) {
	if p.i < len(p.t) && p.t[p.i].ident() {
		p.i++
		return p.t[p.i-1].s, nil
	}
	return "", p.fail()
}

func (p *parser) str() (string, error) {
	if p.i < len(p.t) && p.t[p.i].k == 's' {
		p.i++
		return p.t[p.i-1].s, nil
	}
	return "", p.fail()
}

// name reads [database.]name in lower case; def is the database when none is given.
func (p *parser) name(def string) (string, string, error) {
	name, err := p.ident()
	db := def
	if err == nil && p.accept(".") {
		db = name
		name, err = p.ident()
	}
	return strings.ToLower(db), strings.ToLower(name), err
}

// more reports whether the statement goes on (a trailing ; ends it).
func (p *parser) more() bool {
	p.accept(";")
	return p.i < len(p.t)
}

// end checks that the statement is over.
func (p *parser) end() error {
	if p.more() {
		return p.fail()
	}
	return nil
}

// props reads ('key' = 'value', …).
func (p *parser) props() (map[string]string, error) {
	out := map[string]string{}
	if err := p.expect("("); err != nil {
		return nil, err
	}
	for {
		k, err := p.str()
		if err == nil {
			err = p.expect("=")
		}
		var v string
		if err == nil {
			v, err = p.str()
		}
		if err != nil {
			return nil, err
		}
		out[k] = v
		if p.accept(")") {
			return out, nil
		}
		if err := p.expect(","); err != nil {
			return nil, err
		}
	}
}

// columns reads (name type [COMMENT 'text'], …).
func (p *parser) columns() ([]gColumn, error) {
	if err := p.expect("("); err != nil {
		return nil, err
	}
	var cols []gColumn
	for {
		n, err := p.ident()
		if err != nil {
			return nil, err
		}
		c := gColumn{Name: strings.ToLower(n), Type: p.typ()}
		if c.Type == "" {
			return nil, p.fail()
		}
		if p.accept("COMMENT") {
			if c.Comment, err = p.str(); err != nil {
				return nil, err
			}
		}
		cols = append(cols, c)
		if p.accept(")") {
			return cols, nil
		}
		if err := p.expect(","); err != nil {
			return nil, err
		}
	}
}

// typ reads a column type: int, decimal(10,2), array<struct<a:int>>…
func (p *parser) typ() string {
	var b strings.Builder
	for depth := 0; p.i < len(p.t); p.i++ {
		t := p.t[p.i]
		if depth == 0 && (t.is(",") || t.is(")") || t.is("COMMENT")) {
			break
		}
		if t.is("<") || t.is("(") {
			depth++
		} else if t.is(">") || t.is(")") {
			depth--
		}
		b.WriteString(t.s)
	}
	return strings.ToLower(b.String())
}

// spec reads a partition spec, (key = 'value', …).
func (p *parser) spec() (map[string]string, error) {
	out := map[string]string{}
	if err := p.expect("("); err != nil {
		return nil, err
	}
	for {
		k, err := p.ident()
		if err == nil {
			err = p.expect("=")
		}
		if err == nil && (p.i >= len(p.t) || p.t[p.i].k == 'p') {
			err = p.fail()
		}
		if err != nil {
			return nil, err
		}
		out[strings.ToLower(k)] = p.t[p.i].s
		if p.i++; p.accept(")") {
			return out, nil
		}
		if err := p.expect(","); err != nil {
			return nil, err
		}
	}
}

// delimited reads the clauses of ROW FORMAT DELIMITED into SerDe parameters.
func (p *parser) delimited(params map[string]string) error {
	clauses := map[string][]string{"field.delim": {"FIELDS", "TERMINATED", "BY"}, "escape.delim": {"ESCAPED", "BY"},
		"collection.delim": {"COLLECTION", "ITEMS", "TERMINATED", "BY"}, "mapkey.delim": {"MAP", "KEYS", "TERMINATED", "BY"},
		"line.delim": {"LINES", "TERMINATED", "BY"}, "serialization.null.format": {"NULL", "DEFINED", "AS"}}
	for found := true; found; {
		found = false
		for _, param := range slices.Sorted(maps.Keys(clauses)) {
			if p.accept(clauses[param]...) {
				v, err := p.str()
				if err != nil {
					return err
				}
				params[param], found = v, true
			}
		}
	}
	if d := params["field.delim"]; d != "" {
		params["serialization.format"] = d
	}
	return nil
}

// formats are the input format, output format and SerDe of STORED AS.
var formats = map[string][3]string{
	"TEXTFILE": {"org.apache.hadoop.mapred.TextInputFormat", "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
	"PARQUET":  {"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
	"ORC":      {"org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcSerde"},
	"AVRO":     {"org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat", "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat", "org.apache.hadoop.hive.serde2.avro.AvroSerDe"},
}

// ddl runs a DDL or utility statement as Glue calls.
func (j *job) ddl(sql string) (*result, error) {
	toks, err := lex(sql, true)
	if err != nil {
		return nil, err
	}
	p := &parser{sql: sql, t: toks}
	known := 0 // keywords of the closest statement, for the error
	for _, st := range statements {
		if p.accept(st.words...) {
			return st.run(j, p)
		}
		for n := len(st.words); n > known; n-- {
			if p.peek(st.words[:n]...) {
				known = n
			}
		}
	}
	p.i = known
	return nil, fmt.Errorf("%w (pogi runs CREATE DATABASE, CREATE EXTERNAL TABLE, DROP, ALTER TABLE … PARTITION, MSCK REPAIR TABLE, SHOW and DESCRIBE)", p.fail())
}

// statements are the DDL and utility statements, by their first keywords.
var statements = []struct {
	words []string
	run   func(*job, *parser) (*result, error)
}{
	{[]string{"CREATE", "DATABASE"}, (*job).createDatabase},
	{[]string{"CREATE", "SCHEMA"}, (*job).createDatabase},
	{[]string{"CREATE", "EXTERNAL", "TABLE"}, (*job).createTable},
	{[]string{"CREATE", "TABLE"}, (*job).createTable},
	{[]string{"DROP", "TABLE"}, (*job).dropTable},
	{[]string{"DROP", "DATABASE"}, (*job).dropDatabase},
	{[]string{"DROP", "SCHEMA"}, (*job).dropDatabase},
	{[]string{"MSCK", "REPAIR", "TABLE"}, (*job).repair},
	{[]string{"ALTER", "TABLE"}, (*job).alterTable},
	{[]string{"SHOW", "TABLES"}, (*job).showTables},
	{[]string{"SHOW", "DATABASES"}, (*job).showDatabases},
	{[]string{"SHOW", "SCHEMAS"}, (*job).showDatabases},
	{[]string{"SHOW", "PARTITIONS"}, (*job).showPartitions},
	{[]string{"SHOW", "COLUMNS"}, (*job).showColumns},
	{[]string{"DESCRIBE"}, (*job).describe},
	{[]string{"DESC"}, (*job).describe},
}

func (j *job) createDatabase(p *parser) (*result, error) {
	ifNot := p.accept("IF", "NOT", "EXISTS")
	name, err := p.ident()
	in := obj{"Name": strings.ToLower(name)}
	for err == nil && p.more() {
		switch {
		case p.accept("COMMENT"):
			in["Description"], err = p.str()
		case p.accept("LOCATION"):
			in["LocationUri"], err = p.str()
		case p.accept("WITH", "DBPROPERTIES"):
			in["Parameters"], err = p.props()
		default:
			err = p.fail()
		}
	}
	if err != nil {
		return nil, err
	}
	err = j.glue("CreateDatabase", obj{"DatabaseInput": in}, nil)
	if code(err) == "AlreadyExistsException" {
		err = nil
		if !ifNot {
			err = fmt.Errorf("FAILED: AlreadyExistsException: Database %s already exists", in["Name"])
		}
	}
	return &result{}, err
}

func (j *job) createTable(p *parser) (*result, error) {
	ifNot := p.accept("IF", "NOT", "EXISTS")
	db, name, err := p.name(j.db)
	if err != nil {
		return nil, err
	}
	if p.peek("AS") || p.peek("WITH") {
		return nil, errors.New("pogi: CREATE TABLE AS SELECT is not supported yet")
	}
	t := gTable{Name: name, TableType: "EXTERNAL_TABLE", Parameters: map[string]string{"EXTERNAL": "TRUE"}, PartitionKeys: []gColumn{}}
	sd := &t.StorageDescriptor
	if sd.Columns, err = p.columns(); err != nil {
		return nil, err
	}
	stored, serde, params := "TEXTFILE", "", map[string]string{}
	for err == nil && p.more() {
		switch {
		case p.accept("COMMENT"):
			t.Description, err = p.str()
		case p.accept("PARTITIONED", "BY"):
			t.PartitionKeys, err = p.columns()
		case p.accept("ROW", "FORMAT", "DELIMITED"):
			err = p.delimited(params)
		case p.accept("ROW", "FORMAT", "SERDE"):
			if serde, err = p.str(); err == nil && p.accept("WITH", "SERDEPROPERTIES") {
				var m map[string]string
				m, err = p.props()
				maps.Copy(params, m)
			}
		case p.accept("STORED", "AS", "INPUTFORMAT"):
			stored = ""
			if sd.InputFormat, err = p.str(); err == nil {
				if err = p.expect("OUTPUTFORMAT"); err == nil {
					sd.OutputFormat, err = p.str()
				}
			}
		case p.accept("STORED", "AS"):
			if stored, err = p.ident(); err == nil && formats[strings.ToUpper(stored)][0] == "" {
				err = fmt.Errorf("pogi: STORED AS %s is not supported", stored)
			}
			stored = strings.ToUpper(stored)
		case p.accept("LOCATION"):
			sd.Location, err = p.str()
		case p.accept("TBLPROPERTIES"):
			var m map[string]string
			m, err = p.props()
			maps.Copy(t.Parameters, m)
		default:
			err = p.fail()
		}
	}
	if err != nil {
		return nil, err
	}
	if f, ok := formats[stored]; ok {
		sd.InputFormat, sd.OutputFormat, serde = f[0], f[1], cmp.Or(serde, f[2])
	}
	serde = cmp.Or(serde, formats["TEXTFILE"][2])
	if strings.Contains(serde, "LazySimpleSerDe") && params["serialization.format"] == "" {
		params["serialization.format"] = "1"
	}
	sd.SerdeInfo = gSerde{SerializationLibrary: serde, Parameters: params}
	err = j.glue("CreateTable", obj{"DatabaseName": db, "TableInput": t}, nil)
	switch code(err) {
	case "AlreadyExistsException":
		err = nil
		if !ifNot {
			err = fmt.Errorf("FAILED: AlreadyExistsException: Table %s.%s already exists", db, name)
		}
	case "EntityNotFoundException":
		err = fmt.Errorf("FAILED: SemanticException [Error 10072]: Database does not exist: %s", db)
	}
	return &result{}, err
}

func (j *job) dropTable(p *parser) (*result, error) {
	ifExists := p.accept("IF", "EXISTS")
	db, name, err := p.name(j.db)
	if err == nil {
		err = p.end()
	}
	if err != nil {
		return nil, err
	}
	err = j.glue("DeleteTable", obj{"DatabaseName": db, "Name": name}, nil)
	if code(err) == "EntityNotFoundException" {
		err = nil
		if !ifExists {
			err = fmt.Errorf("FAILED: SemanticException [Error 10001]: Table not found %s.%s", db, name)
		}
	}
	return &result{}, err
}

// dropDatabase drops a database; without CASCADE it must have no tables.
func (j *job) dropDatabase(p *parser) (*result, error) {
	ifExists := p.accept("IF", "EXISTS")
	name, err := p.ident()
	name = strings.ToLower(name)
	cascade := p.accept("CASCADE")
	if !cascade {
		p.accept("RESTRICT")
	}
	if err == nil {
		err = p.end()
	}
	if err != nil {
		return nil, err
	}
	if tables, err := j.tables(name, ""); err == nil && len(tables) > 0 && !cascade {
		return nil, fmt.Errorf("FAILED: InvalidOperationException(message:Database %s is not empty. One or more tables exist.)", name)
	}
	err = j.glue("DeleteDatabase", obj{"Name": name}, nil)
	if code(err) == "EntityNotFoundException" {
		err = nil
		if !ifExists {
			err = fmt.Errorf("FAILED: SemanticException [Error 10072]: Database does not exist: %s", name)
		}
	}
	return &result{}, err
}

// specValues orders a partition spec by the table's keys and returns the
// values and their folder, k1=v1/k2=v2.
func specValues(t gTable, spec map[string]string) ([]string, string, error) {
	var values, dirs []string
	for _, k := range t.PartitionKeys {
		v, ok := spec[strings.ToLower(k.Name)]
		if !ok || len(spec) != len(t.PartitionKeys) {
			return nil, "", fmt.Errorf("FAILED: ValidationException: the partition spec does not match the partition keys of %s", t.Name)
		}
		values, dirs = append(values, v), append(dirs, k.Name+"="+url.PathEscape(v))
	}
	return values, strings.Join(dirs, "/"), nil
}

// addPartitions registers partitions; existing ones fail unless ignored.
func (j *job) addPartitions(t gTable, parts []obj, ignore bool) error {
	for chunk := range slices.Chunk(parts, 100) {
		var out struct {
			Errors []struct{ ErrorDetail struct{ ErrorMessage string } }
		}
		err := j.glue("BatchCreatePartition", obj{"DatabaseName": t.DatabaseName, "TableName": t.Name, "PartitionInputList": chunk}, &out)
		if err == nil && len(out.Errors) > 0 && !ignore {
			err = errors.New("FAILED: AlreadyExistsException: " + out.Errors[0].ErrorDetail.ErrorMessage)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// repair (MSCK REPAIR TABLE) adds the key=value folders under the table's
// location that are not partitions yet.
func (j *job) repair(p *parser) (*result, error) {
	db, name, err := p.name(j.db)
	if err == nil {
		err = p.end()
	}
	if err != nil {
		return nil, err
	}
	t, err := j.table(db, name)
	if err != nil || len(t.PartitionKeys) == 0 {
		return &result{}, err
	}
	parts, err := j.partitions(t)
	if err != nil {
		return nil, err
	}
	have := map[string]bool{}
	for _, pt := range parts {
		have[strings.Join(pt.Values, "\x00")] = true
	}
	bucket, prefix := splitS3(t.StorageDescriptor.Location)
	keys, err := objects(j.s.hub, j.account, j.region, bucket, prefix)
	if err != nil {
		return nil, err
	}
	var add []obj
	for _, k := range keys {
		dirs := strings.Split(strings.TrimPrefix(k, prefix), "/")
		if len(dirs) <= len(t.PartitionKeys) {
			continue
		}
		var values []string
		for i, key := range t.PartitionKeys {
			if n, v, ok := strings.Cut(dirs[i], "="); ok && strings.EqualFold(n, key.Name) {
				if u, err := url.PathUnescape(v); err == nil {
					v = u
				}
				values = append(values, v)
			}
		}
		if id := strings.Join(values, "\x00"); len(values) == len(t.PartitionKeys) && !have[id] {
			have[id] = true
			sd := t.StorageDescriptor
			sd.Location = "s3://" + bucket + "/" + prefix + strings.Join(dirs[:len(t.PartitionKeys)], "/") + "/"
			add = append(add, obj{"Values": values, "StorageDescriptor": sd})
		}
	}
	return &result{}, j.addPartitions(t, add, true)
}

// alterTable serves ALTER TABLE … ADD PARTITION and DROP PARTITION.
func (j *job) alterTable(p *parser) (*result, error) {
	db, name, err := p.name(j.db)
	if err != nil {
		return nil, err
	}
	t, err := j.table(db, name)
	if err != nil {
		return nil, err
	}
	switch {
	case p.accept("ADD"):
		ifNot := p.accept("IF", "NOT", "EXISTS")
		var add []obj
		for p.accept("PARTITION") {
			spec, err := p.spec()
			if err != nil {
				return nil, err
			}
			values, dir, err := specValues(t, spec)
			if err != nil {
				return nil, err
			}
			sd := t.StorageDescriptor
			sd.Location = strings.TrimSuffix(sd.Location, "/") + "/" + dir + "/"
			if p.accept("LOCATION") {
				if sd.Location, err = p.str(); err != nil {
					return nil, err
				}
			}
			add = append(add, obj{"Values": values, "StorageDescriptor": sd})
		}
		if len(add) == 0 {
			return nil, p.fail()
		}
		if err := p.end(); err != nil {
			return nil, err
		}
		return &result{}, j.addPartitions(t, add, ifNot)
	case p.accept("DROP"):
		ifExists := p.accept("IF", "EXISTS")
		var del []obj
		for len(del) == 0 || p.accept(",") {
			if err := p.expect("PARTITION"); err != nil {
				return nil, err
			}
			spec, err := p.spec()
			if err != nil {
				return nil, err
			}
			values, _, err := specValues(t, spec)
			if err != nil {
				return nil, err
			}
			del = append(del, obj{"Values": values})
		}
		if err := p.end(); err != nil {
			return nil, err
		}
		var out struct{ Errors []any }
		err := j.glue("BatchDeletePartition", obj{"DatabaseName": db, "TableName": name, "PartitionsToDelete": del}, &out)
		if err == nil && len(out.Errors) > 0 && !ifExists {
			err = errors.New("FAILED: SemanticException [Error 10006]: Partition not found")
		}
		return &result{}, err
	}
	return nil, errors.New("pogi: Athena supports ALTER TABLE ADD PARTITION and DROP PARTITION only")
}

// lines is a utility statement's result: one string column, no header row.
func lines(name string, values []string) *result {
	r := &result{cols: []column{{CatalogName: "hive", Name: name, Label: name, Type: "string", Nullable: "UNKNOWN", CaseSensitive: true}}}
	for _, v := range values {
		r.rows = append(r.rows, []*string{&v})
	}
	return r
}

// pattern reads an optional 'pattern' of SHOW statements.
func (p *parser) pattern() string {
	p.accept("LIKE")
	if p.i < len(p.t) && p.t[p.i].k == 's' {
		p.i++
		return p.t[p.i-1].s
	}
	return ""
}

func (j *job) showTables(p *parser) (*result, error) {
	db := j.db
	if p.accept("IN") || p.accept("FROM") {
		name, err := p.ident()
		if err != nil {
			return nil, err
		}
		db = strings.ToLower(name)
	}
	pattern := p.pattern()
	if err := p.end(); err != nil {
		return nil, err
	}
	tables, err := j.tables(db, pattern)
	if err != nil {
		return nil, err
	}
	return lines("tab_name", slices.Sorted(maps.Keys(tables))), nil
}

func (j *job) showDatabases(p *parser) (*result, error) {
	pattern := p.pattern()
	if err := p.end(); err != nil {
		return nil, err
	}
	re, err := regexp.Compile("(?i)^(?:" + strings.ReplaceAll(cmp.Or(pattern, "*"), "*", ".*") + ")$")
	if err != nil {
		return nil, err
	}
	var names []string
	for token := ""; ; {
		var page struct {
			DatabaseList []gDatabase
			NextToken    string
		}
		if err := j.glue("GetDatabases", obj{"NextToken": token, "MaxResults": 1000}, &page); err != nil {
			return nil, err
		}
		for _, d := range page.DatabaseList {
			if re.MatchString(d.Name) {
				names = append(names, d.Name)
			}
		}
		if token = page.NextToken; token == "" {
			return lines("database_name", names), nil
		}
	}
}

func (j *job) showPartitions(p *parser) (*result, error) {
	db, name, err := p.name(j.db)
	if err == nil {
		err = p.end()
	}
	var t gTable
	if err == nil {
		t, err = j.table(db, name)
	}
	var parts []gPartition
	if err == nil {
		parts, err = j.partitions(t)
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, pt := range parts {
		var dirs []string
		for i, k := range t.PartitionKeys {
			dirs = append(dirs, k.Name+"="+pt.Values[i])
		}
		out = append(out, strings.Join(dirs, "/"))
	}
	return lines("partition", out), nil
}

func (j *job) showColumns(p *parser) (*result, error) {
	if !p.accept("IN") {
		if err := p.expect("FROM"); err != nil {
			return nil, err
		}
	}
	db, name, err := p.name(j.db)
	if err == nil && (p.accept("IN") || p.accept("FROM")) {
		db, err = p.ident()
		db = strings.ToLower(db)
	}
	if err == nil {
		err = p.end()
	}
	var t gTable
	if err == nil {
		t, err = j.table(db, name)
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, c := range append(t.StorageDescriptor.Columns, t.PartitionKeys...) {
		out = append(out, c.Name)
	}
	return lines("field", out), nil
}

// describe lists a table's columns, then its partition keys, Hive style.
func (j *job) describe(p *parser) (*result, error) {
	if !p.accept("FORMATTED") {
		p.accept("EXTENDED")
	}
	db, name, err := p.name(j.db)
	col := ""
	if err == nil && p.more() {
		col, err = p.ident()
	}
	if err == nil {
		err = p.end()
	}
	var t gTable
	if err == nil {
		t, err = j.table(db, name)
	}
	if err != nil {
		return nil, err
	}
	line := func(c gColumn) string { return fmt.Sprintf("%-20s\t%-20s\t%-20s", c.Name, c.Type, c.Comment) }
	var out []string
	for _, c := range append(slices.Clone(t.StorageDescriptor.Columns), t.PartitionKeys...) {
		if col == "" || strings.EqualFold(col, c.Name) {
			out = append(out, line(c))
		}
	}
	if col == "" && len(t.PartitionKeys) > 0 {
		out = append(out, "\t \t ", "# Partition Information\t \t ", line(gColumn{"# col_name", "data_type", "comment"}), "\t \t ")
		for _, c := range t.PartitionKeys {
			out = append(out, line(c))
		}
	}
	return lines("col_name", out), nil
}
