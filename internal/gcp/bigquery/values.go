package bigquery

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func sprintf(format string, a ...any) string { return fmt.Sprintf(format, a...) }

// Canonical values, stored in rows (a missing or nil value is NULL):
//
//	INTEGER   decimal string        FLOAT      float64, or "NaN"/"Infinity"/"-Infinity"
//	BOOLEAN   bool                  STRING     string
//	BYTES     base64 string         TIMESTAMP  "2006-01-02T15:04:05.999999999Z" (UTC)
//	DATE      "2006-01-02"          TIME       "15:04:05.999999"
//	DATETIME  "2006-01-02T15:04:05.999999"
//	NUMERIC   decimal string        JSON, GEOGRAPHY, INTERVAL  string
//	RECORD    map by field name     REPEATED   []any

// canonType maps the names of a type to the one BigQuery reports.
func canonType(t string) (string, bool) {
	switch u := strings.ToUpper(strings.TrimSpace(t)); u {
	case "STRING", "BYTES", "TIMESTAMP", "DATE", "TIME", "DATETIME", "NUMERIC", "BIGNUMERIC", "GEOGRAPHY", "JSON", "INTERVAL":
		return u, true
	case "INTEGER", "INT64":
		return "INTEGER", true
	case "FLOAT", "FLOAT64":
		return "FLOAT", true
	case "BOOLEAN", "BOOL":
		return "BOOLEAN", true
	case "RECORD", "STRUCT":
		return "RECORD", true
	}
	return "", false
}

// checkSchema canonicalizes and validates fields in place.
func checkSchema(fs []*Field) error {
	seen := map[string]bool{}
	for _, f := range fs {
		if !fieldRE.MatchString(f.Name) {
			return invalid("Invalid field name %q. Fields must contain the allowed characters, and be at most 300 characters long.", f.Name)
		}
		if seen[lower(f.Name)] {
			return invalid("Duplicate field name %q", f.Name)
		}
		seen[lower(f.Name)] = true
		t, ok := canonType(f.Type)
		if !ok {
			return invalid("Invalid field type %q for field %s", f.Type, f.Name)
		}
		f.Type = t
		switch f.Mode = strings.ToUpper(f.Mode); f.Mode {
		case "":
			f.Mode = "NULLABLE"
		case "NULLABLE", "REQUIRED", "REPEATED":
		default:
			return invalid("Invalid field mode %q for field %s", f.Mode, f.Name)
		}
		if t == "RECORD" {
			if len(f.Fields) == 0 {
				return invalid("Field %s is a RECORD with no fields", f.Name)
			}
			if err := checkSchema(f.Fields); err != nil {
				return err
			}
		} else if len(f.Fields) > 0 {
			return invalid("Field %s has sub-fields but is not a RECORD", f.Name)
		}
	}
	return nil
}

func (f *Field) repeated() bool { return f.Mode == "REPEATED" }

// norm turns a JSON value (a streaming insert, a CSV cell, a DuckDB result)
// into the canonical value of a field. ignoreUnknown drops keys of records
// that the schema lacks.
func norm(f *Field, v any, ignoreUnknown bool) (any, error) {
	if v == nil {
		if f.repeated() {
			return []any{}, nil
		}
		return nil, nil
	}
	if f.repeated() {
		arr, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("repeated field %s needs an array", f.Name)
		}
		out := make([]any, len(arr))
		for i, e := range arr {
			x, err := scalar(f, e, ignoreUnknown)
			if err != nil {
				return nil, err
			}
			if x == nil {
				return nil, fmt.Errorf("repeated field %s has a NULL element", f.Name)
			}
			out[i] = x
		}
		return out, nil
	}
	return scalar(f, v, ignoreUnknown)
}

var decimalRE = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$`)

func scalar(f *Field, v any, ignoreUnknown bool) (any, error) {
	if v == nil {
		return nil, nil
	}
	bad := func() (any, error) {
		return nil, fmt.Errorf("cannot convert %v to %s for field %s", v, f.Type, f.Name)
	}
	text := func() (string, bool) {
		switch x := v.(type) {
		case string:
			return x, true
		case json.Number:
			return x.String(), true
		case float64:
			return strconv.FormatFloat(x, 'f', -1, 64), true
		case bool:
			return strconv.FormatBool(x), true
		case int:
			return strconv.Itoa(x), true
		case int64:
			return strconv.FormatInt(x, 10), true
		}
		return "", false
	}
	switch f.Type {
	case "RECORD":
		m, ok := v.(map[string]any)
		if !ok {
			return bad()
		}
		out := map[string]any{}
		for k := range m {
			known := false
			for _, sub := range f.Fields {
				known = known || sub.Name == k
			}
			if !known && !ignoreUnknown {
				return nil, fmt.Errorf("no such field: %s", k)
			}
		}
		for _, sub := range f.Fields {
			x, err := norm(sub, m[sub.Name], ignoreUnknown)
			if err != nil {
				return nil, err
			}
			if x == nil && sub.Mode == "REQUIRED" {
				return nil, fmt.Errorf("missing required field: %s", sub.Name)
			}
			if x != nil {
				out[sub.Name] = x
			}
		}
		return out, nil
	case "INTEGER":
		s, ok := text()
		if !ok {
			return bad()
		}
		if n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
			return strconv.FormatInt(n, 10), nil
		}
		if fl, err := strconv.ParseFloat(s, 64); err == nil && fl == math.Trunc(fl) && math.Abs(fl) < 1<<63 {
			return strconv.FormatInt(int64(fl), 10), nil
		}
		if r, ok := new(big.Int).SetString(strings.TrimSpace(s), 10); ok { // beyond INT64, as from HUGEINT
			return r.String(), nil
		}
		return bad()
	case "FLOAT":
		if s, ok := v.(string); ok {
			switch strings.ToLower(s) {
			case "nan":
				return "NaN", nil
			case "inf", "+inf", "infinity", "+infinity":
				return "Infinity", nil
			case "-inf", "-infinity":
				return "-Infinity", nil
			}
		}
		s, ok := text()
		if !ok {
			return bad()
		}
		fl, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			return bad()
		}
		switch {
		case math.IsNaN(fl):
			return "NaN", nil
		case math.IsInf(fl, 1):
			return "Infinity", nil
		case math.IsInf(fl, -1):
			return "-Infinity", nil
		}
		return fl, nil
	case "BOOLEAN":
		switch x := v.(type) {
		case bool:
			return x, nil
		case string:
			if b, err := strconv.ParseBool(strings.ToLower(x)); err == nil {
				return b, nil
			}
		}
		return bad()
	case "STRING", "GEOGRAPHY", "INTERVAL":
		s, ok := text()
		if !ok {
			return bad()
		}
		return s, nil
	case "JSON":
		if s, ok := v.(string); ok {
			return s, nil
		}
		b, err := json.Marshal(v)
		return string(b), err
	case "BYTES":
		s, ok := v.(string)
		if !ok {
			return bad()
		}
		if _, err := base64.StdEncoding.DecodeString(s); err != nil {
			return nil, fmt.Errorf("invalid base64 for field %s", f.Name)
		}
		return s, nil
	case "TIMESTAMP":
		t, err := parseTimestamp(v)
		if err != nil {
			return bad()
		}
		return t.UTC().Format("2006-01-02T15:04:05.999999999Z"), nil
	case "DATE":
		s, _ := v.(string)
		t, err := time.Parse("2006-1-2", strings.TrimSpace(s))
		if err != nil {
			return bad()
		}
		return t.Format("2006-01-02"), nil
	case "TIME":
		s, _ := v.(string)
		for _, layout := range []string{"15:04:05.999999999", "15:04:05", "15:04"} {
			if t, err := time.Parse(layout, strings.TrimSpace(s)); err == nil {
				return t.Format("15:04:05.999999"), nil
			}
		}
		return bad()
	case "DATETIME":
		s, _ := v.(string)
		s = strings.Replace(strings.TrimSpace(s), " ", "T", 1)
		for _, layout := range []string{"2006-1-2T15:04:05.999999999", "2006-1-2T15:04", "2006-1-2"} {
			if t, err := time.Parse(layout, s); err == nil {
				return t.Format("2006-01-02T15:04:05.999999"), nil
			}
		}
		return bad()
	case "NUMERIC", "BIGNUMERIC":
		s, ok := text()
		if !ok || !decimalRE.MatchString(strings.TrimSpace(s)) {
			return bad()
		}
		r, ok := new(big.Rat).SetString(strings.TrimSpace(s))
		if !ok {
			return bad()
		}
		out := r.FloatString(9)
		if f.Type == "BIGNUMERIC" {
			out = r.FloatString(38)
		}
		if strings.Contains(out, ".") {
			out = strings.TrimRight(strings.TrimRight(out, "0"), ".")
		}
		if out == "-0" || out == "" {
			out = "0"
		}
		return out, nil
	}
	return bad()
}

// parseTimestamp reads the forms of TIMESTAMP BigQuery accepts: epoch
// seconds, and date-times with or without a zone.
func parseTimestamp(v any) (time.Time, error) {
	var s string
	switch x := v.(type) {
	case json.Number:
		s = x.String()
	case float64:
		s = strconv.FormatFloat(x, 'f', -1, 64)
	case int64:
		s = strconv.FormatInt(x, 10)
	case string:
		s = strings.TrimSpace(x)
	default:
		return time.Time{}, fmt.Errorf("not a timestamp")
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil && !strings.ContainsAny(s, "-:T ") || err == nil && strings.HasPrefix(s, "-") && !strings.ContainsAny(s[1:], "-:T ") {
		sec := math.Floor(f)
		return time.Unix(int64(sec), int64(math.Round((f-sec)*1e6))*1000).UTC(), nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-1-2 15:04:05.999999999 -07:00", "2006-1-2 15:04:05.999999999 MST", "2006-1-2 15:04:05.999999999Z07:00",
		"2006-1-2T15:04:05.999999999", "2006-1-2 15:04:05.999999999", "2006-1-2 15:04:05.999999999 Z07:00", "2006-1-2"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("not a timestamp")
}

// ---- REST encoding ----

// wire returns the value of a cell in the f/v form of tabledata.list and
// query results: strings for scalars, {"f": [...]} for records.
func wire(f *Field, v any, i64 bool) any {
	if f.repeated() {
		arr, _ := v.([]any)
		out := make([]any, len(arr))
		for i, e := range arr {
			out[i] = map[string]any{"v": wireScalar(f, e, i64)}
		}
		return out
	}
	return wireScalar(f, v, i64)
}

func wireScalar(f *Field, v any, i64 bool) any {
	if v == nil {
		return nil
	}
	switch f.Type {
	case "RECORD":
		m, _ := v.(map[string]any)
		cells := make([]any, len(f.Fields))
		for i, sub := range f.Fields {
			cells[i] = map[string]any{"v": wire(sub, m[sub.Name], i64)}
		}
		return map[string]any{"f": cells}
	case "FLOAT":
		if fl, ok := v.(float64); ok {
			return strconv.FormatFloat(fl, 'g', -1, 64)
		}
		return v
	case "BOOLEAN":
		return strconv.FormatBool(v.(bool))
	case "TIMESTAMP":
		t, _ := time.Parse(time.RFC3339Nano, v.(string))
		if i64 { // formatOptions.useInt64Timestamp: microseconds, as the Go client asks
			return strconv.FormatInt(t.UnixMicro(), 10)
		}
		return strconv.FormatFloat(float64(t.UnixNano())/1e9, 'E', -1, 64)
	}
	return v
}

// wireRow encodes one row of a schema.
func wireRow(fs []*Field, r row, i64 bool) map[string]any {
	cells := make([]any, len(fs))
	for i, f := range fs {
		cells[i] = map[string]any{"v": wire(f, r[f.Name], i64)}
	}
	return map[string]any{"f": cells}
}

// ---- DuckDB ----

// duckType is the DuckDB type of a column. Inside records, BYTES and JSON
// are text (nested values are read from JSON as they are stored).
func duckType(f *Field, nested bool) string {
	var t string
	switch f.Type {
	case "INTEGER":
		t = "BIGINT"
	case "FLOAT":
		t = "DOUBLE"
	case "BOOLEAN":
		t = "BOOLEAN"
	case "TIMESTAMP":
		t = "TIMESTAMPTZ"
	case "DATETIME":
		t = "TIMESTAMP"
	case "DATE", "TIME", "INTERVAL":
		t = f.Type
	case "NUMERIC", "BIGNUMERIC":
		t = "DECIMAL(38,9)" // ponytail: BIGNUMERIC is read as NUMERIC, DuckDB's DECIMAL stops at 38 digits
	case "BYTES":
		t = "BLOB"
		if nested {
			t = "VARCHAR"
		}
	case "JSON":
		t = "JSON"
		if nested {
			t = "VARCHAR"
		}
	case "RECORD":
		parts := make([]string, len(f.Fields))
		for i, sub := range f.Fields {
			parts[i] = ident(sub.Name) + " " + duckType(sub, true)
		}
		t = "STRUCT(" + strings.Join(parts, ", ") + ")"
	default:
		t = "VARCHAR"
	}
	if f.repeated() {
		t += "[]"
	}
	return t
}

func ident(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// forDuck converts a canonical value to what DuckDB's JSON reader takes for
// the field's duckType: integers as numbers, the rest as stored.
func forDuck(f *Field, v any) any {
	if v == nil {
		return nil
	}
	if f.repeated() {
		arr, _ := v.([]any)
		out := make([]any, len(arr))
		for i, e := range arr {
			out[i] = forDuckScalar(f, e)
		}
		return out
	}
	return forDuckScalar(f, v)
}

func forDuckScalar(f *Field, v any) any {
	switch f.Type {
	case "INTEGER":
		return json.Number(v.(string))
	case "RECORD":
		m, _ := v.(map[string]any)
		out := map[string]any{}
		for _, sub := range f.Fields {
			out[sub.Name] = forDuck(sub, m[sub.Name])
		}
		return out
	}
	return v
}

// ---- results from DuckDB ----

// dtype is a parsed DuckDB type name.
type dtype struct {
	base   string // upper-case name, "LIST" or "STRUCT"
	elem   *dtype
	fields []dfield
}

type dfield struct {
	name string
	t    *dtype
}

func parseDType(s string) *dtype {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "]") {
		if i := strings.LastIndex(s, "["); i > 0 {
			return &dtype{base: "LIST", elem: parseDType(s[:i])}
		}
	}
	if u := strings.ToUpper(s); strings.HasPrefix(u, "STRUCT(") && strings.HasSuffix(s, ")") {
		d := &dtype{base: "STRUCT"}
		for _, part := range splitTop(s[len("STRUCT(") : len(s)-1]) {
			part = strings.TrimSpace(part)
			var name, rest string
			if strings.HasPrefix(part, `"`) {
				end := 1
				for end < len(part) {
					if part[end] == '"' {
						if end+1 < len(part) && part[end+1] == '"' {
							end += 2
							continue
						}
						break
					}
					end++
				}
				name, rest = strings.ReplaceAll(part[1:end], `""`, `"`), part[min(end+1, len(part)):]
			} else {
				name, rest, _ = strings.Cut(part, " ")
			}
			d.fields = append(d.fields, dfield{name, parseDType(rest)})
		}
		return d
	}
	return &dtype{base: strings.ToUpper(s)}
}

// splitTop splits at the commas outside parentheses and quotes.
func splitTop(s string) []string {
	var out []string
	depth, start, quoted := 0, 0, false
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '"':
			quoted = !quoted
		case quoted:
		case c == '(' || c == '[':
			depth++
		case c == ')' || c == ']':
			depth--
		case c == ',' && depth == 0:
			out, start = append(out, s[start:i]), i+1
		}
	}
	return append(out, s[start:])
}

// fieldOf is the schema field BigQuery shows for a DuckDB column.
func fieldOf(name string, d *dtype) *Field {
	f := &Field{Name: name, Mode: "NULLABLE"}
	if d.base == "LIST" {
		f = fieldOf(name, d.elem)
		f.Mode = "REPEATED"
		return f
	}
	switch {
	case d.base == "STRUCT":
		f.Type = "RECORD"
		for i, sub := range d.fields {
			f.Fields = append(f.Fields, fieldOf(colName(sub.name, i), sub.t))
		}
	case strings.HasPrefix(d.base, "DECIMAL"):
		f.Type = "NUMERIC"
	case d.base == "TIMESTAMP WITH TIME ZONE":
		f.Type = "TIMESTAMP"
	case strings.HasPrefix(d.base, "TIMESTAMP"):
		f.Type = "DATETIME"
	default:
		f.Type = map[string]string{"BIGINT": "INTEGER", "INTEGER": "INTEGER", "SMALLINT": "INTEGER", "TINYINT": "INTEGER", "HUGEINT": "INTEGER",
			"UBIGINT": "INTEGER", "UINTEGER": "INTEGER", "USMALLINT": "INTEGER", "UTINYINT": "INTEGER", "UHUGEINT": "INTEGER",
			"DOUBLE": "FLOAT", "FLOAT": "FLOAT", "BOOLEAN": "BOOLEAN", "BLOB": "BYTES", "DATE": "DATE", "TIME": "TIME", "JSON": "JSON"}[d.base]
		if f.Type == "" {
			f.Type = "STRING"
		}
	}
	return f
}

var colRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// colName is a result column's name: BigQuery calls the unnamed ones f0_, f1_, ...
func colName(n string, i int) string {
	if colRE.MatchString(n) {
		return n
	}
	return "f" + strconv.Itoa(i) + "_"
}
