package bigquery

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

type loadCfg struct {
	SourceURIs          []string `json:"sourceUris"`
	DestinationTable    tableRef `json:"destinationTable"`
	SourceFormat        string   `json:"sourceFormat"`
	Schema              *schema  `json:"schema"`
	Autodetect          bool     `json:"autodetect"`
	SkipLeadingRows     int      `json:"skipLeadingRows"`
	FieldDelimiter      string   `json:"fieldDelimiter"`
	Quote               *string  `json:"quote"`
	AllowJaggedRows     bool     `json:"allowJaggedRows"`
	MaxBadRecords       int      `json:"maxBadRecords"`
	NullMarker          string   `json:"nullMarker"`
	IgnoreUnknownValues bool     `json:"ignoreUnknownValues"`
	WriteDisposition    string   `json:"writeDisposition"`
	CreateDisposition   string   `json:"createDisposition"`
}

type extractCfg struct {
	SourceTable       tableRef `json:"sourceTable"`
	DestinationURIs   []string `json:"destinationUris"`
	DestinationFormat string   `json:"destinationFormat"`
	FieldDelimiter    string   `json:"fieldDelimiter"`
	PrintHeader       *bool    `json:"printHeader"`
	Compression       string   `json:"compression"`
}

type copyCfg struct {
	SourceTable       *tableRef  `json:"sourceTable"`
	SourceTables      []tableRef `json:"sourceTables"`
	DestinationTable  tableRef   `json:"destinationTable"`
	WriteDisposition  string     `json:"writeDisposition"`
	CreateDisposition string     `json:"createDisposition"`
}

// failed records an error on a job that ran: the job is returned, DONE, with the error in its status.
func failed(j *job, err error) error {
	e, ok := err.(*apiErr)
	if !ok {
		e = &apiErr{500, "backendError", err.Error()}
	}
	j.Err = &jobErr{e.reason, e.msg}
	return nil
}

// ---- Cloud Storage ----

func parseGS(uri string) (bucket, name string, err error) {
	rest, ok := strings.CutPrefix(uri, "gs://")
	if !ok {
		return "", "", invalid("Invalid URI %q: want gs://bucket/object", uri)
	}
	bucket, name, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", invalid("Invalid URI %q: no bucket", uri)
	}
	return bucket, name, nil
}

func (s *Service) gcs(method, path string, body []byte, ctype string) (int, []byte) {
	req, _ := http.NewRequest(method, path, bytes.NewReader(body))
	req.Host = "localhost"
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	return s.hub.DoService("gcs", req)
}

// objects expands a source URI, which may have one * wildcard, to object names.
func (s *Service) objects(uri string) (bucket string, names []string, err error) {
	bucket, name, err := parseGS(uri)
	if err != nil {
		return "", nil, err
	}
	if !strings.Contains(name, "*") {
		return bucket, []string{name}, nil
	}
	prefix, _, _ := strings.Cut(name, "*")
	re := regexp.MustCompile("^" + strings.ReplaceAll(regexp.QuoteMeta(name), `\*`, ".*") + "$")
	for token := ""; ; {
		q := url.Values{"prefix": {prefix}}
		if token != "" {
			q.Set("pageToken", token)
		}
		st, body := s.gcs("GET", "/storage/v1/b/"+url.PathEscape(bucket)+"/o?"+q.Encode(), nil, "")
		if st != 200 {
			return "", nil, notFound("Not found: Bucket %s", bucket)
		}
		var out struct {
			Items         []struct{ Name string }
			NextPageToken string
		}
		json.Unmarshal(body, &out)
		for _, it := range out.Items {
			if re.MatchString(it.Name) {
				names = append(names, it.Name)
			}
		}
		if token = out.NextPageToken; token == "" {
			return bucket, names, nil
		}
	}
}

// maxUnzipped caps one gunzipped load file (4 GiB, BigQuery's limit for a compressed file).
// It is a var so a test can lower it.
var maxUnzipped int64 = 4 << 30

func (s *Service) readObject(bucket, name string) ([]byte, error) {
	st, body := s.gcs("GET", "/storage/v1/b/"+url.PathEscape(bucket)+"/o/"+url.PathEscape(name)+"?alt=media", nil, "")
	if st != 200 {
		return nil, notFound("Not found: URI gs://%s/%s", bucket, name)
	}
	if len(body) > 2 && body[0] == 0x1f && body[1] == 0x8b { // gzip
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err == nil {
			// Read one byte past the cap so a gzip bomb is refused, not unpacked.
			out, err := io.ReadAll(io.LimitReader(zr, maxUnzipped+1))
			if err == nil && int64(len(out)) > maxUnzipped {
				err = invalid("gs://%s/%s is over the 4 GiB limit for a compressed file", bucket, name)
			}
			return out, err
		}
	}
	return body, nil
}

func (s *Service) writeObject(bucket, name string, data []byte, ctype string) error {
	st, body := s.gcs("POST", "/upload/storage/v1/b/"+url.PathEscape(bucket)+"/o?uploadType=media&name="+url.QueryEscape(name), data, ctype)
	if st/100 != 2 {
		return errf(400, "invalid", "Cannot write gs://%s/%s: %s", bucket, name, strings.TrimSpace(string(body)))
	}
	return nil
}

// ---- load ----

func delimiter(d string) rune {
	switch strings.ToLower(d) {
	case "", ",":
		return ','
	case "\\t", "tab", "\t":
		return '\t'
	}
	return []rune(d)[0]
}

func (s *Service) runLoad(r *http.Request, j *job, cfg *loadCfg) error {
	ref := cfg.DestinationTable
	if ref.TableID == "" || ref.DatasetID == "" {
		return invalid("Required parameter is missing: load.destinationTable")
	}
	if ref.ProjectID == "" {
		ref.ProjectID = j.Ref.ProjectID
	}
	if len(cfg.SourceURIs) == 0 {
		return invalid("Required parameter is missing: load.sourceUris")
	}
	format := strings.ToUpper(cfg.SourceFormat)
	if format == "" {
		format = "CSV"
	}
	if !slices.Contains([]string{"CSV", "NEWLINE_DELIMITED_JSON", "PARQUET"}, format) {
		return invalid("pogi cannot load %s files (CSV, NEWLINE_DELIMITED_JSON and PARQUET)", format)
	}
	s.mu.Lock()
	_, err := s.getDS(ref.ProjectID, ref.DatasetID)
	var existing []*Field
	if d := s.st.datasets[ref.ProjectID+"/"+ref.DatasetID]; d != nil && d.tables[ref.TableID] != nil {
		existing = d.tables[ref.TableID].fields()
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	// The files.
	var files [][]byte
	var size int64
	for _, uri := range cfg.SourceURIs {
		bucket, names, err := s.objects(uri)
		if err != nil {
			return failed(j, err)
		}
		if len(names) == 0 {
			return failed(j, notFound("Not found: Uris List of gs://%s: no files match %s", bucket, uri))
		}
		for _, n := range names {
			b, err := s.readObject(bucket, n)
			if err != nil {
				return failed(j, err)
			}
			files, size = append(files, b), size+int64(len(b))
		}
	}
	var fs []*Field
	switch {
	case cfg.Schema != nil:
		fs = cfg.Schema.Fields
		if err := checkSchema(fs); err != nil {
			return err
		}
	case existing != nil && !strings.EqualFold(cfg.WriteDisposition, "WRITE_TRUNCATE"):
		fs = existing
	case existing != nil && !cfg.Autodetect:
		fs = existing
	}
	var rows []row
	bad := 0
	switch format {
	case "CSV":
		rows, fs, bad, err = parseCSV(files, cfg, fs)
	case "NEWLINE_DELIMITED_JSON":
		rows, fs, bad, err = parseJSONLines(files, cfg, fs)
	case "PARQUET":
		rows, fs, err = s.parseParquet(r, files, fs)
	}
	if err != nil {
		return failed(j, err)
	}
	if fs == nil {
		return failed(j, invalid("Schema is required when the destination table does not exist and autodetect is off"))
	}
	if err := s.putRows(ref, fs, rows, cfg.WriteDisposition, cfg.CreateDisposition, true); err != nil {
		return failed(j, err)
	}
	j.Stats = map[string]any{"inputFiles": strconv.Itoa(len(files)), "inputFileBytes": strconv.FormatInt(size, 10), "outputRows": strconv.Itoa(len(rows)),
		"outputBytes": strconv.FormatInt(size, 10), "badRecords": strconv.Itoa(bad)}
	return nil
}

var nonName = regexp.MustCompile(`[^A-Za-z0-9_]`)

// parseCSV reads CSV files with the schema fs (or detects one).
func parseCSV(files [][]byte, cfg *loadCfg, fs []*Field) ([]row, []*Field, int, error) {
	var records [][]string
	var header []string
	for _, f := range files {
		rd := csv.NewReader(bytes.NewReader(f))
		rd.Comma, rd.FieldsPerRecord, rd.LazyQuotes = delimiter(cfg.FieldDelimiter), -1, true
		if cfg.Quote != nil && *cfg.Quote == "" {
			rd.LazyQuotes = true
		}
		all, err := rd.ReadAll()
		if err != nil {
			return nil, nil, 0, errf(400, "invalid", "Error while reading data, error message: CSV processing encountered too many errors, giving up: %v", err)
		}
		if skip := cfg.SkipLeadingRows; skip > 0 && len(all) > 0 {
			if header == nil {
				header = all[0]
			}
			all = all[min(skip, len(all)):]
		}
		records = append(records, all...)
	}
	if fs == nil {
		if !cfg.Autodetect {
			return nil, nil, 0, nil
		}
		width := 0
		for _, rec := range records {
			width = max(width, len(rec))
		}
		for i := 0; i < max(width, len(header)); i++ {
			name := "string_field_" + strconv.Itoa(i)
			if i < len(header) && nonName.ReplaceAllString(header[i], "_") != "" {
				name = nonName.ReplaceAllString(header[i], "_")
				if name[0] >= '0' && name[0] <= '9' {
					name = "_" + name
				}
			}
			var col []string
			for _, rec := range records {
				if i < len(rec) {
					col = append(col, rec[i])
				}
			}
			fs = append(fs, &Field{Name: name, Type: detectType(col), Mode: "NULLABLE"})
		}
	}
	var rows []row
	bad := 0
	for n, rec := range records {
		rw := row{}
		var perr error
		if len(rec) > len(fs) || len(rec) < len(fs) && !cfg.AllowJaggedRows {
			perr = fmt.Errorf("Too many or too few values in row %d: got %d, want %d", n+1, len(rec), len(fs))
		}
		for i, f := range fs {
			if perr != nil || i >= len(rec) || rec[i] == cfg.NullMarker {
				continue
			}
			if f.Type == "RECORD" || f.repeated() {
				perr = fmt.Errorf("CSV files cannot hold the nested or repeated field %s", f.Name)
				break
			}
			v, err := scalar(f, rec[i], false)
			if err != nil {
				perr = err
				break
			}
			rw[f.Name] = v
		}
		for _, f := range fs {
			if perr == nil && f.Mode == "REQUIRED" && rw[f.Name] == nil {
				perr = fmt.Errorf("Required field %s cannot be null", f.Name)
			}
		}
		if perr != nil {
			if bad++; bad > cfg.MaxBadRecords {
				return nil, nil, bad, errf(400, "invalid", "Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: %d; errors: %d; max bad: %d; error percent: 0 (%v)", n+1, bad, cfg.MaxBadRecords, perr)
			}
			continue
		}
		rows = append(rows, rw)
	}
	return rows, fs, bad, nil
}

var (
	intRE   = regexp.MustCompile(`^[+-]?\d+$`)
	floatRE = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$`)
	dateRE  = regexp.MustCompile(`^\d{4}-\d{1,2}-\d{1,2}$`)
	tsRE    = regexp.MustCompile(`^\d{4}-\d{1,2}-\d{1,2}[ T]\d{1,2}:\d{2}(:\d{2}(\.\d+)?)?\s*(Z|UTC|[+-]\d{2}:?\d{2})?$`)
)

// detectType picks the narrowest type that fits every non-empty value.
func detectType(col []string) string {
	kinds := []struct {
		name string
		ok   func(string) bool
	}{
		{"INTEGER", func(v string) bool { _, err := strconv.ParseInt(v, 10, 64); return intRE.MatchString(v) && err == nil }},
		{"FLOAT", floatRE.MatchString},
		{"BOOLEAN", func(v string) bool { v = strings.ToLower(v); return v == "true" || v == "false" }},
		{"DATE", dateRE.MatchString},
		{"TIMESTAMP", tsRE.MatchString},
	}
	for _, k := range kinds {
		fits, any := true, false
		for _, v := range col {
			if v = strings.TrimSpace(v); v != "" {
				any = true
				fits = fits && k.ok(v)
			}
		}
		if fits && any {
			return k.name
		}
	}
	return "STRING"
}

// parseJSONLines reads newline-delimited JSON objects.
func parseJSONLines(files [][]byte, cfg *loadCfg, fs []*Field) ([]row, []*Field, int, error) {
	var objs []map[string]any
	var order []string // the keys in the order they first appear
	for _, f := range files {
		sc := bufio.NewScanner(bytes.NewReader(f))
		sc.Buffer(nil, 64<<20)
		for sc.Scan() {
			if len(bytes.TrimSpace(sc.Bytes())) == 0 {
				continue
			}
			var m map[string]any
			d := json.NewDecoder(bytes.NewReader(sc.Bytes()))
			d.UseNumber()
			if err := d.Decode(&m); err != nil {
				return nil, nil, 0, errf(400, "invalid", "Error while reading data, error message: JSON parsing error in row starting at position %d: %v", len(objs), err)
			}
			objs = append(objs, m)
			for _, k := range firstKeys(sc.Bytes()) {
				if !slices.Contains(order, k) {
					order = append(order, k)
				}
			}
		}
	}
	if fs == nil {
		if !cfg.Autodetect {
			return nil, nil, 0, nil
		}
		fs = detectFields(objs, order)
	}
	rec := &Field{Name: "row", Type: "RECORD", Fields: fs}
	var rows []row
	bad := 0
	for n, o := range objs {
		v, err := scalar(rec, o, cfg.IgnoreUnknownValues)
		if err != nil {
			if bad++; bad > cfg.MaxBadRecords {
				return nil, nil, bad, errf(400, "invalid", "Error while reading data, error message: JSON processing encountered too many errors, giving up. Rows: %d; errors: %d; max bad: %d (%v)", n+1, bad, cfg.MaxBadRecords, err)
			}
			continue
		}
		rows = append(rows, v.(map[string]any))
	}
	return rows, fs, bad, nil
}

// detectFields infers a schema from JSON objects: the types of the values, nested objects as records.
func detectFields(objs []map[string]any, order []string) []*Field {
	vals := map[string][]any{}
	for _, o := range objs {
		for k, v := range o {
			vals[k] = append(vals[k], v)
		}
	}
	names := order
	if names == nil { // ponytail: nested records list their fields sorted, not in the order of the file
		names = core.SortedKeys(vals)
	}
	var fs []*Field
	for _, n := range names {
		f := &Field{Name: nonName.ReplaceAllString(n, "_"), Mode: "NULLABLE"}
		var items []any
		for _, v := range vals[n] {
			if arr, ok := v.([]any); ok {
				f.Mode = "REPEATED"
				items = append(items, arr...)
			} else if v != nil {
				items = append(items, v)
			}
		}
		switch first := firstNonNil(items).(type) {
		case map[string]any:
			f.Type = "RECORD"
			var subs []map[string]any
			for _, it := range items {
				if m, ok := it.(map[string]any); ok {
					subs = append(subs, m)
				}
			}
			f.Fields = detectFields(subs, nil)
		case bool:
			f.Type = "BOOLEAN"
		case json.Number:
			f.Type = "INTEGER"
			for _, it := range items {
				if n, ok := it.(json.Number); ok && !intRE.MatchString(n.String()) {
					f.Type = "FLOAT"
				}
			}
		case string:
			var strs []string
			for _, it := range items {
				if s, ok := it.(string); ok {
					strs = append(strs, s)
				}
			}
			f.Type = "STRING"
			if t := detectType(strs); t == "TIMESTAMP" || t == "DATE" {
				f.Type = t
			}
			_ = first
		default:
			f.Type = "STRING"
		}
		fs = append(fs, f)
	}
	return fs
}

// firstKeys lists the keys of a JSON object in order.
func firstKeys(line []byte) []string {
	d := json.NewDecoder(bytes.NewReader(line))
	var keys []string
	if tok, err := d.Token(); err != nil || tok != json.Delim('{') {
		return nil
	}
	for d.More() {
		k, err := d.Token()
		if err != nil {
			break
		}
		keys = append(keys, k.(string))
		var skip json.RawMessage
		if d.Decode(&skip) != nil {
			break
		}
	}
	return keys
}

func firstNonNil(items []any) any {
	if len(items) > 0 {
		return items[0]
	}
	return nil
}

// parseParquet reads Parquet files with DuckDB.
func (s *Service) parseParquet(r *http.Request, files [][]byte, fs []*Field) ([]row, []*Field, error) {
	data := map[string][]byte{}
	var list []string
	token := "pq" + strconv.FormatInt(time.Now().UnixNano(), 36)
	for i, f := range files {
		name := token + "/" + strconv.Itoa(i) + ".parquet"
		data[name] = f
		list = append(list, literal("/data/"+name))
	}
	req := map[string]any{"setup": []string{"SET TimeZone='UTC'"},
		"stmts": []map[string]any{{"sql": "SELECT * FROM read_parquet([" + strings.Join(list, ", ") + "], union_by_name = true)", "query": true}}}
	out, err := s.engine(r.Context(), req, data)
	if err != nil {
		return nil, nil, err
	}
	res := out.Results[0]
	derived := make([]*Field, len(res.Columns))
	for i, c := range res.Columns {
		derived[i] = fieldOf(colName(c[0], i), parseDType(c[1]))
	}
	var rows []row
	for _, raw := range res.Rows {
		rw := row{}
		for i, f := range derived {
			v, err := norm(f, raw[i], true)
			if err != nil {
				return nil, nil, errf(400, "invalid", "Error while reading data, error message: %v", err)
			}
			if v != nil {
				rw[f.Name] = v
			}
		}
		rows = append(rows, rw)
	}
	if fs == nil {
		fs = derived
	}
	return rows, fs, nil
}

// ---- extract ----

func csvCell(f *Field, v any) string {
	if v == nil {
		return ""
	}
	switch f.Type {
	case "TIMESTAMP":
		t, _ := time.Parse(time.RFC3339Nano, v.(string))
		return t.UTC().Format("2006-01-02 15:04:05.999999") + " UTC"
	case "BOOLEAN":
		return strconv.FormatBool(v.(bool))
	case "FLOAT":
		if fl, ok := v.(float64); ok {
			return strconv.FormatFloat(fl, 'g', -1, 64)
		}
	}
	return fmt.Sprint(v)
}

// jsonCell is a value as BigQuery exports it to JSON.
func jsonCell(f *Field, v any) any {
	if v == nil {
		return nil
	}
	if f.repeated() {
		arr, _ := v.([]any)
		out := make([]any, len(arr))
		for i, e := range arr {
			out[i] = jsonScalar(f, e)
		}
		return out
	}
	return jsonScalar(f, v)
}

func jsonScalar(f *Field, v any) any {
	switch f.Type {
	case "INTEGER":
		return json.Number(v.(string))
	case "TIMESTAMP":
		t, _ := time.Parse(time.RFC3339Nano, v.(string))
		return t.UTC().Format("2006-01-02T15:04:05.999999") + "Z"
	case "RECORD":
		m, _ := v.(map[string]any)
		out := map[string]any{}
		for _, sub := range f.Fields {
			out[sub.Name] = jsonCell(sub, m[sub.Name])
		}
		return out
	}
	return v
}

func (s *Service) runExtract(j *job, cfg *extractCfg) error {
	ref := cfg.SourceTable
	if ref.ProjectID == "" {
		ref.ProjectID = j.Ref.ProjectID
	}
	if len(cfg.DestinationURIs) == 0 {
		return invalid("Required parameter is missing: extract.destinationUris")
	}
	format := strings.ToUpper(cfg.DestinationFormat)
	if format == "" {
		format = "CSV"
	}
	if format != "CSV" && format != "NEWLINE_DELIMITED_JSON" {
		return invalid("pogi can extract CSV and NEWLINE_DELIMITED_JSON, not %s", format)
	}
	s.mu.Lock()
	_, t, err := s.getTbl(ref.ProjectID, ref.DatasetID, ref.TableID)
	var fs []*Field
	var rows []row
	if err == nil {
		if t.Type == "VIEW" {
			err = invalid("Operation cannot be performed on a view: %s", ref)
		}
		fs, rows = t.fields(), t.Rows[:len(t.Rows):len(t.Rows)]
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	var b bytes.Buffer
	if format == "CSV" {
		w := csv.NewWriter(&b)
		w.Comma = delimiter(cfg.FieldDelimiter)
		for _, f := range fs {
			if f.Type == "RECORD" || f.repeated() {
				return invalid("Operation cannot be performed on a nested schema. Field: %s", f.Name)
			}
		}
		if cfg.PrintHeader == nil || *cfg.PrintHeader {
			names := make([]string, len(fs))
			for i, f := range fs {
				names[i] = f.Name
			}
			w.Write(names)
		}
		for _, rw := range rows {
			rec := make([]string, len(fs))
			for i, f := range fs {
				rec[i] = csvCell(f, rw[f.Name])
			}
			w.Write(rec)
		}
		w.Flush()
	} else {
		for _, rw := range rows {
			m := map[string]any{}
			for _, f := range fs {
				m[f.Name] = jsonCell(f, rw[f.Name])
			}
			line, _ := json.Marshal(m)
			b.Write(line)
			b.WriteByte('\n')
		}
	}
	data, ctype := b.Bytes(), map[string]string{"CSV": "text/csv", "NEWLINE_DELIMITED_JSON": "application/json"}[format]
	if strings.EqualFold(cfg.Compression, "GZIP") {
		var z bytes.Buffer
		zw := gzip.NewWriter(&z)
		zw.Write(data)
		zw.Close()
		data, ctype = z.Bytes(), "application/gzip"
	}
	var counts []string
	for _, uri := range cfg.DestinationURIs {
		bucket, name, err := parseGS(uri)
		if err != nil {
			return err
		}
		name = strings.Replace(name, "*", "000000000000", 1)
		if err := s.writeObject(bucket, name, data, ctype); err != nil {
			return failed(j, err)
		}
		counts = append(counts, "1")
	}
	j.Stats = map[string]any{"destinationUriFileCounts": counts, "inputBytes": strconv.Itoa(len(data))}
	return nil
}

// ---- copy ----

func (s *Service) runCopy(j *job, cfg *copyCfg) error {
	srcs := cfg.SourceTables
	if cfg.SourceTable != nil {
		srcs = append(srcs, *cfg.SourceTable)
	}
	if len(srcs) == 0 {
		return invalid("Required parameter is missing: copy.sourceTable")
	}
	dst := cfg.DestinationTable
	if dst.ProjectID == "" {
		dst.ProjectID = j.Ref.ProjectID
	}
	var fs []*Field
	var rows []row
	s.mu.Lock()
	for i, r := range srcs {
		if r.ProjectID == "" {
			r.ProjectID = j.Ref.ProjectID
		}
		_, t, err := s.getTbl(r.ProjectID, r.DatasetID, r.TableID)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		if t.Type != "TABLE" {
			s.mu.Unlock()
			return invalid("Cannot copy %s %s", strings.ToLower(t.Type), r)
		}
		if i == 0 {
			fs = cloneFields(t.fields())
		}
		for _, rw := range t.Rows {
			rows = append(rows, cloneRow(rw))
		}
	}
	s.mu.Unlock()
	if err := s.putRows(dst, fs, rows, cfg.WriteDisposition, cfg.CreateDisposition, true); err != nil {
		return failed(j, err)
	}
	return nil
}

func cloneFields(fs []*Field) []*Field {
	out := make([]*Field, len(fs))
	for i, f := range fs {
		c := *f
		c.Fields = cloneFields(f.Fields)
		out[i] = &c
	}
	return out
}

func cloneRow(r row) row {
	b, _ := json.Marshal(r)
	var out row
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	d.Decode(&out)
	// numbers come back as json.Number: canonical floats are float64
	return fixNumbers(out).(row)
}

func fixNumbers(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = fixNumbers(e)
		}
		return x
	case []any:
		for i, e := range x {
			x[i] = fixNumbers(e)
		}
		return x
	case json.Number:
		f, _ := x.Float64()
		return f
	}
	return v
}
