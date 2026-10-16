package glue

import (
	"bytes"
	"cmp"
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Hive formats and SerDes of the tables crawlers write.
const (
	textIn       = "org.apache.hadoop.mapred.TextInputFormat"
	textOut      = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
	parquetIn    = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
	parquetOut   = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
	lazySimple   = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
	jsonSerde    = "org.openx.data.jsonserde.JsonSerDe"
	parquetSerde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
)

func running(name string) error {
	return core.Errorf(400, "CrawlerRunningException", "Crawler with name %s has already started", name)
}

// num reads a stored number: an int, or a float64 after a reload.
func num(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	}
	return 0
}

// schedule turns the cron expression a crawler is given into the object Glue shows.
func schedule(cr obj) {
	if e, ok := cr["Schedule"].(string); ok {
		delete(cr, "Schedule")
		if e != "" {
			cr["Schedule"] = obj{"ScheduleExpression": e, "State": "SCHEDULED"}
		}
	}
}

func (s *Service) createCrawler(c *core.Ctx, in *obj) (any, error) {
	cr := maps.Clone(*in)
	tags, _ := cr["Tags"].(obj)
	delete(cr, "Tags")
	if str(cr["Role"]) == "" || cr["Targets"] == nil {
		return nil, invalid("Role and Targets are required.")
	}
	at := now()
	cr["State"], cr["CreationTime"], cr["LastUpdated"], cr["Version"], cr["CrawlElapsedTime"] = "READY", at, at, 1, 0
	cr["SchemaChangePolicy"] = cmp.Or(cr["SchemaChangePolicy"], any(obj{"UpdateBehavior": "UPDATE_IN_DATABASE", "DeleteBehavior": "DEPRECATE_IN_DATABASE"}))
	cr["RecrawlPolicy"] = cmp.Or(cr["RecrawlPolicy"], any(obj{"RecrawlBehavior": "CRAWL_EVERYTHING"}))
	schedule(cr)
	return nil, s.put(c, "Crawler", str(cr["Name"]), cr, tagMap(tags))
}

func (s *Service) updateCrawler(c *core.Ctx, in *obj) (any, error) {
	_, err := s.change(c, "Crawler", str((*in)["Name"]), func(cr obj) error {
		if cr["State"] != "READY" {
			return running(str(cr["Name"]))
		}
		maps.Copy(cr, *in)
		schedule(cr)
		cr["Version"], cr["LastUpdated"] = num(cr["Version"])+1, now()
		return nil
	})
	return nil, err
}

func (s *Service) deleteCrawler(c *core.Ctx, in *struct{ Name string }) (any, error) {
	cr, err := s.get(c, "Crawler", in.Name)
	if err == nil && cr["State"] != "READY" {
		err = running(in.Name)
	}
	if err != nil {
		return nil, err
	}
	s.drop(c, "Metrics", in.Name)
	return nil, s.drop(c, "Crawler", in.Name)
}

// startCrawler crawls in the background: READY → RUNNING → READY.
func (s *Service) startCrawler(c *core.Ctx, in *struct{ Name string }) (any, error) {
	ctx, stop := context.WithCancel(s.ctx)
	cr, err := s.change(c, "Crawler", in.Name, func(cr obj) error {
		if cr["State"] != "READY" {
			return running(in.Name)
		}
		cr["State"] = "RUNNING"
		s.stops[key(c, in.Name)] = stop
		return nil
	})
	if err != nil {
		stop()
		return nil, err
	}
	s.wg.Add(1)
	core.Go("glue.crawl", func() { s.crawl(ctx, &core.Ctx{Account: c.Account, Region: c.Region, Principal: c.Principal}, cr) })
	return nil, nil
}

func (s *Service) stopCrawler(c *core.Ctx, in *struct{ Name string }) (any, error) {
	_, err := s.change(c, "Crawler", in.Name, func(cr obj) error {
		if cr["State"] != "RUNNING" {
			return core.Errorf(400, "CrawlerNotRunningException", "Crawler with name %s isn't running.", in.Name)
		}
		cr["State"] = "STOPPING"
		s.stops[key(c, in.Name)]()
		return nil
	})
	return nil, err
}

func (s *Service) getCrawlerMetrics(c *core.Ctx, in *struct {
	CrawlerNameList []string
	pageInput
}) (any, error) {
	out := []obj{}
	for _, cr := range s.all(c, "Crawler", "") {
		name := str(cr["Name"])
		if len(in.CrawlerNameList) > 0 && !slices.Contains(in.CrawlerNameList, name) {
			continue
		}
		m, err := s.get(c, "Metrics", name)
		if err != nil {
			m = obj{"CrawlerName": name, "TablesCreated": 0, "TablesUpdated": 0, "TablesDeleted": 0, "StillEstimating": false}
		}
		out = append(out, m)
	}
	items, next := page(out, in.NextToken, in.MaxResults)
	return listOut("CrawlerMetricsList", items, next), nil
}

// crawl runs one crawl and records its outcome.
func (s *Service) crawl(ctx context.Context, c *core.Ctx, cr obj) {
	defer s.wg.Done()
	name, start := str(cr["Name"]), time.Now()
	created, updated, err := s.infer(ctx, c, cr)
	last := obj{"Status": "SUCCEEDED", "StartTime": core.Time(start.UTC()), "LogGroup": "/aws-glue/crawlers", "LogStream": name, "MessagePrefix": core.UUID()}
	switch {
	case ctx.Err() != nil:
		last["Status"] = "CANCELLED"
	case err != nil:
		last["Status"], last["ErrorMessage"] = "FAILED", err.Error()
	}
	took := time.Since(start)
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c, name)
	if stop := s.stops[k]; stop != nil {
		stop()
	}
	delete(s.stops, k)
	o := s.st.Objects["Crawler/"+k]
	if o == nil { // reset meanwhile
		return
	}
	o = maps.Clone(o)
	o["State"], o["LastCrawl"], o["CrawlElapsedTime"] = "READY", last, 0
	s.st.Objects["Crawler/"+k] = o
	s.st.Objects["Metrics/"+k] = obj{"CrawlerName": name, "LastRuntimeSeconds": took.Seconds(), "MedianRuntimeSeconds": took.Seconds(),
		"TablesCreated": created, "TablesUpdated": updated, "TablesDeleted": 0, "StillEstimating": false, "TimeLeftSeconds": 0}
}

// crawled is a set of files that becomes one table.
type crawled struct {
	name string   // table name before the crawler's prefix
	root string   // the table's prefix, "" or ending in /
	keys []string // its objects
}

// infer crawls a crawler's S3 targets into its database: one table per data
// set, one partition per folder under it.
// ponytail: S3 targets only, without Exclusions or SampleSize; compressed
// files are not sniffed.
func (s *Service) infer(ctx context.Context, c *core.Ctx, cr obj) (created, updated int, err error) {
	var def struct {
		Name, DatabaseName, TablePrefix string
		Targets                         struct{ S3Targets []struct{ Path string } }
		SchemaChangePolicy              struct{ UpdateBehavior string }
	}
	b, _ := json.Marshal(cr)
	json.Unmarshal(b, &def)
	db := strings.ToLower(def.DatabaseName)
	if _, err := s.createDatabase(c, &dbInput{DatabaseInput: obj{"Name": db}}); err != nil && code(err) != "AlreadyExistsException" {
		return 0, 0, err
	}
	for _, target := range def.Targets.S3Targets {
		bucket, prefix := splitS3(target.Path)
		keys, err := s.objects(c, bucket, prefix)
		if err != nil {
			return created, updated, err
		}
		for _, set := range group(bucket, prefix, keys) {
			if ctx.Err() != nil {
				return created, updated, ctx.Err()
			}
			tbl, parts, err := s.describe(c, bucket, set)
			if err != nil {
				return created, updated, err
			}
			name := tableName(def.TablePrefix + set.name)
			tbl["Name"] = name
			tbl["Parameters"].(obj)["UPDATED_BY_CRAWLER"] = def.Name
			in := &tableInput{DatabaseName: db, TableInput: jsonObj(tbl)}
			_, err = s.createTable(c, in)
			switch {
			case err == nil:
				created++
			case code(err) != "AlreadyExistsException":
				return created, updated, err
			case def.SchemaChangePolicy.UpdateBehavior != "LOG" && s.changed(c, db, name, in.TableInput):
				if _, err := s.updateTable(c, in); err != nil {
					return created, updated, err
				}
				updated++
			}
			for _, p := range parts {
				_, err := s.createPartition(c, &partInput{DatabaseName: db, TableName: name, PartitionInput: jsonObj(p)})
				if err != nil && code(err) != "AlreadyExistsException" {
					return created, updated, err
				}
			}
		}
	}
	return created, updated, nil
}

// changed reports whether a crawled table's columns or partition keys differ from the stored ones.
func (s *Service) changed(c *core.Ctx, db, name string, n obj) bool {
	schema := func(t obj) string {
		sd, _ := t["StorageDescriptor"].(obj)
		b, _ := json.Marshal([]any{sd["Columns"], t["PartitionKeys"]})
		return string(b)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, db, name)
	return err == nil && schema(t.cur()) != schema(n)
}

// group splits the files under a crawler path into tables: one at the path
// when files sit there or its folders are all key=value partitions, else
// one per folder.
func group(bucket, prefix string, keys []string) []crawled {
	single := true
	for _, k := range keys {
		if d, _, ok := strings.Cut(strings.TrimPrefix(k, prefix), "/"); ok && !strings.Contains(d, "=") {
			single = false
		} else if !ok {
			single = true
			break
		}
	}
	if single {
		name := path.Base(strings.TrimSuffix(prefix, "/"))
		if prefix == "" {
			name = bucket
		}
		if len(keys) == 0 {
			return nil
		}
		return []crawled{{name: name, root: prefix, keys: keys}}
	}
	var out []crawled
	for _, k := range keys {
		d, _, _ := strings.Cut(strings.TrimPrefix(k, prefix), "/")
		if n := len(out); n > 0 && out[n-1].name == d {
			out[n-1].keys = append(out[n-1].keys, k)
		} else {
			out = append(out, crawled{name: d, root: prefix + d + "/", keys: []string{k}})
		}
	}
	return out
}

// tableName makes a Hive table name: lower case, _ for anything else than letters and digits.
func tableName(s string) string {
	return strings.Map(func(r rune) rune {
		if 'a' <= r && r <= 'z' || '0' <= r && r <= '9' || r == '_' {
			return r
		}
		return '_'
	}, strings.ToLower(s))
}

// describe infers a table from the first of its files: the format, the
// columns, and partition keys from the folders under the table (key=value
// names, else partition_0, partition_1…).
func (s *Service) describe(c *core.Ctx, bucket string, set crawled) (obj, []obj, error) {
	sample := set.keys[0]
	dirs := strings.Split(strings.TrimPrefix(sample, set.root), "/")
	dirs = dirs[:len(dirs)-1]
	keys := []obj{}
	for i, d := range dirs {
		k, _, ok := strings.Cut(d, "=")
		if !ok {
			k = "partition_" + strconv.Itoa(i)
		}
		keys = append(keys, obj{"Name": strings.ToLower(k), "Type": "string"})
	}
	data, err := s.head(c, bucket, sample)
	if err != nil {
		return nil, nil, err
	}
	format, loc := sniff(sample, data), "s3://"+bucket+"/"+set.root
	params := obj{"classification": format, "typeOfData": "file", "compressionType": "none", "objectCount": strconv.Itoa(len(set.keys))}
	sd := obj{"Location": loc, "InputFormat": textIn, "OutputFormat": textOut, "Compressed": false, "NumberOfBuckets": -1,
		"StoredAsSubDirectories": false, "Columns": []obj{}, "Parameters": params}
	switch format {
	case "csv":
		cols, delim := csvSchema(data)
		sd["Columns"], sd["SerdeInfo"] = cols, obj{"SerializationLibrary": lazySimple, "Parameters": obj{"field.delim": delim}}
		params["delimiter"], params["skip.header.line.count"] = delim, "1"
	case "json":
		cols := jsonSchema(data)
		var names []string
		for _, c := range cols {
			names = append(names, str(c["Name"]))
		}
		sd["Columns"], sd["SerdeInfo"] = cols, obj{"SerializationLibrary": jsonSerde, "Parameters": obj{"paths": strings.Join(names, ",")}}
	default:
		// ponytail: Parquet footers are not read, so the table has no
		// columns; Athena still sees every column of the files.
		sd["InputFormat"], sd["OutputFormat"] = parquetIn, parquetOut
		sd["SerdeInfo"] = obj{"SerializationLibrary": parquetSerde, "Parameters": obj{"serialization.format": "1"}}
	}
	seen := map[string]bool{}
	var parts []obj
	for _, k := range set.keys {
		p := strings.Split(strings.TrimPrefix(k, set.root), "/")
		chain := strings.Join(p[:len(p)-1], "/")
		if len(dirs) == 0 || len(p) != len(dirs)+1 || seen[chain] {
			continue
		}
		seen[chain] = true
		var values []string
		for _, d := range p[:len(dirs)] {
			if _, v, ok := strings.Cut(d, "="); ok {
				d = v
				if u, err := url.PathUnescape(v); err == nil {
					d = u
				}
			}
			values = append(values, d)
		}
		psd := maps.Clone(sd)
		psd["Location"] = loc + chain + "/"
		parts = append(parts, obj{"Values": values, "StorageDescriptor": psd})
	}
	return obj{"TableType": "EXTERNAL_TABLE", "Parameters": params, "PartitionKeys": keys, "StorageDescriptor": sd, "Retention": 0}, parts, nil
}

// sniff guesses a file's format from its name, else from its first bytes.
func sniff(key string, data []byte) string {
	switch path.Ext(strings.ToLower(key)) {
	case ".parquet":
		return "parquet"
	case ".json", ".jsonl", ".ndjson":
		return "json"
	case ".csv", ".tsv", ".txt":
		return "csv"
	}
	if bytes.HasPrefix(data, []byte("PAR1")) {
		return "parquet"
	}
	if b := bytes.TrimSpace(data); len(b) > 0 && b[0] == '{' {
		return "json"
	}
	return "csv"
}

// csvSchema takes the column names from a CSV header and their types from
// the rows after it; the delimiter is the commonest of , tab | ; in the header.
func csvSchema(data []byte) ([]obj, string) {
	header, _, _ := bytes.Cut(data, []byte("\n"))
	delim, best := ",", 0
	for _, d := range []string{",", "\t", "|", ";"} {
		if n := bytes.Count(header, []byte(d)); n > best {
			delim, best = d, n
		}
	}
	r := csv.NewReader(bytes.NewReader(data))
	r.Comma, r.FieldsPerRecord, r.LazyQuotes = rune(delim[0]), -1, true
	var recs [][]string
	for len(recs) < 101 {
		rec, err := r.Read()
		if err == io.EOF {
			break
		} else if err == nil {
			recs = append(recs, rec)
		}
	}
	cols := []obj{}
	if len(recs) == 0 {
		return cols, delim
	}
	for i, name := range recs[0] {
		t := ""
		for _, rec := range recs[1:] {
			if i < len(rec) && rec[i] != "" {
				t = widen(t, valueType(rec[i]))
			}
		}
		cols = append(cols, obj{"Name": tableName(strings.TrimSpace(name)), "Type": cmp.Or(t, "string")})
	}
	return cols, delim
}

func valueType(v string) string {
	if _, err := strconv.ParseInt(v, 10, 64); err == nil {
		return "bigint"
	}
	if _, err := strconv.ParseFloat(v, 64); err == nil {
		return "double"
	}
	if strings.EqualFold(v, "true") || strings.EqualFold(v, "false") {
		return "boolean"
	}
	return "string"
}

// widen merges two inferred types ("" is unknown): bigint and double make
// double, any other mix makes string.
func widen(a, b string) string {
	switch {
	case a == "" || a == b:
		return b
	case b == "":
		return a
	case a+b == "bigintdouble" || a+b == "doublebigint":
		return "double"
	}
	return "string"
}

// jsonSchema takes the keys of the first JSON lines as columns, in the
// order they first appear.
func jsonSchema(data []byte) []obj {
	var names []string
	types := map[string]string{}
	d := json.NewDecoder(bytes.NewReader(data))
	for n := 0; n < 100 && d.More(); n++ {
		var raw json.RawMessage
		if d.Decode(&raw) != nil {
			break
		}
		for _, f := range fields(raw) {
			if _, ok := types[f.k]; !ok {
				names = append(names, f.k)
			}
			types[f.k] = widen(types[f.k], jsonType(f.v))
		}
	}
	cols := []obj{}
	for _, n := range names {
		cols = append(cols, obj{"Name": n, "Type": cmp.Or(types[n], "string")})
	}
	return cols
}

type field struct {
	k string
	v json.RawMessage
}

// fields returns the members of a JSON object in document order.
func fields(b []byte) []field {
	d := json.NewDecoder(bytes.NewReader(b))
	if t, err := d.Token(); err != nil || t != json.Delim('{') {
		return nil
	}
	var out []field
	for d.More() {
		k, err := d.Token()
		var v json.RawMessage
		if err != nil || d.Decode(&v) != nil {
			break
		}
		out = append(out, field{fmt.Sprint(k), v})
	}
	return out
}

// jsonType is the Hive type of a JSON value; "" for null.
func jsonType(v json.RawMessage) string {
	b := bytes.TrimSpace(v)
	switch {
	case len(b) == 0 || b[0] == 'n':
		return ""
	case b[0] == '"':
		return "string"
	case b[0] == 't' || b[0] == 'f':
		return "boolean"
	case b[0] == '{':
		var fs []string
		for _, f := range fields(b) {
			fs = append(fs, f.k+":"+cmp.Or(jsonType(f.v), "string"))
		}
		return "struct<" + strings.Join(fs, ",") + ">"
	case b[0] == '[':
		var items []json.RawMessage
		json.Unmarshal(b, &items)
		t := ""
		for _, it := range items {
			t = widen(t, jsonType(it))
		}
		return "array<" + cmp.Or(t, "string") + ">"
	case bytes.ContainsAny(b, ".eE"):
		return "double"
	}
	return "bigint"
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
// and the hidden files Athena skips too (names starting with _ or .).
func (s *Service) objects(c *core.Ctx, bucket, prefix string) ([]string, error) {
	var keys []string
	for token := ""; ; {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		st, _, body := s.hub.Do(c.Account, c.Region, "s3", http.MethodGet, "/"+bucket+"?"+q.Encode(), nil, nil)
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

// head reads the first MiB of an object, enough to infer its schema, cut
// at the last whole line.
func (s *Service) head(c *core.Ctx, bucket, key string) ([]byte, error) {
	const n = 1 << 20
	st, _, body := s.hub.Do(c.Account, c.Region, "s3", http.MethodGet, (&url.URL{Path: "/" + bucket + "/" + key}).EscapedPath(),
		http.Header{"Range": {"bytes=0-" + strconv.Itoa(n-1)}}, nil)
	switch {
	case st == http.StatusRequestedRangeNotSatisfiable: // empty
		return nil, nil
	case st != http.StatusOK && st != http.StatusPartialContent:
		return nil, fmt.Errorf("cannot read s3://%s/%s (HTTP %d)", bucket, key, st)
	case len(body) == n:
		if i := bytes.LastIndexByte(body, '\n'); i > 0 {
			body = body[:i+1]
		}
	}
	return body, nil
}
