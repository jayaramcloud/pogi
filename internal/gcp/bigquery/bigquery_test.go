package bigquery_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/nrndev/pogi/internal/core"
	bqsvc "github.com/nrndev/pogi/internal/gcp/bigquery"
	gcssvc "github.com/nrndev/pogi/internal/gcp/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type env struct {
	svc    *bqsvc.Service
	hub    *core.Hub
	url    string
	client *bigquery.Client
}

func setup(t *testing.T) *env {
	t.Helper()
	e := &env{}
	e.hub, e.url = core.ServeTest(t, func(h *core.Hub) core.Service { e.svc = bqsvc.New(h); return e.svc },
		func(h *core.Hub) core.Service { return gcssvc.New(h) })
	e.client = must(bigquery.NewClient(ctx, "proj", option.WithEndpoint(e.url+"/bigquery/v2/"), option.WithoutAuthentication()))
	t.Cleanup(func() { e.client.Close() })
	return e
}

// needDocker skips a test that runs queries without a container engine.
func (e *env) needDocker(t *testing.T) {
	t.Helper()
	if !e.hub.Docker().Available() {
		t.Skip("queries run on DuckDB in a container: no container engine")
	}
}

func apiCode(err error) int {
	var ge *googleapi.Error
	if errors.As(err, &ge) {
		return ge.Code
	}
	return 0
}

type address struct {
	City string
	Zip  int64
}

type event struct {
	ID     int64
	User   string
	Tags   []string
	Addr   address
	TS     time.Time
	Amount *big.Rat
	OK     bool
	Day    civil.Date
	Note   bigquery.NullString
}

var eventSchema = bigquery.Schema{
	{Name: "ID", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "User", Type: bigquery.StringFieldType},
	{Name: "Tags", Type: bigquery.StringFieldType, Repeated: true},
	{Name: "Addr", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
		{Name: "City", Type: bigquery.StringFieldType}, {Name: "Zip", Type: bigquery.IntegerFieldType}}},
	{Name: "TS", Type: bigquery.TimestampFieldType},
	{Name: "Amount", Type: bigquery.NumericFieldType},
	{Name: "OK", Type: bigquery.BooleanFieldType},
	{Name: "Day", Type: bigquery.DateFieldType},
	{Name: "Note", Type: bigquery.StringFieldType},
}

var t0 = time.Date(2026, 3, 1, 10, 0, 0, 123000000, time.UTC)

func events() []*event {
	return []*event{
		{ID: 1, User: "ada", Tags: []string{"go", "db"}, Addr: address{"Oslo", 150}, TS: t0, Amount: big.NewRat(1250, 100), OK: true, Day: civil.Date{Year: 2026, Month: 3, Day: 1}, Note: bigquery.NullString{StringVal: "first", Valid: true}},
		{ID: 2, User: "bob", Tags: []string{"go"}, Addr: address{"Rome", 100}, TS: t0.Add(time.Hour), Amount: big.NewRat(5, 1), OK: true, Day: civil.Date{Year: 2026, Month: 3, Day: 2}},
		{ID: 3, User: "ada", Tags: []string{"db", "ops", "go"}, Addr: address{"Oslo", 150}, TS: t0.Add(2 * time.Hour), Amount: big.NewRat(-3, 2), OK: false, Day: civil.Date{Year: 2026, Month: 3, Day: 2}},
		{ID: 4, User: "cy", Addr: address{"Paris", 750}, TS: t0.Add(3 * time.Hour), Amount: big.NewRat(0, 1), OK: true, Day: civil.Date{Year: 2026, Month: 3, Day: 3}},
	}
}

// loaded makes dataset ds with table events and the four events in it.
func (e *env) loaded(t *testing.T) *bigquery.Table {
	t.Helper()
	ds := e.client.Dataset("ds")
	if err := ds.Create(ctx, &bigquery.DatasetMetadata{Location: "US"}); err != nil {
		t.Fatal(err)
	}
	tbl := ds.Table("events")
	if err := tbl.Create(ctx, &bigquery.TableMetadata{Schema: eventSchema}); err != nil {
		t.Fatal(err)
	}
	if err := tbl.Inserter().Put(ctx, events()); err != nil {
		t.Fatal(err)
	}
	return tbl
}

func rows(t *testing.T, it *bigquery.RowIterator) [][]bigquery.Value {
	t.Helper()
	var out [][]bigquery.Value
	for {
		var r []bigquery.Value
		err := it.Next(&r)
		if err == iterator.Done {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
}

func query(t *testing.T, e *env, sql string, params ...bigquery.QueryParameter) [][]bigquery.Value {
	t.Helper()
	q := e.client.Query(sql)
	q.Parameters = params
	it, err := q.Read(ctx)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return rows(t, it)
}

func show(r [][]bigquery.Value) string { return fmt.Sprint(r) }

// A negative or non-numeric maxResults or startIndex is a 400 (startIndex=-1 once panicked).
func TestBadPaging(t *testing.T) {
	e := setup(t)
	for _, q := range []string{"datasets?maxResults=-1", "datasets?maxResults=abc", "datasets/d/tables/t/data?startIndex=-1"} {
		resp := must(http.Get(e.url + "/bigquery/v2/projects/proj/" + q))
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Errorf("%s = %d, want 400", q, resp.StatusCode)
		}
	}
}

func TestDatasetsAndTables(t *testing.T) {
	e := setup(t)
	ds := e.client.Dataset("ds")
	if err := ds.Create(ctx, &bigquery.DatasetMetadata{Location: "EU", Labels: map[string]string{"env": "dev"}, Description: "d1"}); err != nil {
		t.Fatal(err)
	}
	if apiCode(ds.Create(ctx, nil)) != 409 {
		t.Fatal("a duplicate dataset was created")
	}
	if apiCode(e.client.Dataset("bad-id").Create(ctx, nil)) != 400 {
		t.Fatal("a bad dataset id was accepted")
	}
	md := must(ds.Metadata(ctx))
	if md.Location != "EU" || md.Labels["env"] != "dev" || md.Description != "d1" || md.FullID != "proj:ds" || md.CreationTime.IsZero() {
		t.Fatalf("metadata = %+v", md)
	}
	if _, err := e.client.Dataset("nope").Metadata(ctx); apiCode(err) != 404 {
		t.Fatalf("missing dataset: %v", err)
	}
	upd := bigquery.DatasetMetadataToUpdate{Description: "d2"}
	upd.SetLabel("team", "x")
	upd.DeleteLabel("env")
	md = must(ds.Update(ctx, upd, md.ETag))
	if md.Description != "d2" || md.Labels["team"] != "x" || len(md.Labels) != 1 {
		t.Fatalf("updated = %+v", md)
	}
	must(0, e.client.Dataset("other").Create(ctx, &bigquery.DatasetMetadata{Labels: map[string]string{"team": "y"}}))
	var ids []string
	for it := e.client.Datasets(ctx); ; {
		d, err := it.Next()
		if err != nil {
			break
		}
		ids = append(ids, d.DatasetID)
	}
	if strings.Join(ids, ",") != "ds,other" {
		t.Fatalf("datasets = %v", ids)
	}
	it := e.client.Datasets(ctx)
	it.Filter = "labels.team:y"
	if d, err := it.Next(); err != nil || d.DatasetID != "other" {
		t.Fatalf("filtered = %v, %v", d, err)
	}

	// A table with a nested schema, partitioning and clustering.
	tbl := ds.Table("events")
	if err := tbl.Create(ctx, &bigquery.TableMetadata{Schema: eventSchema, Description: "events", Labels: map[string]string{"a": "b"},
		TimePartitioning: &bigquery.TimePartitioning{Field: "TS", Expiration: 24 * time.Hour}, Clustering: &bigquery.Clustering{Fields: []string{"User", "ID"}}}); err != nil {
		t.Fatal(err)
	}
	if apiCode(tbl.Create(ctx, nil)) != 409 {
		t.Fatal("a duplicate table was created")
	}
	tm := must(tbl.Metadata(ctx))
	if tm.Type != bigquery.RegularTable || len(tm.Schema) != 9 || tm.Schema[3].Type != bigquery.RecordFieldType || len(tm.Schema[3].Schema) != 2 || !tm.Schema[0].Required ||
		!tm.Schema[2].Repeated || tm.TimePartitioning.Field != "TS" || tm.TimePartitioning.Expiration != 24*time.Hour ||
		strings.Join(tm.Clustering.Fields, ",") != "User,ID" || tm.FullID != "proj:ds.events" || tm.NumRows != 0 {
		t.Fatalf("table = %+v", tm)
	}

	// Streaming inserts, and reading them back by position and into structs.
	if err := tbl.Inserter().Put(ctx, events()); err != nil {
		t.Fatal(err)
	}
	if n := must(tbl.Metadata(ctx)).NumRows; n != 4 {
		t.Fatalf("numRows = %d", n)
	}
	var got []event
	for it := tbl.Read(ctx); ; {
		var ev event
		err := it.Next(&ev)
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, ev)
	}
	if len(got) != 4 {
		t.Fatalf("read %d rows", len(got))
	}
	a := got[0]
	if a.ID != 1 || a.User != "ada" || strings.Join(a.Tags, "|") != "go|db" || a.Addr != (address{"Oslo", 150}) || !a.TS.Equal(t0) || a.Amount.Cmp(big.NewRat(25, 2)) != 0 ||
		!a.OK || a.Day.String() != "2026-03-01" || a.Note.StringVal != "first" || !a.Note.Valid {
		t.Fatalf("first row = %+v", a)
	}
	if got[3].Tags != nil || got[3].Note.Valid {
		t.Fatalf("fourth row = %+v", got[3])
	}
	// Paging through the rows, and positions.
	it2 := tbl.Read(ctx)
	it2.PageInfo().MaxSize = 3
	if r := rows(t, it2); len(r) != 4 || r[1][1] != "bob" {
		t.Fatalf("paged = %v", r)
	}
	// A bad row is refused with the reason; the others are stopped; with SkipInvalidRows they go in.
	ins := tbl.Inserter()
	err := ins.Put(ctx, []*bigquery.ValuesSaver{
		{Schema: eventSchema, InsertID: "ok1", Row: []bigquery.Value{int64(10), "zed", []bigquery.Value{}, []bigquery.Value{"X", int64(1)}, t0, nil, true, civil.Date{Year: 2026, Month: 1, Day: 1}, nil}},
		{Schema: bigquery.Schema{{Name: "User", Type: bigquery.StringFieldType}}, InsertID: "bad", Row: []bigquery.Value{"missing the required ID"}}})
	var multi bigquery.PutMultiError
	if !errors.As(err, &multi) || len(multi) != 2 {
		t.Fatalf("insert errors = %v", err)
	}
	if n := must(tbl.Metadata(ctx)).NumRows; n != 4 {
		t.Fatalf("rows after a rejected insert: %d", n)
	}
	ins.SkipInvalidRows = true
	_ = ins.Put(ctx, []*bigquery.ValuesSaver{
		{Schema: eventSchema, InsertID: "ok2", Row: []bigquery.Value{int64(11), "zed", []bigquery.Value{}, []bigquery.Value{"X", int64(1)}, t0, nil, true, civil.Date{Year: 2026, Month: 1, Day: 1}, nil}},
		{Schema: bigquery.Schema{{Name: "User", Type: bigquery.StringFieldType}}, InsertID: "bad2", Row: []bigquery.Value{"x"}}})
	// The same insert id is a duplicate.
	_ = ins.Put(ctx, []*bigquery.ValuesSaver{{Schema: eventSchema, InsertID: "ok2", Row: []bigquery.Value{int64(11), "zed", []bigquery.Value{}, []bigquery.Value{"X", int64(1)}, t0, nil, true, civil.Date{Year: 2026, Month: 1, Day: 1}, nil}}})
	if n := must(tbl.Metadata(ctx)).NumRows; n != 5 {
		t.Fatalf("rows = %d, want 5", n)
	}

	// Schema changes: add a column; dropping or retyping one is refused.
	tm = must(tbl.Update(ctx, bigquery.TableMetadataToUpdate{Description: "more", Schema: append(append(bigquery.Schema{}, eventSchema...), &bigquery.FieldSchema{Name: "Extra", Type: bigquery.StringFieldType})}, ""))
	if tm.Description != "more" || len(tm.Schema) != 10 {
		t.Fatalf("updated table = %+v", tm)
	}
	if _, err := tbl.Update(ctx, bigquery.TableMetadataToUpdate{Schema: eventSchema[:3]}, ""); apiCode(err) != 400 {
		t.Fatalf("dropping columns: %v", err)
	}
	var names []string
	for it := ds.Tables(ctx); ; {
		x, err := it.Next()
		if err != nil {
			break
		}
		names = append(names, x.TableID)
	}
	if strings.Join(names, ",") != "events" {
		t.Fatalf("tables = %v", names)
	}

	// Views.
	v := ds.Table("big")
	if err := v.Create(ctx, &bigquery.TableMetadata{ViewQuery: "SELECT User FROM `proj.ds.events` WHERE ID > 1"}); err != nil {
		t.Fatal(err)
	}
	if m := must(v.Metadata(ctx)); m.Type != bigquery.ViewTable || m.ViewQuery != "SELECT User FROM `proj.ds.events` WHERE ID > 1" {
		t.Fatalf("view = %+v", m)
	}
	if err := ds.Table("bad").Create(ctx, &bigquery.TableMetadata{ViewQuery: "SELECT 1 FROM `proj.ds.none`"}); apiCode(err) != 404 {
		t.Fatalf("a view over a missing table: %v", err)
	}

	// Deleting: a dataset with tables needs DeleteWithContents.
	if err := ds.Delete(ctx); apiCode(err) != 400 {
		t.Fatalf("delete without contents: %v", err)
	}
	if err := v.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := v.Delete(ctx); apiCode(err) != 404 {
		t.Fatalf("delete twice: %v", err)
	}
	if err := ds.DeleteWithContents(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Metadata(ctx); apiCode(err) != 404 {
		t.Fatalf("table after dataset delete: %v", err)
	}

	// Everything survives Save and Load.
	e.loaded(t)
	b := must(e.svc.Save())
	e.svc.Reset()
	if _, err := e.client.Dataset("ds").Metadata(ctx); apiCode(err) != 404 {
		t.Fatal("reset kept the dataset")
	}
	if err := e.svc.Load(b); err != nil {
		t.Fatal(err)
	}
	if r := rows(t, e.client.Dataset("ds").Table("events").Read(ctx)); len(r) != 4 {
		t.Fatalf("after load: %v", r)
	}
	if res := e.svc.Resources(); len(res) != 3 || len(res[1].Rows) != 1 || e.svc.Inspect("Tables", res[1].Rows[0][0]) == nil {
		t.Fatalf("console = %v", res)
	}
}

func TestQueries(t *testing.T) {
	e := setup(t)
	e.needDocker(t)
	e.loaded(t)

	// WHERE, with typed results.
	r := query(t, e, "SELECT ID, User, TS, Amount, Day, Addr FROM `proj.ds.events` WHERE OK AND ID > 1 ORDER BY ID")
	if len(r) != 2 || r[0][0] != int64(2) || r[0][1] != "bob" || !r[0][2].(time.Time).Equal(t0.Add(time.Hour)) || r[0][3].(*big.Rat).Cmp(big.NewRat(5, 1)) != 0 ||
		r[0][4].(civil.Date).String() != "2026-03-02" || show([][]bigquery.Value{{r[0][5]}}) != "[[[Rome 100]]]" || r[1][0] != int64(4) {
		t.Fatalf("where = %v", show(r))
	}
	// GROUP BY and aggregates.
	r = query(t, e, "SELECT User, COUNT(*) AS n, SUM(Amount) AS total, MAX(TS) AS last FROM ds.events GROUP BY User ORDER BY n DESC, User")
	if len(r) != 3 || r[0][0] != "ada" || r[0][1] != int64(2) || r[0][2].(*big.Rat).Cmp(big.NewRat(11, 1)) != 0 || !r[0][3].(time.Time).Equal(t0.Add(2*time.Hour)) || r[1][0] != "bob" {
		t.Fatalf("group by = %v", show(r))
	}
	// UNNEST of a repeated column; unnamed result columns are f0_, f1_.
	r = query(t, e, "SELECT tag, COUNT(*) AS n FROM `proj.ds.events`, UNNEST(Tags) AS tag GROUP BY tag ORDER BY n DESC, tag")
	if show(r) != "[[go 3] [db 2] [ops 1]]" {
		t.Fatalf("unnest = %v", show(r))
	}
	q := e.client.Query("SELECT COUNT(*), 1 + 1 FROM ds.events")
	it := must(q.Read(ctx))
	rows(t, it)
	if it.Schema[0].Name != "f0_" || it.Schema[1].Name != "f1_" || it.Schema[0].Type != bigquery.IntegerFieldType {
		t.Fatalf("result schema = %v", it.Schema)
	}
	// Arrays and structs in results, with SAFE_CAST, STRUCT and ARRAY literals.
	r = query(t, e, "SELECT ARRAY_AGG(User ORDER BY ID) AS users, STRUCT(MIN(ID) AS lo, MAX(ID) AS hi) AS span, SAFE_CAST('x' AS INT64) AS bad, [1, 2, 3] AS xs FROM ds.events")
	if show(r) != "[[[ada bob ada cy] [1 4] <nil> [1 2 3]]]" {
		t.Fatalf("nested results = %v", show(r))
	}
	// Parameters: named, array and positional.
	r = query(t, e, "SELECT ID FROM ds.events WHERE User = @who AND ID IN UNNEST(@ids) ORDER BY ID",
		bigquery.QueryParameter{Name: "who", Value: "ada"}, bigquery.QueryParameter{Name: "ids", Value: []int64{1, 2, 3}})
	if show(r) != "[[1] [3]]" {
		t.Fatalf("parameters = %v", show(r))
	}
	r = query(t, e, "SELECT ID FROM ds.events WHERE TS >= ? AND Amount > ? ORDER BY ID", bigquery.QueryParameter{Value: t0.Add(time.Hour)}, bigquery.QueryParameter{Value: big.NewRat(0, 1)})
	if show(r) != "[[2]]" {
		t.Fatalf("positional = %v", show(r))
	}
	// Dates and functions.
	r = query(t, e, "SELECT DATE_ADD(Day, INTERVAL 1 DAY) AS next, FORMAT_TIMESTAMP('%Y-%m', TS) AS month, IF(OK, 'y', 'n') AS ok, SAFE_DIVIDE(ID, 0) AS div, CURRENT_TIMESTAMP() > TS AS past FROM ds.events WHERE ID = 1")
	if show(r) != "[[2026-03-02 2026-03 y <nil> true]]" {
		t.Fatalf("functions = %v", show(r))
	}
	// Errors: a missing table and bad SQL.
	if _, err := e.client.Query("SELECT * FROM ds.none").Read(ctx); apiCode(err) != 404 {
		t.Fatalf("missing table: %v", err)
	}
	if _, err := e.client.Query("SELEC 1").Read(ctx); apiCode(err) != 400 {
		t.Fatalf("bad sql: %v", err)
	}
	// A query into a destination table, with a write disposition.
	q = e.client.Query("SELECT User, COUNT(*) AS n FROM ds.events GROUP BY User")
	q.Dst = e.client.Dataset("ds").Table("per_user")
	job := must(q.Run(ctx))
	if st := must(job.Wait(ctx)); st.Err() != nil {
		t.Fatal(st.Err())
	}
	if r := rows(t, e.client.Dataset("ds").Table("per_user").Read(ctx)); len(r) != 3 {
		t.Fatalf("destination = %v", show(r))
	}
	q = e.client.Query("SELECT 'x' AS User, 1 AS n")
	q.Dst = e.client.Dataset("ds").Table("per_user")
	q.WriteDisposition = bigquery.WriteAppend
	must(must(q.Run(ctx)).Wait(ctx))
	if n := must(e.client.Dataset("ds").Table("per_user").Metadata(ctx)).NumRows; n != 4 {
		t.Fatalf("appended rows = %d", n)
	}

	// A view runs its query when it is read or queried.
	if err := e.client.Dataset("ds").Table("v").Create(ctx, &bigquery.TableMetadata{ViewQuery: "SELECT User, ID FROM `proj.ds.events` WHERE ID >= 3"}); err != nil {
		t.Fatal(err)
	}
	if r := query(t, e, "SELECT User FROM ds.v ORDER BY ID"); show(r) != "[[ada] [cy]]" {
		t.Fatalf("view query = %v", show(r))
	}
	if r := rows(t, e.client.Dataset("ds").Table("v").Read(ctx)); len(r) != 2 {
		t.Fatalf("view read = %v", show(r))
	}
}

func TestDML(t *testing.T) {
	e := setup(t)
	e.needDocker(t)
	tbl := e.loaded(t)

	dml := func(sql string) int64 {
		t.Helper()
		job := must(e.client.Query(sql).Run(ctx))
		st := must(job.Wait(ctx))
		if st.Err() != nil {
			t.Fatalf("%s: %v", sql, st.Err())
		}
		return st.Statistics.Details.(*bigquery.QueryStatistics).NumDMLAffectedRows
	}
	if n := dml("UPDATE ds.events SET User = UPPER(User), Tags = ['x'] WHERE ID = 1"); n != 1 {
		t.Fatalf("update affected %d", n)
	}
	r := query(t, e, "SELECT User, Tags, TS, Addr.City FROM ds.events WHERE ID = 1")
	if show(r) != "[[ADA [x] "+t0.Format("2006-01-02 15:04:05.999999999 -0700 MST")+" Oslo]]" {
		t.Fatalf("after update = %v", show(r))
	}
	// The table itself changed, with its schema and the other rows intact.
	byID := map[int64]string{} // rows have no order after a DML statement
	for _, row := range rows(t, tbl.Read(ctx)) {
		byID[row[0].(int64)] = row[1].(string)
	}
	if len(byID) != 4 || byID[1] != "ADA" || byID[2] != "bob" {
		t.Fatalf("table after update = %v", byID)
	}
	if n := dml("INSERT INTO ds.events (ID, User, OK, Amount) VALUES (5, 'eve', false, 1.5), (6, 'fay', true, 2)"); n != 2 {
		t.Fatalf("insert affected %d", n)
	}
	if n := dml("DELETE FROM ds.events WHERE User = 'bob' OR ID = 6"); n != 2 {
		t.Fatalf("delete affected %d", n)
	}
	if n := must(tbl.Metadata(ctx)).NumRows; n != 4 {
		t.Fatalf("rows = %d", n)
	}
	if n := dml("INSERT INTO ds.events SELECT * FROM ds.events WHERE ID = 4"); n != 1 {
		t.Fatalf("insert select affected %d", n)
	}
	// MERGE.
	must(0, e.client.Dataset("ds").Table("upd").Create(ctx, &bigquery.TableMetadata{Schema: bigquery.Schema{{Name: "ID", Type: bigquery.IntegerFieldType}, {Name: "User", Type: bigquery.StringFieldType}}}))
	dml("INSERT INTO ds.upd VALUES (3, 'merged'), (9, 'new')")
	dml("MERGE ds.events T USING ds.upd S ON T.ID = S.ID WHEN MATCHED THEN UPDATE SET User = S.User")
	if r := query(t, e, "SELECT User FROM ds.events WHERE ID = 3"); show(r) != "[[merged]]" {
		t.Fatalf("merge = %v", show(r))
	}
	// DDL: create a table with a schema, CREATE TABLE AS, a view, and drops.
	dml("CREATE TABLE ds.made (a INT64, b ARRAY<STRING>, c STRUCT<x FLOAT64, y TIMESTAMP>) CLUSTER BY a")
	made := e.client.Dataset("ds").Table("made")
	mm := must(made.Metadata(ctx))
	if len(mm.Schema) != 3 || mm.Schema[0].Type != bigquery.IntegerFieldType || !mm.Schema[1].Repeated || mm.Schema[2].Type != bigquery.RecordFieldType || mm.Schema[2].Schema[1].Type != bigquery.TimestampFieldType ||
		mm.Clustering == nil || mm.Clustering.Fields[0] != "a" {
		t.Fatalf("created table = %+v", mm)
	}
	dml("INSERT INTO ds.made VALUES (1, ['p', 'q'], STRUCT(1.5 AS x, TIMESTAMP '2026-01-01 00:00:00 UTC' AS y))")
	if r := query(t, e, "SELECT a, b, c.x FROM ds.made"); show(r) != "[[1 [p q] 1.5]]" {
		t.Fatalf("made = %v", show(r))
	}
	dml("CREATE OR REPLACE TABLE ds.copy AS SELECT User, COUNT(*) AS n FROM ds.events GROUP BY User")
	if m := must(e.client.Dataset("ds").Table("copy").Metadata(ctx)); len(m.Schema) != 2 || m.NumRows < 3 {
		t.Fatalf("ctas = %+v", m)
	}
	dml("CREATE VIEW ds.vw AS SELECT ID FROM `proj.ds.events`")
	if m := must(e.client.Dataset("ds").Table("vw").Metadata(ctx)); m.Type != bigquery.ViewTable {
		t.Fatalf("created view = %+v", m)
	}
	dml("DROP VIEW ds.vw")
	dml("DROP TABLE IF EXISTS ds.copy")
	dml("ALTER TABLE ds.made ADD COLUMN d STRING")
	if m := must(made.Metadata(ctx)); len(m.Schema) != 4 || m.Schema[3].Name != "d" {
		t.Fatalf("altered = %+v", m.Schema)
	}
	dml("CREATE SCHEMA other OPTIONS (location = 'EU')")
	if m := must(e.client.Dataset("other").Metadata(ctx)); m.Location != "EU" {
		t.Fatalf("created dataset = %+v", m)
	}
	dml("DROP SCHEMA other")
	if _, err := e.client.Dataset("other").Metadata(ctx); apiCode(err) != 404 {
		t.Fatalf("dropped dataset: %v", err)
	}
	// A script runs its statements in order.
	if r := query(t, e, "INSERT INTO ds.upd VALUES (100, 'x'); SELECT COUNT(*) FROM ds.upd"); show(r) != "[[3]]" {
		t.Fatalf("script = %v", show(r))
	}
}

func put(t *testing.T, e *env, bucket, name, body string) {
	t.Helper()
	resp := must(http.Post(e.url+"/storage/v1/b?project=proj", "application/json", strings.NewReader(`{"name":"`+bucket+`"}`)))
	resp.Body.Close()
	resp = must(http.Post(e.url+"/upload/storage/v1/b/"+bucket+"/o?uploadType=media&name="+name, "text/plain", strings.NewReader(body)))
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("put %s = %d", name, resp.StatusCode)
	}
}

func object(t *testing.T, e *env, bucket, name string) string {
	t.Helper()
	resp := must(http.Get(e.url + "/storage/v1/b/" + bucket + "/o/" + name + "?alt=media"))
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("object %s = %d", name, resp.StatusCode)
	}
	return string(b)
}

func TestLoadExtractCopy(t *testing.T) {
	e := setup(t)
	if err := e.client.Dataset("ds").Create(ctx, nil); err != nil {
		t.Fatal(err)
	}
	put(t, e, "lake", "people.csv", "id,name,born,score,active\n1,Ada,1815-12-10,9.5,true\n2,\"Grace, Hopper\",1906-12-09,8,false\n3,Linus,,7.25,true\n")
	put(t, e, "lake", "part/1.csv", "4,Dan\n5,Eve\n")
	put(t, e, "lake", "part/2.csv", "6,Fay\n")
	put(t, e, "lake", "events.json", `{"id":1,"tags":["a","b"],"user":{"name":"ada","age":36},"ok":true}`+"\n"+`{"id":2,"tags":[],"user":{"name":"bob"},"ok":false,"extra":1}`+"\n")
	ds := e.client.Dataset("ds")

	wait := func(j *bigquery.Job, err error) *bigquery.JobStatus {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return must(j.Wait(ctx))
	}
	// CSV with a schema, skipping the header.
	ref := bigquery.NewGCSReference("gs://lake/people.csv")
	ref.SkipLeadingRows = 1
	ref.Schema = bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}, {Name: "name", Type: bigquery.StringFieldType}, {Name: "born", Type: bigquery.DateFieldType},
		{Name: "score", Type: bigquery.FloatFieldType}, {Name: "active", Type: bigquery.BooleanFieldType}}
	loader := ds.Table("people").LoaderFrom(ref)
	st := wait(loader.Run(ctx))
	if st.Err() != nil {
		t.Fatal(st.Err())
	}
	if ls := st.Statistics.Details.(*bigquery.LoadStatistics); ls.OutputRows != 3 || ls.InputFiles != 1 {
		t.Fatalf("load statistics = %+v", ls)
	}
	r := rows(t, ds.Table("people").Read(ctx))
	if show(r) != "[[1 Ada 1815-12-10 9.5 true] [2 Grace, Hopper 1906-12-09 8 false] [3 Linus <nil> 7.25 true]]" {
		t.Fatalf("people = %v", show(r))
	}
	// Loading again into a non-empty table needs a write disposition.
	if st := wait(loader.Run(ctx)); st.Err() == nil {
		t.Fatal("WRITE_EMPTY loaded into a table with rows")
	}
	loader.WriteDisposition = bigquery.WriteAppend
	wait(loader.Run(ctx))
	if n := must(ds.Table("people").Metadata(ctx)).NumRows; n != 6 {
		t.Fatalf("appended rows = %d", n)
	}
	loader.WriteDisposition = bigquery.WriteTruncate
	wait(loader.Run(ctx))
	if n := must(ds.Table("people").Metadata(ctx)).NumRows; n != 3 {
		t.Fatalf("truncated rows = %d", n)
	}
	// Bad data fails the job; maxBadRecords skips it.
	put(t, e, "lake", "bad.csv", "x,y\n1,2\nnot a number,3\n")
	bad := bigquery.NewGCSReference("gs://lake/bad.csv")
	bad.SkipLeadingRows = 1
	bad.Schema = bigquery.Schema{{Name: "x", Type: bigquery.IntegerFieldType}, {Name: "y", Type: bigquery.IntegerFieldType}}
	if st := wait(ds.Table("bad").LoaderFrom(bad).Run(ctx)); st.Err() == nil || !strings.Contains(st.Err().Error(), "errors") {
		t.Fatalf("bad load = %v", st.Err())
	}
	bad.MaxBadRecords = 1
	wait(ds.Table("bad2").LoaderFrom(bad).Run(ctx))
	if n := must(ds.Table("bad2").Metadata(ctx)).NumRows; n != 1 {
		t.Fatalf("rows with a bad record skipped = %d", n)
	}
	// A wildcard, and autodetect.
	wild := bigquery.NewGCSReference("gs://lake/part/*.csv")
	wild.Schema = bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}, {Name: "name", Type: bigquery.StringFieldType}}
	wait(ds.Table("parts").LoaderFrom(wild).Run(ctx))
	if n := must(ds.Table("parts").Metadata(ctx)).NumRows; n != 3 {
		t.Fatalf("wildcard rows = %d", n)
	}
	auto := bigquery.NewGCSReference("gs://lake/people.csv")
	auto.SkipLeadingRows = 1
	auto.AutoDetect = true
	wait(ds.Table("auto").LoaderFrom(auto).Run(ctx))
	am := must(ds.Table("auto").Metadata(ctx))
	if len(am.Schema) != 5 || am.Schema[0].Type != bigquery.IntegerFieldType || am.Schema[2].Type != bigquery.DateFieldType || am.Schema[3].Type != bigquery.FloatFieldType || am.Schema[4].Type != bigquery.BooleanFieldType || am.Schema[1].Name != "name" {
		t.Fatalf("autodetected = %v", am.Schema)
	}
	if st := wait(ds.Table("nowhere").LoaderFrom(bigquery.NewGCSReference("gs://lake/missing.csv")).Run(ctx)); st.Err() == nil {
		t.Fatal("loaded a missing file")
	}
	// Newline-delimited JSON with a nested, repeated schema.
	js := bigquery.NewGCSReference("gs://lake/events.json")
	js.SourceFormat = bigquery.JSON
	js.AutoDetect = true
	js.IgnoreUnknownValues = true
	wait(ds.Table("ev").LoaderFrom(js).Run(ctx))
	em := must(ds.Table("ev").Metadata(ctx))
	if len(em.Schema) != 5 || em.NumRows != 2 || em.Schema[0].Name != "id" || em.Schema[4].Name != "extra" {
		t.Fatalf("json table = %+v", em)
	}
	if r := rows(t, ds.Table("ev").Read(ctx)); show(r) != "[[1 [a b] [36 ada] true <nil>] [2 [] [<nil> bob] false 1]]" {
		t.Fatalf("json rows = %v", show(r))
	}

	// Extract to CSV and JSON.
	wait(ds.Table("people").ExtractorTo(bigquery.NewGCSReference("gs://out/people.csv")).Run(ctx))
	_ = must(http.Post(e.url+"/storage/v1/b?project=proj", "application/json", strings.NewReader(`{"name":"out"}`))).Body.Close()
	st = wait(ds.Table("people").ExtractorTo(bigquery.NewGCSReference("gs://out/people-*.csv")).Run(ctx))
	if st.Err() != nil {
		t.Fatal(st.Err())
	}
	want := "id,name,born,score,active\n1,Ada,1815-12-10,9.5,true\n2,\"Grace, Hopper\",1906-12-09,8,false\n3,Linus,,7.25,true\n"
	if got := object(t, e, "out", "people-000000000000.csv"); got != want {
		t.Fatalf("csv export = %q", got)
	}
	jref := bigquery.NewGCSReference("gs://out/ev.json")
	jref.DestinationFormat = bigquery.JSON
	wait(ds.Table("ev").ExtractorTo(jref).Run(ctx))
	if got := object(t, e, "out", "ev.json"); got != `{"extra":null,"id":1,"ok":true,"tags":["a","b"],"user":{"age":36,"name":"ada"}}`+"\n"+`{"extra":1,"id":2,"ok":false,"tags":[],"user":{"age":null,"name":"bob"}}`+"\n" {
		t.Fatalf("json export = %q", got)
	}
	// Copy.
	cp := ds.Table("people2").CopierFrom(ds.Table("people"))
	wait(cp.Run(ctx))
	if r := rows(t, ds.Table("people2").Read(ctx)); len(r) != 3 || len(must(ds.Table("people2").Metadata(ctx)).Schema) != 5 {
		t.Fatalf("copy = %v", show(r))
	}
	if st := wait(cp.Run(ctx)); st.Err() == nil {
		t.Fatal("copied over a table with rows")
	}
	// Jobs are listed.
	n := 0
	for it := e.client.Jobs(ctx); ; n++ {
		if _, err := it.Next(); err != nil {
			break
		}
	}
	if n < 10 {
		t.Fatalf("%d jobs listed", n)
	}
}

func TestQueryOnLoadedData(t *testing.T) {
	e := setup(t)
	e.needDocker(t)
	must(0, e.client.Dataset("ds").Create(ctx, nil))
	put(t, e, "lake", "sales.csv", "region,amount\nnorth,10\nsouth,20\nnorth,5\n")
	ref := bigquery.NewGCSReference("gs://lake/sales.csv")
	ref.SkipLeadingRows = 1
	ref.AutoDetect = true
	must(must(e.client.Dataset("ds").Table("sales").LoaderFrom(ref).Run(ctx)).Wait(ctx))
	r := query(t, e, "SELECT region, SUM(amount) AS total FROM ds.sales GROUP BY region ORDER BY total DESC")
	if show(r) != "[[south 20] [north 15]]" {
		t.Fatalf("query on loaded csv = %v", show(r))
	}
}

func TestNoEngine(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	e := setup(t)
	tbl := e.loaded(t) // metadata and streaming inserts work without Docker
	if n := must(tbl.Metadata(ctx)).NumRows; n != 4 {
		t.Fatalf("rows = %d", n)
	}
	_, err := e.client.Query("SELECT 1").Read(ctx)
	if err == nil || !strings.Contains(err.Error(), "Docker is not available, so BigQuery cannot run queries") {
		t.Fatalf("query without Docker: %v", err)
	}
}

func TestDeliveredToTables(t *testing.T) {
	e := setup(t)
	must(0, e.client.Dataset("logs").Create(ctx, nil))
	err := e.hub.Deliver("//bigquery.googleapis.com/projects/proj/datasets/logs", &core.Message{Subject: "app", Body: `{"logName":"projects/proj/logs/app","severity":"ERROR","textPayload":"boom","timestamp":"2026-03-01T10:00:00.5Z","resource":{"type":"global"}}`})
	if err != nil {
		t.Fatal(err)
	}
	r := rows(t, e.client.Dataset("logs").Table("app").Read(ctx))
	if len(r) != 1 || r[0][3] != "ERROR" || r[0][5] != "boom" || r[0][7] != `{"type":"global"}` {
		t.Fatalf("delivered = %v", show(r))
	}
	if err := e.hub.Deliver("//bigquery.googleapis.com/projects/proj/datasets/none", &core.Message{Subject: "app", Body: "{}"}); err == nil {
		t.Fatal("delivered to a dataset that does not exist")
	}
}
