package bigquery_test

import (
	"bytes"
	"io"
	"math/big"
	"slices"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/bigquery/storage/apiv1"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/civil"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func (e *env) grpcOpts() []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(strings.TrimPrefix(e.url, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))}
}

func byID(r [][]bigquery.Value) [][]bigquery.Value {
	slices.SortFunc(r, func(a, b []bigquery.Value) int { return int(a[0].(int64) - b[0].(int64)) })
	return r
}

// The Go client reads a table through the Storage Read API and gets the same rows as over REST.
func TestStorageReadClient(t *testing.T) {
	e := setup(t)
	tbl := e.loaded(t)
	want := byID(rows(t, tbl.Read(ctx)))

	fast := must(bigquery.NewClient(ctx, "proj", option.WithEndpoint(e.url+"/bigquery/v2/"), option.WithoutAuthentication()))
	defer fast.Close()
	if err := fast.EnableStorageReadClient(ctx, e.grpcOpts()...); err != nil {
		t.Fatal(err)
	}
	it := fast.Dataset("ds").Table("events").Read(ctx)
	got := byID(rows(t, it))
	if !it.IsAccelerated() {
		t.Fatal("the read did not use the Storage Read API")
	}
	if show(got) != show(want) {
		t.Fatalf("storage read:\n%s\nREST:\n%s", show(got), show(want))
	}

	// the other types, and NULLs
	types := e.client.Dataset("ds").Table("types")
	sc := bigquery.Schema{{Name: "ID", Type: bigquery.IntegerFieldType}, {Name: "F", Type: bigquery.FloatFieldType}, {Name: "B", Type: bigquery.BytesFieldType},
		{Name: "DT", Type: bigquery.DateTimeFieldType}, {Name: "TM", Type: bigquery.TimeFieldType}, {Name: "BN", Type: bigquery.BigNumericFieldType},
		{Name: "J", Type: bigquery.JSONFieldType}, {Name: "G", Type: bigquery.GeographyFieldType}}
	if err := types.Create(ctx, &bigquery.TableMetadata{Schema: sc}); err != nil {
		t.Fatal(err)
	}
	err := types.Inserter().Put(ctx, []*bigquery.ValuesSaver{
		{Schema: sc, Row: []bigquery.Value{1, 1.5, []byte("hi"), civil.DateTime{Date: civil.Date{Year: 2026, Month: 3, Day: 1}, Time: civil.Time{Hour: 10, Nanosecond: 5000}},
			civil.Time{Hour: 1, Minute: 2, Second: 3, Nanosecond: 456000000}, big.NewRat(-1, 3), `{"a":1}`, "POINT(1 2)"}},
		{Schema: sc, Row: []bigquery.Value{2, -0.25, nil, nil, nil, nil, nil, nil}},
	})
	if err != nil {
		t.Fatal(err)
	}
	want = byID(rows(t, types.Read(ctx)))
	it = fast.Dataset("ds").Table("types").Read(ctx)
	if got = byID(rows(t, it)); !it.IsAccelerated() || show(got) != show(want) {
		t.Fatalf("storage read:\n%s\nREST:\n%s", show(got), show(want))
	}

	// an empty table has no streams
	empty := fast.Dataset("ds").Table("empty")
	if err := empty.Create(ctx, &bigquery.TableMetadata{Schema: eventSchema}); err != nil {
		t.Fatal(err)
	}
	if r := rows(t, empty.Read(ctx)); len(r) != 0 {
		t.Fatalf("%v", r)
	}
}

// readAll reads every stream of a session and decodes the Arrow batches.
func readAll(t *testing.T, rc *storage.BigQueryReadClient, s *storagepb.ReadSession) (*arrow.Schema, [][]string) {
	t.Helper()
	var out [][]string
	schemaBytes := s.GetArrowSchema().GetSerializedSchema()
	var sc *arrow.Schema
	for _, st := range s.Streams {
		stream := must(rc.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: st.Name}))
		var lines []string
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			r := must(ipc.NewReader(bytes.NewReader(append(slices.Clone(schemaBytes), resp.GetArrowRecordBatch().SerializedRecordBatch...))))
			sc = r.Schema()
			for r.Next() {
				rec := r.RecordBatch()
				if rec.NumRows() != resp.RowCount {
					t.Fatalf("batch has %d rows, response says %d", rec.NumRows(), resp.RowCount)
				}
				for i := 0; i < int(rec.NumRows()); i++ {
					var cells []string
					for _, col := range rec.Columns() {
						cells = append(cells, col.ValueStr(i))
					}
					lines = append(lines, strings.Join(cells, " "))
				}
			}
			r.Release()
		}
		out = append(out, lines)
	}
	return sc, out
}

func TestStorageReadSessions(t *testing.T) {
	e := setup(t)
	e.loaded(t)
	rc := must(storage.NewBigQueryReadClient(ctx, e.grpcOpts()...))
	defer rc.Close()
	const tbl = "projects/proj/datasets/ds/tables/events"
	session := func(rs *storagepb.ReadSession, streams int32) (*storagepb.ReadSession, error) {
		if rs.Table == "" {
			rs.Table = tbl
		}
		if rs.DataFormat == 0 {
			rs.DataFormat = storagepb.DataFormat_ARROW
		}
		return rc.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{Parent: "projects/proj", ReadSession: rs, MaxStreamCount: streams})
	}

	// selected fields, a nested one among them, over two streams
	s := must(session(&storagepb.ReadSession{ReadOptions: &storagepb.ReadSession_TableReadOptions{SelectedFields: []string{"Addr.City", "ID"}}}, 2))
	if len(s.Streams) != 2 || s.EstimatedRowCount != 4 || !strings.HasPrefix(s.Name, "projects/proj/locations/us/sessions/") {
		t.Fatalf("%+v", s)
	}
	sc, streams := readAll(t, rc, s)
	if got := sc.String(); !strings.Contains(got, "ID: type=int64") || !strings.Contains(got, "Addr: type=struct<City: utf8") || strings.Contains(got, "Zip") || strings.Contains(got, "User") {
		t.Fatalf("schema: %s", got)
	}
	if strings.Join(streams[0], "|")+"|"+strings.Join(streams[1], "|") != `1 {"City":"Oslo"}|2 {"City":"Rome"}|3 {"City":"Oslo"}|4 {"City":"Paris"}` {
		t.Fatalf("%q", streams)
	}

	// a stream read from an offset
	rr := must(rc.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: s.Streams[0].Name, Offset: 1}))
	if resp := must(rr.Recv()); resp.RowCount != 1 {
		t.Fatalf("%+v", resp)
	}

	// big rows come in batches under 4 MiB, the default limit of a gRPC client
	big := e.client.Dataset("ds").Table("big")
	if err := big.Create(ctx, &bigquery.TableMetadata{Schema: bigquery.Schema{{Name: "S", Type: bigquery.StringFieldType}}}); err != nil {
		t.Fatal(err)
	}
	var savers []*bigquery.ValuesSaver
	for range 6 {
		savers = append(savers, &bigquery.ValuesSaver{Schema: bigquery.Schema{{Name: "S", Type: bigquery.StringFieldType}}, Row: []bigquery.Value{strings.Repeat("x", 1<<20)}})
	}
	if err := big.Inserter().Put(ctx, savers); err != nil {
		t.Fatal(err)
	}
	s = must(session(&storagepb.ReadSession{Table: "projects/proj/datasets/ds/tables/big"}, 1))
	rr = must(rc.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: s.Streams[0].Name}))
	for n := int64(0); n < 6; {
		resp := must(rr.Recv())
		if resp.RowCount > 2 {
			t.Fatalf("a batch of %d rows of 1 MiB", resp.RowCount)
		}
		n += resp.RowCount
	}

	// failures
	wantCode := func(err error, code codes.Code, msg string) {
		t.Helper()
		if status.Code(err) != code || !strings.Contains(err.Error(), msg) {
			t.Fatalf("want %v %q, got %v", code, msg, err)
		}
	}
	_, err := session(&storagepb.ReadSession{Table: "projects/proj/datasets/ds/tables/nope"}, 1)
	wantCode(err, codes.NotFound, "nope")
	_, err = session(&storagepb.ReadSession{Table: "ds.events"}, 1)
	wantCode(err, codes.InvalidArgument, "projects/")
	_, err = session(&storagepb.ReadSession{ReadOptions: &storagepb.ReadSession_TableReadOptions{SelectedFields: []string{"Nope"}}}, 1)
	wantCode(err, codes.InvalidArgument, "Nope")
	_, err = session(&storagepb.ReadSession{DataFormat: storagepb.DataFormat_AVRO}, 1)
	wantCode(err, codes.Unimplemented, "ARROW")
	_, err = session(&storagepb.ReadSession{}, -1)
	wantCode(err, codes.InvalidArgument, "max_stream_count")
	_, err = must(rc.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: s.Name + "/streams/9"})).Recv()
	wantCode(err, codes.NotFound, "streams/9")
	_, err = must(rc.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: s.Streams[0].Name, Offset: 9})).Recv()
	wantCode(err, codes.OutOfRange, "9")

	// a row restriction runs as a query on DuckDB
	if !e.hub.Docker().Available() {
		_, err = session(&storagepb.ReadSession{ReadOptions: &storagepb.ReadSession_TableReadOptions{RowRestriction: "User = 'ada'"}}, 1)
		wantCode(err, codes.FailedPrecondition, "Docker is not available")
		return
	}
	s = must(session(&storagepb.ReadSession{ReadOptions: &storagepb.ReadSession_TableReadOptions{SelectedFields: []string{"ID", "User"},
		RowRestriction: "User = 'ada' AND Addr.Zip > 100"}}, 1))
	if _, streams = readAll(t, rc, s); strings.Join(streams[0], "|") != `1 ada|3 ada` {
		t.Fatalf("%q", streams)
	}
	_, err = session(&storagepb.ReadSession{ReadOptions: &storagepb.ReadSession_TableReadOptions{RowRestriction: "Nope = 1"}}, 1)
	wantCode(err, codes.InvalidArgument, "Nope")
}
