package bigquery

// The BigQuery Storage Read API (google.cloud.bigquery.storage.v1.BigQueryRead), over gRPC on pogi's
// port. A read session takes a snapshot of a table's rows when it is made: the fields asked for, the
// rows a restriction keeps, split into streams. Each stream answers ReadRows with Arrow record
// batches. A restriction runs as a query on DuckDB, so it needs Docker; a plain read does not.

import (
	"cmp"
	"context"
	"encoding/json"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/nrndev/pogi/internal/core"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	sessionTTL  = 6 * time.Hour // as BigQuery's
	maxSessions = 64            // ponytail: the oldest session goes when there are more; each holds a snapshot of its table's rows
	maxStreams  = 100
	batchRows   = 1000    // rows per ReadRows response, at most
	batchBytes  = 3 << 20 // and a size under the 4 MiB a gRPC client takes by default
)

type readSession struct {
	expire  time.Time
	fields  []*Field
	schema  []byte // the schema message
	comp    bufferCompression
	streams [][]row
}

type readAPI struct {
	storagepb.UnimplementedBigQueryReadServer
	s *Service
}

// RegisterGRPC serves the Storage Read API on the shared gRPC server.
func (s *Service) RegisterGRPC(g *grpc.Server) {
	storagepb.RegisterBigQueryReadServer(g, &readAPI{s: s})
}

var storageTableRE = regexp.MustCompile(`^projects/([^/]+)/datasets/([^/]+)/tables/([^/]+)$`)

func (a *readAPI) CreateReadSession(ctx context.Context, req *storagepb.CreateReadSessionRequest) (*storagepb.ReadSession, error) {
	s, in := a.s, req.GetReadSession()
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "read_session is required")
	}
	if p, ok := strings.CutPrefix(req.Parent, "projects/"); !ok || p == "" || strings.Contains(p, "/") {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid parent %q: want projects/<project>", req.Parent)
	}
	m := storageTableRE.FindStringSubmatch(in.Table)
	if m == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid table %q: want projects/<project>/datasets/<dataset>/tables/<table>", in.Table)
	}
	switch in.DataFormat {
	case storagepb.DataFormat_ARROW:
	case storagepb.DataFormat_AVRO:
		return nil, status.Error(codes.Unimplemented, "pogi serves the data format ARROW only, not AVRO")
	default:
		return nil, status.Error(codes.InvalidArgument, "data_format is required: ARROW")
	}
	if req.MaxStreamCount < 0 || req.PreferredMinStreamCount < 0 {
		return nil, status.Error(codes.InvalidArgument, "max_stream_count and preferred_min_stream_count must be 0 or more")
	}
	opts := in.GetReadOptions()
	if opts.GetSamplePercentage() != 0 {
		return nil, status.Error(codes.Unimplemented, "pogi does not sample tables: leave sample_percentage unset")
	}
	ref := tref{m[1], m[2], m[3]}
	var st *stmt
	if r := opts.GetRowRestriction(); r != "" {
		if !s.hub.Docker().Available() {
			return nil, status.Error(codes.FailedPrecondition, "pogi: Docker is not available, so BigQuery cannot run the row restriction, which runs as a query on DuckDB: read without one")
		}
		tr, err := translate("SELECT rowid FROM `"+ref.project+"."+ref.dataset+"."+ref.table+"` WHERE ("+r+") ORDER BY rowid", tctx{ref.project, ""}, nil)
		if err != nil || len(tr.stmts) != 1 || !tr.stmts[0].query {
			return nil, status.Errorf(codes.InvalidArgument, "Invalid row restriction %q: %v", r, err)
		}
		st = tr.stmts[0]
	}

	// The snapshot: the rows as they are now. Rows are never changed in place, only replaced or
	// appended to, so a copy of the slice is enough. snapshot_time is taken as now: pogi keeps no history.
	s.mu.Lock()
	d, t, err := s.getTbl(ref.project, ref.dataset, ref.table)
	if err == nil && t.Type == "VIEW" {
		err = invalid("%s is a view: the Storage Read API reads tables only", t.Ref)
	}
	var fields []*Field
	if err == nil {
		fields, err = selectFields(t.fields(), opts.GetSelectedFields(), "")
	}
	var rows []row
	var setup []string
	var files map[string][]byte
	if err == nil {
		rows = t.Rows[:len(t.Rows):len(t.Rows)]
		if st != nil {
			setup, files, err = s.snapshot(st.refs)
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, grpcErr(err)
	}
	if st != nil {
		if rows, err = s.restrict(ctx, rows, st, setup, files); err != nil {
			return nil, grpcErr(err)
		}
	}

	sess := &readSession{expire: time.Now().Add(sessionTTL), fields: fields, schema: arrowSchemaBytes(fields), comp: compressionOf(opts.GetArrowSerializationOptions().GetBufferCompression())}
	n := min(int(cmp.Or(req.MaxStreamCount, req.PreferredMinStreamCount, 1)), maxStreams, len(rows))
	for i := range n { // contiguous parts of nearly the same size
		sess.streams = append(sess.streams, rows[i*len(rows)/n:(i+1)*len(rows)/n])
	}
	name := "projects/" + ref.project + "/locations/" + cmp.Or(lower(d.Location), "us") + "/sessions/" + core.RandHex(12)
	s.mu.Lock()
	s.keepSession(name, sess)
	s.mu.Unlock()

	out := &storagepb.ReadSession{Name: name, ExpireTime: timestamppb.New(sess.expire), DataFormat: storagepb.DataFormat_ARROW,
		Schema: &storagepb.ReadSession_ArrowSchema{ArrowSchema: &storagepb.ArrowSchema{SerializedSchema: sess.schema}},
		Table:  in.Table, TableModifiers: in.TableModifiers, ReadOptions: in.ReadOptions, TraceId: in.TraceId,
		EstimatedRowCount: int64(len(rows)), EstimatedTotalBytesScanned: int64(len(rows)) * int64(max(len(fields), 1)) * 8}
	for i := range sess.streams {
		out.Streams = append(out.Streams, &storagepb.ReadStream{Name: name + "/streams/" + strconv.Itoa(i)})
	}
	return out, nil
}

// keepSession stores a session, dropping the expired ones and, past the limit, the oldest. Lock held.
func (s *Service) keepSession(name string, sess *readSession) {
	if s.sessions == nil {
		s.sessions = map[string]*readSession{}
	}
	oldest := ""
	for k, v := range s.sessions {
		if time.Now().After(v.expire) {
			delete(s.sessions, k)
		} else if oldest == "" || v.expire.Before(s.sessions[oldest].expire) {
			oldest = k
		}
	}
	if len(s.sessions) >= maxSessions {
		delete(s.sessions, oldest)
	}
	s.sessions[name] = sess
}

// restrict keeps the rows the restriction selects: DuckDB answers with the row numbers of the
// snapshot it was given, so the rows keep their stored values and types.
func (s *Service) restrict(ctx context.Context, rows []row, st *stmt, setup []string, files map[string][]byte) ([]row, error) {
	out, err := s.engine(ctx, map[string]any{"setup": setup, "stmts": []map[string]any{{"sql": st.sql, "query": true}}}, files)
	if err != nil {
		return nil, err
	}
	var kept []row
	for _, r := range out.Results[0].Rows {
		n, ok := r[0].(json.Number)
		i, err := n.Int64()
		if !ok || err != nil || i < 0 || i >= int64(len(rows)) {
			return nil, errf(500, "backendError", "pogi: the DuckDB engine sent a bad row number: %v", r[0])
		}
		kept = append(kept, rows[i])
	}
	return kept, nil
}

func (a *readAPI) stream(name string) (*readSession, []row, error) {
	s := a.s
	sessName, idx, _ := strings.Cut(name, "/streams/")
	s.mu.Lock()
	defer s.mu.Unlock()
	sess := s.sessions[sessName]
	i, err := strconv.Atoi(idx)
	if sess == nil || time.Now().After(sess.expire) || err != nil || i < 0 || i >= len(sess.streams) {
		return nil, nil, status.Errorf(codes.NotFound, "Not found: read stream %s (sessions last 6 hours, and are not kept over a restart)", name)
	}
	return sess, sess.streams[i], nil
}

func (a *readAPI) ReadRows(req *storagepb.ReadRowsRequest, srv storagepb.BigQueryRead_ReadRowsServer) error {
	sess, rows, err := a.stream(req.ReadStream)
	if err != nil {
		return err
	}
	if req.Offset < 0 || req.Offset > int64(len(rows)) {
		return status.Errorf(codes.OutOfRange, "Offset %d is out of range: the stream has %d rows", req.Offset, len(rows))
	}
	for start, end := int(req.Offset), 0; start < len(rows); start = end {
		end = min(start+batchRows, len(rows))
		b, err := encodeBatch(sess, rows[start:end])
		for err == nil && len(b) > batchBytes && end-start > 1 { // too big: half as many rows
			end = start + (end-start)/2
			b, err = encodeBatch(sess, rows[start:end])
		}
		if err != nil {
			return grpcErr(err)
		}
		resp := &storagepb.ReadRowsResponse{RowCount: int64(end - start),
			Rows:  &storagepb.ReadRowsResponse_ArrowRecordBatch{ArrowRecordBatch: &storagepb.ArrowRecordBatch{SerializedRecordBatch: b}},
			Stats: &storagepb.StreamStats{Progress: &storagepb.StreamStats_Progress{AtResponseStart: float64(start) / float64(len(rows)), AtResponseEnd: float64(end) / float64(len(rows))}}}
		if start == int(req.Offset) {
			resp.Schema = &storagepb.ReadRowsResponse_ArrowSchema{ArrowSchema: &storagepb.ArrowSchema{SerializedSchema: sess.schema}}
		}
		if err := srv.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

// SplitReadStream answers that the stream cannot be split: both halves empty.
func (a *readAPI) SplitReadStream(_ context.Context, req *storagepb.SplitReadStreamRequest) (*storagepb.SplitReadStreamResponse, error) {
	if _, _, err := a.stream(req.Name); err != nil {
		return nil, err
	}
	return &storagepb.SplitReadStreamResponse{}, nil
}

func grpcErr(err error) error {
	e, ok := err.(*apiErr)
	if !ok {
		return status.Error(codes.Internal, err.Error())
	}
	code := map[int]codes.Code{400: codes.InvalidArgument, 404: codes.NotFound, 409: codes.AlreadyExists, 501: codes.Unimplemented}[e.code]
	if code == codes.OK {
		code = codes.Internal
	}
	return status.Error(code, e.msg)
}

// selectFields keeps the fields that selected names, in the table's order. A name may be a path
// into a record (address.city). No names keeps every field.
func selectFields(fs []*Field, selected []string, prefix string) ([]*Field, error) {
	if len(selected) == 0 {
		return fs, nil
	}
	subs := map[string][]string{} // a top-level field, lower case -> the paths below it ("" for all of it)
	for _, p := range selected {
		head, rest, _ := strings.Cut(p, ".")
		found := false
		for _, f := range fs {
			if lower(f.Name) == lower(head) {
				if rest != "" && f.Type != "RECORD" {
					return nil, invalid("Invalid selected field %q: %s is not a RECORD", prefix+p, prefix+f.Name)
				}
				found = true
			}
		}
		if !found {
			return nil, invalid("Invalid selected field %q: no such field in the table schema", prefix+p)
		}
		subs[lower(head)] = append(subs[lower(head)], rest)
	}
	var out []*Field
	for _, f := range fs {
		paths, ok := subs[lower(f.Name)]
		switch {
		case !ok:
		case f.Type != "RECORD" || slices.Contains(paths, ""):
			out = append(out, f)
		default:
			sub, err := selectFields(f.Fields, paths, prefix+f.Name+".")
			if err != nil {
				return nil, err
			}
			nf := *f
			nf.Fields = sub
			out = append(out, &nf)
		}
	}
	return out, nil
}
