// Package firestore emulates Cloud Firestore in Native mode over gRPC, like
// the official emulator: point clients at it with
// FIRESTORE_EMULATOR_HOST=localhost:7644 (any credentials are accepted).
// It covers documents, queries and aggregations, optimistic transactions,
// field transforms, reads at a past read_time (one hour of history), realtime
// listeners and DELETE /emulator/v1/projects/{p}/databases/{d}/documents.
package firestore

import (
	"cmp"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/nrndev/pogi/internal/core"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service is the Firestore emulator.
type Service struct {
	hub   *core.Hub
	mu    sync.Mutex
	dbs   map[string]map[string][]rev // database → document name → revisions, oldest first
	txns  map[string]*txn
	subs  map[*listener]bool // open Listen streams
	last  time.Time          // the clock: each commit and read gets a later instant
	swept time.Time
}

// rev is a document as of instant at; doc is nil when it was deleted then.
type rev struct {
	doc *pb.Document
	at  time.Time
}

// txn is an open transaction. Read-write ones are optimistic: the update
// times of the documents they read are checked again at commit.
type txn struct {
	db       string
	readOnly bool
	at       time.Time            // read-only snapshot
	reads    map[string]time.Time // document → update time seen (zero: missing)
	expires  time.Time
}

// New creates the Firestore service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, subs: map[*listener]bool{}}
	s.Reset()
	if hub != nil {
		hub.Emulator(func(r *http.Request) bool {
			return strings.Contains(r.URL.Path, "/databases/") && strings.HasSuffix(r.URL.Path, "/documents")
		}, s)
	}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "firestore", Cloud: core.GCP, Title: "Firestore", Targets: []string{"google.firestore.v1.Firestore"}}
}

// RegisterGRPC serves the Firestore API on the shared gRPC server.
func (s *Service) RegisterGRPC(g *grpc.Server) { pb.RegisterFirestoreServer(g, server{Service: s}) }

// server implements the gRPC API. It is a separate type because
// Service.DeleteDocument is the console helper.
type server struct {
	pb.UnimplementedFirestoreServer
	*Service
}

// ServeHTTP serves the emulator's REST extras, which the hub dispatches from /emulator/v1/:
// DELETE /emulator/v1/projects/{p}/databases/{d}/documents clears a database.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	db, ok := strings.CutSuffix(strings.TrimPrefix(r.URL.Path, "/emulator/v1/"), "/documents")
	if p := strings.Split(db, "/"); r.Method != http.MethodDelete || !ok || len(p) != 4 || p[0] != "projects" || p[2] != "databases" {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}
	s.mu.Lock()
	delete(s.dbs, db)
	s.wake(db)
	s.mu.Unlock()
	s.dirty()
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("{}"))
}

func (s *Service) dirty() {
	if s.hub != nil {
		s.hub.MarkDirty()
	}
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.dbs, s.txns = map[string]map[string][]rev{}, map[string]*txn{}
	s.wake("")
	s.mu.Unlock()
}

// saved is the persisted state: live documents only, as protojson.
type saved struct {
	Documents []json.RawMessage `json:"documents"`
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st saved
	for _, db := range core.SortedKeys(s.dbs) {
		for _, name := range core.SortedKeys(s.dbs[db]) {
			if d := at(s.dbs[db][name], s.last); d != nil {
				b, err := protojson.Marshal(d)
				if err != nil {
					return nil, err
				}
				st.Documents = append(st.Documents, b)
			}
		}
	}
	return json.Marshal(st)
}

func (s *Service) Load(b []byte) error {
	var st saved
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	dbs, last := map[string]map[string][]rev{}, time.Time{}
	for _, raw := range st.Documents {
		d := &pb.Document{}
		if err := protojson.Unmarshal(raw, d); err != nil {
			return err
		}
		db, err := dbOf(d.Name, true)
		if err != nil {
			return err
		}
		if dbs[db] == nil {
			dbs[db] = map[string][]rev{}
		}
		t := d.UpdateTime.AsTime()
		dbs[db][d.Name] = []rev{{d, t}}
		if t.After(last) {
			last = t
		}
	}
	s.mu.Lock()
	s.dbs, s.txns = dbs, map[string]*txn{}
	if last.After(s.last) {
		s.last = last
	}
	s.wake("")
	s.mu.Unlock()
	return nil
}

// ---- storage ----

// at returns the document as of instant t, or nil.
func at(revs []rev, t time.Time) *pb.Document {
	for i := len(revs) - 1; i >= 0; i-- {
		if !revs[i].at.After(t) {
			return revs[i].doc
		}
	}
	return nil
}

func updated(d *pb.Document) time.Time {
	if d == nil {
		return time.Time{}
	}
	return d.UpdateTime.AsTime()
}

// tick advances the clock: microseconds, strictly increasing.
func (s *Service) tick() time.Time {
	t := time.Now().UTC().Truncate(time.Microsecond)
	if !t.After(s.last) {
		t = s.last.Add(time.Microsecond)
	}
	s.last = t
	return t
}

// dbOf validates a document name (doc) or a parent (the database's
// ".../documents" root or a document) and returns its database name.
func dbOf(name string, doc bool) (string, error) {
	p := strings.Split(name, "/")
	if len(p) < 5 || p[0] != "projects" || p[2] != "databases" || p[4] != "documents" ||
		slices.Contains(p, "") || (len(p)-5)%2 != 0 || doc && len(p) == 5 {
		return "", status.Errorf(codes.InvalidArgument, "invalid resource name %q", name)
	}
	return strings.Join(p[:4], "/"), nil
}

func invalid(format string, a ...any) error {
	return status.Errorf(codes.InvalidArgument, format, a...)
}

// prune forgets revisions no read_time can reach any more (older than an hour); once a minute.
func (s *Service) prune(now time.Time) {
	if now.Sub(s.swept) < time.Minute {
		return
	}
	s.swept = now
	cut := now.Add(-time.Hour)
	for _, docs := range s.dbs {
		for name, revs := range docs {
			i := 0
			for i < len(revs) && !revs[i].at.After(cut) && (revs[i].doc == nil || i+1 < len(revs) && !revs[i+1].at.After(cut)) {
				i++
			}
			if i == len(revs) {
				delete(docs, name)
			} else if i > 0 {
				docs[name] = slices.Delete(revs, 0, i)
			}
		}
	}
}

// ---- transactions ----

func (s *Service) begin(db string, o *pb.TransactionOptions) []byte {
	now := s.tick()
	for id, t := range s.txns {
		if now.After(t.expires) {
			delete(s.txns, id)
		}
	}
	if rw := o.GetReadWrite(); rw != nil {
		delete(s.txns, string(rw.RetryTransaction))
	}
	t := &txn{db: db, reads: map[string]time.Time{}, expires: now.Add(5 * time.Minute)}
	if ro := o.GetReadOnly(); ro != nil {
		t.readOnly, t.at = true, now
		if rt := ro.GetReadTime(); rt != nil {
			t.at = rt.AsTime()
		}
	}
	id := []byte(core.NewID())
	s.txns[string(id)] = t
	return id
}

func (s *Service) tx(db string, id []byte) (*txn, error) {
	if t := s.txns[string(id)]; t != nil && t.db == db {
		return t, nil
	}
	return nil, invalid("The referenced transaction has expired or is no longer valid.")
}

// snapshot resolves a read's consistency selector to the instant to read at
// and the read-write transaction recording the reads; newTx begins one and
// returns its id.
func (s *Service) snapshot(db string, id []byte, newTx *pb.TransactionOptions, readTime *timestamppb.Timestamp) (time.Time, *txn, []byte, error) {
	var created []byte
	if newTx != nil {
		id = s.begin(db, newTx)
		created = id
	}
	switch {
	case id != nil:
		t, err := s.tx(db, id)
		if err != nil {
			return time.Time{}, nil, nil, err
		}
		if t.readOnly {
			return t.at, nil, created, nil
		}
		return s.tick(), t, created, nil
	case readTime != nil:
		return readTime.AsTime(), nil, nil, nil
	}
	return s.tick(), nil, nil, nil
}

// saw records a transactional read for the conflict check at commit.
func (t *txn) saw(name string, d *pb.Document) {
	if t == nil {
		return
	}
	if _, ok := t.reads[name]; !ok {
		t.reads[name] = updated(d)
	}
}

// ---- writes ----

// commit applies writes atomically at a new instant; t is the transaction
// being committed, if any. Callers hold s.mu.
func (s *Service) commit(db string, writes []*pb.Write, t *txn) ([]*pb.WriteResult, time.Time, error) {
	if t != nil {
		if t.readOnly && len(writes) > 0 {
			return nil, time.Time{}, invalid("Cannot modify entities in a read-only transaction.")
		}
		for name, seen := range t.reads {
			if !updated(at(s.dbs[db][name], s.last)).Equal(seen) {
				return nil, time.Time{}, status.Errorf(codes.Aborted,
					"Aborted due to cross-transaction contention: %s changed since the transaction read it.", name)
			}
		}
	}
	now := s.tick()
	staged := map[string]*pb.Document{} // nil: deleted
	results := make([]*pb.WriteResult, len(writes))
	for i, w := range writes {
		r, err := s.apply(db, staged, w, now)
		if err != nil {
			return nil, time.Time{}, err
		}
		results[i] = r
	}
	if len(staged) > 0 {
		if s.dbs[db] == nil {
			s.dbs[db] = map[string][]rev{}
		}
		for name, d := range staged {
			if revs := s.dbs[db][name]; at(revs, now) != d { // skip no-ops
				s.dbs[db][name] = append(revs, rev{d, now})
			}
		}
		s.prune(now)
		s.wake(db)
		s.dirty()
	}
	return results, now, nil
}

// apply stages one write on top of staged (this commit's documents so far).
func (s *Service) apply(db string, staged map[string]*pb.Document, w *pb.Write, now time.Time) (*pb.WriteResult, error) {
	var name string
	switch op := w.Operation.(type) {
	case *pb.Write_Update:
		name = op.Update.GetName()
	case *pb.Write_Delete:
		name = op.Delete
	case *pb.Write_Transform:
		name = op.Transform.GetDocument()
	default:
		return nil, invalid("write has no operation")
	}
	if d, err := dbOf(name, true); err != nil || d != db {
		return nil, cmp.Or(err, invalid("document %s is not in database %s", name, db))
	}
	cur, ok := staged[name]
	if !ok {
		cur = at(s.dbs[db][name], now)
	}
	switch c := w.GetCurrentDocument().GetConditionType().(type) {
	case *pb.Precondition_Exists:
		if c.Exists && cur == nil {
			return nil, status.Errorf(codes.NotFound, "No document to update: %s", name)
		}
		if !c.Exists && cur != nil {
			return nil, status.Errorf(codes.AlreadyExists, "Document already exists: %s", name)
		}
	case *pb.Precondition_UpdateTime:
		if cur == nil || !cur.UpdateTime.AsTime().Equal(c.UpdateTime.AsTime()) {
			return nil, status.Errorf(codes.FailedPrecondition, "The update time of %s does not match the precondition.", name)
		}
	}
	res := &pb.WriteResult{}
	fields, transforms := cur.GetFields(), w.UpdateTransforms
	switch op := w.Operation.(type) {
	case *pb.Write_Delete:
		staged[name] = nil
		return res, nil
	case *pb.Write_Update:
		if w.UpdateMask == nil {
			fields = op.Update.Fields
		}
		for _, p := range w.GetUpdateMask().GetFieldPaths() {
			sp := splitPath(p)
			v, _ := get(op.Update.Fields, sp) // in the mask but not the document: deleted
			fields = set(fields, sp, v)
		}
	case *pb.Write_Transform:
		transforms = op.Transform.FieldTransforms
	}
	for _, ft := range transforms {
		sp := splitPath(ft.FieldPath)
		v, _ := get(fields, sp)
		v, r, err := transform(v, ft, now)
		if err != nil {
			return nil, err
		}
		fields = set(fields, sp, v)
		res.TransformResults = append(res.TransformResults, r)
	}
	d := &pb.Document{Name: name, Fields: fields, CreateTime: timestamppb.New(now), UpdateTime: timestamppb.New(now)}
	if cur != nil {
		d.CreateTime = cur.CreateTime
		if proto.Equal(&pb.MapValue{Fields: cur.Fields}, &pb.MapValue{Fields: fields}) {
			d = cur // writes that change nothing keep the update time
		}
	}
	staged[name] = d
	res.UpdateTime = d.UpdateTime
	return res, nil
}

// write1 commits one write and returns the document after it (nil if deleted).
func (s *Service) write1(name string, w *pb.Write, m *pb.DocumentMask) (*pb.Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db, err := dbOf(name, true)
	if err != nil {
		return nil, err
	}
	if _, _, err := s.commit(db, []*pb.Write{w}, nil); err != nil {
		return nil, err
	}
	if d := at(s.dbs[db][name], s.last); d != nil {
		return mask(d, m), nil
	}
	return nil, nil
}

// locked runs f under the lock; streaming RPCs send its result after unlocking.
func locked[T any](s *Service, f func() (T, error)) (T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return f()
}

func sendAll[T any](send func(T) error, msgs []T) error {
	for _, m := range msgs {
		if err := send(m); err != nil {
			return err
		}
	}
	return nil
}

// ---- gRPC: documents, transactions, writes ----

func (s server) GetDocument(_ context.Context, req *pb.GetDocumentRequest) (*pb.Document, error) {
	return locked(s.Service, func() (*pb.Document, error) {
		db, err := dbOf(req.Name, true)
		if err != nil {
			return nil, err
		}
		t, tx, _, err := s.snapshot(db, req.GetTransaction(), nil, req.GetReadTime())
		if err != nil {
			return nil, err
		}
		d := at(s.dbs[db][req.Name], t)
		tx.saw(req.Name, d)
		if d == nil {
			return nil, status.Errorf(codes.NotFound, "Document %q not found.", req.Name)
		}
		return mask(d, req.Mask), nil
	})
}

func (s server) BatchGetDocuments(req *pb.BatchGetDocumentsRequest, stream pb.Firestore_BatchGetDocumentsServer) error {
	out, err := locked(s.Service, func() ([]*pb.BatchGetDocumentsResponse, error) {
		for _, name := range req.Documents {
			if db, err := dbOf(name, true); err != nil || db != req.Database {
				return nil, cmp.Or(err, invalid("document %s is not in database %s", name, req.Database))
			}
		}
		t, tx, id, err := s.snapshot(req.Database, req.GetTransaction(), req.GetNewTransaction(), req.GetReadTime())
		if err != nil {
			return nil, err
		}
		var out []*pb.BatchGetDocumentsResponse
		for i, name := range req.Documents {
			d := at(s.dbs[req.Database][name], t)
			tx.saw(name, d)
			r := &pb.BatchGetDocumentsResponse{ReadTime: timestamppb.New(t), Result: &pb.BatchGetDocumentsResponse_Missing{Missing: name}}
			if d != nil {
				r.Result = &pb.BatchGetDocumentsResponse_Found{Found: mask(d, req.Mask)}
			}
			if i == 0 {
				r.Transaction = id
			}
			out = append(out, r)
		}
		return out, nil
	})
	if err != nil {
		return err
	}
	return sendAll(stream.Send, out)
}

func (s server) CreateDocument(_ context.Context, req *pb.CreateDocumentRequest) (*pb.Document, error) {
	id := cmp.Or(req.DocumentId, core.RandString(20, core.AlphaNum))
	name := req.Parent + "/" + req.CollectionId + "/" + id
	return s.write1(name, &pb.Write{
		Operation:       &pb.Write_Update{Update: &pb.Document{Name: name, Fields: req.GetDocument().GetFields()}},
		CurrentDocument: &pb.Precondition{ConditionType: &pb.Precondition_Exists{Exists: false}},
	}, req.Mask)
}

func (s server) UpdateDocument(_ context.Context, req *pb.UpdateDocumentRequest) (*pb.Document, error) {
	return s.write1(req.GetDocument().GetName(), &pb.Write{Operation: &pb.Write_Update{Update: req.Document},
		UpdateMask: req.UpdateMask, CurrentDocument: req.CurrentDocument}, req.Mask)
}

func (s server) DeleteDocument(_ context.Context, req *pb.DeleteDocumentRequest) (*emptypb.Empty, error) {
	_, err := s.write1(req.Name, &pb.Write{Operation: &pb.Write_Delete{Delete: req.Name}, CurrentDocument: req.CurrentDocument}, nil)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s server) BeginTransaction(_ context.Context, req *pb.BeginTransactionRequest) (*pb.BeginTransactionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &pb.BeginTransactionResponse{Transaction: s.begin(req.Database, req.Options)}, nil
}

func (s server) Commit(_ context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var t *txn
	if len(req.Transaction) > 0 {
		var err error
		if t, err = s.tx(req.Database, req.Transaction); err != nil {
			return nil, err
		}
		delete(s.txns, string(req.Transaction))
	}
	res, now, err := s.commit(req.Database, req.Writes, t)
	if err != nil {
		return nil, err
	}
	return &pb.CommitResponse{WriteResults: res, CommitTime: timestamppb.New(now)}, nil
}

func (s server) Rollback(_ context.Context, req *pb.RollbackRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.tx(req.Database, req.Transaction); err != nil {
		return nil, err
	}
	delete(s.txns, string(req.Transaction))
	return &emptypb.Empty{}, nil
}

// BatchWrite applies each write on its own (non-atomic), reporting a status per write.
func (s server) BatchWrite(_ context.Context, req *pb.BatchWriteRequest) (*pb.BatchWriteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &pb.BatchWriteResponse{}
	for _, w := range req.Writes {
		st, wr := &spb.Status{}, &pb.WriteResult{}
		if res, _, err := s.commit(req.Database, []*pb.Write{w}, nil); err != nil {
			st = status.Convert(err).Proto()
		} else {
			wr = res[0]
		}
		resp.WriteResults, resp.Status = append(resp.WriteResults, wr), append(resp.Status, st)
	}
	return resp, nil
}

// Write is the legacy write stream: the first request gets a stream id and
// token; each later request's writes are committed like Commit.
func (s server) Write(stream pb.Firestore_WriteServer) error {
	id, db := core.NewID(), ""
	for n := 1; ; n++ {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if n == 1 {
			db = req.Database
		}
		resp := &pb.WriteResponse{StreamId: id, StreamToken: []byte(strconv.Itoa(n))}
		if len(req.Writes) > 0 {
			s.mu.Lock()
			res, now, err := s.commit(db, req.Writes, nil)
			s.mu.Unlock()
			if err != nil {
				return err
			}
			resp.WriteResults, resp.CommitTime = res, timestamppb.New(now)
		} else if n > 1 {
			continue // a token acknowledgement needs no reply
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// ---- console ----

// resolve turns a console path (relative like "users/alice", or a full
// resource name) into a full name and its database.
func resolve(project, database, path string) (name, db string) {
	if !strings.HasPrefix(path, "projects/") {
		path = "projects/" + project + "/databases/" + cmp.Or(database, "(default)") + "/documents/" + strings.Trim(path, "/")
	}
	db, _, _ = strings.Cut(path, "/documents")
	return path, db
}

// plain renders a document as natural JSON with its name under "__name__".
func plain(d *pb.Document) map[string]any {
	m := toJSON(mapValue(d.Fields)).(map[string]any)
	m["__name__"] = d.Name
	return m
}

// collections counts the live documents directly in each collection path of db;
// ancestors of nested documents are listed too.
func (s *Service) collections(db string) map[string]int {
	out := map[string]int{}
	for name, revs := range s.dbs[db] {
		if at(revs, s.last) == nil {
			continue
		}
		p := strings.Split(strings.TrimPrefix(name, db+"/documents/"), "/")
		for i := 1; i < len(p); i += 2 {
			out[strings.Join(p[:i], "/")] += b2i(i == len(p)-1)
		}
	}
	return out
}

// Collections lists the collection paths ("users", "users/alice/orders") of a database.
func (s *Service) Collections(project, database string) []string {
	_, db := resolve(project, database, "")
	s.mu.Lock()
	defer s.mu.Unlock()
	return core.SortedKeys(s.collections(db))
}

// Documents returns up to limit (0: all) documents of a collection as plain JSON with "__name__".
func (s *Service) Documents(project, database, collectionPath string, limit int) []map[string]any {
	coll, db := resolve(project, database, collectionPath)
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, name := range core.SortedKeys(s.dbs[db]) {
		id, ok := strings.CutPrefix(name, coll+"/")
		if d := at(s.dbs[db][name], s.last); ok && d != nil && !strings.Contains(id, "/") {
			if limit > 0 && len(out) == limit {
				break
			}
			out = append(out, plain(d))
		}
	}
	return out
}

// SetDocument replaces a document with plain JSON data.
func (s *Service) SetDocument(project, database, path string, data map[string]any) error {
	name, _ := resolve(project, database, path)
	_, err := s.write1(name, &pb.Write{Operation: &pb.Write_Update{Update: &pb.Document{Name: name, Fields: fromJSON(data).GetMapValue().GetFields()}}}, nil)
	return err
}

// DeleteDocument deletes a document.
func (s *Service) DeleteDocument(project, database, path string) error {
	name, _ := resolve(project, database, path)
	_, err := s.write1(name, &pb.Write{Operation: &pb.Write_Delete{Delete: name}}, nil)
	return err
}

// Resources lists collections and documents (the first 500 of each).
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	colls := core.Table{Name: "Collections", Columns: []string{"Path", "Project", "Database", "Documents"}}
	docs := core.Table{Name: "Documents", Columns: []string{"Name", "Updated"}}
	for _, db := range core.SortedKeys(s.dbs) {
		p, counts := strings.Split(db, "/"), s.collections(db)
		for _, c := range core.SortedKeys(counts) {
			if len(colls.Rows) < 500 {
				colls.Rows = append(colls.Rows, []string{c, p[1], p[3], strconv.Itoa(counts[c])})
			}
		}
		for _, name := range core.SortedKeys(s.dbs[db]) {
			if d := at(s.dbs[db][name], s.last); d != nil && len(docs.Rows) < 500 {
				docs.Rows = append(docs.Rows, []string{name, d.UpdateTime.AsTime().Format(time.DateTime)})
			}
		}
	}
	return []core.Table{colls, docs}
}

// Inspect returns a document (Documents) or a collection's first documents
// in every database (Collections) as plain JSON.
func (s *Service) Inspect(table, id string) any {
	if table == "Collections" {
		s.mu.Lock()
		dbs := core.SortedKeys(s.dbs)
		s.mu.Unlock()
		var out []map[string]any
		for _, db := range dbs {
			out = append(out, s.Documents("", "", db+"/documents/"+id, 100)...)
		}
		return out
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if db, err := dbOf(id, true); err == nil {
		if d := at(s.dbs[db][id], s.last); d != nil {
			return plain(d)
		}
	}
	return nil
}
