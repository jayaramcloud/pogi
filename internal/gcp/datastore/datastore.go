// Package datastore emulates Cloud Firestore in Datastore mode over gRPC
// (google.datastore.v1.Datastore) and its REST mirror, like the official
// emulator: point clients at it with DATASTORE_EMULATOR_HOST=localhost:7644
// (any credentials are accepted). It covers lookups, queries (filters, orders,
// projections, distinct on, ancestors, cursors, offsets and limits),
// aggregations (count, sum, avg), optimistic transactions, mutations with
// property masks and transforms, allocateIds and reserveIds, and POST /reset.
//
// The entities live in this package's own small store, not in the Firestore
// package's: Datastore's keys with ancestor paths, multi-valued properties,
// projections and entity-group transactions map onto Firestore documents
// poorly, and a store of its own is shorter than the mapping.
package datastore

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service is the Datastore emulator.
type Service struct {
	pb.UnimplementedDatastoreServer
	hub  *core.Hub
	mu   sync.Mutex
	dbs  map[string]*db // project/database
	txns map[string]*txn
	last int64 // the last version issued: microseconds, strictly increasing
}

// db holds the entities of one project and database.
type db struct {
	ents     map[string]*ent // by skey
	next     int64           // the last id allocated
	reserved map[string]bool // skeys whose ids are not to be allocated
}

type ent struct {
	e                *pb.Entity // the key carries the project, database and namespace
	version          int64
	created, updated time.Time
}

// txn is an open transaction. Read-write ones are optimistic: what they read
// is checked again at commit.
// ponytail: a query reads only the entities it returned, so a transaction
// does not see phantoms; and there is no read_time.
type txn struct {
	db       string
	readOnly bool
	reads    map[string]int64 // skey → version seen (0: missing)
	expires  time.Time
}

// New creates the Datastore service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "datastore", Cloud: core.GCP, Title: "Datastore", Match: match, Targets: []string{"google.datastore.v1.Datastore"}}
}

// RegisterGRPC serves the Datastore API on the shared gRPC server.
func (s *Service) RegisterGRPC(g *grpc.Server) { pb.RegisterDatastoreServer(g, s) }

func (s *Service) dirty() {
	if s.hub != nil {
		s.hub.MarkDirty()
	}
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.dbs, s.txns = map[string]*db{}, map[string]*txn{}
	s.mu.Unlock()
}

// ---- REST ----

var verbs = map[string]bool{"lookup": true, "runQuery": true, "runAggregationQuery": true, "beginTransaction": true,
	"commit": true, "rollback": true, "allocateIds": true, "reserveIds": true}

// restPath splits /v1/projects/<p>:<verb>.
func restPath(p string) (project, verb string, ok bool) {
	rest, _ := strings.CutPrefix(p, "/v1/projects/")
	project, verb, ok = strings.Cut(rest, ":")
	return project, verb, ok && project != "" && !strings.Contains(project, "/") && verbs[verb]
}

// match claims the REST methods (/v1/projects/p:lookup and the like) and the
// emulator's POST /reset.
func match(r *http.Request) bool {
	if r.Method == http.MethodPost && r.URL.Path == "/reset" {
		return true
	}
	_, _, ok := restPath(r.URL.Path)
	return ok && r.Method == http.MethodPost
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	if r.URL.Path == "/reset" {
		c.Op = "Reset"
		s.Reset()
		s.dirty()
		w.Write([]byte("Ok"))
		return
	}
	project, verb, _ := restPath(r.URL.Path)
	c.Op = verb
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		gapi.JSONErr(w, c, e.Status, e.Code, e.Msg)
		return
	}
	var req proto.Message
	var call func(context.Context) (proto.Message, error)
	switch verb {
	case "lookup":
		m := &pb.LookupRequest{}
		req, call = m, func(ctx context.Context) (proto.Message, error) { return s.Lookup(ctx, m) }
	case "runQuery":
		m := &pb.RunQueryRequest{}
		req, call = m, func(ctx context.Context) (proto.Message, error) { return s.RunQuery(ctx, m) }
	case "runAggregationQuery":
		m := &pb.RunAggregationQueryRequest{}
		req, call = m, func(ctx context.Context) (proto.Message, error) { return s.RunAggregationQuery(ctx, m) }
	case "beginTransaction":
		m := &pb.BeginTransactionRequest{}
		req, call = m, func(ctx context.Context) (proto.Message, error) { return s.BeginTransaction(ctx, m) }
	case "commit":
		m := &pb.CommitRequest{}
		req, call = m, func(ctx context.Context) (proto.Message, error) { return s.Commit(ctx, m) }
	case "rollback":
		m := &pb.RollbackRequest{}
		req, call = m, func(ctx context.Context) (proto.Message, error) { return s.Rollback(ctx, m) }
	case "allocateIds":
		m := &pb.AllocateIdsRequest{}
		req, call = m, func(ctx context.Context) (proto.Message, error) { return s.AllocateIds(ctx, m) }
	default:
		m := &pb.ReserveIdsRequest{}
		req, call = m, func(ctx context.Context) (proto.Message, error) { return s.ReserveIds(ctx, m) }
	}
	if b := core.Body(r); len(strings.TrimSpace(string(b))) > 0 {
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(b, req); err != nil {
			gapi.Err(w, c, gapi.Invalid("Invalid JSON payload received. %v", err))
			return
		}
	}
	m := req.ProtoReflect()
	m.Set(m.Descriptor().Fields().ByName("project_id"), protoreflect.ValueOfString(project))
	out, err := call(r.Context())
	if err != nil {
		gapi.Err(w, c, err)
		return
	}
	b, _ := protojson.Marshal(out)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Write(b)
}

// ---- persistence ----

type savedEntity struct {
	Entity  json.RawMessage `json:"entity"`
	Version int64           `json:"version"`
	Created time.Time       `json:"created"`
	Updated time.Time       `json:"updated"`
}

type savedDB struct {
	Name     string        `json:"name"`
	Next     int64         `json:"next"`
	Reserved []string      `json:"reserved,omitempty"`
	Entities []savedEntity `json:"entities"`
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []savedDB{}
	for _, name := range core.SortedKeys(s.dbs) {
		d := s.dbs[name]
		sd := savedDB{Name: name, Next: d.next, Reserved: core.SortedKeys(d.reserved)}
		for _, k := range core.SortedKeys(d.ents) {
			e := d.ents[k]
			b, err := protojson.Marshal(e.e)
			if err != nil {
				return nil, err
			}
			sd.Entities = append(sd.Entities, savedEntity{b, e.version, e.created, e.updated})
		}
		out = append(out, sd)
	}
	return json.Marshal(out)
}

func (s *Service) Load(b []byte) error {
	var in []savedDB
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	dbs := map[string]*db{}
	var last int64
	for _, sd := range in {
		d := newDB()
		d.next = sd.Next
		for _, k := range sd.Reserved {
			d.reserved[k] = true
		}
		for _, se := range sd.Entities {
			e := &pb.Entity{}
			if err := protojson.Unmarshal(se.Entity, e); err != nil {
				return err
			}
			d.ents[skey(e.Key)] = &ent{e, se.Version, se.Created, se.Updated}
			last = max(last, se.Version)
		}
		dbs[sd.Name] = d
	}
	s.mu.Lock()
	s.dbs, s.txns = dbs, map[string]*txn{}
	s.last = max(s.last, last)
	s.mu.Unlock()
	return nil
}

func newDB() *db { return &db{ents: map[string]*ent{}, reserved: map[string]bool{}} }

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	counts := map[string]int{}
	for name, d := range s.dbs {
		for _, e := range d.ents {
			k := e.e.Key
			counts[name+"\x00"+k.PartitionId.GetNamespaceId()+"\x00"+k.Path[len(k.Path)-1].Kind]++
		}
	}
	t := core.Table{Name: "Kinds", Columns: []string{"Kind", "Project", "Database", "Namespace", "Entities"}}
	for _, k := range core.SortedKeys(counts) {
		p := strings.Split(k, "\x00")
		proj, database, _ := strings.Cut(p[0], "/")
		t.Rows = append(t.Rows, []string{p[2], proj, database, p[1], strconv.Itoa(counts[k])})
	}
	return []core.Table{t}
}

// Inspect lists the entities of a kind (id project/database/namespace/kind), up to 200, as JSON.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := strings.Split(id, "/")
	if table != "Kinds" || len(p) != 4 || s.dbs[p[0]+"/"+p[1]] == nil {
		return nil
	}
	var keys []string
	for k, e := range s.dbs[p[0]+"/"+p[1]].ents {
		if e.e.Key.PartitionId.GetNamespaceId() == p[2] && e.e.Key.Path[len(e.e.Key.Path)-1].Kind == p[3] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := []json.RawMessage{}
	for _, k := range keys[:min(len(keys), 200)] {
		b, _ := protojson.Marshal(s.dbs[p[0]+"/"+p[1]].ents[k].e)
		out = append(out, b)
	}
	return out
}

// ---- state helpers ----

// database finds or makes the database a request names. Lock held.
func (s *Service) database(project, database string) (*db, string) {
	name := project + "/" + database
	d := s.dbs[name]
	if d == nil {
		d = newDB()
		s.dbs[name] = d
	}
	return d, name
}

// tick returns a new version: the current microsecond, later than any before. Lock held.
func (s *Service) tick() int64 {
	s.last = max(time.Now().UnixMicro(), s.last+1)
	return s.last
}

func invalid(format string, a ...any) error {
	return status.Errorf(codes.InvalidArgument, format, a...)
}

// qualify gives a key the partition the request names, keeping its namespace.
func qualify(project, database string, k *pb.Key) *pb.Key {
	k = proto.Clone(k).(*pb.Key)
	if k.PartitionId == nil {
		k.PartitionId = &pb.PartitionId{}
	}
	k.PartitionId.ProjectId, k.PartitionId.DatabaseId = project, database
	return k
}

func checkKey(k *pb.Key, needComplete bool) error {
	if k == nil || len(k.Path) == 0 {
		return invalid("Invalid key: the path is empty.")
	}
	if len(k.Path) > 100 {
		return invalid("Invalid key: the path has more than 100 elements.")
	}
	for i, p := range k.Path {
		switch {
		case p.Kind == "":
			return invalid("Invalid key: a path element has no kind.")
		case n(p) == "" && p.IdType != nil && p.GetId() == 0:
			return invalid("Invalid key: an id must not be zero.")
		case p.IdType == nil && (i < len(k.Path)-1 || needComplete):
			return invalid("Invalid key: path element %d (kind %q) has neither an id nor a name.", i, p.Kind)
		}
	}
	return nil
}

// n is a path element's name, "" for ids.
func n(p *pb.Key_PathElement) string { return p.GetName() }

// readTxn resolves the transaction of a read: an open one, a new one (whose
// id the response returns) or none. Lock held.
func (s *Service) readTxn(dbName string, ro *pb.ReadOptions) (*txn, []byte, error) {
	switch c := ro.GetConsistencyType().(type) {
	case *pb.ReadOptions_Transaction:
		t, err := s.txn(dbName, c.Transaction)
		return t, nil, err
	case *pb.ReadOptions_NewTransaction:
		id := s.begin(dbName, c.NewTransaction)
		return s.txns[string(id)], id, nil
	}
	return nil, nil, nil
}

func (s *Service) begin(dbName string, o *pb.TransactionOptions) []byte {
	now := time.Now()
	for id, t := range s.txns {
		if now.After(t.expires) {
			delete(s.txns, id)
		}
	}
	if rw := o.GetReadWrite(); rw != nil {
		delete(s.txns, string(rw.PreviousTransaction))
	}
	id := []byte(core.NewID())
	s.txns[string(id)] = &txn{db: dbName, readOnly: o.GetReadOnly() != nil, reads: map[string]int64{}, expires: now.Add(5 * time.Minute)}
	return id
}

func (s *Service) txn(dbName string, id []byte) (*txn, error) {
	if t := s.txns[string(id)]; t != nil && t.db == dbName {
		return t, nil
	}
	return nil, invalid("The referenced transaction has expired or is no longer valid.")
}

// saw records a read of a key (version 0: missing) for the conflict check at commit.
func (t *txn) saw(key string, version int64) {
	if t != nil && !t.readOnly {
		if _, ok := t.reads[key]; !ok {
			t.reads[key] = version
		}
	}
}

func ts(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t) }
