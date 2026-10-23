// Package cosmos emulates Azure Cosmos DB for NoSQL in gateway mode: the
// database account, databases, containers (hash partition keys, unique keys,
// TTL), items with ETags and partial updates, SQL queries with paging and
// query plans, transactional batches, the change feed, throughput offers, and
// stored procedure/trigger/UDF definitions. SDKs connect to
// https://localhost:7644 with any key (the emulator's well-known key works);
// signatures are not checked. Each container is one partition key range.
package cosmos

import (
	"bytes"
	"cmp"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Cosmos DB emulator.
type Service struct {
	hub  *core.Hub
	box  *rds.Box // the MongoDB containers of MongoDB-API accounts
	mu   sync.Mutex
	st   state
	stop func()
}

type state struct {
	LSN    int64                     `json:"lsn"` // global log sequence number: session tokens, change feed
	DBs    map[string]*Database      `json:"dbs"`
	Offers map[string]map[string]any `json:"offers"`          // throughput by offer id
	ARM    armres.Store              `json:"arm,omitempty"`   // database accounts as the resource manager has them
	Mongo  map[string]*mongoServer   `json:"mongo,omitempty"` // MongoDB servers of MongoDB-API accounts, by lower-cased account ID
}

// Database holds containers.
type Database struct {
	Props map[string]any        `json:"props"`
	Colls map[string]*Container `json:"colls"`
}

// Container holds items keyed by partition key and id. Stored props and
// documents are replaced, never changed in place, so responses may share them.
type Container struct {
	Props   map[string]any                       `json:"props"`
	Paths   []string                             `json:"paths"`            // partition key paths
	Unique  [][]string                           `json:"unique,omitempty"` // unique key paths
	TTL     *float64                             `json:"ttl,omitempty"`    // defaultTtl; nil turns expiry off
	Items   map[string]*Item                     `json:"items"`
	Scripts map[string]map[string]map[string]any `json:"scripts,omitempty"` // sprocs, triggers, udfs by id
	Seq     uint64                               `json:"seq"`
}

// Item is a stored document.
type Item struct {
	PK  string         `json:"pk"`  // partition key value as a canonical JSON array
	Seq uint64         `json:"seq"` // creation order, encoded in the _rid
	LSN int64          `json:"lsn"`
	Doc map[string]any `json:"doc"`
}

// New creates the Cosmos DB service and starts its TTL sweeper.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.box = rds.NewBox(hub, "cosmos", "failed", &s.mu, s.owns)
	s.Reset()
	s.registerARM()
	s.stop = core.Every(time.Second, s.Tick)
	return s
}

// Close stops the TTL sweeper.
func (s *Service) Close() error {
	s.stop()
	s.mu.Lock()
	keep := s.containers()
	s.mu.Unlock()
	s.box.Close(keep)
	return nil
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "cosmos", Cloud: core.Azure, Title: "Cosmos DB", Protocol: "cosmos-json", Match: match}
}

// match claims Cosmos DB gateway traffic: Cosmos auth tokens ("type=master&ver=1.0&sig=…",
// URL-encoded) or x-ms-version on Cosmos paths. AWS and Google clients never send
// x-ms-version, and Azure Storage adds comp or restype to "/".
func match(r *http.Request) bool {
	if a := strings.ToLower(r.Header.Get("Authorization")); strings.HasPrefix(a, "type%3d") || strings.HasPrefix(a, "type=") {
		return true
	}
	p := r.URL.Path
	return r.Header.Get("x-ms-version") != "" && (p == "/" && r.URL.RawQuery == "" ||
		p == "/dbs" || strings.HasPrefix(p, "/dbs/") || p == "/offers" || strings.HasPrefix(p, "/offers/"))
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.DBs == nil {
		st.DBs = map[string]*Database{}
	}
	if st.Offers == nil {
		st.Offers = map[string]map[string]any{}
	}
	s.mu.Lock()
	old := s.containers()
	s.st = st
	keep := s.containers()
	docker := len(keep) > 0 && s.box.Available()
	for _, m := range s.st.Mongo {
		s.box.Resume(&m.Pogi, m.spec(), docker)
	}
	s.mu.Unlock()
	for n := range old { // containers of accounts the new state lacks
		if !keep[n] {
			s.box.Remove(n)
		}
	}
	return nil
}

// Tick deletes expired items (TTL).
func (s *Service) Tick(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, db := range s.st.DBs {
		for _, c := range db.Colls {
			if c.TTL == nil {
				continue
			}
			for k, it := range c.Items {
				if c.expired(it, now) {
					delete(c.Items, k)
					s.hub.MarkDirty()
				}
			}
		}
	}
}

func notFound() error {
	return core.Errorf(404, "NotFound", "Resource Not Found. Learn more: https://aka.ms/cosmosdb-tsg-not-found")
}
func conflict(msg string) error { return core.Errorf(409, "Conflict", "%s", msg) }
func preconditionFailed() error {
	return core.Errorf(412, "PreconditionFailed", "One of the specified pre-condition is not met.")
}
func badRequest(format string, a ...any) error { return core.Errorf(400, "BadRequest", format, a...) }

// handler runs under the service lock and returns the status and the body to encode.
type handler func() (int, any, error)

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	op, h := s.route(w, r)
	c.Op = op
	if h == nil {
		h = func() (int, any, error) {
			return 0, nil, badRequest("pogi does not implement %s %s", r.Method, r.URL.Path)
		}
	}
	core.Body(r) // read the body before taking the lock
	status, body, err := 0, []byte(nil), c.Intercept()
	s.mu.Lock()
	if err == nil {
		var v any
		if status, v, err = h(); err == nil && v != nil {
			body, err = encode(v)
		}
	}
	lsn := s.st.LSN
	s.mu.Unlock()

	hd := w.Header()
	hd.Set("Content-Type", "application/json")
	hd.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	hd.Set("x-ms-activity-id", cmp.Or(r.Header.Get("x-ms-activity-id"), core.UUID()))
	hd.Set("x-ms-request-charge", "1")
	hd.Set("x-ms-session-token", "0:1#"+strconv.FormatInt(lsn, 10))
	hd.Set("x-ms-schemaversion", "1.18")
	hd.Set("x-ms-serviceversion", "version=2.14.0.0")
	hd.Set("x-ms-gatewayversion", "version=2.14.0")
	if err != nil {
		e := core.AsError(err)
		c.ErrCode, status = e.Code, e.Status
		body, _ = encode(map[string]string{"code": e.Code, "message": e.Msg})
	}
	w.WriteHeader(status)
	w.Write(body)
}

func encode(v any) ([]byte, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	err := e.Encode(v)
	return b.Bytes(), err
}

func (s *Service) route(w http.ResponseWriter, r *http.Request) (string, handler) {
	seg := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if seg[0] == "" {
		seg = nil
	}
	n, m, q := len(seg), r.Method, isQuery(r)
	switch {
	case n == 0 && m == http.MethodGet:
		return "GetDatabaseAccount", func() (int, any, error) { return 200, account(r), nil }
	case n == 0:
	case seg[0] == "offers":
		switch {
		case n == 1 && (m == http.MethodGet || q):
			return "ReadOffers", func() (int, any, error) { return feed(w, r, "Offers", "", list(s.st.Offers, self)) }
		case n == 2 && m == http.MethodGet:
			return "ReadOffer", func() (int, any, error) {
				if o := s.st.Offers[seg[1]]; o != nil {
					return reply(w, 200, o)
				}
				return 0, nil, notFound()
			}
		case n == 2 && m == http.MethodPut:
			return "ReplaceOffer", func() (int, any, error) { return s.replaceOffer(w, r, seg[1]) }
		}
	case seg[0] != "dbs":
	case n == 1 && (m == http.MethodGet || q):
		return "ReadDatabases", func() (int, any, error) {
			return feed(w, r, "Databases", "", list(s.st.DBs, func(d *Database) map[string]any { return d.Props }))
		}
	case n == 1 && m == http.MethodPost:
		return "CreateDatabase", func() (int, any, error) { return s.createDB(w, r) }
	case n == 2 && m == http.MethodGet:
		return "ReadDatabase", func() (int, any, error) {
			if db := s.st.DBs[seg[1]]; db != nil {
				return reply(w, 200, db.Props)
			}
			return 0, nil, notFound()
		}
	case n == 2 && m == http.MethodDelete:
		return "DeleteDatabase", func() (int, any, error) { return s.deleteDB(seg[1]) }
	case n < 3 || seg[2] != "colls":
	case n == 3 && (m == http.MethodGet || q):
		return "ReadContainers", func() (int, any, error) {
			db := s.st.DBs[seg[1]]
			if db == nil {
				return 0, nil, notFound()
			}
			return feed(w, r, "DocumentCollections", db.Props["_rid"].(string), list(db.Colls, func(c *Container) map[string]any { return c.Props }))
		}
	case n == 3 && m == http.MethodPost:
		return "CreateContainer", func() (int, any, error) { return s.createColl(w, r, seg[1]) }
	case n == 4:
		return s.routeColl(w, r, seg[1], seg[3])
	case n > 4:
		return s.routeChild(w, r, seg)
	}
	return "", nil
}

func (s *Service) routeColl(w http.ResponseWriter, r *http.Request, db, id string) (string, handler) {
	switch r.Method {
	case http.MethodGet:
		return "ReadContainer", func() (int, any, error) {
			c, err := s.coll(db, id)
			if err != nil {
				return 0, nil, err
			}
			return reply(w, 200, c.Props)
		}
	case http.MethodPut:
		return "ReplaceContainer", func() (int, any, error) { return s.replaceColl(w, r, db, id) }
	case http.MethodDelete:
		return "DeleteContainer", func() (int, any, error) {
			c, err := s.coll(db, id)
			switch {
			case err != nil:
				return 0, nil, err
			case !matches(r.Header.Get("If-Match"), c.Props["_etag"]):
				return 0, nil, preconditionFailed()
			}
			delete(s.st.DBs[db].Colls, id)
			s.dropOffers(c.Props["_rid"])
			return 204, nil, nil
		}
	}
	return "", nil
}

var scripts = map[string]struct {
	feed, op string
	kind     uint64 // _rid type nibble
}{
	"sprocs":   {"StoredProcedures", "StoredProcedure", 8},
	"triggers": {"Triggers", "Trigger", 7},
	"udfs":     {"UserDefinedFunctions", "UserDefinedFunction", 6},
}

// routeChild routes /dbs/{db}/colls/{c}/{docs|pkranges|sprocs|triggers|udfs}[/{id}].
func (s *Service) routeChild(w http.ResponseWriter, r *http.Request, seg []string) (string, handler) {
	if len(seg) > 6 {
		return "", nil
	}
	kind, id, m := seg[4], "", r.Method
	if len(seg) == 6 {
		id = seg[5]
	}
	in := func(f func(c *Container) (int, any, error)) handler {
		return func() (int, any, error) {
			c, err := s.coll(seg[1], seg[3])
			if err != nil {
				return 0, nil, err
			}
			return f(c)
		}
	}
	switch {
	case kind == "pkranges" && id == "" && m == http.MethodGet:
		return "ReadPartitionKeyRanges", in(func(c *Container) (int, any, error) { return pkRanges(w, r, c) })
	case scripts[kind].op != "":
		verb := map[string]string{"GET": "Read", "POST": "Create", "PUT": "Replace", "DELETE": "Delete"}[m]
		switch {
		case isQuery(r):
			verb = "Query"
		case m == http.MethodPost && id != "":
			verb = "Execute"
		}
		return verb + scripts[kind].op, in(func(c *Container) (int, any, error) { return s.script(w, r, c, kind, id) })
	case kind != "docs":
	case id != "" && m == http.MethodGet:
		return "ReadItem", in(func(c *Container) (int, any, error) { return readItem(w, r, c, id) })
	case id != "" && m == http.MethodPut:
		return "ReplaceItem", in(func(c *Container) (int, any, error) { return s.writeItem(w, r, c, "replace", id) })
	case id != "" && m == http.MethodDelete:
		return "DeleteItem", in(func(c *Container) (int, any, error) {
			pk, err := needPK(r, c)
			if err == nil {
				err = s.remove(c, pk, id, r.Header.Get("If-Match"))
			}
			return 204, nil, err
		})
	case id != "" && m == http.MethodPatch:
		return "PatchItem", in(func(c *Container) (int, any, error) {
			pk, err := needPK(r, c)
			if err != nil {
				return 0, nil, err
			}
			it, err := s.patch(c, pk, id, core.Body(r), r.Header.Get("If-Match"))
			if err != nil {
				return 0, nil, err
			}
			return itemReply(w, r, 200, it)
		})
	case id != "":
	case m == http.MethodGet && r.Header.Get("A-IM") != "":
		return "ReadChangeFeed", in(func(c *Container) (int, any, error) { return s.changeFeed(w, r, c) })
	case m == http.MethodGet:
		return "ReadItems", in(func(c *Container) (int, any, error) { return queryItems(w, r, c) })
	case m != http.MethodPost:
	case isTrue(r, "x-ms-cosmos-is-query-plan-request"):
		return "QueryPlan", in(func(*Container) (int, any, error) { return queryPlan(r) })
	case isTrue(r, "x-ms-cosmos-is-batch-request"):
		return "Batch", in(func(c *Container) (int, any, error) { return s.batch(r, c) })
	case isQuery(r):
		return "QueryItems", in(func(c *Container) (int, any, error) { return queryItems(w, r, c) })
	case isTrue(r, "x-ms-documentdb-is-upsert"):
		return "UpsertItem", in(func(c *Container) (int, any, error) { return s.writeItem(w, r, c, "upsert", "") })
	default:
		return "CreateItem", in(func(c *Container) (int, any, error) { return s.writeItem(w, r, c, "create", "") })
	}
	return "", nil
}

func isTrue(r *http.Request, h string) bool { return strings.EqualFold(r.Header.Get(h), "true") }

func isQuery(r *http.Request) bool {
	return r.Method == http.MethodPost && (strings.HasPrefix(r.Header.Get("Content-Type"), "application/query+json") ||
		isTrue(r, "x-ms-documentdb-isquery") || isTrue(r, "x-ms-documentdb-query"))
}

const queryEngineConfig = `{"maxSqlQueryInputLength":262144,"maxJoinsPerSqlQuery":5,"maxLogicalAndPerSqlQuery":500,"maxLogicalOrPerSqlQuery":500,"maxUdfRefPerSqlQuery":10,"maxInExpressionItemsCount":16000,"queryTimeoutInSeconds":30,"maxDocumentsPerSqlQuery":1000,"maxQueryRequestTimeoutFraction":0.9,"sqlAllowNonFiniteNumbers":false,"sqlAllowAggregateFunctions":true,"sqlAllowSubQuery":true,"sqlAllowScalarSubQuery":true,"allowNewKeywords":true,"sqlAllowLike":true,"sqlAllowGroupByClause":true,"maxSpatialQueryCells":12,"spatialMaxGeometryPointCount":256,"sqlDisableQueryILOptimization":false,"sqlDisableFilterPlanOptimization":false}`

// account is the DatabaseAccount. Its one region points back at the address the
// client used: SDKs send every later request to that endpoint.
func account(r *http.Request) map[string]any {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	region := []any{map[string]any{"name": "South Central US", "databaseAccountEndpoint": scheme + "://" + r.Host + "/"}}
	return map[string]any{
		"id": "pogi", "_rid": "pogi", "_self": "", "media": "//media/", "addresses": "//addresses/", "_dbs": "//dbs/",
		"writableLocations": region, "readableLocations": region, "enableMultipleWriteLocations": false,
		"userReplicationPolicy":    map[string]any{"asyncReplication": false, "minReplicaSetSize": 1, "maxReplicasetSize": 4},
		"userConsistencyPolicy":    map[string]any{"defaultConsistencyLevel": "Session"},
		"systemReplicationPolicy":  map[string]any{"minReplicaSetSize": 1, "maxReplicasetSize": 4},
		"readPolicy":               map[string]any{"primaryReadCoefficient": 1, "secondaryReadCoefficient": 1},
		"queryEngineConfiguration": queryEngineConfig,
	}
}

// ---- resources ----

func etag() string { return `"` + core.UUID() + `"` }

// rid encodes a resource id the way Cosmos DB does: base64 with '-' for '/'.
func rid(b []byte) string { return strings.ReplaceAll(base64.StdEncoding.EncodeToString(b), "/", "-") }

func unrid(s string) []byte {
	b, _ := base64.StdEncoding.DecodeString(strings.ReplaceAll(s, "-", "/"))
	return b
}

// newRID appends four random bytes to a parent _rid ("" for a database); collections set the top bit.
func newRID(parent string, coll bool) string {
	b := make([]byte, 4)
	rand.Read(b)
	if coll {
		b[0] |= 0x80
	}
	return rid(append(unrid(parent), b...))
}

// childRID is the _rid of a container child: a document (kind 0), pkrange (5),
// udf (6), trigger (7) or sproc (8).
func childRID(parent string, kind, seq uint64) string {
	return rid(binary.LittleEndian.AppendUint64(unrid(parent), kind<<60|seq))
}

// stamp sets a resource's system properties.
func stamp(props map[string]any, rid, self string) map[string]any {
	props["_rid"], props["_self"], props["_etag"], props["_ts"] = rid, self, etag(), float64(time.Now().Unix())
	return props
}

// reply answers with a resource and its ETag.
func reply(w http.ResponseWriter, status int, props map[string]any) (int, any, error) {
	if e, ok := props["_etag"].(string); ok {
		w.Header().Set("etag", e)
	}
	return status, props, nil
}

func matches(ifMatch string, etag any) bool {
	return ifMatch == "" || ifMatch == "*" || ifMatch == etag
}

func self(m map[string]any) map[string]any { return m }

// list returns the resources of m ordered by id.
func list[V any](m map[string]V, props func(V) map[string]any) []any {
	out := []any{}
	for _, k := range core.SortedKeys(m) {
		out = append(out, props(m[k]))
	}
	return out
}

// parseDoc decodes a resource or document body (at most 2 MB, like Cosmos DB).
func parseDoc(b []byte) (map[string]any, error) {
	if len(b) > 2<<20 {
		return nil, core.Errorf(413, "RequestEntityTooLarge", "Request size is too large.")
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil || doc == nil {
		return nil, badRequest("The request payload is invalid: it must be a JSON object.")
	}
	return doc, nil
}

func checkID(props map[string]any) (string, error) {
	switch id, _ := props["id"].(string); {
	case id == "":
		return "", badRequest("The input content is invalid because the required properties - 'id; ' - are missing.")
	case strings.ContainsAny(id, `/\?#`) || len(id) > 255:
		return "", badRequest("The input name '%s' is invalid. Ensure to provide a unique non-empty string less than '255' characters without '/', '\\', '?' or '#'.", id)
	default:
		return id, nil
	}
}

// feed answers a read feed, or a query when the request carries one, a page at a time.
func feed(w http.ResponseWriter, r *http.Request, name, rid string, docs []any) (int, any, error) {
	if isQuery(r) {
		var err error
		if docs, err = runQuery(core.Body(r), docs); err != nil {
			return 0, nil, err
		}
	}
	skip := 0
	if t := r.Header.Get("x-ms-continuation"); t != "" {
		n, err := strconv.Atoi(strings.TrimPrefix(t, "skip:"))
		if err != nil || n < 0 {
			return 0, nil, badRequest("Invalid continuation token %q.", t)
		}
		skip = min(n, len(docs))
	}
	end := len(docs)
	if n := pageSize(r); n > 0 && skip+n < end {
		end = skip + n
		w.Header().Set("x-ms-continuation", "skip:"+strconv.Itoa(end))
	}
	page := append([]any{}, docs[skip:end]...)
	w.Header().Set("x-ms-item-count", strconv.Itoa(len(page)))
	return 200, map[string]any{"_rid": rid, name: page, "_count": len(page)}, nil
}

// pageSize is x-ms-max-item-count: 100 when absent, everything when -1.
func pageSize(r *http.Request) int {
	n, err := strconv.Atoi(r.Header.Get("x-ms-max-item-count"))
	if err != nil {
		return 100
	}
	return n
}

// runQuery runs the SQL query of a request body over docs.
func runQuery(body []byte, docs []any) ([]any, error) {
	var in struct {
		Query      string `json:"query"`
		Parameters []struct {
			Name  string `json:"name"`
			Value any    `json:"value"`
		} `json:"parameters"`
	}
	if json.Unmarshal(body, &in) != nil {
		in.Query = string(body) // application/sql
	}
	params := map[string]any{}
	for _, p := range in.Parameters {
		params[p.Name] = p.Value
	}
	out, err := execute(in.Query, params, docs)
	if err != nil {
		return nil, badRequest("%v", err)
	}
	return out, nil
}

// queryPlan answers SDKs that plan cross-partition queries. pogi runs the whole
// query in the container's one range, so the plan asks for no client-side
// ordering, aggregation, DISTINCT or TOP/OFFSET: SDKs pass results through.
func queryPlan(r *http.Request) (int, any, error) {
	var in struct {
		Query string `json:"query"`
	}
	json.Unmarshal(core.Body(r), &in)
	q, err := parseSQL(in.Query)
	if err != nil {
		return 0, nil, badRequest("%v", err)
	}
	return 200, map[string]any{
		"partitionedQueryExecutionInfoVersion": 2,
		"queryInfo": map[string]any{
			"distinctType": "None", "top": nil, "offset": nil, "limit": nil,
			"orderBy": []any{}, "orderByExpressions": []any{}, "groupByExpressions": []any{}, "groupByAliases": []any{},
			"aggregates": []any{}, "groupByAliasToAggregateType": map[string]any{}, "rewrittenQuery": "",
			"hasSelectValue": q.value, "dCountInfo": nil, "hasNonStreamingOrderBy": false,
		},
		"queryRanges": []any{map[string]any{"min": "", "max": "FF", "isMinInclusive": true, "isMaxInclusive": false}},
	}, nil
}

// ---- databases and containers ----

func (s *Service) createDB(w http.ResponseWriter, r *http.Request) (int, any, error) {
	props, err := parseDoc(core.Body(r))
	if err != nil {
		return 0, nil, err
	}
	id, err := checkID(props)
	if err != nil {
		return 0, nil, err
	}
	if s.st.DBs[id] != nil {
		return 0, nil, conflict("Resource with specified id or name already exists.")
	}
	rid := newRID("", false)
	props["_colls"], props["_users"] = "colls/", "users/"
	db := &Database{Props: stamp(props, rid, "dbs/"+rid+"/"), Colls: map[string]*Container{}}
	s.st.DBs[id] = db
	s.newOffer(r, db.Props, 0)
	return reply(w, 201, db.Props)
}

func (s *Service) deleteDB(id string) (int, any, error) {
	db := s.st.DBs[id]
	if db == nil {
		return 0, nil, notFound()
	}
	delete(s.st.DBs, id)
	s.dropOffers(db.Props["_rid"])
	for _, c := range db.Colls {
		s.dropOffers(c.Props["_rid"])
	}
	return 204, nil, nil
}

func (s *Service) coll(db, id string) (*Container, error) {
	if d := s.st.DBs[db]; d != nil && d.Colls[id] != nil {
		return d.Colls[id], nil
	}
	return nil, notFound()
}

func (c *Container) rid() string  { return c.Props["_rid"].(string) }
func (c *Container) self() string { return c.Props["_self"].(string) }

type collSpec struct {
	PartitionKey struct {
		Paths []string `json:"paths"`
	} `json:"partitionKey"`
	DefaultTTL      *float64 `json:"defaultTtl"`
	UniqueKeyPolicy struct {
		UniqueKeys []struct {
			Paths []string `json:"paths"`
		} `json:"uniqueKeys"`
	} `json:"uniqueKeyPolicy"`
}

// collProps parses a container body and fills in the service defaults.
func collProps(r *http.Request) (map[string]any, *collSpec, error) {
	props, err := parseDoc(core.Body(r))
	if err != nil {
		return nil, nil, err
	}
	var spec collSpec
	if err := json.Unmarshal(core.Body(r), &spec); err != nil {
		return nil, nil, badRequest("The container definition is invalid: %v", err)
	}
	for k, v := range map[string]any{
		"indexingPolicy": map[string]any{"indexingMode": "consistent", "automatic": true,
			"includedPaths": []any{map[string]any{"path": "/*"}}, "excludedPaths": []any{map[string]any{"path": `/"_etag"/?`}}},
		"conflictResolutionPolicy": map[string]any{"mode": "LastWriterWins", "conflictResolutionPath": "/_ts", "conflictResolutionProcedure": ""},
		"uniqueKeyPolicy":          map[string]any{"uniqueKeys": []any{}},
		"geospatialConfig":         map[string]any{"type": "Geography"},
	} {
		if props[k] == nil {
			props[k] = v
		}
	}
	return props, &spec, nil
}

func (s *Service) createColl(w http.ResponseWriter, r *http.Request, dbID string) (int, any, error) {
	db := s.st.DBs[dbID]
	if db == nil {
		return 0, nil, notFound()
	}
	props, spec, err := collProps(r)
	if err != nil {
		return 0, nil, err
	}
	id, err := checkID(props)
	switch {
	case err != nil:
		return 0, nil, err
	case len(spec.PartitionKey.Paths) == 0:
		return 0, nil, badRequest("A partition key definition (partitionKey.paths) is required.")
	case db.Colls[id] != nil:
		return 0, nil, conflict("Resource with specified id or name already exists.")
	}
	pk := props["partitionKey"].(map[string]any)
	if pk["kind"] == nil {
		pk["kind"] = map[bool]string{false: "Hash", true: "MultiHash"}[len(spec.PartitionKey.Paths) > 1]
	}
	if pk["version"] == nil {
		pk["version"] = 2
	}
	rid := newRID(db.Props["_rid"].(string), true)
	props["_docs"], props["_sprocs"], props["_triggers"], props["_udfs"], props["_conflicts"] = "docs/", "sprocs/", "triggers/", "udfs/", "conflicts/"
	c := &Container{Props: stamp(props, rid, db.Props["_self"].(string)+"colls/"+rid+"/"), Paths: spec.PartitionKey.Paths,
		TTL: spec.DefaultTTL, Items: map[string]*Item{}}
	for _, u := range spec.UniqueKeyPolicy.UniqueKeys {
		c.Unique = append(c.Unique, u.Paths)
	}
	db.Colls[id] = c
	def := 400 // a dedicated 400 RU/s unless the database shares its throughput
	for _, o := range s.st.Offers {
		if o["offerResourceId"] == db.Props["_rid"] {
			def = 0
		}
	}
	s.newOffer(r, c.Props, def)
	return reply(w, 201, c.Props)
}

func (s *Service) replaceColl(w http.ResponseWriter, r *http.Request, db, id string) (int, any, error) {
	c, err := s.coll(db, id)
	if err != nil {
		return 0, nil, err
	}
	props, spec, err := collProps(r)
	if err != nil {
		return 0, nil, err
	}
	if !matches(r.Header.Get("If-Match"), c.Props["_etag"]) {
		return 0, nil, preconditionFailed()
	}
	for _, k := range []string{"id", "partitionKey", "uniqueKeyPolicy", "_docs", "_sprocs", "_triggers", "_udfs", "_conflicts"} {
		props[k] = c.Props[k] // immutable
	}
	c.Props, c.TTL = stamp(props, c.rid(), c.self()), spec.DefaultTTL
	return reply(w, 200, c.Props)
}

// ---- offers ----

// newOffer provisions throughput for a new database or container from the
// request (manual or autoscale), else def RU/s (0: none).
func (s *Service) newOffer(r *http.Request, res map[string]any, def int) {
	content := map[string]any{"offerThroughput": def, "offerIsRUPerMinuteThroughputEnabled": false}
	if v := r.Header.Get("x-ms-offer-throughput"); v != "" {
		content["offerThroughput"], _ = strconv.Atoi(v)
	} else if v := r.Header.Get("x-ms-cosmos-offer-autopilot-settings"); v != "" {
		var auto map[string]any
		json.Unmarshal([]byte(v), &auto)
		top, _ := auto["maxThroughput"].(float64)
		content["offerThroughput"], content["offerAutopilotSettings"] = int(top)/10, auto
	} else if def == 0 {
		return
	}
	id := core.RandString(4, core.AlphaNum)
	s.st.Offers[id] = stamp(map[string]any{"id": id, "offerVersion": "V2", "offerType": "Invalid", "content": content,
		"resource": res["_self"], "offerResourceId": res["_rid"]}, id, "offers/"+id+"/")
}

func (s *Service) replaceOffer(w http.ResponseWriter, r *http.Request, id string) (int, any, error) {
	o := s.st.Offers[id]
	if o == nil {
		return 0, nil, notFound()
	}
	var in struct {
		Content map[string]any `json:"content"`
	}
	if err := json.Unmarshal(core.Body(r), &in); err != nil || in.Content == nil {
		return 0, nil, badRequest("The offer content is invalid.")
	}
	if auto, ok := in.Content["offerAutopilotSettings"].(map[string]any); ok && in.Content["offerThroughput"] == nil {
		top, _ := auto["maxThroughput"].(float64)
		in.Content["offerThroughput"] = int(top) / 10
	}
	o = maps.Clone(o)
	o["content"] = in.Content
	s.st.Offers[id] = stamp(o, id, o["_self"].(string))
	return reply(w, 200, o)
}

func (s *Service) dropOffers(rid any) {
	maps.DeleteFunc(s.st.Offers, func(_ string, o map[string]any) bool { return o["offerResourceId"] == rid })
}

// ---- stored procedures, triggers, UDFs (stored, never run) ----

func (s *Service) script(w http.ResponseWriter, r *http.Request, c *Container, kind, id string) (int, any, error) {
	if c.Scripts == nil {
		c.Scripts = map[string]map[string]map[string]any{}
	}
	all := c.Scripts[kind]
	if all == nil {
		all = map[string]map[string]any{}
		c.Scripts[kind] = all
	}
	m := r.Method
	switch {
	case id == "" && (m == http.MethodGet || isQuery(r)):
		return feed(w, r, scripts[kind].feed, c.rid(), list(all, self))
	case id == "" && m == http.MethodPost:
		props, err := parseDoc(core.Body(r))
		if err != nil {
			return 0, nil, err
		}
		if id, err = checkID(props); err != nil {
			return 0, nil, err
		}
		if all[id] != nil {
			return 0, nil, conflict("Resource with specified id or name already exists.")
		}
		c.Seq++
		rid := childRID(c.rid(), scripts[kind].kind, c.Seq)
		all[id] = stamp(props, rid, c.self()+kind+"/"+rid+"/")
		return reply(w, 201, all[id])
	}
	old := all[id]
	switch {
	case old == nil:
		return 0, nil, notFound()
	case m == http.MethodGet:
		return reply(w, 200, old)
	case m == http.MethodPut:
		props, err := parseDoc(core.Body(r))
		if err != nil {
			return 0, nil, err
		}
		props["id"] = id
		all[id] = stamp(props, old["_rid"].(string), old["_self"].(string))
		return reply(w, 200, all[id])
	case m == http.MethodDelete:
		delete(all, id)
		return 204, nil, nil
	case m == http.MethodPost && kind == "sprocs":
		return 0, nil, badRequest("pogi stores stored procedures but does not execute them.")
	}
	return 0, nil, badRequest("pogi does not implement %s %s", m, r.URL.Path)
}

// ---- items ----

// hdrPK is the partition key header as a canonical JSON array, "" when absent.
func hdrPK(r *http.Request, c *Container) (string, error) {
	return canonPK(r.Header.Get("x-ms-documentdb-partitionkey"), c)
}

// maxPKBytes is Cosmos DB's limit for a partition key value.
const maxPKBytes = 2048

// canonPK checks the header against the container's key paths: no more values than paths, and not too long.
func canonPK(h string, c *Container) (string, error) {
	var v []any
	switch {
	case h == "":
		return "", nil
	case json.Unmarshal([]byte(h), &v) != nil:
		return "", badRequest("The partition key %s is invalid.", h)
	case len(v) == 0:
		return "", nil
	case len(v) > len(c.Paths), len(h) > maxPKBytes:
		return "", badRequest("Partition key provided either doesn't correspond to definition in the collection or doesn't match partition key field values specified in the document.")
	}
	return canon(v), nil
}

func needPK(r *http.Request, c *Container) (string, error) {
	pk, err := hdrPK(r, c)
	if err == nil && pk == "" {
		err = badRequest("PartitionKey value must be supplied for this operation.")
	}
	return pk, err
}

func canon(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// keyOf encodes the values at key paths like "/address/city"; a missing value
// is undefined, which encodes as {} like the partition key header does.
func keyOf(doc map[string]any, paths []string) string {
	vals := make([]any, len(paths))
	for i, p := range paths {
		var v any = doc
		for _, k := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
			v = member(v, k)
		}
		vals[i] = v
	}
	return canon(vals)
}

func (c *Container) expired(it *Item, now time.Time) bool {
	if c.TTL == nil {
		return false
	}
	ttl := *c.TTL
	if v, ok := it.Doc["ttl"].(float64); ok {
		ttl = v
	}
	ts, _ := it.Doc["_ts"].(float64)
	return ttl > 0 && float64(now.Unix()) >= ts+ttl
}

func (c *Container) item(pk, id string, now time.Time) *Item {
	if it := c.Items[pk+"/"+id]; it != nil && !c.expired(it, now) {
		return it
	}
	return nil
}

// live returns the live items of partition pk ("" for all; a prefix of a
// hierarchical key matches its sub-partitions) in creation order.
func (c *Container) live(pk string, now time.Time) []*Item {
	var out []*Item
	prefix := strings.TrimSuffix(pk, "]") + ","
	for _, it := range c.Items {
		if (pk == "" || it.PK == pk || strings.HasPrefix(it.PK, prefix)) && !c.expired(it, now) {
			out = append(out, it)
		}
	}
	slices.SortFunc(out, func(a, b *Item) int { return cmp.Compare(a.Seq, b.Seq) })
	return out
}

func (c *Container) docs(pk string, now time.Time) []any {
	out := []any{}
	for _, it := range c.live(pk, now) {
		out = append(out, it.Doc)
	}
	return out
}

func queryItems(w http.ResponseWriter, r *http.Request, c *Container) (int, any, error) {
	pk, err := hdrPK(r, c)
	if err != nil {
		return 0, nil, err
	}
	return feed(w, r, "Documents", c.rid(), c.docs(pk, time.Now()))
}

func readItem(w http.ResponseWriter, r *http.Request, c *Container, id string) (int, any, error) {
	pk, err := needPK(r, c)
	if err != nil {
		return 0, nil, err
	}
	it := c.item(pk, id, time.Now())
	if it == nil {
		return 0, nil, notFound()
	}
	if r.Header.Get("If-None-Match") == it.Doc["_etag"] {
		w.Header().Set("etag", it.Doc["_etag"].(string))
		return http.StatusNotModified, nil, nil
	}
	return reply(w, 200, it.Doc)
}

func (s *Service) writeItem(w http.ResponseWriter, r *http.Request, c *Container, mode, id string) (int, any, error) {
	pk, err := hdrPK(r, c)
	if err != nil {
		return 0, nil, err
	}
	doc, err := parseDoc(core.Body(r))
	if err != nil {
		return 0, nil, err
	}
	it, status, err := s.put(c, mode, pk, id, doc, r.Header.Get("If-Match"))
	if err != nil {
		return 0, nil, err
	}
	return itemReply(w, r, status, it)
}

// itemReply answers a write with the item, or only its headers for Prefer: return=minimal.
func itemReply(w http.ResponseWriter, r *http.Request, status int, it *Item) (int, any, error) {
	if r.Header.Get("Prefer") == "return=minimal" {
		w.Header().Set("etag", it.Doc["_etag"].(string))
		return status, nil, nil
	}
	return reply(w, status, it.Doc)
}

// put creates, upserts or replaces (mode) doc in partition pk ("" takes the key
// from doc) and stamps its system properties.
func (s *Service) put(c *Container, mode, pk, id string, doc map[string]any, ifMatch string) (*Item, int, error) {
	did, err := checkID(doc)
	if err != nil {
		return nil, 0, err
	}
	if mode == "replace" && did != id {
		return nil, 0, badRequest("The id '%s' in the body does not match the id '%s' in the URL.", did, id)
	}
	if dpk := keyOf(doc, c.Paths); pk == "" {
		pk = dpk
	} else if pk != dpk {
		return nil, 0, badRequest("PartitionKey extracted from document doesn't match the one specified in the header. Learn more: https://aka.ms/CosmosDB/sql/errors/wrong-pk-value")
	}
	now := time.Now()
	old := c.item(pk, did, now)
	switch {
	case mode == "create" && old != nil:
		return nil, 0, conflict("Entity with the specified id already exists in the system.")
	case mode == "replace" && old == nil:
		return nil, 0, notFound()
	case old != nil && !matches(ifMatch, old.Doc["_etag"]):
		return nil, 0, preconditionFailed()
	}
	for _, paths := range c.Unique {
		k := keyOf(doc, paths)
		for _, o := range c.Items {
			if o.PK == pk && o.Doc["id"] != did && !c.expired(o, now) && keyOf(o.Doc, paths) == k {
				return nil, 0, conflict("Unique index constraint violation.")
			}
		}
	}
	s.st.LSN++
	it, status := &Item{PK: pk, LSN: s.st.LSN, Doc: doc}, http.StatusCreated
	if old != nil {
		it.Seq, status = old.Seq, http.StatusOK
	} else {
		c.Seq++
		it.Seq = c.Seq
	}
	rid := childRID(c.rid(), 0, it.Seq)
	stamp(doc, rid, c.self()+"docs/"+rid+"/")["_attachments"] = "attachments/"
	c.Items[pk+"/"+did] = it
	return it, status, nil
}

func (s *Service) remove(c *Container, pk, id, ifMatch string) error {
	it := c.item(pk, id, time.Now())
	switch {
	case it == nil:
		return notFound()
	case !matches(ifMatch, it.Doc["_etag"]):
		return preconditionFailed()
	}
	delete(c.Items, pk+"/"+id)
	s.st.LSN++
	return nil
}

type patchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	From  string `json:"from"`
	Value any    `json:"value"`
}

// patch applies partial-update operations (all or nothing) and an optional
// "from c where …" condition.
func (s *Service) patch(c *Container, pk, id string, body []byte, ifMatch string) (*Item, error) {
	var spec struct {
		Condition  string    `json:"condition"`
		Operations []patchOp `json:"operations"`
	}
	if err := json.Unmarshal(body, &spec); err != nil || len(spec.Operations) == 0 || len(spec.Operations) > 10 {
		return nil, badRequest("A patch request needs 1 to 10 operations.")
	}
	it := c.item(pk, id, time.Now())
	switch {
	case it == nil:
		return nil, notFound()
	case !matches(ifMatch, it.Doc["_etag"]):
		return nil, preconditionFailed()
	}
	if spec.Condition != "" {
		res, err := execute("SELECT * "+spec.Condition, nil, []any{it.Doc})
		if err != nil {
			return nil, badRequest("%v", err)
		}
		if len(res) == 0 {
			return nil, preconditionFailed()
		}
	}
	doc := clone(it.Doc).(map[string]any)
	for _, op := range spec.Operations {
		if err := op.apply(doc); err != nil {
			return nil, badRequest("Patch operation '%s' at '%s' failed: %v", op.Op, op.Path, err)
		}
	}
	nit, _, err := s.put(c, "replace", pk, id, doc, "")
	return nit, err
}

var errPath = errors.New("the path does not exist")

func (p patchOp) apply(doc map[string]any) error {
	if !strings.Contains(" add set replace remove incr move ", " "+p.Op+" ") {
		return errors.New("unknown operation")
	}
	if p.Op == "move" {
		v, err := patchAt(doc, p.From, "remove", nil)
		if err == nil {
			_, err = patchAt(doc, p.Path, "add", v)
		}
		return err
	}
	_, err := patchAt(doc, p.Path, p.Op, p.Value)
	return err
}

// patchAt applies op at the JSON pointer ptr and returns the value it removed.
func patchAt(doc map[string]any, ptr, op string, val any) (any, error) {
	if !strings.HasPrefix(ptr, "/") {
		return nil, errPath
	}
	keys := strings.Split(ptr[1:], "/")
	for i, k := range keys {
		keys[i] = strings.ReplaceAll(strings.ReplaceAll(k, "~1", "/"), "~0", "~")
	}
	var removed any
	_, err := edit(doc, keys, op, val, &removed)
	return removed, err
}

// edit applies op at keys below v and returns v's new value (arrays may move).
func edit(v any, keys []string, op string, val any, removed *any) (any, error) {
	k, last := keys[0], len(keys) == 1
	switch x := v.(type) {
	case map[string]any:
		old, ok := x[k]
		switch {
		case !last:
			if !ok {
				return nil, errPath
			}
			nv, err := edit(old, keys[1:], op, val, removed)
			if err != nil {
				return nil, err
			}
			x[k] = nv
		case op == "add" || op == "set" || op == "replace" && ok:
			x[k] = val
		case op == "remove" && ok:
			*removed = old
			delete(x, k)
		case op == "incr":
			sum, err := incr(old, ok, val)
			if err != nil {
				return nil, err
			}
			x[k] = sum
		default:
			return nil, errPath
		}
		return x, nil
	case []any:
		i, err := strconv.Atoi(k)
		if k == "-" {
			i, err = len(x), nil
		}
		if err != nil || i < 0 || i > len(x) || i == len(x) && !(last && (op == "add" || op == "set")) {
			return nil, errPath
		}
		switch {
		case !last:
			nv, err := edit(x[i], keys[1:], op, val, removed)
			if err != nil {
				return nil, err
			}
			x[i] = nv
		case op == "add":
			return slices.Insert(x, i, val), nil
		case i == len(x): // set appends
			return append(x, val), nil
		case op == "remove":
			*removed = x[i]
			return slices.Delete(x, i, i+1), nil
		case op == "incr":
			sum, err := incr(x[i], true, val)
			if err != nil {
				return nil, err
			}
			x[i] = sum
		default: // set, replace
			x[i] = val
		}
		return x, nil
	}
	return nil, errPath
}

func incr(old any, exists bool, by any) (any, error) {
	b, ok := by.(float64)
	if !ok {
		return nil, errors.New("incr needs a number")
	}
	if !exists {
		return b, nil
	}
	a, ok := old.(float64)
	if !ok {
		return nil, errors.New("incr target is not a number")
	}
	return a + b, nil
}

func clone(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, e := range x {
			m[k] = clone(e)
		}
		return m
	case []any:
		s := make([]any, len(x))
		for i, e := range x {
			s[i] = clone(e)
		}
		return s
	}
	return v
}

// ---- transactional batch ----

type batchOp struct {
	OperationType string          `json:"operationType"`
	ID            string          `json:"id"`
	PartitionKey  string          `json:"partitionKey"` // non-atomic (bulk) requests
	IfMatch       string          `json:"ifMatch"`
	ResourceBody  json.RawMessage `json:"resourceBody"`
}

// batch runs operations on one partition. Atomic batches are all or nothing:
// on failure the state is rolled back and the answer is 207 with the failed
// operation's status and 424 for the others.
func (s *Service) batch(r *http.Request, c *Container) (int, any, error) {
	var ops []batchOp
	if err := json.Unmarshal(core.Body(r), &ops); err != nil || len(ops) == 0 || len(ops) > 100 {
		return 0, nil, badRequest("A batch request needs 1 to 100 operations.")
	}
	pk, err := hdrPK(r, c)
	if err != nil {
		return 0, nil, err
	}
	atomic := !strings.EqualFold(r.Header.Get("x-ms-cosmos-batch-atomic"), "false")
	minimal := r.Header.Get("Prefer") == "return=minimal"
	items, lsn, seq := maps.Clone(c.Items), s.st.LSN, c.Seq
	out := make([]map[string]any, len(ops))
	failed := -1
	for i, op := range ops {
		status, it, err := s.batchOp(c, pk, op)
		out[i] = map[string]any{"statusCode": status, "requestCharge": 1}
		if err != nil {
			out[i]["statusCode"] = core.AsError(err).Status
			if failed = i; atomic {
				break
			}
			continue
		}
		if it != nil {
			out[i]["eTag"] = it.Doc["_etag"]
			if !minimal || strings.EqualFold(op.OperationType, "read") {
				out[i]["resourceBody"] = it.Doc
			}
		}
	}
	if failed < 0 || !atomic {
		return 200, out, nil
	}
	c.Items, s.st.LSN, c.Seq = items, lsn, seq
	for i := range out {
		if i != failed {
			out[i] = map[string]any{"statusCode": 424, "requestCharge": 0}
		}
	}
	return http.StatusMultiStatus, out, nil
}

func (s *Service) batchOp(c *Container, pk string, op batchOp) (int, *Item, error) {
	if op.PartitionKey != "" {
		var err error
		if pk, err = canonPK(op.PartitionKey, c); err != nil {
			return 0, nil, err
		}
	}
	switch kind := strings.ToLower(op.OperationType); {
	case kind == "create" || kind == "upsert" || kind == "replace":
		doc, err := parseDoc(op.ResourceBody)
		if err != nil {
			return 0, nil, err
		}
		it, status, err := s.put(c, kind, pk, op.ID, doc, op.IfMatch)
		return status, it, err
	case pk == "":
		return 0, nil, badRequest("PartitionKey value must be supplied for this operation.")
	case kind == "read":
		if it := c.item(pk, op.ID, time.Now()); it != nil {
			return 200, it, nil
		}
		return 0, nil, notFound()
	case kind == "delete":
		return 204, nil, s.remove(c, pk, op.ID, op.IfMatch)
	case kind == "patch":
		it, err := s.patch(c, pk, op.ID, op.ResourceBody, op.IfMatch)
		return 200, it, err
	}
	return 0, nil, badRequest("Unknown batch operation '%s'.", op.OperationType)
}

// ---- change feed and partition key ranges ----

// changeFeed returns the latest version of items changed after the
// If-None-Match LSN (none: from the beginning, "*": from now), oldest first;
// the ETag is the continuation and 304 means nothing changed.
func (s *Service) changeFeed(w http.ResponseWriter, r *http.Request, c *Container) (int, any, error) {
	pk, err := hdrPK(r, c)
	if err != nil {
		return 0, nil, err
	}
	var since int64
	var after time.Time
	switch inm := r.Header.Get("If-None-Match"); {
	case inm == "*":
		since = s.st.LSN
	case inm != "":
		if since, err = strconv.ParseInt(strings.Trim(inm, `"`), 10, 64); err != nil {
			return 0, nil, badRequest("Invalid change feed continuation %s.", inm)
		}
	default:
		after, _ = time.Parse(time.RFC1123, r.Header.Get("If-Modified-Since"))
	}
	var changed []*Item
	for _, it := range c.live(pk, time.Now()) {
		if ts, _ := it.Doc["_ts"].(float64); it.LSN > since && ts >= float64(after.Unix()) {
			changed = append(changed, it)
		}
	}
	slices.SortFunc(changed, func(a, b *Item) int { return cmp.Compare(a.LSN, b.LSN) })
	if n := pageSize(r); n > 0 && len(changed) > n {
		changed = changed[:n]
	}
	if len(changed) == 0 {
		w.Header().Set("etag", strconv.Quote(strconv.FormatInt(max(since, s.st.LSN), 10)))
		return http.StatusNotModified, nil, nil
	}
	docs := make([]any, len(changed))
	for i, it := range changed {
		d := maps.Clone(it.Doc)
		d["_lsn"] = it.LSN
		docs[i] = d
	}
	w.Header().Set("etag", strconv.Quote(strconv.FormatInt(changed[len(changed)-1].LSN, 10)))
	w.Header().Set("x-ms-item-count", strconv.Itoa(len(docs)))
	return 200, map[string]any{"_rid": c.rid(), "Documents": docs, "_count": len(docs)}, nil
}

// pkRanges lists the container's single range; as a change feed (A-IM) a
// known ETag means "not modified".
func pkRanges(w http.ResponseWriter, r *http.Request, c *Container) (int, any, error) {
	w.Header().Set("etag", `"1"`)
	if r.Header.Get("A-IM") != "" && r.Header.Get("If-None-Match") != "" {
		return http.StatusNotModified, nil, nil
	}
	rid := childRID(c.rid(), 5, 1)
	pr := map[string]any{"id": "0", "_rid": rid, "_self": c.self() + "pkranges/" + rid + "/", "_etag": `"00000000-0000-0000-0000-000000000000"`,
		"minInclusive": "", "maxExclusive": "FF", "ridPrefix": 0, "throughputFraction": 1, "status": "online",
		"parents": []any{}, "_ts": c.Props["_ts"], "lsn": 1}
	w.Header().Set("x-ms-item-count", "1")
	return 200, map[string]any{"_rid": c.rid(), "PartitionKeyRanges": []any{pr}, "_count": 1}, nil
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	dbs := core.Table{Name: "Databases", Columns: []string{"Name", "Containers"}}
	colls := core.Table{Name: "Containers", Columns: []string{"Name", "Database", "Items", "Partition key"}}
	now := time.Now()
	for _, dn := range core.SortedKeys(s.st.DBs) {
		db := s.st.DBs[dn]
		dbs.Rows = append(dbs.Rows, []string{dn, strconv.Itoa(len(db.Colls))})
		for _, cn := range core.SortedKeys(db.Colls) {
			c := db.Colls[cn]
			colls.Rows = append(colls.Rows, []string{cn, dn, strconv.Itoa(len(c.live("", now))), strings.Join(c.Paths, ", ")})
		}
	}
	return []core.Table{dbs, colls}
}

// Inspect returns a database, or a container's settings and first 20 items
// (id is the container name, or "db/container" when names repeat).
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if table == "Databases" {
		if db := s.st.DBs[id]; db != nil {
			return map[string]any{"database": db.Props, "containers": core.SortedKeys(db.Colls)}
		}
		return nil
	}
	dbName, cn, ok := strings.Cut(id, "/")
	for _, dn := range core.SortedKeys(s.st.DBs) {
		c := s.st.DBs[dn].Colls[id]
		if ok && dn == dbName {
			c = s.st.DBs[dn].Colls[cn]
		}
		if c != nil {
			docs := c.docs("", time.Now())
			return map[string]any{"database": dn, "container": c.Props, "count": len(docs), "items": docs[:min(20, len(docs))]}
		}
	}
	return nil
}

// Query runs a SQL query across all partitions of a container.
func (s *Service) Query(db, container, sql string) ([]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := s.coll(db, container)
	if err != nil {
		return nil, err
	}
	return execute(sql, nil, c.docs("", time.Now()))
}

// Upsert writes an item, taking its partition key from the item.
func (s *Service) Upsert(db, container string, item map[string]any) error {
	b, err := json.Marshal(item)
	if err != nil {
		return err
	}
	doc, err := parseDoc(b)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := s.coll(db, container)
	if err == nil {
		_, _, err = s.put(c, "upsert", "", "", doc, "")
	}
	s.hub.MarkDirty()
	return err
}

// Containers lists every (database, container) pair.
func (s *Service) Containers() [][2]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out [][2]string
	for _, dn := range core.SortedKeys(s.st.DBs) {
		for _, cn := range core.SortedKeys(s.st.DBs[dn].Colls) {
			out = append(out, [2]string{dn, cn})
		}
	}
	return out
}
