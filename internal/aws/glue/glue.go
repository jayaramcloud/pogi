// Package glue emulates the AWS Glue Data Catalog over JSON 1.1: databases,
// tables with versions, partitions with expression filters, and crawlers that
// infer tables from CSV, JSON and Parquet files in S3. Jobs, connections,
// triggers and user-defined functions are metadata only. The Schema
// Registry is in registry.go.
package glue

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// obj is a resource as the API shows it: what the caller sent plus what Glue
// fills in. Stored objects are never changed in place; a change stores a copy.
type obj = map[string]any

// Service is the Glue emulator.
type Service struct {
	hub    *core.Hub
	rpc    *core.RPC
	ctx    context.Context // crawls stop with it
	cancel context.CancelFunc
	wg     sync.WaitGroup // running crawls

	mu    sync.Mutex
	st    state
	stops map[string]context.CancelFunc // running crawls by crawler key
}

type state struct {
	// Objects are databases, crawlers and the metadata-only resources by
	// kind/account/region/name (functions …/database/name, job runs …/job/id).
	Objects map[string]obj               `json:"objects"`
	Tables  map[string]*table            `json:"tables"` // account/region/database/name
	Tags    map[string]map[string]string `json:"tags"`   // account/region/resource, e.g. database/sales
}

// table is a table's versions (the current one last), partitions and indexes.
type table struct {
	Versions   []obj          `json:"versions"`
	Partitions map[string]obj `json:"partitions"` // values joined by \x00
	Indexes    []obj          `json:"indexes,omitempty"`
}

func (t *table) cur() obj { return t.Versions[len(t.Versions)-1] }

// keys are the names of the table's partition keys.
func (t *table) keys() []string {
	var out []string
	for _, k := range list(t.cur()["PartitionKeys"]) {
		out = append(out, str(k["Name"]))
	}
	return out
}

// New creates the Glue service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, stops: map[string]context.CancelFunc{}}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateDatabase":                   core.H(s.createDatabase),
		"GetDatabase":                      core.H(s.getDatabase),
		"GetDatabases":                     s.lister("Database", "DatabaseList"),
		"UpdateDatabase":                   core.H(s.updateDatabase),
		"DeleteDatabase":                   core.H(s.deleteDatabase),
		"CreateTable":                      core.H(s.createTable),
		"GetTable":                         core.H(s.getTable),
		"GetTables":                        core.H(s.getTables),
		"UpdateTable":                      core.H(s.updateTable),
		"DeleteTable":                      core.H(s.deleteTable),
		"BatchDeleteTable":                 core.H(s.batchDeleteTable),
		"GetTableVersions":                 core.H(s.getTableVersions),
		"GetTableVersion":                  core.H(s.getTableVersion),
		"GetPartitionIndexes":              core.H(s.getPartitionIndexes),
		"CreatePartition":                  core.H(s.createPartition),
		"BatchCreatePartition":             core.H(s.batchCreatePartition),
		"GetPartition":                     core.H(s.getPartition),
		"GetPartitions":                    core.H(s.getPartitions),
		"BatchGetPartition":                core.H(s.batchGetPartition),
		"UpdatePartition":                  core.H(s.updatePartition),
		"DeletePartition":                  core.H(s.deletePartition),
		"BatchDeletePartition":             core.H(s.batchDeletePartition),
		"CreateCrawler":                    core.H(s.createCrawler),
		"GetCrawler":                       s.getter("Crawler", "Name", "Crawler"),
		"GetCrawlers":                      s.lister("Crawler", "Crawlers"),
		"UpdateCrawler":                    core.H(s.updateCrawler),
		"DeleteCrawler":                    core.H(s.deleteCrawler),
		"StartCrawler":                     core.H(s.startCrawler),
		"StopCrawler":                      core.H(s.stopCrawler),
		"GetCrawlerMetrics":                core.H(s.getCrawlerMetrics),
		"CreateJob":                        core.H(s.createJob),
		"GetJob":                           s.getter("Job", "JobName", "Job"),
		"GetJobs":                          s.lister("Job", "Jobs"),
		"DeleteJob":                        core.H(s.deleteJob),
		"StartJobRun":                      core.H(s.startJobRun),
		"GetJobRun":                        core.H(s.getJobRun),
		"GetJobRuns":                       core.H(s.getJobRuns),
		"CreateConnection":                 core.H(s.createConnection),
		"GetConnection":                    s.getter("Connection", "Name", "Connection"),
		"GetConnections":                   s.lister("Connection", "ConnectionList"),
		"UpdateConnection":                 core.H(s.updateConnection),
		"DeleteConnection":                 core.H(s.deleteConnection),
		"CreateTrigger":                    core.H(s.createTrigger),
		"GetTrigger":                       s.getter("Trigger", "Name", "Trigger"),
		"GetTriggers":                      s.lister("Trigger", "Triggers"),
		"UpdateTrigger":                    core.H(s.updateTrigger),
		"DeleteTrigger":                    core.H(s.deleteTrigger),
		"StartTrigger":                     core.H(s.triggerState("ACTIVATED")),
		"StopTrigger":                      core.H(s.triggerState("DEACTIVATED")),
		"CreateUserDefinedFunction":        core.H(s.createFunction),
		"GetUserDefinedFunction":           core.H(s.getFunction),
		"GetUserDefinedFunctions":          core.H(s.getFunctions),
		"UpdateUserDefinedFunction":        core.H(s.updateFunction),
		"DeleteUserDefinedFunction":        core.H(s.deleteFunction),
		"TagResource":                      core.H(s.tagResource),
		"UntagResource":                    core.H(s.tagResource),
		"GetTags":                          core.H(s.getTags),
		"CreateRegistry":                   core.H(s.createRegistry),
		"GetRegistry":                      core.H(s.getRegistry),
		"ListRegistries":                   core.H(s.listRegistries),
		"UpdateRegistry":                   core.H(s.updateRegistry),
		"DeleteRegistry":                   core.H(s.deleteRegistry),
		"CreateSchema":                     core.H(s.createSchema),
		"GetSchema":                        core.H(s.getSchema),
		"ListSchemas":                      core.H(s.listSchemas),
		"UpdateSchema":                     core.H(s.updateSchema),
		"DeleteSchema":                     core.H(s.deleteSchema),
		"RegisterSchemaVersion":            core.H(s.registerVersion),
		"GetSchemaVersion":                 core.H(s.getVersion),
		"ListSchemaVersions":               core.H(s.listVersions),
		"GetSchemaByDefinition":            core.H(s.schemaByDefinition),
		"CheckSchemaVersionValidity":       core.H(s.checkValidity),
		"DeleteSchemaVersions":             core.H(s.deleteVersions),
		"PutSchemaVersionMetadata":         core.H(s.putMeta),
		"RemoveSchemaVersionMetadata":      core.H(s.removeMeta),
		"QuerySchemaVersionMetadata":       core.H(s.queryMeta),
		"GetDataCatalogEncryptionSettings": core.H(s.getEncryption),
		"PutDataCatalogEncryptionSettings": core.H(s.putEncryption),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "glue", Cloud: core.AWS, Title: "Glue", Targets: []string{"AWSGlue"}, Signing: []string{"glue"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Objects: map[string]obj{}, Tables: map[string]*table{}, Tags: map[string]map[string]string{}}
	s.mu.Unlock()
}

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
	s.Reset()
	s.mu.Lock()
	defer s.mu.Unlock()
	maps.Copy(s.st.Objects, st.Objects)
	maps.Copy(s.st.Tables, st.Tables)
	maps.Copy(s.st.Tags, st.Tags)
	for k, o := range s.st.Objects { // a crawl that a restart cut short
		if strings.HasPrefix(k, "Crawler/") && o["State"] != "READY" {
			o["State"] = "READY"
		}
	}
	return nil
}

// Close stops running crawls.
func (s *Service) Close() error {
	s.cancel()
	s.wg.Wait()
	return nil
}

func notFound(f string, a ...any) error { return core.Errorf(400, "EntityNotFoundException", f, a...) }
func exists(f string, a ...any) error   { return core.Errorf(400, "AlreadyExistsException", f, a...) }
func invalid(f string, a ...any) error  { return core.Errorf(400, "InvalidInputException", f, a...) }

func code(err error) string {
	var e *core.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

func key(c *core.Ctx, parts ...string) string {
	return c.Account + "/" + c.Region + "/" + strings.Join(parts, "/")
}

func now() core.Time { return core.Time(time.Now().UTC()) }

func str(v any) string {
	s, _ := v.(string)
	return s
}

// list reads a JSON array of objects.
func list(v any) []obj {
	var out []obj
	a, _ := v.([]any)
	for _, x := range a {
		if o, ok := x.(obj); ok {
			out = append(out, o)
		}
	}
	return out
}

// strs reads a JSON array of strings.
func strs(v any) []string {
	var out []string
	a, _ := v.([]any)
	for _, x := range a {
		out = append(out, str(x))
	}
	return out
}

// jsonObj turns a Go value into a stored object (JSON types only).
func jsonObj(v any) obj {
	b, _ := json.Marshal(v)
	var o obj
	json.Unmarshal(b, &o)
	return o
}

// page returns up to limit items (default 100, at most 1000) from the
// offset in token, and the token of the next page.
func page[T any](items []T, token string, limit int) ([]T, string) {
	i, _ := strconv.Atoi(token)
	i = min(max(i, 0), len(items))
	if limit <= 0 {
		limit = 100
	}
	if j := i + min(limit, 1000); j < len(items) {
		return items[i:j], strconv.Itoa(j)
	}
	return items[i:], ""
}

// listOut is a list response with its next token when there is one.
func listOut(field string, items any, next string) obj {
	o := obj{field: items}
	if next != "" {
		o["NextToken"] = next
	}
	return o
}

// arnKinds maps object kinds to their ARN resource type, for tags.
var arnKinds = map[string]string{"Database": "database", "Crawler": "crawler", "Job": "job", "Connection": "connection", "Trigger": "trigger", "Registry": "registry", "Schema": "schema"}

// put stores a new object; it fails when the name is taken.
func (s *Service) put(c *core.Ctx, kind, name string, o obj, tags map[string]string) error {
	if name == "" {
		return invalid("Name is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := kind + "/" + key(c, name)
	if s.st.Objects[k] != nil {
		return exists("%s %s already exists.", kind, name)
	}
	s.st.Objects[k] = o
	if t := arnKinds[kind]; t != "" && len(tags) > 0 {
		s.st.Tags[key(c, t, name)] = maps.Clone(tags)
	}
	return nil
}

func (s *Service) get(c *core.Ctx, kind, name string) (obj, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o := s.st.Objects[kind+"/"+key(c, name)]; o != nil {
		return o, nil
	}
	return nil, notFound("%s %s not found.", kind, name)
}

// change stores what f makes of a copy of an object.
func (s *Service) change(c *core.Ctx, kind, name string, f func(obj) error) (obj, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := kind + "/" + key(c, name)
	o := s.st.Objects[k]
	if o == nil {
		return nil, notFound("%s %s not found.", kind, name)
	}
	o = maps.Clone(o)
	if err := f(o); err != nil {
		return nil, err
	}
	s.st.Objects[k] = o
	return o, nil
}

// drop deletes an object and its tags.
func (s *Service) drop(c *core.Ctx, kind, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := kind + "/" + key(c, name)
	if s.st.Objects[k] == nil {
		return notFound("%s %s not found.", kind, name)
	}
	delete(s.st.Objects, k)
	delete(s.st.Tags, key(c, arnKinds[kind], name))
	return nil
}

// all returns the objects of a kind whose names start with prefix, by name.
func (s *Service) all(c *core.Ctx, kind, prefix string) []obj {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := kind + "/" + key(c, prefix)
	out := []obj{}
	for _, k := range core.SortedKeys(s.st.Objects) {
		if strings.HasPrefix(k, p) {
			out = append(out, s.st.Objects[k])
		}
	}
	return out
}

// getter serves Get<Kind>: the name is in the field in, the object goes out as out.
func (s *Service) getter(kind, in, out string) core.Handler {
	return core.H(func(c *core.Ctx, req *obj) (any, error) {
		o, err := s.get(c, kind, str((*req)[in]))
		if err != nil {
			return nil, err
		}
		return obj{out: o}, nil
	})
}

type pageInput struct {
	NextToken  string
	MaxResults int
}

// lister serves Get<Kind>s: a page of every object of the kind.
func (s *Service) lister(kind, out string) core.Handler {
	return core.H(func(c *core.Ctx, in *pageInput) (any, error) {
		items, next := page(s.all(c, kind, ""), in.NextToken, in.MaxResults)
		return listOut(out, items, next), nil
	})
}

// ---- databases ----

type dbInput struct {
	Name          string
	DatabaseInput obj
	Tags          map[string]string
}

func (s *Service) createDatabase(c *core.Ctx, in *dbInput) (any, error) {
	if in.DatabaseInput == nil {
		return nil, invalid("DatabaseInput is required.")
	}
	d := maps.Clone(in.DatabaseInput)
	name := strings.ToLower(str(d["Name"]))
	d["Name"], d["CatalogId"], d["CreateTime"] = name, c.Account, now()
	if d["CreateTableDefaultPermissions"] == nil {
		d["CreateTableDefaultPermissions"] = []obj{{"Principal": obj{"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}, "Permissions": []string{"ALL"}}}
	}
	return nil, s.put(c, "Database", name, d, in.Tags)
}

func (s *Service) getDatabase(c *core.Ctx, in *dbInput) (any, error) {
	d, err := s.get(c, "Database", strings.ToLower(in.Name))
	if err != nil {
		return nil, err
	}
	return obj{"Database": d}, nil
}

func (s *Service) updateDatabase(c *core.Ctx, in *dbInput) (any, error) {
	if in.DatabaseInput == nil {
		return nil, invalid("DatabaseInput is required.")
	}
	_, err := s.change(c, "Database", strings.ToLower(in.Name), func(d obj) error {
		n := maps.Clone(in.DatabaseInput)
		n["Name"], n["CatalogId"], n["CreateTime"] = d["Name"], d["CatalogId"], d["CreateTime"]
		n["CreateTableDefaultPermissions"] = cmp.Or(n["CreateTableDefaultPermissions"], d["CreateTableDefaultPermissions"])
		clear(d)
		maps.Copy(d, n)
		return nil
	})
	return nil, err
}

// deleteDatabase deletes a database with its tables and functions.
func (s *Service) deleteDatabase(c *core.Ctx, in *dbInput) (any, error) {
	name := strings.ToLower(in.Name)
	if err := s.drop(c, "Database", name); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.st.Tables {
		if strings.HasPrefix(k, key(c, name, "")) {
			delete(s.st.Tables, k)
		}
	}
	for k := range s.st.Objects {
		if strings.HasPrefix(k, "Function/"+key(c, name, "")) {
			delete(s.st.Objects, k)
		}
	}
	return nil, nil
}

// ---- tables ----

type tableInput struct {
	DatabaseName     string
	Name             string
	TableName        string
	VersionId        string
	Expression       string
	NextToken        string
	MaxResults       int
	TableInput       obj
	PartitionIndexes []obj
	SkipArchive      bool
	TablesToDelete   []string
}

// table returns a table; lock held.
func (s *Service) table(c *core.Ctx, db, name string) (*table, error) {
	db, name = strings.ToLower(db), strings.ToLower(name)
	if s.st.Objects["Database/"+key(c, db)] == nil {
		return nil, notFound("Database %s not found.", db)
	}
	if t := s.st.Tables[key(c, db, name)]; t != nil {
		return t, nil
	}
	return nil, notFound("Table %s not found.", name)
}

func (s *Service) createTable(c *core.Ctx, in *tableInput) (any, error) {
	t := maps.Clone(in.TableInput)
	name, db := strings.ToLower(str(t["Name"])), strings.ToLower(in.DatabaseName)
	if name == "" {
		return nil, invalid("TableInput.Name is required.")
	}
	at := now()
	t["Name"], t["DatabaseName"], t["CatalogId"], t["CreateTime"], t["UpdateTime"] = name, db, c.Account, at, at
	t["VersionId"], t["IsRegisteredWithLakeFormation"], t["CreatedBy"] = "0", false, c.Principal
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Objects["Database/"+key(c, db)] == nil {
		return nil, notFound("Database %s not found.", db)
	}
	if s.st.Tables[key(c, db, name)] != nil {
		return nil, exists("Table %s already exists.", name)
	}
	s.st.Tables[key(c, db, name)] = &table{Versions: []obj{t}, Partitions: map[string]obj{}, Indexes: in.PartitionIndexes}
	return nil, nil
}

func (s *Service) getTable(c *core.Ctx, in *tableInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.DatabaseName, in.Name)
	if err != nil {
		return nil, err
	}
	return obj{"Table": t.cur()}, nil
}

// matcher compiles a Glue name pattern: a case-insensitive regular
// expression matching whole names, where a lone * means any characters.
func matcher(expr string) (*regexp.Regexp, error) {
	if expr == "" {
		expr = ".*"
	}
	re, err := regexp.Compile("(?i)^(?:" + regexp.MustCompile(`(^|[^.])\*`).ReplaceAllString(expr, "$1.*") + ")$")
	if err != nil {
		return nil, invalid("Invalid expression %q: %v", expr, err)
	}
	return re, nil
}

func (s *Service) getTables(c *core.Ctx, in *tableInput) (any, error) {
	re, err := matcher(in.Expression)
	if err != nil {
		return nil, err
	}
	db := strings.ToLower(in.DatabaseName)
	s.mu.Lock()
	if s.st.Objects["Database/"+key(c, db)] == nil {
		s.mu.Unlock()
		return nil, notFound("Database %s not found.", db)
	}
	out := []obj{}
	for _, k := range core.SortedKeys(s.st.Tables) {
		if t := s.st.Tables[k].cur(); strings.HasPrefix(k, key(c, db, "")) && re.MatchString(str(t["Name"])) {
			out = append(out, t)
		}
	}
	s.mu.Unlock()
	items, next := page(out, in.NextToken, in.MaxResults)
	return listOut("TableList", items, next), nil
}

// updateTable stores a new version of a table (or replaces the current one with SkipArchive).
func (s *Service) updateTable(c *core.Ctx, in *tableInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.DatabaseName, str(in.TableInput["Name"]))
	if err != nil {
		return nil, err
	}
	old := t.cur()
	if in.VersionId != "" && in.VersionId != str(old["VersionId"]) { // the caller read an older version
		return nil, core.Errorf(400, "ConcurrentModificationException", "Update table failed due to concurrent modifications.")
	}
	n := maps.Clone(in.TableInput)
	v, _ := strconv.Atoi(str(old["VersionId"]))
	for _, k := range []string{"Name", "DatabaseName", "CatalogId", "CreateTime", "CreatedBy", "IsRegisteredWithLakeFormation"} {
		n[k] = old[k]
	}
	n["UpdateTime"], n["VersionId"] = now(), strconv.Itoa(v+1)
	if in.SkipArchive {
		t.Versions = slices.Clone(t.Versions[:len(t.Versions)-1])
	}
	t.Versions = append(t.Versions, n)
	return nil, nil
}

func (s *Service) deleteTable(c *core.Ctx, in *tableInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.table(c, in.DatabaseName, in.Name); err != nil {
		return nil, err
	}
	delete(s.st.Tables, key(c, strings.ToLower(in.DatabaseName), strings.ToLower(in.Name)))
	return nil, nil
}

func (s *Service) batchDeleteTable(c *core.Ctx, in *tableInput) (any, error) {
	errs := []obj{}
	for _, name := range in.TablesToDelete {
		if _, err := s.deleteTable(c, &tableInput{DatabaseName: in.DatabaseName, Name: name}); err != nil {
			e := core.AsError(err)
			errs = append(errs, obj{"TableName": name, "ErrorDetail": obj{"ErrorCode": e.Code, "ErrorMessage": e.Msg}})
		}
	}
	return obj{"Errors": errs}, nil
}

func (s *Service) getTableVersions(c *core.Ctx, in *tableInput) (any, error) {
	s.mu.Lock()
	t, err := s.table(c, in.DatabaseName, in.TableName)
	var out []obj
	if err == nil {
		for _, v := range slices.Backward(t.Versions) {
			out = append(out, obj{"Table": v, "VersionId": v["VersionId"]})
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	items, next := page(out, in.NextToken, in.MaxResults)
	return listOut("TableVersions", items, next), nil
}

func (s *Service) getTableVersion(c *core.Ctx, in *tableInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}
	v := t.cur()
	if in.VersionId != "" {
		i := slices.IndexFunc(t.Versions, func(v obj) bool { return v["VersionId"] == in.VersionId })
		if i < 0 {
			return nil, notFound("Version %s of table %s not found.", in.VersionId, in.TableName)
		}
		v = t.Versions[i]
	}
	return obj{"TableVersion": obj{"Table": v, "VersionId": v["VersionId"]}}, nil
}

func (s *Service) getPartitionIndexes(c *core.Ctx, in *tableInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}
	types := map[string]any{}
	for _, k := range list(t.cur()["PartitionKeys"]) {
		types[str(k["Name"])] = k["Type"]
	}
	out := []obj{}
	for _, ix := range t.Indexes {
		var keys []obj
		for _, k := range strs(ix["Keys"]) {
			keys = append(keys, obj{"Name": k, "Type": types[k]})
		}
		out = append(out, obj{"IndexName": ix["IndexName"], "Keys": keys, "IndexStatus": "ACTIVE"})
	}
	return obj{"PartitionIndexDescriptorList": out}, nil
}

// ---- partitions ----

type partInput struct {
	DatabaseName       string
	TableName          string
	Expression         string
	NextToken          string
	MaxResults         int
	PartitionValues    []string
	PartitionValueList []string
	PartitionInput     obj
	PartitionInputList []obj
	PartitionsToDelete []struct{ Values []string }
	PartitionsToGet    []struct{ Values []string }
}

// add stores a new partition; lock held.
func (t *table) add(c *core.Ctx, p obj) error {
	vals := strs(p["Values"])
	if len(vals) != len(t.keys()) {
		return invalid("The number of partition keys do not match the number of partition values.")
	}
	k := strings.Join(vals, "\x00")
	if t.Partitions[k] != nil {
		return exists("Partition already exists.")
	}
	p = maps.Clone(p)
	p["DatabaseName"], p["TableName"], p["CatalogId"], p["CreationTime"] = t.cur()["DatabaseName"], t.cur()["Name"], c.Account, now()
	t.Partitions[k] = p
	return nil
}

func (s *Service) createPartition(c *core.Ctx, in *partInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}
	return nil, t.add(c, in.PartitionInput)
}

// partError is a failed item of a batch partition call.
func partError(values []string, err error) obj {
	e := core.AsError(err)
	return obj{"PartitionValues": values, "ErrorDetail": obj{"ErrorCode": e.Code, "ErrorMessage": e.Msg}}
}

func (s *Service) batchCreatePartition(c *core.Ctx, in *partInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}
	errs := []obj{}
	for _, p := range in.PartitionInputList {
		if err := t.add(c, p); err != nil {
			errs = append(errs, partError(strs(p["Values"]), err))
		}
	}
	return obj{"Errors": errs}, nil
}

// partition returns a partition by its values; lock held.
func (s *Service) partition(c *core.Ctx, in *partInput, values []string) (*table, obj, error) {
	t, err := s.table(c, in.DatabaseName, in.TableName)
	if err != nil {
		return nil, nil, err
	}
	if p := t.Partitions[strings.Join(values, "\x00")]; p != nil {
		return t, p, nil
	}
	return nil, nil, notFound("Partition not found.")
}

func (s *Service) getPartition(c *core.Ctx, in *partInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, p, err := s.partition(c, in, in.PartitionValues)
	if err != nil {
		return nil, err
	}
	return obj{"Partition": p}, nil
}

func (s *Service) getPartitions(c *core.Ctx, in *partInput) (any, error) {
	s.mu.Lock()
	t, err := s.table(c, in.DatabaseName, in.TableName)
	var keep func([]string) bool
	if err == nil {
		keep, err = filter(in.Expression, t.keys())
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	out := []obj{}
	for _, k := range core.SortedKeys(t.Partitions) {
		if p := t.Partitions[k]; keep(strs(p["Values"])) {
			out = append(out, p)
		}
	}
	s.mu.Unlock()
	items, next := page(out, in.NextToken, in.MaxResults)
	return listOut("Partitions", items, next), nil
}

func (s *Service) batchGetPartition(c *core.Ctx, in *partInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.table(c, in.DatabaseName, in.TableName); err != nil {
		return nil, err
	}
	out := []obj{}
	for _, v := range in.PartitionsToGet {
		if _, p, err := s.partition(c, in, v.Values); err == nil {
			out = append(out, p)
		}
	}
	return obj{"Partitions": out, "UnprocessedKeys": []obj{}}, nil
}

func (s *Service) updatePartition(c *core.Ctx, in *partInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, old, err := s.partition(c, in, in.PartitionValueList)
	if err != nil {
		return nil, err
	}
	k := strings.Join(in.PartitionValueList, "\x00")
	delete(t.Partitions, k)
	n := maps.Clone(in.PartitionInput)
	if n["Values"] == nil {
		n["Values"] = old["Values"]
	}
	if err := t.add(c, n); err != nil {
		t.Partitions[k] = old
		return nil, err
	}
	nk := strings.Join(strs(n["Values"]), "\x00")
	t.Partitions[nk]["CreationTime"] = old["CreationTime"] // the copy made by add
	return nil, nil
}

func (s *Service) deletePartition(c *core.Ctx, in *partInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, _, err := s.partition(c, in, in.PartitionValues)
	if err != nil {
		return nil, err
	}
	delete(t.Partitions, strings.Join(in.PartitionValues, "\x00"))
	return nil, nil
}

func (s *Service) batchDeletePartition(c *core.Ctx, in *partInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.table(c, in.DatabaseName, in.TableName); err != nil {
		return nil, err
	}
	errs := []obj{}
	for _, p := range in.PartitionsToDelete {
		if t, _, err := s.partition(c, in, p.Values); err != nil {
			errs = append(errs, partError(p.Values, err))
		} else {
			delete(t.Partitions, strings.Join(p.Values, "\x00"))
		}
	}
	return obj{"Errors": errs}, nil
}

var (
	andRe    = regexp.MustCompile(`(?i)\s+and\s+`)
	clauseRe = regexp.MustCompile("(?is)^\\s*`?(\\w+)`?\\s*(<>|!=|<=|>=|=|<|>|in\\b)\\s*(.+?)\\s*$")
)

// filter compiles a GetPartitions Expression: comparisons of a key with a
// value (=, <>, !=, <, <=, >, >=, or IN (…)) joined by AND.
// ponytail: no OR, NOT, LIKE or BETWEEN; add a real parser when needed.
func filter(expr string, keys []string) (func([]string) bool, error) {
	type cond struct {
		i    int
		op   string
		vals []string
	}
	var conds []cond
	if expr = strings.TrimSpace(expr); expr != "" {
		for _, part := range andRe.Split(expr, -1) {
			m := clauseRe.FindStringSubmatch(part)
			if m == nil {
				return nil, invalid("Unsupported expression: %s", expr)
			}
			i := slices.IndexFunc(keys, func(k string) bool { return strings.EqualFold(k, m[1]) })
			if i < 0 {
				return nil, invalid("Unknown partition key %s in expression: %s", m[1], expr)
			}
			cd := cond{i: i, op: strings.ToUpper(m[2])}
			if cd.op == "IN" {
				for _, v := range strings.Split(strings.Trim(m[3], "()"), ",") {
					cd.vals = append(cd.vals, unquote(v))
				}
			} else {
				cd.vals = []string{unquote(m[3])}
			}
			conds = append(conds, cd)
		}
	}
	return func(v []string) bool {
		for _, cd := range conds {
			n := compare(v[cd.i], cd.vals[0])
			var ok bool
			switch cd.op {
			case "=":
				ok = n == 0
			case "<>", "!=":
				ok = n != 0
			case "<":
				ok = n < 0
			case "<=":
				ok = n <= 0
			case ">":
				ok = n > 0
			case ">=":
				ok = n >= 0
			case "IN":
				ok = slices.ContainsFunc(cd.vals, func(x string) bool { return compare(v[cd.i], x) == 0 })
			}
			if !ok {
				return false
			}
		}
		return true
	}, nil
}

// compare orders values as numbers when both are numbers, else as strings.
func compare(a, b string) int {
	x, errA := strconv.ParseFloat(a, 64)
	y, errB := strconv.ParseFloat(b, 64)
	if errA == nil && errB == nil {
		return cmp.Compare(x, y)
	}
	return strings.Compare(a, b)
}

func unquote(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && (s[0] == '\'' || s[0] == '"') && s[len(s)-1] == s[0] {
		return s[1 : len(s)-1]
	}
	return s
}

// ---- jobs, connections, triggers, functions (metadata) ----

func (s *Service) createJob(c *core.Ctx, in *obj) (any, error) {
	j := maps.Clone(*in)
	tags, _ := j["Tags"].(obj)
	delete(j, "Tags")
	at := now()
	j["CreatedOn"], j["LastModifiedOn"] = at, at
	j["Timeout"], j["MaxRetries"] = cmp.Or(j["Timeout"], any(float64(2880))), cmp.Or(j["MaxRetries"], any(float64(0)))
	return obj{"Name": j["Name"]}, s.put(c, "Job", str(j["Name"]), j, tagMap(tags))
}

func tagMap(o obj) map[string]string {
	m := map[string]string{}
	for k, v := range o {
		m[k] = str(v)
	}
	return m
}

func (s *Service) deleteJob(c *core.Ctx, in *struct{ JobName string }) (any, error) {
	s.drop(c, "Job", in.JobName) // a missing job is no error
	return obj{"JobName": in.JobName}, nil
}

// startJobRun records a run that succeeded at once: jobs do not execute.
func (s *Service) startJobRun(c *core.Ctx, in *obj) (any, error) {
	name := str((*in)["JobName"])
	j, err := s.get(c, "Job", name)
	if err != nil {
		return nil, err
	}
	id, at := "jr_"+core.RandHex(32), now()
	run := obj{"Id": id, "Attempt": 0, "JobName": name, "StartedOn": at, "LastModifiedOn": at, "CompletedOn": at,
		"JobRunState": "SUCCEEDED", "ExecutionTime": 0, "Timeout": j["Timeout"], "GlueVersion": j["GlueVersion"],
		"Arguments": cmp.Or((*in)["Arguments"], j["DefaultArguments"]), "WorkerType": j["WorkerType"], "NumberOfWorkers": j["NumberOfWorkers"]}
	return obj{"JobRunId": id}, s.put(c, "JobRun", name+"/"+id, run, nil)
}

func (s *Service) getJobRun(c *core.Ctx, in *struct{ JobName, RunId string }) (any, error) {
	r, err := s.get(c, "JobRun", in.JobName+"/"+in.RunId)
	if err != nil {
		return nil, err
	}
	return obj{"JobRun": r}, nil
}

func (s *Service) getJobRuns(c *core.Ctx, in *struct {
	JobName string
	pageInput
}) (any, error) {
	if _, err := s.get(c, "Job", in.JobName); err != nil {
		return nil, err
	}
	runs := s.all(c, "JobRun", in.JobName+"/")
	slices.SortStableFunc(runs, func(a, b obj) int { return cmp.Compare(unix(b["StartedOn"]), unix(a["StartedOn"])) })
	items, next := page(runs, in.NextToken, in.MaxResults)
	return listOut("JobRuns", items, next), nil
}

// unix reads a stored time: core.Time, or epoch seconds after a reload.
func unix(v any) float64 {
	if t, ok := v.(core.Time); ok {
		return core.Unix(time.Time(t))
	}
	f, _ := v.(float64)
	return f
}

type connInput struct {
	Name            string
	ConnectionName  string
	ConnectionInput obj
	Tags            map[string]string
}

func (s *Service) createConnection(c *core.Ctx, in *connInput) (any, error) {
	if in.ConnectionInput == nil {
		return nil, invalid("ConnectionInput is required.")
	}
	o := maps.Clone(in.ConnectionInput)
	at := now()
	o["CreationTime"], o["LastUpdatedTime"], o["Status"] = at, at, "READY"
	return nil, s.put(c, "Connection", str(o["Name"]), o, in.Tags)
}

func (s *Service) updateConnection(c *core.Ctx, in *connInput) (any, error) {
	if in.ConnectionInput == nil {
		return nil, invalid("ConnectionInput is required.")
	}
	_, err := s.change(c, "Connection", in.Name, func(o obj) error {
		n := maps.Clone(in.ConnectionInput)
		n["Name"], n["CreationTime"], n["LastUpdatedTime"], n["Status"] = o["Name"], o["CreationTime"], now(), o["Status"]
		clear(o)
		maps.Copy(o, n)
		return nil
	})
	return nil, err
}

func (s *Service) deleteConnection(c *core.Ctx, in *connInput) (any, error) {
	return nil, s.drop(c, "Connection", in.ConnectionName)
}

func (s *Service) createTrigger(c *core.Ctx, in *obj) (any, error) {
	t := maps.Clone(*in)
	tags, _ := t["Tags"].(obj)
	start := t["StartOnCreation"] == true && t["Type"] != "ON_DEMAND"
	delete(t, "Tags")
	delete(t, "StartOnCreation")
	t["Id"], t["State"] = t["Name"], map[bool]string{true: "ACTIVATED", false: "CREATED"}[start]
	return obj{"Name": t["Name"]}, s.put(c, "Trigger", str(t["Name"]), t, tagMap(tags))
}

func (s *Service) updateTrigger(c *core.Ctx, in *struct {
	Name          string
	TriggerUpdate obj
}) (any, error) {
	t, err := s.change(c, "Trigger", in.Name, func(t obj) error {
		maps.Copy(t, in.TriggerUpdate)
		t["Name"] = in.Name
		return nil
	})
	if err != nil {
		return nil, err
	}
	return obj{"Trigger": t}, nil
}

func (s *Service) deleteTrigger(c *core.Ctx, in *struct{ Name string }) (any, error) {
	return obj{"Name": in.Name}, s.drop(c, "Trigger", in.Name)
}

func (s *Service) triggerState(state string) func(*core.Ctx, *struct{ Name string }) (any, error) {
	return func(c *core.Ctx, in *struct{ Name string }) (any, error) {
		_, err := s.change(c, "Trigger", in.Name, func(t obj) error { t["State"] = state; return nil })
		return obj{"Name": in.Name}, err
	}
}

type fnInput struct {
	DatabaseName  string
	FunctionName  string
	Pattern       string
	FunctionInput obj
	pageInput
}

func (s *Service) createFunction(c *core.Ctx, in *fnInput) (any, error) {
	db := strings.ToLower(in.DatabaseName)
	if _, err := s.get(c, "Database", db); err != nil {
		return nil, err
	}
	f := maps.Clone(in.FunctionInput)
	f["DatabaseName"], f["CatalogId"], f["CreateTime"] = db, c.Account, now()
	return nil, s.put(c, "Function", db+"/"+str(f["FunctionName"]), f, nil)
}

func (s *Service) getFunction(c *core.Ctx, in *fnInput) (any, error) {
	f, err := s.get(c, "Function", strings.ToLower(in.DatabaseName)+"/"+in.FunctionName)
	if err != nil {
		return nil, err
	}
	return obj{"UserDefinedFunction": f}, nil
}

// getFunctions lists the functions of a database (all when none is named) whose names match Pattern.
func (s *Service) getFunctions(c *core.Ctx, in *fnInput) (any, error) {
	re, err := matcher(in.Pattern)
	if err != nil {
		return nil, err
	}
	prefix := ""
	if in.DatabaseName != "" {
		prefix = strings.ToLower(in.DatabaseName) + "/"
	}
	out := []obj{}
	for _, f := range s.all(c, "Function", prefix) {
		if re.MatchString(str(f["FunctionName"])) {
			out = append(out, f)
		}
	}
	items, next := page(out, in.NextToken, in.MaxResults)
	return listOut("UserDefinedFunctions", items, next), nil
}

func (s *Service) updateFunction(c *core.Ctx, in *fnInput) (any, error) {
	_, err := s.change(c, "Function", strings.ToLower(in.DatabaseName)+"/"+in.FunctionName, func(f obj) error {
		n := maps.Clone(in.FunctionInput)
		n["FunctionName"], n["DatabaseName"], n["CatalogId"], n["CreateTime"] = f["FunctionName"], f["DatabaseName"], f["CatalogId"], f["CreateTime"]
		clear(f)
		maps.Copy(f, n)
		return nil
	})
	return nil, err
}

func (s *Service) deleteFunction(c *core.Ctx, in *fnInput) (any, error) {
	return nil, s.drop(c, "Function", strings.ToLower(in.DatabaseName)+"/"+in.FunctionName)
}

// ---- tags and settings ----

type tagInput struct {
	ResourceArn  string
	TagsToAdd    map[string]string
	TagsToRemove []string
}

// tagKey is where a resource's tags live, account/region/resource, with the
// caller's account and region when the ARN lacks them (Terraform without an
// account id builds such ARNs).
func tagKey(c *core.Ctx, arn string) (string, error) {
	_, region, account, res, ok := core.ParseARN(arn)
	if !ok {
		return "", invalid("Invalid ARN: %s", arn)
	}
	return cmp.Or(account, c.Account) + "/" + cmp.Or(region, c.Region) + "/" + res, nil
}

// tagResource serves TagResource and UntagResource.
func (s *Service) tagResource(c *core.Ctx, in *tagInput) (any, error) {
	k, err := tagKey(c, in.ResourceArn)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := maps.Clone(s.st.Tags[k])
	if t == nil {
		t = map[string]string{}
	}
	maps.Copy(t, in.TagsToAdd)
	for _, r := range in.TagsToRemove {
		delete(t, r)
	}
	s.st.Tags[k] = t
	return nil, nil
}

func (s *Service) getTags(c *core.Ctx, in *tagInput) (any, error) {
	k, err := tagKey(c, in.ResourceArn)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.st.Tags[k]; t != nil {
		return obj{"Tags": t}, nil
	}
	return obj{"Tags": obj{}}, nil
}

// TaggedResources lists the tags of the resources in an account and region
// (ARN → tags), for the tagging API and Resource Explorer.
func (s *Service) TaggedResources(account, region string) map[string]map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]map[string]string{}
	prefix := account + "/" + region + "/"
	for k, t := range s.st.Tags {
		if res, ok := strings.CutPrefix(k, prefix); ok {
			out[core.ARN("glue", region, account, res)] = maps.Clone(t)
		}
	}
	return out
}

func (s *Service) getEncryption(c *core.Ctx, _ *struct{}) (any, error) {
	if o, err := s.get(c, "Encryption", "catalog"); err == nil {
		return obj{"DataCatalogEncryptionSettings": o}, nil
	}
	return obj{"DataCatalogEncryptionSettings": obj{"EncryptionAtRest": obj{"CatalogEncryptionMode": "DISABLED"},
		"ConnectionPasswordEncryption": obj{"ReturnConnectionPasswordEncrypted": false}}}, nil
}

func (s *Service) putEncryption(c *core.Ctx, in *struct{ DataCatalogEncryptionSettings obj }) (any, error) {
	s.mu.Lock()
	s.st.Objects["Encryption/"+key(c, "catalog")] = in.DataCatalogEncryptionSettings
	s.mu.Unlock()
	return nil, nil
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	dbs := core.Table{Name: "Databases", Columns: []string{"Database", "Tables", "Location", "Region", "Account"}}
	tables := core.Table{Name: "Tables", Columns: []string{"Table", "Location", "Format", "Partitions", "Version"}}
	crawlers := core.Table{Name: "Crawlers", Columns: []string{"Crawler", "State", "Database", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.Objects) {
		o := s.st.Objects[k]
		p := strings.Split(k, "/")
		switch p[0] {
		case "Database":
			n := 0
			for tk := range s.st.Tables {
				if strings.HasPrefix(tk, p[1]+"/"+p[2]+"/"+p[3]+"/") {
					n++
				}
			}
			dbs.Rows = append(dbs.Rows, []string{p[3], strconv.Itoa(n), str(o["LocationUri"]), p[2], p[1]})
		case "Crawler":
			crawlers.Rows = append(crawlers.Rows, []string{p[3], str(o["State"]), str(o["DatabaseName"]), p[2], p[1]})
		}
	}
	for _, k := range core.SortedKeys(s.st.Tables) {
		t := s.st.Tables[k]
		v := t.cur()
		sd, _ := v["StorageDescriptor"].(obj)
		params, _ := v["Parameters"].(obj)
		tables.Rows = append(tables.Rows, []string{str(v["DatabaseName"]) + "." + str(v["Name"]), str(sd["Location"]),
			str(params["classification"]), strconv.Itoa(len(t.Partitions)), str(v["VersionId"])})
	}
	return []core.Table{dbs, tables, crawlers}
}

// Inspect returns a database, a table (database.table) with its partitions, or a crawler.
func (s *Service) Inspect(kind, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Tables) {
		if t := s.st.Tables[k]; kind == "Tables" && str(t.cur()["DatabaseName"])+"."+str(t.cur()["Name"]) == id {
			return map[string]any{"table": t.cur(), "partitions": t.Partitions, "versions": len(t.Versions)}
		}
	}
	for _, k := range core.SortedKeys(s.st.Objects) {
		if p := strings.Split(k, "/"); strings.TrimSuffix(kind, "s") == p[0] && p[len(p)-1] == id {
			return s.st.Objects[k]
		}
	}
	return nil
}
