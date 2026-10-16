// Package athena emulates Amazon Athena over JSON 1.1: workgroups, named
// queries, prepared statements, data catalogs and query executions. DDL
// becomes AWS Glue Data Catalog calls; SELECTs run on DuckDB in a container
// over copies of the tables' S3 files, and results go to S3 as CSV.
package athena

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/duckdb"
)

type obj = map[string]any

// Service is the Athena emulator.
type Service struct {
	hub    *core.Hub
	ops    map[string]core.Handler
	eng    *duckdb.Engine
	ctx    context.Context // queries stop with it
	cancel context.CancelFunc
	wg     sync.WaitGroup // running queries

	mu    sync.Mutex
	st    state
	stops map[string]context.CancelFunc // running queries by key
}

type state struct {
	WorkGroups map[string]obj               `json:"workGroups"`         // account/region/name
	Named      map[string]obj               `json:"namedQueries"`       // account/region/id
	Prepared   map[string]obj               `json:"preparedStatements"` // account/region/workgroup/name
	Catalogs   map[string]obj               `json:"dataCatalogs"`       // account/region/lower-case name
	Queries    map[string]*query            `json:"queries"`            // account/region/id
	Tokens     map[string]string            `json:"tokens"`             // account/region/client request token → id
	Tags       map[string]map[string]string `json:"tags"`               // account/region/resource, e.g. workgroup/etl
	Seq        int64                        `json:"seq"`
}

// query is a query execution and its results.
// ponytail: results stay in memory (and snapshots) with the execution; read
// them back from the CSV in S3 if big results make that heavy.
type query struct {
	X       execution   `json:"execution"` // the API view
	Account string      `json:"account"`
	Region  string      `json:"region"`
	Seq     int64       `json:"seq"`
	Cols    []column    `json:"columns,omitempty"`
	Rows    [][]*string `json:"rows,omitempty"` // a SELECT's first row holds the column names
}

type execution struct {
	QueryExecutionId      string
	Query                 string
	StatementType         string
	ResultConfiguration   resultConfig
	QueryExecutionContext *execContext `json:",omitempty"`
	Status                status
	Statistics            *statistics `json:",omitempty"`
	WorkGroup             string
	EngineVersion         obj
	ExecutionParameters   []string `json:",omitempty"`
}

type resultConfig struct{ OutputLocation string }

type execContext struct {
	Database, Catalog string `json:",omitempty"`
}

type status struct {
	State              string
	StateChangeReason  string `json:",omitempty"`
	SubmissionDateTime core.Time
	CompletionDateTime *core.Time   `json:",omitempty"`
	AthenaError        *athenaError `json:",omitempty"`
}

type athenaError struct {
	ErrorCategory int // 1 system, 2 user
	Retryable     bool
	ErrorMessage  string
}

type statistics struct {
	EngineExecutionTimeInMillis      int64
	DataScannedInBytes               int64
	TotalExecutionTimeInMillis       int64
	QueryQueueTimeInMillis           int64
	ServicePreProcessingTimeInMillis int64
	QueryPlanningTimeInMillis        int64
	ServiceProcessingTimeInMillis    int64
	ResultReuseInformation           struct{ ReusedPreviousResult bool }
}

// column is a result column's ColumnInfo.
type column struct {
	CatalogName, SchemaName, TableName, Name, Label, Type string
	Precision, Scale                                      int
	Nullable                                              string
	CaseSensitive                                         bool
}

// New creates the Athena service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, eng: &duckdb.Engine{Hub: hub, Name: "Athena", Service: "athena", Script: server}, stops: map[string]context.CancelFunc{}}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.Reset()
	s.ops = map[string]core.Handler{
		"CreateWorkGroup":         core.H(s.createWorkGroup),
		"GetWorkGroup":            core.H(s.getWorkGroup),
		"ListWorkGroups":          core.H(s.listWorkGroups),
		"UpdateWorkGroup":         core.H(s.updateWorkGroup),
		"DeleteWorkGroup":         core.H(s.deleteWorkGroup),
		"CreateNamedQuery":        core.H(s.createNamedQuery),
		"GetNamedQuery":           core.H(s.getNamedQuery),
		"ListNamedQueries":        core.H(s.listNamedQueries),
		"BatchGetNamedQuery":      core.H(s.batchGetNamedQuery),
		"UpdateNamedQuery":        core.H(s.updateNamedQuery),
		"DeleteNamedQuery":        core.H(s.deleteNamedQuery),
		"CreatePreparedStatement": core.H(s.createPrepared),
		"GetPreparedStatement":    core.H(s.getPrepared),
		"ListPreparedStatements":  core.H(s.listPrepared),
		"UpdatePreparedStatement": core.H(s.updatePrepared),
		"DeletePreparedStatement": core.H(s.deletePrepared),
		"CreateDataCatalog":       core.H(s.createCatalog),
		"GetDataCatalog":          core.H(s.getCatalog),
		"ListDataCatalogs":        core.H(s.listCatalogs),
		"UpdateDataCatalog":       core.H(s.updateCatalog),
		"DeleteDataCatalog":       core.H(s.deleteCatalog),
		"TagResource":             core.H(s.tagResource),
		"UntagResource":           core.H(s.untagResource),
		"ListTagsForResource":     core.H(s.listTags),
		"StartQueryExecution":     core.H(s.startQuery),
		"GetQueryExecution":       core.H(s.getQuery),
		"BatchGetQueryExecution":  core.H(s.batchGetQuery),
		"ListQueryExecutions":     core.H(s.listQueries),
		"GetQueryResults":         core.H(s.getResults),
		"StopQueryExecution":      core.H(s.stopQuery),
		"GetDatabase":             core.H(s.getDatabase),
		"ListDatabases":           core.H(s.listDatabases),
		"GetTableMetadata":        core.H(s.getTableMetadata),
		"ListTableMetadata":       core.H(s.listTableMetadata),
	}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "athena", Cloud: core.AWS, Title: "Athena", Targets: []string{"AmazonAthena"}, Signing: []string{"athena"}, Protocol: "json"}
}

// ServeHTTP is core.RPC's JSON 1.1 path, plus the AthenaErrorCode member of
// InvalidRequestException, which core.Error cannot carry.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	_, c.Op, _ = strings.Cut(r.Header.Get("X-Amz-Target"), ".")
	out, err := s.call(c, core.Body(r))
	if err != nil {
		e := core.AsError(err)
		c.ErrCode = e.Code
		w.Header().Set("X-Amzn-ErrorType", e.Code)
		body := obj{"__type": e.Code, "Message": e.Msg}
		if e.Code == "InvalidRequestException" {
			body["AthenaErrorCode"] = "INVALID_INPUT"
		}
		core.WriteJSON(w, c, "1.1", e.Status, body)
		return
	}
	core.WriteJSON(w, c, "1.1", http.StatusOK, out)
}

func (s *Service) call(c *core.Ctx, body []byte) (any, error) {
	h := s.ops[c.Op]
	if h == nil {
		return nil, core.Errorf(400, "UnknownOperationException", "pogi does not implement %s.%s yet", c.Service, c.Op)
	}
	if err := c.Intercept(); err != nil {
		return nil, err
	}
	return h(c, func(v any) error {
		if len(body) == 0 {
			return nil
		}
		return json.Unmarshal(body, v)
	})
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{WorkGroups: map[string]obj{}, Named: map[string]obj{}, Prepared: map[string]obj{}, Catalogs: map[string]obj{},
		Queries: map[string]*query{}, Tokens: map[string]string{}, Tags: map[string]map[string]string{}}
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
	maps.Copy(s.st.WorkGroups, st.WorkGroups)
	maps.Copy(s.st.Named, st.Named)
	maps.Copy(s.st.Prepared, st.Prepared)
	maps.Copy(s.st.Catalogs, st.Catalogs)
	maps.Copy(s.st.Queries, st.Queries)
	maps.Copy(s.st.Tokens, st.Tokens)
	maps.Copy(s.st.Tags, st.Tags)
	s.st.Seq = st.Seq
	for _, q := range s.st.Queries { // cut short by a restart
		if q.X.Status.State == "QUEUED" || q.X.Status.State == "RUNNING" {
			q.finish(errors.New("pogi restarted before the query finished"))
		}
	}
	return nil
}

// Close stops running queries and removes the DuckDB container.
func (s *Service) Close() error {
	s.cancel()
	s.wg.Wait()
	s.eng.Close()
	return nil
}

func invalid(f string, a ...any) error { return core.Errorf(400, "InvalidRequestException", f, a...) }
func notFound(f string, a ...any) error {
	return core.Errorf(400, "ResourceNotFoundException", f, a...)
}

func key(c *core.Ctx, parts ...string) string {
	return c.Account + "/" + c.Region + "/" + strings.Join(parts, "/")
}

func now() core.Time { return core.Time(time.Now().UTC()) }

func str(v any) string {
	s, _ := v.(string)
	return s
}

// code is the error code of a Glue error.
func code(err error) string {
	var e *core.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// page returns up to limit items (def when not given) from the offset in
// token, and the token of the next page.
func page[T any](items []T, token string, limit, def int) ([]T, string) {
	i, _ := strconv.Atoi(token)
	i = min(max(i, 0), len(items))
	if limit <= 0 {
		limit = def
	}
	if j := i + limit; j < len(items) {
		return items[i:j], strconv.Itoa(j)
	}
	return items[i:], ""
}

// orEmpty makes a nil slice empty, so that JSON shows [] rather than null.
func orEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

func orEmptyMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// listOut is a list response with its next token when there is one.
func listOut(field string, items any, next string) obj {
	o := obj{field: items}
	if next != "" {
		o["NextToken"] = next
	}
	return o
}

// ---- workgroups ----

type tag struct{ Key, Value string }

// withDefaults fills in the configuration Athena shows when it was not given.
func withDefaults(cfg obj) obj {
	cfg = maps.Clone(cfg)
	if cfg == nil {
		cfg = obj{}
	}
	for k, v := range map[string]any{"EnforceWorkGroupConfiguration": true, "PublishCloudWatchMetricsEnabled": true, "RequesterPaysEnabled": false} {
		if _, ok := cfg[k]; !ok {
			cfg[k] = v
		}
	}
	ev, _ := cfg["EngineVersion"].(obj)
	sel := cmp.Or(str(ev["SelectedEngineVersion"]), "AUTO")
	eff := sel
	if sel == "AUTO" {
		eff = "Athena engine version 3"
	}
	cfg["EngineVersion"] = obj{"SelectedEngineVersion": sel, "EffectiveEngineVersion": eff}
	return cfg
}

// workgroup returns a workgroup; primary exists everywhere. Lock held.
func (s *Service) workgroup(c *core.Ctx, name string) (obj, error) {
	name = cmp.Or(name, "primary")
	if wg := s.st.WorkGroups[key(c, name)]; wg != nil {
		return wg, nil
	}
	if name != "primary" {
		return nil, invalid("WorkGroup %s is not found.", name)
	}
	wg := obj{"Name": "primary", "State": "ENABLED", "Description": "", "CreationTime": now(),
		"Configuration": withDefaults(obj{"EnforceWorkGroupConfiguration": false, "PublishCloudWatchMetricsEnabled": false})}
	s.st.WorkGroups[key(c, name)] = wg
	return wg, nil
}

func (s *Service) createWorkGroup(c *core.Ctx, in *struct {
	Name, Description string
	Configuration     obj
	Tags              []tag
}) (any, error) {
	if in.Name == "" {
		return nil, invalid("WorkGroup name is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.workgroup(c, in.Name); err == nil {
		return nil, invalid("WorkGroup is already created")
	}
	s.st.WorkGroups[key(c, in.Name)] = obj{"Name": in.Name, "State": "ENABLED", "Description": in.Description, "CreationTime": now(),
		"Configuration": withDefaults(in.Configuration)}
	s.addTags(key(c, "workgroup", in.Name), in.Tags)
	return nil, nil
}

func (s *Service) getWorkGroup(c *core.Ctx, in *struct{ WorkGroup string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wg, err := s.workgroup(c, in.WorkGroup)
	if err != nil {
		return nil, err
	}
	return obj{"WorkGroup": wg}, nil
}

type pageInput struct {
	NextToken  string
	MaxResults int
}

func (s *Service) listWorkGroups(c *core.Ctx, in *pageInput) (any, error) {
	s.mu.Lock()
	s.workgroup(c, "primary")
	var out []obj
	for _, k := range core.SortedKeys(s.st.WorkGroups) {
		if wg := s.st.WorkGroups[k]; strings.HasPrefix(k, key(c, "")) {
			cfg, _ := wg["Configuration"].(obj)
			out = append(out, obj{"Name": wg["Name"], "State": wg["State"], "Description": wg["Description"],
				"CreationTime": wg["CreationTime"], "EngineVersion": cfg["EngineVersion"]})
		}
	}
	s.mu.Unlock()
	items, next := page(out, in.NextToken, in.MaxResults, 50)
	return listOut("WorkGroups", items, next), nil
}

// merge applies an update to m: a true RemoveX deletes X, other fields replace.
func merge(m, update obj) {
	for k, v := range update {
		if x, ok := strings.CutPrefix(k, "Remove"); ok {
			if v == true {
				delete(m, x)
			}
		} else if k != "ResultConfigurationUpdates" {
			m[k] = v
		}
	}
}

func (s *Service) updateWorkGroup(c *core.Ctx, in *obj) (any, error) {
	name := str((*in)["WorkGroup"])
	s.mu.Lock()
	defer s.mu.Unlock()
	wg, err := s.workgroup(c, name)
	if err != nil {
		return nil, err
	}
	wg = maps.Clone(wg)
	if d, ok := (*in)["Description"]; ok {
		wg["Description"] = d
	}
	if st := str((*in)["State"]); st != "" {
		wg["State"] = st
	}
	if u, ok := (*in)["ConfigurationUpdates"].(obj); ok {
		cfg := maps.Clone(wg["Configuration"].(obj))
		if ru, ok := u["ResultConfigurationUpdates"].(obj); ok {
			rc, _ := cfg["ResultConfiguration"].(obj)
			rc = maps.Clone(rc)
			if rc == nil {
				rc = obj{}
			}
			merge(rc, ru)
			cfg["ResultConfiguration"] = rc
		}
		merge(cfg, u)
		wg["Configuration"] = withDefaults(cfg)
	}
	s.st.WorkGroups[key(c, cmp.Or(name, "primary"))] = wg
	return nil, nil
}

func (s *Service) deleteWorkGroup(c *core.Ctx, in *struct {
	WorkGroup             string
	RecursiveDeleteOption bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.WorkGroup == "primary" {
		return nil, invalid("Primary workgroup cannot be deleted.")
	}
	if _, err := s.workgroup(c, in.WorkGroup); err != nil {
		return nil, err
	}
	var owned []func()
	for k, nq := range s.st.Named {
		if strings.HasPrefix(k, key(c, "")) && nq["WorkGroup"] == in.WorkGroup {
			owned = append(owned, func() { delete(s.st.Named, k) })
		}
	}
	for k := range s.st.Prepared {
		if strings.HasPrefix(k, key(c, in.WorkGroup, "")) {
			owned = append(owned, func() { delete(s.st.Prepared, k) })
		}
	}
	if len(owned) > 0 && !in.RecursiveDeleteOption {
		return nil, invalid("WorkGroup %s is not empty", in.WorkGroup)
	}
	for _, del := range owned {
		del()
	}
	delete(s.st.WorkGroups, key(c, in.WorkGroup))
	delete(s.st.Tags, key(c, "workgroup", in.WorkGroup))
	return nil, nil
}

// ---- named queries ----

type namedInput struct {
	NamedQueryId, Name, Description, Database, QueryString, ClientRequestToken, WorkGroup string
	NamedQueryIds                                                                         []string
	pageInput
}

func (s *Service) createNamedQuery(c *core.Ctx, in *namedInput) (any, error) {
	if in.Name == "" || in.Database == "" || in.QueryString == "" {
		return nil, invalid("Name, Database and QueryString are required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.workgroup(c, in.WorkGroup); err != nil {
		return nil, err
	}
	tk := key(c, "named", in.ClientRequestToken)
	if id := s.st.Tokens[tk]; id != "" && in.ClientRequestToken != "" {
		return obj{"NamedQueryId": id}, nil
	}
	id := core.UUID()
	s.st.Named[key(c, id)] = obj{"NamedQueryId": id, "Name": in.Name, "Description": in.Description, "Database": in.Database,
		"QueryString": in.QueryString, "WorkGroup": cmp.Or(in.WorkGroup, "primary")}
	if in.ClientRequestToken != "" {
		s.st.Tokens[tk] = id
	}
	return obj{"NamedQueryId": id}, nil
}

// named returns a named query; lock held.
func (s *Service) named(c *core.Ctx, id string) (obj, error) {
	if nq := s.st.Named[key(c, id)]; nq != nil {
		return nq, nil
	}
	return nil, invalid("Named query %s does not exist.", id)
}

func (s *Service) getNamedQuery(c *core.Ctx, in *namedInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nq, err := s.named(c, in.NamedQueryId)
	if err != nil {
		return nil, err
	}
	return obj{"NamedQuery": nq}, nil
}

func (s *Service) listNamedQueries(c *core.Ctx, in *namedInput) (any, error) {
	s.mu.Lock()
	wg := cmp.Or(in.WorkGroup, "primary")
	_, err := s.workgroup(c, wg)
	ids := []string{}
	for _, k := range core.SortedKeys(s.st.Named) {
		if nq := s.st.Named[k]; strings.HasPrefix(k, key(c, "")) && nq["WorkGroup"] == wg {
			ids = append(ids, str(nq["NamedQueryId"]))
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	items, next := page(ids, in.NextToken, in.MaxResults, 50)
	return listOut("NamedQueryIds", items, next), nil
}

func (s *Service) batchGetNamedQuery(c *core.Ctx, in *namedInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	found, missing := []obj{}, []obj{}
	for _, id := range in.NamedQueryIds {
		if nq, err := s.named(c, id); err == nil {
			found = append(found, nq)
		} else {
			missing = append(missing, obj{"NamedQueryId": id, "ErrorCode": "INVALID_INPUT", "ErrorMessage": core.AsError(err).Msg})
		}
	}
	return obj{"NamedQueries": found, "UnprocessedNamedQueryIds": missing}, nil
}

func (s *Service) updateNamedQuery(c *core.Ctx, in *namedInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nq, err := s.named(c, in.NamedQueryId)
	if err != nil {
		return nil, err
	}
	nq = maps.Clone(nq)
	nq["Name"], nq["Description"], nq["QueryString"] = in.Name, in.Description, in.QueryString
	s.st.Named[key(c, in.NamedQueryId)] = nq
	return nil, nil
}

func (s *Service) deleteNamedQuery(c *core.Ctx, in *namedInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.named(c, in.NamedQueryId); err != nil {
		return nil, err
	}
	delete(s.st.Named, key(c, in.NamedQueryId))
	return nil, nil
}

// ---- prepared statements ----

type preparedInput struct {
	StatementName, WorkGroup, QueryStatement, Description string
	pageInput
}

// prepared returns a prepared statement's key and value; lock held.
func (s *Service) prepared(c *core.Ctx, wg, name string) (string, obj, error) {
	if _, err := s.workgroup(c, wg); err != nil {
		return "", nil, err
	}
	k := key(c, cmp.Or(wg, "primary"), name)
	if ps := s.st.Prepared[k]; ps != nil {
		return k, ps, nil
	}
	return k, nil, notFound("Prepared statement %s does not exist in workgroup %s.", name, cmp.Or(wg, "primary"))
}

func (s *Service) createPrepared(c *core.Ctx, in *preparedInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ps, err := s.prepared(c, in.WorkGroup, in.StatementName)
	if ps != nil {
		return nil, invalid("Prepared statement %s already exists.", in.StatementName)
	}
	if code(err) == "InvalidRequestException" {
		return nil, err
	}
	s.st.Prepared[k] = obj{"StatementName": in.StatementName, "QueryStatement": in.QueryStatement, "WorkGroupName": cmp.Or(in.WorkGroup, "primary"),
		"Description": in.Description, "LastModifiedTime": now()}
	return nil, nil
}

func (s *Service) getPrepared(c *core.Ctx, in *preparedInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ps, err := s.prepared(c, in.WorkGroup, in.StatementName)
	if err != nil {
		return nil, err
	}
	return obj{"PreparedStatement": ps}, nil
}

func (s *Service) listPrepared(c *core.Ctx, in *preparedInput) (any, error) {
	s.mu.Lock()
	_, err := s.workgroup(c, in.WorkGroup)
	out := []obj{}
	for _, k := range core.SortedKeys(s.st.Prepared) {
		if ps := s.st.Prepared[k]; strings.HasPrefix(k, key(c, cmp.Or(in.WorkGroup, "primary"), "")) {
			out = append(out, obj{"StatementName": ps["StatementName"], "LastModifiedTime": ps["LastModifiedTime"]})
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	items, next := page(out, in.NextToken, in.MaxResults, 50)
	return listOut("PreparedStatements", items, next), nil
}

func (s *Service) updatePrepared(c *core.Ctx, in *preparedInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ps, err := s.prepared(c, in.WorkGroup, in.StatementName)
	if err != nil {
		return nil, err
	}
	ps = maps.Clone(ps)
	ps["QueryStatement"], ps["Description"], ps["LastModifiedTime"] = in.QueryStatement, in.Description, now()
	s.st.Prepared[k] = ps
	return nil, nil
}

func (s *Service) deletePrepared(c *core.Ctx, in *preparedInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, _, err := s.prepared(c, in.WorkGroup, in.StatementName)
	if err != nil {
		return nil, err
	}
	delete(s.st.Prepared, k)
	return nil, nil
}

// ---- data catalogs ----

type catalogInput struct {
	Name, Type, Description string
	Parameters              map[string]string
	Tags                    []tag
	pageInput
}

// catalog returns a data catalog; AwsDataCatalog (Glue) exists everywhere. Lock held.
func (s *Service) catalog(c *core.Ctx, name string) (obj, error) {
	name = cmp.Or(name, "AwsDataCatalog")
	k := key(c, strings.ToLower(name))
	if dc := s.st.Catalogs[k]; dc != nil {
		return dc, nil
	}
	if !strings.EqualFold(name, "AwsDataCatalog") {
		return nil, invalid("Catalog %s is not found.", name)
	}
	dc := obj{"Name": "AwsDataCatalog", "Type": "GLUE", "Description": "", "Parameters": obj{"catalog-id": c.Account}, "Status": "CREATE_COMPLETE"}
	s.st.Catalogs[k] = dc
	return dc, nil
}

func (s *Service) createCatalog(c *core.Ctx, in *catalogInput) (any, error) {
	if in.Name == "" || in.Type == "" {
		return nil, invalid("Name and Type are required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.catalog(c, in.Name); err == nil {
		return nil, invalid("Catalog %s already exists.", in.Name)
	}
	dc := obj{"Name": in.Name, "Type": in.Type, "Description": in.Description, "Parameters": in.Parameters, "Status": "CREATE_COMPLETE"}
	s.st.Catalogs[key(c, strings.ToLower(in.Name))] = dc
	s.addTags(key(c, "datacatalog", in.Name), in.Tags)
	return obj{"DataCatalog": dc}, nil
}

func (s *Service) getCatalog(c *core.Ctx, in *catalogInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dc, err := s.catalog(c, in.Name)
	if err != nil {
		return nil, err
	}
	return obj{"DataCatalog": dc}, nil
}

func (s *Service) listCatalogs(c *core.Ctx, in *catalogInput) (any, error) {
	s.mu.Lock()
	s.catalog(c, "AwsDataCatalog")
	var out []obj
	for _, k := range core.SortedKeys(s.st.Catalogs) {
		if dc := s.st.Catalogs[k]; strings.HasPrefix(k, key(c, "")) {
			out = append(out, obj{"CatalogName": dc["Name"], "Type": dc["Type"], "Status": dc["Status"]})
		}
	}
	s.mu.Unlock()
	items, next := page(out, in.NextToken, in.MaxResults, 50)
	return listOut("DataCatalogsSummary", items, next), nil
}

func (s *Service) updateCatalog(c *core.Ctx, in *catalogInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dc, err := s.catalog(c, in.Name)
	if err != nil {
		return nil, err
	}
	dc = maps.Clone(dc)
	dc["Type"], dc["Description"], dc["Parameters"] = cmp.Or(in.Type, str(dc["Type"])), in.Description, in.Parameters
	s.st.Catalogs[key(c, strings.ToLower(in.Name))] = dc
	return nil, nil
}

func (s *Service) deleteCatalog(c *core.Ctx, in *catalogInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.EqualFold(in.Name, "AwsDataCatalog") {
		return nil, invalid("AwsDataCatalog cannot be deleted.")
	}
	dc, err := s.catalog(c, in.Name)
	if err != nil {
		return nil, err
	}
	delete(s.st.Catalogs, key(c, strings.ToLower(in.Name)))
	delete(s.st.Tags, key(c, "datacatalog", in.Name))
	return obj{"DataCatalog": dc}, nil
}

// ---- tags ----

type tagInput struct {
	ResourceARN string
	Tags        []tag
	TagKeys     []string
	pageInput
}

// addTags sets tags on a resource; lock held.
func (s *Service) addTags(k string, tags []tag) {
	if len(tags) == 0 {
		return
	}
	t := maps.Clone(s.st.Tags[k])
	if t == nil {
		t = map[string]string{}
	}
	for _, tg := range tags {
		t[tg.Key] = tg.Value
	}
	s.st.Tags[k] = t
}

// tagKey finds the workgroup or data catalog an ARN names. The ARN may lack
// the account (Terraform without an account id builds such ARNs). Lock held.
func (s *Service) tagKey(c *core.Ctx, arn string) (string, error) {
	_, region, account, res, ok := core.ParseARN(arn)
	kind, name, _ := strings.Cut(res, "/")
	if !ok || name == "" {
		return "", invalid("Invalid ARN %s", arn)
	}
	rc := &core.Ctx{Account: cmp.Or(account, c.Account), Region: cmp.Or(region, c.Region)}
	var err error
	switch kind {
	case "workgroup":
		_, err = s.workgroup(rc, name)
	case "datacatalog":
		_, err = s.catalog(rc, name)
	default:
		err = errors.New("unknown")
	}
	if err != nil {
		return "", notFound("Resource %s does not exist.", arn)
	}
	return key(rc, kind, name), nil
}

func (s *Service) tagResource(c *core.Ctx, in *tagInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.tagKey(c, in.ResourceARN)
	if err == nil {
		s.addTags(k, in.Tags)
	}
	return nil, err
}

func (s *Service) untagResource(c *core.Ctx, in *tagInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.tagKey(c, in.ResourceARN)
	if err != nil {
		return nil, err
	}
	t := maps.Clone(s.st.Tags[k])
	for _, tk := range in.TagKeys {
		delete(t, tk)
	}
	s.st.Tags[k] = t
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, in *tagInput) (any, error) {
	s.mu.Lock()
	k, err := s.tagKey(c, in.ResourceARN)
	out := []tag{}
	for _, tk := range core.SortedKeys(s.st.Tags[k]) {
		out = append(out, tag{tk, s.st.Tags[k][tk]})
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	items, next := page(out, in.NextToken, in.MaxResults, 100)
	return listOut("Tags", items, next), nil
}

// ---- query executions ----

type startInput struct {
	QueryString           string
	ClientRequestToken    string
	QueryExecutionContext *execContext
	ResultConfiguration   *resultConfig
	WorkGroup             string
	ExecutionParameters   []string
}

// startQuery queues a query; it runs in the background.
func (s *Service) startQuery(c *core.Ctx, in *startInput) (any, error) {
	if strings.TrimSpace(in.QueryString) == "" {
		return nil, invalid("QueryString is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tk := key(c, "query", in.ClientRequestToken)
	if id := s.st.Tokens[tk]; id != "" && in.ClientRequestToken != "" {
		return obj{"QueryExecutionId": id}, nil
	}
	wg, err := s.workgroup(c, in.WorkGroup)
	if err != nil {
		return nil, err
	}
	if wg["State"] == "DISABLED" {
		return nil, invalid("WorkGroup %s is disabled.", wg["Name"])
	}
	cfg, _ := wg["Configuration"].(obj)
	rc, _ := cfg["ResultConfiguration"].(obj)
	out := str(rc["OutputLocation"])
	if in.ResultConfiguration != nil && in.ResultConfiguration.OutputLocation != "" && (out == "" || cfg["EnforceWorkGroupConfiguration"] != true) {
		out = in.ResultConfiguration.OutputLocation
	}
	if out == "" {
		return nil, invalid("No output location provided. An output location is required either through the Workgroup result configuration setting or as an API input.")
	}
	if !strings.HasPrefix(out, "s3://") {
		return nil, invalid("%s is not a valid S3 output location.", out)
	}
	id, kind := core.UUID(), statementType(in.QueryString)
	ext := ".txt"
	if kind == "DML" {
		ext = ".csv"
	}
	s.st.Seq++
	q := &query{Account: c.Account, Region: c.Region, Seq: s.st.Seq, X: execution{QueryExecutionId: id, Query: in.QueryString, StatementType: kind,
		ResultConfiguration: resultConfig{strings.TrimSuffix(out, "/") + "/" + id + ext}, QueryExecutionContext: in.QueryExecutionContext,
		Status: status{State: "QUEUED", SubmissionDateTime: now()}, WorkGroup: str(wg["Name"]), EngineVersion: cfg["EngineVersion"].(obj),
		ExecutionParameters: in.ExecutionParameters}}
	s.st.Queries[key(c, id)] = q
	if in.ClientRequestToken != "" {
		s.st.Tokens[tk] = id
	}
	s.wg.Add(1)
	qk := key(c, id)
	core.Go("athena.run", func() { s.run(qk) })
	return obj{"QueryExecutionId": id}, nil
}

// query returns a query execution; lock held.
func (s *Service) query(c *core.Ctx, id string) (*query, error) {
	if q := s.st.Queries[key(c, id)]; q != nil {
		return q, nil
	}
	return nil, invalid("QueryExecution %s was not found", id)
}

type queryInput struct {
	QueryExecutionId  string
	QueryExecutionIds []string
	WorkGroup         string
	pageInput
}

func (s *Service) getQuery(c *core.Ctx, in *queryInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.query(c, in.QueryExecutionId)
	if err != nil {
		return nil, err
	}
	return obj{"QueryExecution": q.X}, nil
}

func (s *Service) batchGetQuery(c *core.Ctx, in *queryInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	found, missing := []execution{}, []obj{}
	for _, id := range in.QueryExecutionIds {
		if q, err := s.query(c, id); err == nil {
			found = append(found, q.X)
		} else {
			missing = append(missing, obj{"QueryExecutionId": id, "ErrorCode": "INVALID_INPUT", "ErrorMessage": core.AsError(err).Msg})
		}
	}
	return obj{"QueryExecutions": found, "UnprocessedQueryExecutionIds": missing}, nil
}

// listQueries lists a workgroup's query executions, newest first.
func (s *Service) listQueries(c *core.Ctx, in *queryInput) (any, error) {
	s.mu.Lock()
	wg, err := s.workgroup(c, in.WorkGroup)
	var qs []*query
	for k, q := range s.st.Queries {
		if strings.HasPrefix(k, key(c, "")) && err == nil && q.X.WorkGroup == wg["Name"] {
			qs = append(qs, q)
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	slices.SortFunc(qs, func(a, b *query) int { return cmp.Compare(b.Seq, a.Seq) })
	ids := []string{}
	for _, q := range qs {
		ids = append(ids, q.X.QueryExecutionId)
	}
	items, next := page(ids, in.NextToken, in.MaxResults, 50)
	return listOut("QueryExecutionIds", items, next), nil
}

func (s *Service) getResults(c *core.Ctx, in *queryInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.query(c, in.QueryExecutionId)
	if err != nil {
		return nil, err
	}
	switch st := q.X.Status; st.State {
	case "SUCCEEDED":
	case "FAILED":
		return nil, invalid("%s", st.StateChangeReason)
	case "CANCELLED":
		return nil, invalid("Query has been cancelled")
	default:
		return nil, invalid("Query has not yet finished. Current state: %s", st.State)
	}
	rows, next := page(q.Rows, in.NextToken, min(in.MaxResults, 1000), 1000)
	out := make([]obj, len(rows))
	for i, r := range rows {
		data := make([]obj, len(r))
		for j, v := range r {
			data[j] = obj{}
			if v != nil {
				data[j]["VarCharValue"] = *v
			}
		}
		out[i] = obj{"Data": data}
	}
	res := listOut("ResultSet", obj{"Rows": out, "ResultSetMetadata": obj{"ColumnInfo": orEmpty(q.Cols)}}, next)
	res["UpdateCount"] = 0
	return res, nil
}

func (s *Service) stopQuery(c *core.Ctx, in *queryInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.query(c, in.QueryExecutionId)
	if err != nil {
		return nil, err
	}
	if st := q.X.Status.State; st == "QUEUED" || st == "RUNNING" {
		done := now()
		q.X.Status.State, q.X.Status.CompletionDateTime = "CANCELLED", &done
		if stop := s.stops[key(c, in.QueryExecutionId)]; stop != nil {
			stop()
		}
	}
	return nil, nil
}

// finish records the outcome of a query that ran (or could not).
func (q *query) finish(err error) {
	done := now()
	st := &q.X.Status
	st.CompletionDateTime = &done
	if err == nil {
		st.State = "SUCCEEDED"
		return
	}
	cat := 2
	if errors.Is(err, duckdb.ErrNoDocker) {
		cat = 1
	}
	st.State, st.StateChangeReason, st.AthenaError = "FAILED", err.Error(), &athenaError{ErrorCategory: cat, ErrorMessage: err.Error()}
}

// run executes a queued query: DDL through Glue, SELECTs on DuckDB.
func (s *Service) run(k string) {
	defer s.wg.Done()
	s.mu.Lock()
	q := s.st.Queries[k]
	if q == nil || q.X.Status.State != "QUEUED" {
		s.mu.Unlock()
		return
	}
	ctx, stop := context.WithCancel(s.ctx)
	defer stop()
	s.stops[k] = stop
	q.X.Status.State = "RUNNING"
	x, j := q.X, &job{s: s, ctx: ctx, account: q.Account, region: q.Region, id: q.X.QueryExecutionId, db: "default"}
	if x.QueryExecutionContext != nil && x.QueryExecutionContext.Database != "" {
		j.db = strings.ToLower(x.QueryExecutionContext.Database)
	}
	s.mu.Unlock()

	start := time.Now()
	res, err := j.execute(x.Query, x.ExecutionParameters, x.WorkGroup)
	ran := time.Since(start)
	if err == nil {
		err = j.write(x.ResultConfiguration.OutputLocation, res)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.stops, k)
	if s.st.Queries[k] != q || q.X.Status.State != "RUNNING" { // cancelled or reset meanwhile
		return
	}
	q.finish(err)
	sub := time.Time(q.X.Status.SubmissionDateTime)
	stats := &statistics{EngineExecutionTimeInMillis: ran.Milliseconds(), TotalExecutionTimeInMillis: time.Since(sub).Milliseconds(),
		QueryQueueTimeInMillis: start.Sub(sub).Milliseconds(), ServiceProcessingTimeInMillis: (time.Since(start) - ran).Milliseconds()}
	if err == nil {
		q.Cols, q.Rows, stats.DataScannedInBytes = res.cols, res.rows, res.scanned
	}
	q.X.Statistics = stats
}

// statementType is what Athena calls a statement: DDL, UTILITY (SHOW,
// DESCRIBE) or DML (SELECT and the rest).
func statementType(sql string) string {
	switch firstWord(sql) {
	case "CREATE", "DROP", "ALTER", "MSCK":
		return "DDL"
	case "SHOW", "DESCRIBE", "DESC":
		return "UTILITY"
	}
	return "DML"
}

// firstWord is a statement's first keyword, upper case, after comments and parentheses.
func firstWord(sql string) string {
	toks, _ := lex(sql, false)
	for _, t := range toks {
		if t.k == 'w' {
			return strings.ToUpper(t.s)
		}
		if !t.is("(") {
			break
		}
	}
	return ""
}

// execute runs a statement: EXECUTE resolves a prepared statement, ?
// placeholders take the parameters, DDL goes to Glue and the rest to DuckDB.
func (j *job) execute(sql string, params []string, wg string) (*result, error) {
	sql = strings.TrimSuffix(strings.TrimSpace(sql), ";")
	if f := strings.Fields(sql); len(f) >= 2 && strings.EqualFold(f[0], "EXECUTE") {
		j.s.mu.Lock()
		_, ps, err := j.s.prepared(&core.Ctx{Account: j.account, Region: j.region}, wg, f[1])
		j.s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if rest := strings.TrimSpace(strings.TrimSpace(sql[len(f[0]):])[len(f[1]):]); rest != "" { // USING …
			if !strings.EqualFold(f[2], "USING") {
				return nil, fmt.Errorf("line 1:1: mismatched input '%s'. Expecting: USING", f[2])
			}
			params = nil
			for _, a := range splitArgs(rest[len(f[2]):]) {
				params = append(params, strings.TrimSpace(a))
			}
		}
		sql = strings.TrimSuffix(strings.TrimSpace(str(ps["QueryStatement"])), ";")
	}
	if len(params) > 0 {
		var err error
		if sql, err = bind(sql, params); err != nil {
			return nil, err
		}
	}
	switch w := firstWord(sql); {
	case statementType(sql) != "DML":
		return j.ddl(sql)
	case w == "INSERT" || w == "UPDATE" || w == "DELETE" || w == "MERGE" || w == "UNLOAD":
		return nil, errors.New("pogi: Athena runs SELECT queries only; " + w + " is not supported")
	}
	return j.query(sql)
}

// ---- metadata, read from Glue ----

type metaInput struct {
	CatalogName, DatabaseName, TableName, Expression string
	pageInput
}

// glueCall calls Glue for a metadata request; a missing database or table is a MetadataException.
func (s *Service) glueCall(c *core.Ctx, catalog, op string, in, out any) error {
	s.mu.Lock()
	_, err := s.catalog(c, catalog)
	s.mu.Unlock()
	if err == nil {
		err = s.hub.Call(c.Account, c.Region, "AWSGlue", op, in, out)
	}
	if code(err) == "EntityNotFoundException" {
		return core.Errorf(400, "MetadataException", "%s", core.AsError(err).Msg)
	}
	return err
}

func database(d gDatabase) obj {
	return obj{"Name": d.Name, "Description": d.Description, "Parameters": orEmptyMap(d.Parameters)}
}

func (s *Service) getDatabase(c *core.Ctx, in *metaInput) (any, error) {
	var out struct{ Database gDatabase }
	if err := s.glueCall(c, in.CatalogName, "GetDatabase", obj{"Name": in.DatabaseName}, &out); err != nil {
		return nil, err
	}
	return obj{"Database": database(out.Database)}, nil
}

func (s *Service) listDatabases(c *core.Ctx, in *metaInput) (any, error) {
	var out struct {
		DatabaseList []gDatabase
		NextToken    string
	}
	if err := s.glueCall(c, in.CatalogName, "GetDatabases", obj{"NextToken": in.NextToken, "MaxResults": cmp.Or(in.MaxResults, 50)}, &out); err != nil {
		return nil, err
	}
	dbs := []obj{}
	for _, d := range out.DatabaseList {
		dbs = append(dbs, database(d))
	}
	return listOut("DatabaseList", dbs, out.NextToken), nil
}

// tableMetadata is Athena's view of a Glue table: the storage details go to
// Parameters, as Athena shows them.
func tableMetadata(t gTable) obj {
	sd := t.StorageDescriptor
	params := maps.Clone(t.Parameters)
	if params == nil {
		params = map[string]string{}
	}
	params["location"], params["inputformat"], params["outputformat"] = sd.Location, sd.InputFormat, sd.OutputFormat
	params["serde.serialization.lib"] = sd.SerdeInfo.SerializationLibrary
	for k, v := range sd.SerdeInfo.Parameters {
		params["serde.param."+k] = v
	}
	return obj{"Name": t.Name, "CreateTime": t.CreateTime, "LastAccessTime": t.LastAccessTime, "TableType": t.TableType,
		"Columns": orEmpty(sd.Columns), "PartitionKeys": orEmpty(t.PartitionKeys), "Parameters": params}
}

func (s *Service) getTableMetadata(c *core.Ctx, in *metaInput) (any, error) {
	var out struct{ Table gTable }
	if err := s.glueCall(c, in.CatalogName, "GetTable", obj{"DatabaseName": in.DatabaseName, "Name": in.TableName}, &out); err != nil {
		return nil, err
	}
	return obj{"TableMetadata": tableMetadata(out.Table)}, nil
}

func (s *Service) listTableMetadata(c *core.Ctx, in *metaInput) (any, error) {
	var out struct {
		TableList []gTable
		NextToken string
	}
	err := s.glueCall(c, in.CatalogName, "GetTables", obj{"DatabaseName": in.DatabaseName, "Expression": in.Expression,
		"NextToken": in.NextToken, "MaxResults": cmp.Or(in.MaxResults, 50)}, &out)
	if err != nil {
		return nil, err
	}
	tables := []obj{}
	for _, t := range out.TableList {
		tables = append(tables, tableMetadata(t))
	}
	return listOut("TableMetadataList", tables, out.NextToken), nil
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	wgs := core.Table{Name: "Workgroups", Columns: []string{"Workgroup", "State", "Output location", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.WorkGroups) {
		wg := s.st.WorkGroups[k]
		cfg, _ := wg["Configuration"].(obj)
		rc, _ := cfg["ResultConfiguration"].(obj)
		p := strings.Split(k, "/")
		wgs.Rows = append(wgs.Rows, []string{str(wg["Name"]), str(wg["State"]), str(rc["OutputLocation"]), p[1], p[0]})
	}
	qs := core.Table{Name: "Queries", Columns: []string{"Query execution", "State", "Workgroup", "Submitted", "Query"}}
	var all []*query
	for _, q := range s.st.Queries {
		all = append(all, q)
	}
	slices.SortFunc(all, func(a, b *query) int { return cmp.Compare(b.Seq, a.Seq) })
	for _, q := range all {
		qs.Rows = append(qs.Rows, []string{q.X.QueryExecutionId, q.X.Status.State, q.X.WorkGroup,
			time.Time(q.X.Status.SubmissionDateTime).Format(time.DateTime), q.X.Query})
	}
	return []core.Table{wgs, qs}
}

// Inspect returns a workgroup or a query execution.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, q := range s.st.Queries {
		if strings.HasSuffix(k, "/"+id) {
			return q.X
		}
	}
	for _, wg := range s.st.WorkGroups {
		if wg["Name"] == id {
			return wg
		}
	}
	return nil
}
