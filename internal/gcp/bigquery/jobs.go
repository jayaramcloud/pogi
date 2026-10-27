package bigquery

import (
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
)

type jobRef struct {
	ProjectID string `json:"projectId"`
	JobID     string `json:"jobId"`
	Location  string `json:"location,omitempty"`
}

// job is a job that has run: pogi runs jobs inside the call that makes them,
// so a job is DONE when it is returned (and cancelling it changes nothing).
type job struct {
	Ref     jobRef          `json:"jobReference"`
	Type    string          `json:"type"` // query, load, extract or copy
	Config  json.RawMessage `json:"configuration"`
	Created int64           `json:"created"`
	Ended   int64           `json:"ended"`
	Err     *jobErr         `json:"error,omitempty"`
	Stats   map[string]any  `json:"stats,omitempty"` // the type's statistics
	res     *result         // a query's rows
}

type jobErr struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

func (j *job) api() map[string]any {
	st := map[string]any{"creationTime": strconv.FormatInt(j.Created, 10), "startTime": strconv.FormatInt(j.Created, 10), "endTime": strconv.FormatInt(j.Ended, 10),
		"totalBytesProcessed": "0"}
	if len(j.Stats) > 0 {
		st[j.Type] = j.Stats
		if v, ok := j.Stats["totalBytesProcessed"]; ok {
			st["totalBytesProcessed"] = v
		}
	}
	status := map[string]any{"state": "DONE"}
	if j.Err != nil {
		e := map[string]string{"reason": j.Err.Reason, "message": j.Err.Message}
		status["errorResult"], status["errors"] = e, []map[string]string{e}
	}
	var cfg any = map[string]any{}
	if len(j.Config) > 0 {
		cfg = j.Config
	}
	return map[string]any{"kind": "bigquery#job", "etag": "pogi", "id": j.Ref.ProjectID + ":" + j.Ref.Location + "." + j.Ref.JobID,
		"selfLink":      "https://bigquery.googleapis.com/bigquery/v2/projects/" + j.Ref.ProjectID + "/jobs/" + j.Ref.JobID,
		"user_email":    "pogi@pogi.iam.gserviceaccount.com",
		"configuration": cfg, "jobReference": j.Ref, "statistics": st, "status": status}
}

type queryParam struct {
	Name           string `json:"name"`
	ParameterType  ptype  `json:"parameterType"`
	ParameterValue pvalue `json:"parameterValue"`
}

type ptype struct {
	Type        string `json:"type"`
	ArrayType   *ptype `json:"arrayType"`
	StructTypes []struct {
		Name string `json:"name"`
		Type ptype  `json:"type"`
	} `json:"structTypes"`
}

type pvalue struct {
	Value        *string           `json:"value"`
	ArrayValues  []pvalue          `json:"arrayValues"`
	StructValues map[string]pvalue `json:"structValues"`
}

type queryCfg struct {
	Query             string       `json:"query"`
	UseLegacySQL      *bool        `json:"useLegacySql"`
	DefaultDataset    *datasetRef  `json:"defaultDataset"`
	DestinationTable  *tableRef    `json:"destinationTable"`
	WriteDisposition  string       `json:"writeDisposition"`
	CreateDisposition string       `json:"createDisposition"`
	QueryParameters   []queryParam `json:"queryParameters"`
	ParameterMode     string       `json:"parameterMode"`
}

type jobConfig struct {
	Query   *queryCfg   `json:"query"`
	Load    *loadCfg    `json:"load"`
	Extract *extractCfg `json:"extract"`
	Copy    *copyCfg    `json:"copy"`
	DryRun  bool        `json:"dryRun"`
}

func (s *Service) newJob(project string, ref *jobRef, typ string, cfg json.RawMessage) (*job, error) {
	j := &job{Ref: jobRef{ProjectID: project, JobID: "job_" + core.RandHex(12), Location: "US"}, Type: typ, Config: cfg, Created: ms(time.Now())}
	if ref != nil {
		if ref.JobID != "" {
			j.Ref.JobID = ref.JobID
		}
		if ref.Location != "" {
			j.Ref.Location = ref.Location
		}
	}
	return j, nil
}

// keep stores a finished job (the last 1000 are kept).
func (s *Service) keep(j *job) error {
	j.Ended = ms(time.Now())
	s.mu.Lock()
	defer s.mu.Unlock()
	k := j.Ref.ProjectID + "/" + j.Ref.JobID
	if _, ok := s.st.jobs[k]; ok {
		return errf(409, "duplicate", "Already Exists: Job %s:%s.%s", j.Ref.ProjectID, j.Ref.Location, j.Ref.JobID)
	}
	s.st.jobs[k] = j
	s.st.jobOrder = append(s.st.jobOrder, k)
	if len(s.st.jobOrder) > 1000 {
		delete(s.st.jobs, s.st.jobOrder[0])
		s.st.jobOrder = s.st.jobOrder[1:]
	}
	return nil
}

func (s *Service) insertJob(r *http.Request, seg []string) (any, error) {
	var in struct {
		Configuration json.RawMessage `json:"configuration"`
		JobReference  *jobRef         `json:"jobReference"`
	}
	if err := decodeBody(r, &in); err != nil {
		return nil, err
	}
	var cfg jobConfig
	if err := json.Unmarshal(in.Configuration, &cfg); err != nil {
		return nil, invalid("Invalid job configuration: %v", err)
	}
	project := seg[1]
	var j *job
	var err error
	switch {
	case cfg.Query != nil:
		if j, err = s.newJob(project, in.JobReference, "query", in.Configuration); err == nil {
			err = s.runQueryJob(r, j, cfg.Query, cfg.DryRun)
		}
	case cfg.Load != nil:
		j, err = s.newJob(project, in.JobReference, "load", in.Configuration)
		if err == nil {
			err = s.runLoad(r, j, cfg.Load)
		}
	case cfg.Extract != nil:
		j, err = s.newJob(project, in.JobReference, "extract", in.Configuration)
		if err == nil {
			err = s.runExtract(j, cfg.Extract)
		}
	case cfg.Copy != nil:
		j, err = s.newJob(project, in.JobReference, "copy", in.Configuration)
		if err == nil {
			err = s.runCopy(j, cfg.Copy)
		}
	default:
		return nil, invalid("Job configuration must contain exactly one of query, load, extract or copy")
	}
	if err != nil {
		return nil, err
	}
	if cfg.DryRun {
		return j.api(), nil
	}
	if err := s.keep(j); err != nil {
		return nil, err
	}
	return j.api(), nil
}

func (s *Service) findJob(project, id string) (*job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.st.jobs[project+"/"+id]
	if j == nil {
		return nil, notFound("Not found: Job %s:%s", project, id)
	}
	return j, nil
}

func (s *Service) getJob(_ *http.Request, seg []string) (any, error) {
	j, err := s.findJob(seg[1], seg[3])
	if err != nil {
		return nil, err
	}
	return j.api(), nil
}

// cancelJob does nothing: a job is finished when its call returns.
func (s *Service) cancelJob(_ *http.Request, seg []string) (any, error) {
	j, err := s.findJob(seg[1], seg[3])
	if err != nil {
		return nil, err
	}
	return map[string]any{"kind": "bigquery#jobCancelResponse", "job": j.api()}, nil
}

func (s *Service) listJobs(r *http.Request, seg []string) (any, error) {
	s.mu.Lock()
	var items []map[string]any
	for i := len(s.st.jobOrder) - 1; i >= 0; i-- { // newest first
		if j := s.st.jobs[s.st.jobOrder[i]]; j != nil && j.Ref.ProjectID == seg[1] {
			a := j.api()
			items = append(items, map[string]any{"kind": "bigquery#job", "id": a["id"], "jobReference": j.Ref, "state": "DONE", "configuration": a["configuration"],
				"status": a["status"], "statistics": a["statistics"], "user_email": a["user_email"]})
		}
	}
	s.mu.Unlock()
	page, next := pageOf(items, r.URL.Query().Get("maxResults"), r.URL.Query().Get("pageToken"))
	out := map[string]any{"kind": "bigquery#jobList", "etag": "pogi", "jobs": page}
	if next != "" {
		out["nextPageToken"] = next
	}
	return out, nil
}

// ---- queries ----

// literalOf is the SQL literal of a query parameter's value.
func literalOf(t ptype, v pvalue) (string, error) {
	typ := strings.ToUpper(t.Type)
	null := v.Value == nil && v.ArrayValues == nil && v.StructValues == nil
	switch typ {
	case "ARRAY":
		if t.ArrayType == nil {
			return "", invalid("An ARRAY query parameter needs arrayType")
		}
		if null {
			return "NULL", nil
		}
		var elems []string
		for _, e := range v.ArrayValues {
			l, err := literalOf(*t.ArrayType, e)
			if err != nil {
				return "", err
			}
			elems = append(elems, l)
		}
		if len(elems) == 0 {
			et := (&translator{}).paramType(*t.ArrayType)
			return "CAST([] AS " + et + "[])", nil
		}
		return "[" + strings.Join(elems, ", ") + "]", nil
	case "STRUCT":
		var fields []string
		for _, f := range t.StructTypes {
			l, err := literalOf(f.Type, v.StructValues[f.Name])
			if err != nil {
				return "", err
			}
			fields = append(fields, ident(f.Name)+" := "+l)
		}
		return "struct_pack(" + strings.Join(fields, ", ") + ")", nil
	}
	if null {
		return "CAST(NULL AS " + (&translator{}).paramType(t) + ")", nil
	}
	val := *v.Value
	f := &Field{Name: "parameter", Type: typ}
	if c, ok := canonType(typ); ok {
		f.Type = c
	}
	canon, err := scalar(f, val, false)
	if err != nil {
		return "", invalid("Invalid value %q for a %s query parameter", val, t.Type)
	}
	switch f.Type {
	case "INTEGER":
		return canon.(string), nil
	case "FLOAT":
		if fl, ok := canon.(float64); ok {
			return strconv.FormatFloat(fl, 'g', -1, 64), nil
		}
		return "CAST(" + literal(strings.ToLower(canon.(string))) + " AS DOUBLE)", nil
	case "BOOLEAN":
		return strconv.FormatBool(canon.(bool)), nil
	case "BYTES":
		return "from_base64(" + literal(canon.(string)) + ")", nil
	case "TIMESTAMP":
		return "TIMESTAMPTZ " + literal(canon.(string)), nil
	case "DATE", "TIME":
		return f.Type + " " + literal(canon.(string)), nil
	case "DATETIME":
		return "TIMESTAMP " + literal(canon.(string)), nil
	case "NUMERIC", "BIGNUMERIC":
		return "CAST(" + literal(canon.(string)) + " AS DECIMAL(38,9))", nil
	case "JSON":
		return "CAST(" + literal(canon.(string)) + " AS JSON)", nil
	}
	return literal(canon.(string)), nil
}

// paramType is DuckDB's name for a parameter's type.
func (tr *translator) paramType(t ptype) string {
	switch strings.ToUpper(t.Type) {
	case "ARRAY":
		if t.ArrayType != nil {
			return tr.paramType(*t.ArrayType) + "[]"
		}
	case "STRUCT":
		return "STRUCT()"
	}
	if c, ok := canonType(t.Type); ok {
		return duckType(&Field{Type: c}, false)
	}
	return "VARCHAR"
}

func paramsOf(qp []queryParam, mode string) (*params, error) {
	ps := &params{named: map[string]string{}}
	for _, p := range qp {
		l, err := literalOf(p.ParameterType, p.ParameterValue)
		if err != nil {
			return nil, err
		}
		if p.Name != "" && !strings.EqualFold(mode, "POSITIONAL") {
			ps.named[p.Name] = l
		} else {
			ps.positional = append(ps.positional, l)
		}
	}
	return ps, nil
}

// runQueryJob runs a query job's SQL and records what it made on j.
func (s *Service) runQueryJob(r *http.Request, j *job, cfg *queryCfg, dry bool) error {
	if cfg.UseLegacySQL != nil && *cfg.UseLegacySQL {
		return invalid("Legacy SQL is not supported by pogi: set useLegacySql to false")
	}
	if strings.TrimSpace(cfg.Query) == "" {
		return invalid("Query is empty")
	}
	ps, err := paramsOf(cfg.QueryParameters, cfg.ParameterMode)
	if err != nil {
		return err
	}
	project, dataset := j.Ref.ProjectID, ""
	if cfg.DefaultDataset != nil {
		dataset = cfg.DefaultDataset.DatasetID
		if cfg.DefaultDataset.ProjectID != "" {
			project = cfg.DefaultDataset.ProjectID
		}
	}
	res, err := s.runSQL(r.Context(), project, dataset, cfg.Query, ps, dry)
	if err != nil {
		return err
	}
	j.res = res
	if cfg.DestinationTable != nil && res.fields != nil && !dry {
		d := *cfg.DestinationTable
		if d.ProjectID == "" {
			d.ProjectID = j.Ref.ProjectID
		}
		if err := s.putRows(d, res.fields, res.rows, cfg.WriteDisposition, cfg.CreateDisposition, true); err != nil {
			return err
		}
	}
	j.Stats = map[string]any{"statementType": res.verb, "totalBytesProcessed": strconv.FormatInt(res.processed, 10), "totalBytesBilled": "0",
		"cacheHit": false, "billingTier": 1}
	if res.fields != nil {
		j.Stats["schema"] = schema{Fields: res.fields}
	}
	if res.verb != "SELECT" {
		j.Stats["numDmlAffectedRows"] = strconv.FormatInt(res.affected, 10)
	}
	return nil
}

// query is jobs.query: a query job that answers with its first rows.
func (s *Service) query(r *http.Request, seg []string) (any, error) {
	var in struct {
		queryCfg
		MaxResults    int `json:"maxResults"`
		FormatOptions struct {
			UseInt64Timestamp bool `json:"useInt64Timestamp"`
		} `json:"formatOptions"`
		DryRun   bool   `json:"dryRun"`
		Location string `json:"location"`
	}
	if err := decodeBody(r, &in); err != nil {
		return nil, err
	}
	raw, _ := json.Marshal(map[string]any{"query": in.queryCfg})
	j, _ := s.newJob(seg[1], &jobRef{Location: in.Location}, "query", raw)
	if err := s.runQueryJob(r, j, &in.queryCfg, in.DryRun); err != nil {
		return nil, err
	}
	if !in.DryRun {
		if err := s.keep(j); err != nil {
			return nil, err
		}
	}
	out := j.resultsPage(0, in.MaxResults, "", "bigquery#queryResponse", in.FormatOptions.UseInt64Timestamp)
	return out, nil
}

// resultsPage is the body of getQueryResults and of the answer to jobs.query.
func (j *job) resultsPage(start, max int, token, kind string, i64 bool) map[string]any {
	out := map[string]any{"kind": kind, "etag": "pogi", "jobReference": j.Ref, "jobComplete": true, "cacheHit": false, "totalBytesProcessed": "0"}
	res := j.res
	if res == nil {
		return out
	}
	out["totalBytesProcessed"] = strconv.FormatInt(res.processed, 10)
	if res.fields != nil {
		out["schema"] = schema{Fields: res.fields}
		if token != "" {
			start = atoi(token, 0)
		}
		start = min(start, len(res.rows))
		page, next := pageOf(res.rows[start:], strconv.Itoa(max), "")
		out["totalRows"], out["rows"] = strconv.Itoa(len(res.rows)), wireRows(res.fields, page, i64)
		if next != "" {
			out["pageToken"] = strconv.Itoa(start + atoi(next, 0))
		}
	} else {
		out["totalRows"] = "0"
	}
	if res.verb != "SELECT" {
		out["numDmlAffectedRows"] = strconv.FormatInt(res.affected, 10)
	}
	return out
}

func (s *Service) queryResults(r *http.Request, seg []string) (any, error) {
	j, err := s.findJob(seg[1], seg[3])
	if err != nil {
		return nil, err
	}
	if j.Err != nil {
		return nil, errf(400, j.Err.Reason, "%s", j.Err.Message)
	}
	q := r.URL.Query()
	return j.resultsPage(atoi(q.Get("startIndex"), 0), atoi(q.Get("maxResults"), 0), q.Get("pageToken"), "bigquery#getQueryResultsResponse", gapi.Bool(q.Get("formatOptions.useInt64Timestamp"))), nil
}

// ---- tables written by jobs ----

// putRows writes rows to a table by the dispositions of a job. fs is the
// schema of the rows; with exact, the table takes it when it is created or
// truncated, and otherwise it is made to fit the table's.
func (s *Service) putRows(ref tableRef, fs []*Field, rows []row, write, create string, exact bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.getDS(ref.ProjectID, ref.DatasetID)
	if err != nil {
		return err
	}
	t := d.tables[ref.TableID]
	if t != nil && t.Type != "TABLE" {
		return invalid("Cannot write to %s %s", strings.ToLower(t.Type), ref)
	}
	if t == nil {
		if strings.EqualFold(create, "CREATE_NEVER") {
			return notFound("Not found: Table %s", ref)
		}
		t = &table{Ref: ref, Type: "TABLE", Created: ms(time.Now())}
		d.tables[ref.TableID] = t
		t.Schema = &schema{Fields: fs}
	} else {
		switch strings.ToUpper(write) {
		case "WRITE_TRUNCATE":
			t.Rows = nil
			if exact && fs != nil {
				t.Schema = &schema{Fields: fs}
			}
		case "WRITE_APPEND":
		default: // WRITE_EMPTY
			if len(t.Rows) > 0 {
				return errf(409, "duplicate", "Already Exists: Table %s", ref)
			}
		}
	}
	// Fit the rows to the table's columns by name.
	for _, rw := range rows {
		out := row{}
		for _, f := range t.fields() {
			if v, ok := rw[f.Name]; ok && v != nil {
				out[f.Name] = v
			}
		}
		t.Rows = append(t.Rows, out)
	}
	t.Modified = ms(time.Now())
	return nil
}

// sortedJobs is for the console.
func (s *Service) sortedJobs() []*job {
	var out []*job
	for _, k := range s.st.jobOrder {
		if j := s.st.jobs[k]; j != nil {
			out = append(out, j)
		}
	}
	slices.Reverse(out)
	return out
}
