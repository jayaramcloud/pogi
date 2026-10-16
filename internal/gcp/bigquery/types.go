package bigquery

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// The JSON shapes of the BigQuery v2 REST API that pogi serves.

type tableRef struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	TableID   string `json:"tableId"`
}

// String is the project:dataset.table form of messages.
func (r tableRef) String() string { return r.ProjectID + ":" + r.DatasetID + "." + r.TableID }

type datasetRef struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
}

// Field is one column of a schema. Types use the legacy names BigQuery
// reports (INTEGER, FLOAT, BOOLEAN, RECORD).
type Field struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Mode        string   `json:"mode,omitempty"`
	Description string   `json:"description,omitempty"`
	Fields      []*Field `json:"fields,omitempty"`
	MaxLength   string   `json:"maxLength,omitempty"`
	Precision   string   `json:"precision,omitempty"`
	Scale       string   `json:"scale,omitempty"`
}

type schema struct {
	Fields []*Field `json:"fields"`
}

type timePartitioning struct {
	Type                   string `json:"type,omitempty"`
	Field                  string `json:"field,omitempty"`
	ExpirationMs           string `json:"expirationMs,omitempty"`
	RequirePartitionFilter bool   `json:"requirePartitionFilter,omitempty"`
}

type clustering struct {
	Fields []string `json:"fields,omitempty"`
}

type view struct {
	Query        string `json:"query"`
	UseLegacySQL bool   `json:"useLegacySql"`
}

// table is a table or a view with its rows. Rows are kept apart from the
// REST resource (see api).
type table struct {
	Ref               tableRef          `json:"tableReference"`
	Type              string            `json:"type"` // TABLE or VIEW
	Schema            *schema           `json:"schema,omitempty"`
	View              *view             `json:"view,omitempty"`
	TimePartitioning  *timePartitioning `json:"timePartitioning,omitempty"`
	RangePartitioning json.RawMessage   `json:"rangePartitioning,omitempty"`
	Clustering        *clustering       `json:"clustering,omitempty"`
	Description       string            `json:"description,omitempty"`
	FriendlyName      string            `json:"friendlyName,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	ExpirationTime    string            `json:"expirationTime,omitempty"`
	Created           int64             `json:"creationTime,string"`
	Modified          int64             `json:"lastModifiedTime,string"`
	Rows              []row             `json:"rows,omitempty"`
	insertIDs         map[string]bool   // streaming dedupe; not persisted
}

// row maps field names to canonical values (see values.go); a missing key is NULL.
type row = map[string]any

func ms(t time.Time) int64 { return t.UnixMilli() }

func (t *table) fields() []*Field {
	if t.Schema == nil {
		return nil
	}
	return t.Schema.Fields
}

type dataset struct {
	Ref          datasetRef        `json:"datasetReference"`
	Location     string            `json:"location"`
	FriendlyName string            `json:"friendlyName,omitempty"`
	Description  string            `json:"description,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	DefaultTTLms string            `json:"defaultTableExpirationMs,omitempty"`
	Access       json.RawMessage   `json:"access,omitempty"`
	Created      int64             `json:"creationTime,string"`
	Modified     int64             `json:"lastModifiedTime,string"`
	tables       map[string]*table
}

// ---- errors ----

// apiErr is a BigQuery error: HTTP status, reason and message.
type apiErr struct {
	code   int
	reason string
	msg    string
}

func (e *apiErr) Error() string { return e.msg }

func errf(code int, reason, format string, a ...any) *apiErr {
	return &apiErr{code, reason, sprintf(format, a...)}
}

func notFound(format string, a ...any) *apiErr { return errf(404, "notFound", format, a...) }
func invalid(format string, a ...any) *apiErr  { return errf(400, "invalid", format, a...) }

func writeErr(w http.ResponseWriter, c *core.Ctx, err error) {
	e, ok := err.(*apiErr)
	if !ok {
		e = &apiErr{500, "backendError", err.Error()}
	}
	status := core.GoogleStatus(e.code)
	c.ErrCode = e.reason
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(e.code)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": e.code, "message": e.msg, "status": status,
		"errors": []map[string]string{{"message": e.msg, "domain": "global", "reason": e.reason}}}})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	json.NewEncoder(w).Encode(v)
}

var (
	datasetIDRE = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
	tableIDRE   = regexp.MustCompile(`^[\p{L}\p{M}\p{N}\p{Pc}\p{Pd} ]+$`)
	fieldRE     = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,299}$`)
)

func atoi(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}

func lower(s string) string { return strings.ToLower(s) }
