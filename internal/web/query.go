package web

// Query editors: DynamoDB (Query, Scan, PartiQL SELECT), Cosmos DB (SQL) and
// Firestore (a structured query builder).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/nrndev/pogi/internal/core"
)

type queryData struct {
	Kind    string
	Targets []choice
	V       url.Values
	Res     *result
	Err     string
	Next    string // DynamoDB: link to the next page
	Idx     []string
	Ops     []string
}

func (ui *UI) queryPage(w http.ResponseWriter, r *http.Request) {
	kind, q := r.PathValue("kind"), r.URL.Query()
	d := &queryData{Kind: kind, V: q, Idx: []string{"1", "2", "3"}, Ops: []string{"==", "!=", "<", "<=", ">", ">=", "array-contains", "in"}}
	var run func(url.Values) (*result, error)
	switch kind {
	case "dynamodb":
		for _, t := range ui.rows(r, "dynamodb", "Tables") {
			d.Targets = append(d.Targets, choice{t[2] + "/" + t[1] + "/" + t[0], t[0]})
		}
		run = func(q url.Values) (*result, error) {
			res, next, err := ui.ddbRun(q)
			if next != "" {
				n := url.Values{}
				for k, v := range q {
					n[k] = v
				}
				n.Set("start", next)
				d.Next = "?" + n.Encode()
			}
			return res, err
		}
	case "cosmos":
		for _, c := range ui.rows(r, "cosmos", "Containers") {
			d.Targets = append(d.Targets, choice{c[1] + "/" + c[0], c[1] + " / " + c[0]})
		}
		run = ui.cosmosRun
	case "firestore":
		for _, c := range ui.rows(r, "firestore", "Collections") {
			d.Targets = append(d.Targets, choice{c[1] + "/" + c[2] + "/" + c[0], c[1] + " · " + c[0]})
		}
		run = ui.firestoreRun
	default:
		http.NotFound(w, r)
		return
	}
	if q.Get("run") != "" {
		res, err := run(q)
		d.Res = res
		if err != nil {
			d.Err = err.Error()
		}
	}
	ui.xrender(w, r, "x-query", map[string]string{"dynamodb": "DynamoDB", "cosmos": "Cosmos DB", "firestore": "Firestore"}[kind], kind, d)
}

// ---- DynamoDB ----

var avTypes = map[string]bool{"S": true, "N": true, "B": true, "BOOL": true, "NULL": true, "L": true, "M": true, "SS": true, "NS": true, "BS": true}

// av makes an AttributeValue of a plain JSON value; a value that is already
// one ({"S": "x"}) is kept.
func av(v any) any {
	switch v := v.(type) {
	case string:
		return map[string]any{"S": v}
	case json.Number:
		return map[string]any{"N": v.String()}
	case bool:
		return map[string]any{"BOOL": v}
	case nil:
		return map[string]any{"NULL": true}
	case []any:
		l := make([]any, len(v))
		for i, x := range v {
			l[i] = av(x)
		}
		return map[string]any{"L": l}
	case map[string]any:
		if len(v) == 1 {
			for k := range v {
				if avTypes[k] {
					return v
				}
			}
		}
		m := map[string]any{}
		for k, x := range v {
			m[k] = av(x)
		}
		return map[string]any{"M": m}
	}
	return map[string]any{"S": fmt.Sprint(v)}
}

// plainAV is av's inverse: an AttributeValue as a plain JSON value.
func plainAV(v any) any {
	m, _ := v.(map[string]any)
	for k, x := range m {
		switch k {
		case "N":
			return json.Number(fmt.Sprint(x))
		case "NS":
			var l []any
			for _, n := range x.([]any) {
				l = append(l, json.Number(fmt.Sprint(n)))
			}
			return l
		case "NULL":
			return nil
		case "L":
			l := []any{}
			for _, e := range x.([]any) {
				l = append(l, plainAV(e))
			}
			return l
		case "M":
			o := map[string]any{}
			for n, e := range x.(map[string]any) {
				o[n] = plainAV(e)
			}
			return o
		}
		return x // S, B, BOOL, SS, BS
	}
	return v
}

func decodeJSON(s string, out any) error {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	return dec.Decode(out)
}

// ddbRun runs the form's Query, Scan or PartiQL SELECT on a table.
func (ui *UI) ddbRun(q url.Values) (*result, string, error) {
	t := strings.SplitN(q.Get("target"), "/", 3) // account/region/table
	if len(t) != 3 {
		return nil, "", errors.New("choose a table")
	}
	account, region, table := t[0], t[1], t[2]
	in := map[string]any{"TableName": table}
	op, filter, values := "Scan", strings.TrimSpace(q.Get("filter")), map[string]any{}
	if s := strings.TrimSpace(q.Get("values")); s != "" {
		var m map[string]any
		if err := decodeJSON(s, &m); err != nil {
			return nil, "", fmt.Errorf("values is not a JSON object: %v", err)
		}
		for k, v := range m {
			values[k] = av(v)
		}
	}
	names := map[string]any{}
	if s := strings.TrimSpace(q.Get("names")); s != "" && json.Unmarshal([]byte(s), &names) != nil {
		return nil, "", errors.New("names is not a JSON object")
	}
	switch q.Get("mode") {
	case "query":
		op = "Query"
		in["KeyConditionExpression"] = strings.TrimSpace(q.Get("key"))
		if q.Get("desc") != "" {
			in["ScanIndexForward"] = false
		}
	case "partiql":
		var err error
		if in, err = partiql(q.Get("stmt"), values); err != nil {
			return nil, "", err
		}
		op, filter, names = in["op"].(string), "", nil
		delete(in, "op")
		values = nil
	}
	if filter != "" {
		in["FilterExpression"] = filter
	}
	if len(values) > 0 {
		in["ExpressionAttributeValues"] = values
	}
	if len(names) > 0 {
		in["ExpressionAttributeNames"] = names
	}
	if s := q.Get("index"); s != "" {
		in["IndexName"] = s
	}
	if n, _ := strconv.Atoi(q.Get("limit")); n > 0 {
		in["Limit"] = n
	}
	if s := q.Get("start"); s != "" {
		var k map[string]any
		if json.Unmarshal([]byte(s), &k) == nil {
			in["ExclusiveStartKey"] = k
		}
	}
	var out struct {
		Items            []map[string]any
		LastEvaluatedKey map[string]any
		Count            int
		ScannedCount     int
	}
	if err := ui.hub.Call(account, region, "DynamoDB_20120810", op, in, &out); err != nil {
		return nil, "", err
	}
	docs := make([]any, len(out.Items))
	for i, it := range out.Items {
		m := map[string]any{}
		for k, v := range it {
			m[k] = plainAV(v)
		}
		docs[i] = m
	}
	res := fromDocs(docs)
	res.Note = fmt.Sprintf("%d items (%d scanned)", out.Count, out.ScannedCount)
	next := ""
	if out.LastEvaluatedKey != nil {
		b, _ := json.Marshal(out.LastEvaluatedKey)
		next = string(b)
	}
	return res, next, nil
}

var (
	selectRE = regexp.MustCompile(`(?is)^\s*select\s+(.+?)\s+from\s+"?([\w.\-]+)"?(?:\."([^"]+)")?\s*(?:where\s+(.+?))?\s*(?:limit\s+(\d+))?\s*;?\s*$`)
	andRE    = regexp.MustCompile(`(?i)\s+and\s+`)
	condRE   = regexp.MustCompile(`(?s)^\s*"?([\w.\-]+)"?\s*(=|<>|!=|<=|>=|<|>)\s*(.+?)\s*$`)
)

// partiql turns a PartiQL SELECT into a Scan: attributes become a projection,
// the AND-ed comparisons of WHERE a filter, and `?` takes the next of the
// values (a JSON array in the form's values field, plain or typed).
// ponytail: SELECT only, comparisons joined by AND, always a Scan; the upgrade
// is a real parser in the dynamodb package with INSERT, UPDATE and DELETE.
func partiql(stmt string, params map[string]any) (map[string]any, error) {
	m := selectRE.FindStringSubmatch(stmt)
	if m == nil {
		return nil, errors.New(`PartiQL: only SELECT <attrs|*> FROM "table"["index"] [WHERE a = 1 AND b > 'x'] [LIMIT n]`)
	}
	in := map[string]any{"op": "Scan", "TableName": m[2]}
	names, vals := map[string]any{}, map[string]any{}
	name := func(a string) string {
		for k, v := range names {
			if v == a {
				return k
			}
		}
		k := "#a" + strconv.Itoa(len(names))
		names[k] = a
		return k
	}
	if m[3] != "" {
		in["IndexName"] = m[3]
	}
	if p := strings.TrimSpace(m[1]); p != "*" {
		var ps []string
		for _, a := range strings.Split(p, ",") {
			ps = append(ps, name(strings.Trim(strings.TrimSpace(a), `"`)))
		}
		in["ProjectionExpression"] = strings.Join(ps, ", ")
	}
	var list []any // positional parameters: stored under "?" by the caller as ":p0", ":p1"...
	for i := 0; ; i++ {
		v, ok := params[":p"+strconv.Itoa(i)]
		if !ok {
			break
		}
		list = append(list, v)
	}
	var conds []string
	if m[4] != "" {
		for _, c := range andRE.Split(m[4], -1) {
			cm := condRE.FindStringSubmatch(c)
			if cm == nil {
				return nil, fmt.Errorf("PartiQL: cannot read the condition %q", c)
			}
			var v any
			switch lit := cm[3]; {
			case lit == "?":
				if len(list) == 0 {
					return nil, errors.New(`PartiQL: "?" needs a value: give values as {":p0": ..., ":p1": ...}`)
				}
				v, list = list[0], list[1:]
			case strings.HasPrefix(lit, "'"):
				v = map[string]any{"S": strings.ReplaceAll(strings.Trim(lit, "'"), "''", "'")}
			case lit == "true" || lit == "false":
				v = map[string]any{"BOOL": lit == "true"}
			case lit == "null":
				v = map[string]any{"NULL": true}
			default:
				if _, err := strconv.ParseFloat(lit, 64); err != nil {
					return nil, fmt.Errorf("PartiQL: cannot read the value %s", lit)
				}
				v = map[string]any{"N": lit}
			}
			ph := ":v" + strconv.Itoa(len(vals))
			vals[ph] = v
			op := map[string]string{"!=": "<>"}[cm[2]]
			if op == "" {
				op = cm[2]
			}
			conds = append(conds, name(cm[1])+" "+op+" "+ph)
		}
		in["FilterExpression"] = strings.Join(conds, " AND ")
	}
	if len(names) > 0 {
		in["ExpressionAttributeNames"] = names
	}
	if len(vals) > 0 {
		in["ExpressionAttributeValues"] = vals
	}
	if m[5] != "" {
		in["Limit"], _ = strconv.Atoi(m[5])
	}
	return in, nil
}

// ---- Cosmos DB ----

func (ui *UI) cosmosRun(q url.Values) (*result, error) {
	db, coll, ok := strings.Cut(q.Get("target"), "/")
	if !ok {
		return nil, errors.New("choose a container")
	}
	sql := strings.TrimSpace(q.Get("sql"))
	if sql == "" {
		sql = "SELECT * FROM c"
	}
	body, _ := json.Marshal(map[string]any{"query": sql, "parameters": []any{}})
	st, b := ui.gw("POST", "", "/dbs/"+url.PathEscape(db)+"/colls/"+url.PathEscape(coll)+"/docs", map[string]string{
		"Content-Type": "application/query+json", "x-ms-version": "2018-12-31", "x-ms-documentdb-isquery": "True",
		"x-ms-documentdb-query-enablecrosspartition": "True"}, body)
	if st >= 300 {
		return nil, &httpError{st, apiMessage(b)}
	}
	var out struct{ Documents []any }
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	// ponytail: the first page only; a continuation token is not followed.
	res := fromDocs(out.Documents)
	res.Note = fmt.Sprintf("%d documents", len(out.Documents))
	return res, nil
}

// ---- Firestore ----

var fsOps = map[string]firestorepb.StructuredQuery_FieldFilter_Operator{
	"==": firestorepb.StructuredQuery_FieldFilter_EQUAL, "!=": firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL,
	"<": firestorepb.StructuredQuery_FieldFilter_LESS_THAN, "<=": firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
	">": firestorepb.StructuredQuery_FieldFilter_GREATER_THAN, ">=": firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL,
	"array-contains": firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS, "in": firestorepb.StructuredQuery_FieldFilter_IN,
}

// fsValue reads a form value as JSON (5, true, "x", [1,2]); anything else is a string.
func fsValue(s string) *firestorepb.Value {
	var v any
	if json.Unmarshal([]byte(s), &v) != nil {
		v = s
	}
	return fsOf(v)
}

func fsOf(v any) *firestorepb.Value {
	switch v := v.(type) {
	case string:
		return &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: v}}
	case bool:
		return &firestorepb.Value{ValueType: &firestorepb.Value_BooleanValue{BooleanValue: v}}
	case float64:
		if v == float64(int64(v)) {
			return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: int64(v)}}
		}
		return &firestorepb.Value{ValueType: &firestorepb.Value_DoubleValue{DoubleValue: v}}
	case []any:
		a := &firestorepb.ArrayValue{}
		for _, x := range v {
			a.Values = append(a.Values, fsOf(x))
		}
		return &firestorepb.Value{ValueType: &firestorepb.Value_ArrayValue{ArrayValue: a}}
	}
	return &firestorepb.Value{ValueType: &firestorepb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
}

// fsPlain is a Firestore value as plain JSON.
func fsPlain(v *firestorepb.Value) any {
	switch t := v.GetValueType().(type) {
	case *firestorepb.Value_StringValue:
		return t.StringValue
	case *firestorepb.Value_IntegerValue:
		return t.IntegerValue
	case *firestorepb.Value_DoubleValue:
		return t.DoubleValue
	case *firestorepb.Value_BooleanValue:
		return t.BooleanValue
	case *firestorepb.Value_TimestampValue:
		return t.TimestampValue.AsTime().Format(time.RFC3339Nano)
	case *firestorepb.Value_ReferenceValue:
		return t.ReferenceValue
	case *firestorepb.Value_MapValue:
		m := map[string]any{}
		for k, x := range t.MapValue.GetFields() {
			m[k] = fsPlain(x)
		}
		return m
	case *firestorepb.Value_ArrayValue:
		l := []any{}
		for _, x := range t.ArrayValue.GetValues() {
			l = append(l, fsPlain(x))
		}
		return l
	}
	return nil
}

func (ui *UI) firestoreRun(q url.Values) (*result, error) {
	t := strings.SplitN(q.Get("target"), "/", 3) // project/database/collection path
	if len(t) != 3 {
		return nil, errors.New("choose a collection")
	}
	project, database, path := t[0], t[1], t[2]
	parent := "projects/" + project + "/databases/" + database + "/documents"
	coll := path
	if i := strings.LastIndexByte(path, '/'); i >= 0 { // a subcollection: the parent is the document above it
		parent, coll = parent+"/"+path[:i], path[i+1:]
	}
	sq := &firestorepb.StructuredQuery{From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: coll}}}
	var filters []*firestorepb.StructuredQuery_Filter
	for i := 1; i <= 3; i++ {
		f, op, v := strings.TrimSpace(q.Get("f"+strconv.Itoa(i))), q.Get("op"+strconv.Itoa(i)), strings.TrimSpace(q.Get("v"+strconv.Itoa(i)))
		if f == "" {
			continue
		}
		o, ok := fsOps[op]
		if !ok {
			return nil, fmt.Errorf("unknown operator %q", op)
		}
		filters = append(filters, &firestorepb.StructuredQuery_Filter{FilterType: &firestorepb.StructuredQuery_Filter_FieldFilter{
			FieldFilter: &firestorepb.StructuredQuery_FieldFilter{Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: f}, Op: o, Value: fsValue(v)}}})
	}
	switch len(filters) {
	case 0:
	case 1:
		sq.Where = filters[0]
	default:
		sq.Where = &firestorepb.StructuredQuery_Filter{FilterType: &firestorepb.StructuredQuery_Filter_CompositeFilter{
			CompositeFilter: &firestorepb.StructuredQuery_CompositeFilter{Op: firestorepb.StructuredQuery_CompositeFilter_AND, Filters: filters}}}
	}
	if f := strings.TrimSpace(q.Get("order")); f != "" {
		dir := firestorepb.StructuredQuery_ASCENDING
		if q.Get("dir") == "desc" {
			dir = firestorepb.StructuredQuery_DESCENDING
		}
		sq.OrderBy = []*firestorepb.StructuredQuery_Order{{Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: f}, Direction: dir}}
	}
	if n, _ := strconv.Atoi(q.Get("limit")); n > 0 {
		sq.Limit = wrapperspb.Int32(int32(n))
	}
	conn, err := grpc.NewClient(core.Hostport(ui.hub.URL), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := firestorepb.NewFirestoreClient(conn).RunQuery(ctx, &firestorepb.RunQueryRequest{
		Parent: parent, QueryType: &firestorepb.RunQueryRequest_StructuredQuery{StructuredQuery: sq}})
	if err != nil {
		return nil, err
	}
	var docs []any
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if d := resp.GetDocument(); d != nil {
			m := map[string]any{"__name__": d.Name[strings.LastIndexByte(d.Name, '/')+1:]}
			for k, v := range d.Fields {
				m[k] = fsPlain(v)
			}
			docs = append(docs, m)
		}
	}
	res := fromDocs(docs)
	b, _ := protojson.Marshal(sq)
	res.Note = fmt.Sprintf("%d documents · %s", len(docs), b)
	return res, nil
}
