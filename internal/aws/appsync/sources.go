package appsync

import (
	"cmp"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

func dsErr(typ, msg string) map[string]any { return map[string]any{"type": typ, "message": msg} }

// invoke calls a data source with a request object. It returns the result
// and the error the response step sees as $ctx.error.
func (x *exec) invoke(ds *DataSource, req any, direct bool, rc *rctx) (any, any) {
	m := asMap(req)
	switch ds.Type {
	case "NONE":
		return m["payload"], nil
	case "AWS_LAMBDA":
		return x.lambda(ds, m, direct, rc)
	case "AMAZON_DYNAMODB":
		return x.dynamo(ds, m)
	case "HTTP":
		return httpCall(ds, m)
	case "RELATIONAL_DATABASE":
		return x.rds(ds, m)
	}
	return nil, dsErr("UnsupportedDataSource", "pogi does not run "+ds.Type+" data sources.")
}

// lambda invokes the function with the request's payload, or with the whole
// context for a direct Lambda resolver.
func (x *exec) lambda(ds *DataSource, m map[string]any, direct bool, rc *rctx) (any, any) {
	var cfg struct {
		LambdaFunctionArn string `json:"lambdaFunctionArn"`
	}
	json.Unmarshal(ds.LambdaConfig, &cfg)
	payload := m["payload"]
	batch := m["operation"] == "BatchInvoke"
	if direct {
		var prev any
		if rc.prev != nil {
			prev = map[string]any{"result": rc.prev}
		}
		payload = map[string]any{"arguments": rc.args, "identity": rc.identity, "source": rc.source, "info": rc.info, "prev": prev,
			"stash": rc.stash, "request": map[string]any{"headers": rc.headers, "domainName": nil}}
	} else if batch {
		payload = []any{payload} // ponytail: no batching across list items, a BatchInvoke carries one source
	}
	out, err := x.s.hub.Invoke(cfg.LambdaFunctionArn, []byte(toJSON(payload)))
	var fe *core.FunctionError
	switch {
	case errors.As(err, &fe):
		var e struct{ ErrorMessage string }
		json.Unmarshal(fe.Payload, &e)
		return nil, dsErr("Lambda:Unhandled", cmp.Or(e.ErrorMessage, string(fe.Payload)))
	case err != nil:
		return nil, dsErr("Lambda:"+core.AsError(err).Code, err.Error())
	}
	var v any
	json.Unmarshal(out, &v)
	if l := asList(v); batch && len(l) == 1 {
		v = l[0]
	}
	return v, nil
}

// dynamo translates an AppSync DynamoDB request into the DynamoDB API and
// its answer into plain JSON.
func (x *exec) dynamo(ds *DataSource, m map[string]any) (any, any) {
	var cfg struct{ TableName, AwsRegion string }
	json.Unmarshal(ds.DynamodbConfig, &cfg)
	call := func(op string, in map[string]any) (map[string]any, any) {
		var out map[string]any
		if err := x.s.hub.Call(x.api.Account, cmp.Or(cfg.AwsRegion, x.api.Region), "DynamoDB_20120810", op, in, &out); err != nil {
			e := core.AsError(err)
			return nil, dsErr("DynamoDB:"+e.Code, e.Msg)
		}
		return out, nil
	}
	op := str(m["operation"])
	in := ddbRequest(cfg.TableName, m)
	switch op {
	case "GetItem":
		out, err := call(op, in)
		return plainMap(out["Item"]), err
	case "PutItem":
		item := maps.Clone(asMap(m["key"]))
		maps.Copy(item, asMap(m["attributeValues"]))
		in["Item"] = item
		delete(in, "Key")
		if _, err := call(op, in); err != nil {
			return nil, err
		}
		return plainMap(item), nil
	case "UpdateItem", "DeleteItem":
		in["ReturnValues"] = "ALL_OLD"
		if op == "UpdateItem" {
			in["ReturnValues"] = "ALL_NEW"
		}
		out, err := call(op, in)
		return plainMap(out["Attributes"]), err
	case "Query", "Scan":
		out, err := call(op, in)
		if err != nil {
			return nil, err
		}
		res := map[string]any{"items": plainItems(out["Items"]), "nextToken": nil, "scannedCount": out["ScannedCount"]}
		if k := out["LastEvaluatedKey"]; k != nil {
			res["nextToken"] = base64.StdEncoding.EncodeToString([]byte(toJSON(k)))
		}
		return res, nil
	case "BatchGetItem":
		tables, req := asMap(m["tables"]), map[string]any{}
		for t, spec := range tables {
			r := map[string]any{"Keys": asMap(spec)["keys"]}
			if v := asMap(spec)["consistentRead"]; v != nil {
				r["ConsistentRead"] = v
			}
			req[t] = r
		}
		out, err := call(op, map[string]any{"RequestItems": req})
		if err != nil {
			return nil, err
		}
		data, left := map[string]any{}, map[string]any{}
		for t, spec := range tables {
			got, items := asList(asMap(out["Responses"])[t]), []any{}
			for _, k := range asList(asMap(spec)["keys"]) { // in the order of the keys, null where missing
				i := slices.IndexFunc(got, func(it any) bool { return hasKey(asMap(it), asMap(k)) })
				if i < 0 {
					items = append(items, nil)
				} else {
					items = append(items, plainMap(got[i]))
				}
			}
			data[t], left[t] = items, plainItems(asMap(asMap(out["UnprocessedKeys"])[t])["Keys"])
		}
		return map[string]any{"data": data, "unprocessedKeys": left}, nil
	case "BatchPutItem", "BatchDeleteItem":
		tables, writes, data, left := asMap(m["tables"]), map[string]any{}, map[string]any{}, map[string]any{}
		for t, items := range tables {
			var reqs []any
			for _, e := range asList(items) {
				if op == "BatchPutItem" {
					reqs = append(reqs, map[string]any{"PutRequest": map[string]any{"Item": e}})
				} else {
					reqs = append(reqs, map[string]any{"DeleteRequest": map[string]any{"Key": e}})
				}
			}
			writes[t], data[t], left[t] = reqs, plainItems(items), []any{}
		}
		if _, err := call("BatchWriteItem", map[string]any{"RequestItems": writes}); err != nil {
			return nil, err
		}
		if op == "BatchPutItem" {
			return map[string]any{"data": data, "unprocessedItems": left}, nil
		}
		return map[string]any{"data": data, "unprocessedKeys": left}, nil
	case "TransactWriteItems":
		var items, keys []any
		kinds := map[string]string{"PutItem": "Put", "UpdateItem": "Update", "DeleteItem": "Delete", "ConditionCheck": "ConditionCheck"}
		for _, e := range asList(m["transactItems"]) {
			ti := asMap(e)
			r := ddbRequest(str(ti["table"]), ti)
			if ti["operation"] == "PutItem" {
				item := maps.Clone(asMap(ti["key"]))
				maps.Copy(item, asMap(ti["attributeValues"]))
				r["Item"] = item
				delete(r, "Key")
			}
			items = append(items, map[string]any{kinds[str(ti["operation"])]: r})
			keys = append(keys, plainMap(ti["key"]))
		}
		if _, err := call(op, map[string]any{"TransactItems": items}); err != nil {
			return nil, err
		}
		return map[string]any{"keys": keys, "cancellationReasons": nil}, nil
	}
	return nil, dsErr("DynamoDB:UnsupportedOperation", "pogi does not run the DynamoDB operation "+op+".")
}

// ddbRequest maps the fields of an AppSync DynamoDB request to the DynamoDB API's.
func ddbRequest(table string, m map[string]any) map[string]any {
	in := map[string]any{"TableName": table}
	for field, key := range map[string]string{"Key": "key", "ConsistentRead": "consistentRead", "IndexName": "index", "Limit": "limit",
		"ScanIndexForward": "scanIndexForward", "Select": "select", "Segment": "segment", "TotalSegments": "totalSegments"} {
		if v := m[key]; v != nil {
			in[field] = v
		}
	}
	if t := str(m["nextToken"]); t != "" {
		b, _ := base64.StdEncoding.DecodeString(t)
		var k any
		json.Unmarshal(b, &k)
		in["ExclusiveStartKey"] = k
	}
	names, values := map[string]any{}, map[string]any{}
	for key, field := range map[string]string{"condition": "ConditionExpression", "update": "UpdateExpression", "query": "KeyConditionExpression",
		"filter": "FilterExpression", "projection": "ProjectionExpression"} {
		e := asMap(m[key])
		if str(e["expression"]) == "" {
			continue
		}
		in[field] = e["expression"]
		maps.Copy(names, asMap(e["expressionNames"]))
		maps.Copy(values, asMap(e["expressionValues"]))
	}
	if len(names) > 0 {
		in["ExpressionAttributeNames"] = names
	}
	if len(values) > 0 {
		in["ExpressionAttributeValues"] = values
	}
	return in
}

// hasKey reports whether item has the attributes of key.
func hasKey(item, key map[string]any) bool {
	for k, v := range key {
		if toJSON(item[k]) != toJSON(v) {
			return false
		}
	}
	return true
}

// plain converts a DynamoDB attribute value ({"S": "x"}) into plain JSON.
func plain(av any) any {
	for k, v := range asMap(av) {
		switch k {
		case "S", "B", "BOOL", "SS", "BS":
			return v
		case "N":
			return number(str(v))
		case "M":
			return plainMap(v)
		case "L":
			out := []any{}
			for _, e := range asList(v) {
				out = append(out, plain(e))
			}
			return out
		case "NS":
			var out []any
			for _, n := range asList(v) {
				out = append(out, number(str(n)))
			}
			return out
		}
	}
	return nil
}

func plainMap(v any) any {
	m := asMap(v)
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, e := range m {
		out[k] = plain(e)
	}
	return out
}

// plainItems converts a list of items or keys.
func plainItems(v any) []any {
	out := []any{}
	for _, e := range asList(v) {
		out = append(out, plainMap(e))
	}
	return out
}

func number(s string) any {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// toDDB converts a plain value into a DynamoDB attribute value.
func toDDB(v any) map[string]any {
	switch t := v.(type) {
	case nil:
		return map[string]any{"NULL": true}
	case string:
		return map[string]any{"S": t}
	case bool:
		return map[string]any{"BOOL": t}
	case int64:
		return map[string]any{"N": strconv.FormatInt(t, 10)}
	case int:
		return map[string]any{"N": strconv.Itoa(t)}
	case float64:
		return map[string]any{"N": strconv.FormatFloat(t, 'f', -1, 64)}
	case map[string]any:
		return map[string]any{"M": toDDBMap(t)}
	case []any, *[]any:
		l := []any{}
		for _, e := range asList(t) {
			l = append(l, toDDB(e))
		}
		return map[string]any{"L": l}
	}
	return map[string]any{"S": fmt.Sprint(v)}
}

func toDDBMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = toDDB(v)
	}
	return out
}

var (
	compareOps  = map[string]string{"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
	placeholder = regexp.MustCompile(`[^A-Za-z0-9_]`)
)

// condition turns an AppSync filter ({"title": {"beginsWith": "a"}, "or":
// [...]}) into a DynamoDB expression with its names and values; it serves
// $util.transform and the JS DynamoDB helpers.
func condition(f map[string]any) map[string]any {
	names, values := map[string]any{}, map[string]any{}
	var walk func(m map[string]any) string
	walk = func(m map[string]any) string {
		var parts []string
		for _, k := range core.SortedKeys(m) {
			switch v := m[k]; k {
			case "and", "or":
				var sub []string
				for _, e := range asList(v) {
					sub = append(sub, walk(asMap(e)))
				}
				parts = append(parts, "("+strings.Join(sub, " "+strings.ToUpper(k)+" ")+")")
			case "not":
				parts = append(parts, "(NOT "+walk(asMap(v))+")")
			default:
				p := placeholder.ReplaceAllString(k, "_")
				name := "#" + p
				names[name] = k
				ops := asMap(v)
				for _, op := range core.SortedKeys(ops) {
					arg := ops[op]
					val := func(x any) string {
						ph := fmt.Sprintf(":%s_%s%d", p, op, len(values))
						values[ph] = toDDB(x)
						return ph
					}
					switch l := asList(arg); op {
					case "eq", "ne", "lt", "le", "gt", "ge":
						parts = append(parts, name+" "+compareOps[op]+" "+val(arg))
					case "between":
						if len(l) == 2 {
							parts = append(parts, name+" BETWEEN "+val(l[0])+" AND "+val(l[1]))
						}
					case "beginsWith":
						parts = append(parts, "begins_with("+name+", "+val(arg)+")")
					case "contains":
						parts = append(parts, "contains("+name+", "+val(arg)+")")
					case "notContains":
						parts = append(parts, "NOT contains("+name+", "+val(arg)+")")
					case "attributeExists":
						if arg == true {
							parts = append(parts, "attribute_exists("+name+")")
						} else {
							parts = append(parts, "attribute_not_exists("+name+")")
						}
					case "attributeType":
						parts = append(parts, "attribute_type("+name+", "+val(arg)+")")
					case "in":
						var vs []string
						for _, e := range l {
							vs = append(vs, val(e))
						}
						parts = append(parts, name+" IN ("+strings.Join(vs, ", ")+")")
					}
				}
			}
		}
		if len(parts) == 0 {
			return ""
		}
		return "(" + strings.Join(parts, " AND ") + ")"
	}
	return map[string]any{"expression": walk(f), "expressionNames": names, "expressionValues": values}
}

// httpCall sends the request to the data source's endpoint.
func httpCall(ds *DataSource, m map[string]any) (any, any) {
	var cfg struct{ Endpoint string }
	json.Unmarshal(ds.HttpConfig, &cfg)
	params := asMap(m["params"])
	q := url.Values{}
	for k, v := range asMap(params["query"]) {
		if l := asList(v); l != nil {
			for _, e := range l {
				q.Add(k, fmt.Sprint(e))
			}
		} else {
			q.Set(k, fmt.Sprint(v))
		}
	}
	u := strings.TrimRight(cfg.Endpoint, "/") + str(m["resourcePath"])
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	var body io.Reader
	switch b := params["body"].(type) {
	case nil:
	case string:
		body = strings.NewReader(b)
	default:
		body = strings.NewReader(toJSON(b))
	}
	req, err := http.NewRequest(cmp.Or(str(m["method"]), "GET"), u, body)
	if err != nil {
		return nil, dsErr("HTTPError", err.Error())
	}
	for k, v := range asMap(params["headers"]) {
		req.Header.Set(k, fmt.Sprint(v))
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, dsErr("HTTPError", err.Error())
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	h := map[string]any{}
	for k, v := range resp.Header {
		h[k] = strings.Join(v, ",")
	}
	return map[string]any{"statusCode": resp.StatusCode, "headers": h, "body": string(b)}, nil
}

// rds runs the request's statements on the RDS Data API; the result is the
// JSON string $util.rds.toJsonObject reads.
func (x *exec) rds(ds *DataSource, m map[string]any) (any, any) {
	var cfg struct {
		RdsHttpEndpointConfig struct{ AwsRegion, DbClusterIdentifier, DatabaseName, Schema, AwsSecretStoreArn string }
	}
	json.Unmarshal(ds.RelationalDatabaseConfig, &cfg)
	c := cfg.RdsHttpEndpointConfig
	results := []any{}
	for _, st := range asList(m["statements"]) {
		body := toJSON(map[string]any{"resourceArn": c.DbClusterIdentifier, "secretArn": c.AwsSecretStoreArn, "database": c.DatabaseName,
			"schema": c.Schema, "sql": bindSQL(str(st), asMap(m["variableMap"])), "includeResultMetadata": true})
		status, _, out := x.s.hub.Do(x.api.Account, cmp.Or(c.AwsRegion, x.api.Region), "rds-data", http.MethodPost, "/Execute",
			http.Header{"Content-Type": {"application/json"}}, []byte(body))
		var r map[string]any
		json.Unmarshal(out, &r)
		if status >= 300 {
			return nil, dsErr("RDSHttp:"+str(r["__type"]), str(r["message"]))
		}
		results = append(results, r)
	}
	return toJSON(map[string]any{"sqlStatementResults": results}), nil
}

// bindSQL puts variableMap values (":id") into a statement as SQL literals, longest names first.
func bindSQL(sql string, vars map[string]any) string {
	names := core.SortedKeys(vars)
	slices.SortStableFunc(names, func(a, b string) int { return len(b) - len(a) })
	for _, n := range names {
		lit := "NULL"
		switch v := vars[n].(type) {
		case string:
			lit = "'" + strings.ReplaceAll(v, "'", "''") + "'"
		case bool:
			lit = strings.ToUpper(strconv.FormatBool(v))
		case nil:
		default:
			lit = fmt.Sprint(v)
		}
		sql = strings.ReplaceAll(sql, n, lit)
	}
	return sql
}

// rdsObjects is $util.rds.toJsonObject: each statement's rows as objects keyed by column.
func rdsObjects(s string) []any {
	var res struct {
		SqlStatementResults []struct {
			Records        [][]map[string]any
			ColumnMetadata []struct{ Label, Name string }
		}
	}
	json.Unmarshal([]byte(s), &res)
	out := []any{}
	for _, r := range res.SqlStatementResults {
		rows := []any{}
		for _, rec := range r.Records {
			row := map[string]any{}
			for i, f := range rec {
				if i < len(r.ColumnMetadata) {
					var v any
					for k, fv := range f {
						if k != "isNull" {
							v = fv
						}
					}
					row[cmp.Or(r.ColumnMetadata[i].Label, r.ColumnMetadata[i].Name)] = v
				}
			}
			rows = append(rows, row)
		}
		out = append(out, rows)
	}
	return out
}
