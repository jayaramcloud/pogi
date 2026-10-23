package web_test

import (
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/firestore"
)

// page fetches a console page and fails unless it answers 200.
func page(t *testing.T, srv *pogi.Running, path string) string {
	t.Helper()
	res, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	return body(t, res, path)
}

func form(t *testing.T, srv *pogi.Running, path string, v url.Values) string {
	t.Helper()
	res, err := http.PostForm(srv.URL+path, v)
	if err != nil {
		t.Fatal(err)
	}
	return body(t, res, path)
}

func body(t *testing.T, res *http.Response, what string) string {
	t.Helper()
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		t.Fatalf("%s: %s: %s", what, res.Status, b)
	}
	return string(b)
}

// results is the part of a page after its last form, where results are.
func results(page string) string {
	if i := strings.Index(page, "</main>"); i >= 0 {
		page = page[:i] // the shell's own forms come after main
	}
	if i := strings.LastIndex(page, "</form>"); i >= 0 {
		return page[i:]
	}
	return page
}

// shows and hides check that a page's results do and do not contain each text.
func shows(t *testing.T, page string, texts ...string) {
	t.Helper()
	r := results(page)
	for _, s := range texts {
		if !strings.Contains(r, s) {
			t.Errorf("page lacks %q:\n%s", s, r[:min(len(r), 1500)])
		}
	}
}

func hides(t *testing.T, page string, texts ...string) {
	t.Helper()
	r := results(page)
	for _, s := range texts {
		if strings.Contains(r, s) {
			t.Errorf("page has %q:\n%s", s, r[:min(len(r), 1500)])
		}
	}
}

// has checks the whole page, form and navigation included.
func has(t *testing.T, page string, texts ...string) {
	t.Helper()
	for _, s := range texts {
		if !strings.Contains(page, s) {
			t.Errorf("page lacks %q", s)
		}
	}
}

// call seeds an AWS JSON service the way an SDK would.
func call(t *testing.T, srv *pogi.Running, target, op string, in any) map[string]any {
	t.Helper()
	var out map[string]any
	if err := srv.Hub.Call(core.DefaultAccount, core.DefaultRegion, target, op, in, &out); err != nil {
		t.Fatalf("%s.%s: %v", target, op, err)
	}
	return out
}

func TestIndexAndNav(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"s3"}})
	has(t, page(t, srv, "/_pogi/explore"), "Services", "services running") // the index redirects to the catalog
	has(t, page(t, srv, "/_pogi/"), "/_pogi/sql", "/_pogi/logs", "/_pogi/secrets")
	for _, path := range []string{"/_pogi/query/dynamodb", "/_pogi/query/cosmos", "/_pogi/query/firestore", "/_pogi/sql", "/_pogi/sfn", "/_pogi/logs?kind=kql",
		"/_pogi/logs?kind=gcp", "/_pogi/logs", "/_pogi/crypto", "/_pogi/secrets", "/_pogi/network"} {
		page(t, srv, path) // every explorer renders with its services off
	}
}

func TestDynamoDBQuery(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"dynamodb"}})
	call(t, srv, "DynamoDB_20120810", "CreateTable", map[string]any{"TableName": "Orders", "BillingMode": "PAY_PER_REQUEST",
		"AttributeDefinitions": []any{map[string]any{"AttributeName": "pk", "AttributeType": "S"}, map[string]any{"AttributeName": "sk", "AttributeType": "S"}},
		"KeySchema":            []any{map[string]any{"AttributeName": "pk", "KeyType": "HASH"}, map[string]any{"AttributeName": "sk", "KeyType": "RANGE"}}})
	for _, it := range []map[string]any{
		{"pk": "u1", "sk": "a", "total": 5, "status": "open"},
		{"pk": "u1", "sk": "b", "total": 50, "status": "open"},
		{"pk": "u1", "sk": "c", "total": 70, "status": "closed"},
		{"pk": "u2", "sk": "a", "total": 99, "status": "open"},
	} {
		item := map[string]any{}
		for k, v := range it {
			if n, ok := v.(int); ok {
				item[k] = map[string]any{"N": strconv.Itoa(n)}
			} else {
				item[k] = map[string]any{"S": v}
			}
		}
		call(t, srv, "DynamoDB_20120810", "PutItem", map[string]any{"TableName": "Orders", "Item": item})
	}
	target := core.DefaultAccount + "/" + core.DefaultRegion + "/Orders"
	run := func(v url.Values) string {
		v.Set("run", "1")
		v.Set("target", target)
		return page(t, srv, "/_pogi/query/dynamodb?"+v.Encode())
	}

	// the table is offered
	has(t, page(t, srv, "/_pogi/query/dynamodb"), `value="`+target+`"`)

	// Query: key condition and filter, values as plain JSON
	p := run(url.Values{"mode": {"query"}, "key": {"pk = :pk"}, "filter": {"#t > :min"}, "names": {`{"#t": "total"}`}, "values": {`{":pk": "u1", ":min": 10}`}})
	shows(t, p, "2 items", ">b<", ">c<")
	hides(t, p, "99", ">a<")

	// Query with typed values and descending order
	p = run(url.Values{"mode": {"query"}, "key": {"pk = :pk"}, "values": {`{":pk": {"S": "u2"}}`}, "desc": {"1"}})
	shows(t, p, "1 items", "99")

	// Scan with a filter
	p = run(url.Values{"mode": {"scan"}, "filter": {"#s = :s"}, "names": {`{"#s": "status"}`}, "values": {`{":s": "closed"}`}})
	shows(t, p, "1 items", ">70<")
	hides(t, p, ">50<")

	// Scan with a limit pages
	p = run(url.Values{"mode": {"scan"}, "limit": {"2"}})
	shows(t, p, "2 items", "Next page", "start=")

	// PartiQL SELECT, with a projection, comparisons and a bound parameter
	p = run(url.Values{"mode": {"partiql"}, "stmt": {`SELECT sk, total FROM "Orders" WHERE total > 10 AND status = 'open'`}})
	shows(t, p, "2 items", ">50<", ">99<")
	hides(t, p, ">70<", ">status<")
	p = run(url.Values{"mode": {"partiql"}, "stmt": {`SELECT * FROM "Orders" WHERE sk = ?`}, "values": {`{":p0": "b"}`}})
	shows(t, p, "1 items", ">50<")

	// errors are shown, not thrown
	shows(t, run(url.Values{"mode": {"partiql"}, "stmt": {`DELETE FROM "Orders"`}}), "only SELECT")
	shows(t, run(url.Values{"mode": {"query"}, "key": {"nope ="}}), `class="flash"`)
}

func TestCosmosQuery(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"cosmos"}})
	cosmos := func(path, pk string, doc string) {
		req, _ := http.NewRequest("POST", srv.URL+path, strings.NewReader(doc))
		req.Header.Set("x-ms-version", "2018-12-31")
		req.Header.Set("Content-Type", "application/json")
		if pk != "" {
			req.Header.Set("x-ms-documentdb-partitionkey", pk)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode >= 300 {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("POST %s: %s %s", path, res.Status, b)
		}
		res.Body.Close()
	}
	cosmos("/dbs", "", `{"id":"shop"}`)
	cosmos("/dbs/shop/colls", "", `{"id":"items","partitionKey":{"paths":["/kind"],"kind":"Hash"}}`)
	for _, d := range []string{`{"id":"1","kind":"fruit","name":"apple","price":3}`, `{"id":"2","kind":"fruit","name":"pear","price":8}`, `{"id":"3","kind":"veg","name":"kale","price":12}`} {
		cosmos("/dbs/shop/colls/items/docs", `["`+strings.Split(strings.Split(d, `"kind":"`)[1], `"`)[0]+`"]`, d)
	}
	has(t, page(t, srv, "/_pogi/query/cosmos"), `value="shop/items"`)
	q := func(sql string) string {
		return page(t, srv, "/_pogi/query/cosmos?run=1&target=shop/items&sql="+url.QueryEscape(sql))
	}
	p := q("SELECT c.name, c.price FROM c WHERE c.price > 5 ORDER BY c.price")
	shows(t, p, "2 documents", ">pear<", ">kale<")
	hides(t, p, "apple")
	p = q("SELECT VALUE COUNT(1) FROM c")
	shows(t, p, "1 documents", ">3<")
	shows(t, q("SELECT * FROM nope WHERE"), `class="flash"`)
}

func TestFirestoreQuery(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"firestore"}})
	fs := srv.Hub.Service("firestore").(*firestore.Service)
	for id, d := range map[string]map[string]any{
		"alice": {"name": "Alice", "age": 31, "team": "red"},
		"bob":   {"name": "Bob", "age": 25, "team": "red"},
		"cy":    {"name": "Cy", "age": 40, "team": "blue"},
	} {
		if err := fs.SetDocument(core.DefaultProject, "(default)", "users/"+id, d); err != nil {
			t.Fatal(err)
		}
	}
	has(t, page(t, srv, "/_pogi/query/firestore"), `value="`+core.DefaultProject+`/(default)/users"`)
	q := func(v url.Values) string {
		v.Set("run", "1")
		v.Set("target", core.DefaultProject+"/(default)/users")
		return page(t, srv, "/_pogi/query/firestore?"+v.Encode())
	}
	p := q(url.Values{"f1": {"team"}, "op1": {"=="}, "v1": {"red"}, "f2": {"age"}, "op2": {">"}, "v2": {"28"}})
	shows(t, p, "1 documents", ">Alice<", ">alice<")
	hides(t, p, "Bob", "Cy")
	p = q(url.Values{"order": {"age"}, "dir": {"desc"}, "limit": {"2"}})
	shows(t, p, "2 documents", "Cy", "Alice")
	hides(t, p, "Bob")
	p = q(url.Values{"f1": {"name"}, "op1": {"in"}, "v1": {`["Bob","Cy"]`}})
	shows(t, p, "2 documents")
	hides(t, p, "Alice")
}
