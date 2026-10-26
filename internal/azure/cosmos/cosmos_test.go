package cosmos_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/nrndev/pogi/internal/azure/cosmos"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

// The Cosmos DB emulator's well-known key; pogi accepts any key.
const key = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// status is the HTTP status of an SDK error (0 for none).
func status(err error) int {
	var re *azcore.ResponseError
	if errors.As(err, &re) {
		return re.StatusCode
	}
	if err != nil {
		return -1
	}
	return 0
}

func doc(v map[string]any) []byte { return must(json.Marshal(v)) }

func asJSON(v []any) string {
	if len(v) == 0 {
		return "[]"
	}
	return string(must(json.Marshal(v)))
}

type rawFunc func(method, path, body string, hdr ...string) (int, map[string]any)

// start serves a hub with only Cosmos DB over HTTPS (so routing goes through
// Info.Match) and returns an SDK client, the service and a raw HTTP helper.
func start(t *testing.T) (*azcosmos.Client, *cosmos.Service, rawFunc) {
	var svc *cosmos.Service
	h, _ := core.ServeTest(t, func(h *core.Hub) core.Service { svc = cosmos.New(h); return svc })
	url, pool := core.ListenTLS(t, h, h)
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	c := must(azcosmos.NewClientWithKey(url, must(azcosmos.NewKeyCredential(key)),
		&azcosmos.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: hc}}))
	t.Cleanup(c.Close)
	raw := func(method, path, body string, hdr ...string) (int, map[string]any) {
		req := must(http.NewRequest(method, url+path, strings.NewReader(body)))
		req.Header.Set("x-ms-version", "2020-11-05")
		for i := 0; i+1 < len(hdr); i += 2 {
			req.Header.Set(hdr[i], hdr[i+1])
		}
		resp := must(hc.Do(req))
		defer resp.Body.Close()
		var m map[string]any
		json.NewDecoder(resp.Body).Decode(&m)
		return resp.StatusCode, m
	}
	return c, svc, raw
}

// query drains a query pager and returns the decoded results and the number of pages.
func query(t *testing.T, c *azcosmos.ContainerClient, sql string, pk azcosmos.PartitionKey, o *azcosmos.QueryOptions) ([]any, int) {
	t.Helper()
	pager := c.NewQueryItemsPager(sql, pk, o)
	var out []any
	pages := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		pages++
		for _, b := range page.Items {
			var v any
			json.Unmarshal(b, &v)
			out = append(out, v)
		}
	}
	return out, pages
}

func TestOfficialSDK(t *testing.T) {
	c, _, _ := start(t)

	// Databases.
	must(c.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: "shop"}, nil))
	if _, err := c.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: "shop"}, nil); status(err) != 409 {
		t.Fatalf("duplicate database: %v", err)
	}
	db := must(c.NewDatabase("shop"))
	if p := must(db.Read(ctx, nil)).DatabaseProperties; p.ID != "shop" || p.ResourceID == "" || p.ETag == nil {
		t.Fatalf("database = %+v", p)
	}
	if _, err := must(c.NewDatabase("nope")).Read(ctx, nil); status(err) != 404 {
		t.Fatalf("missing database: %v", err)
	}

	// Containers, with throughput and a unique key.
	tp := azcosmos.NewManualThroughputProperties(400)
	spec := azcosmos.ContainerProperties{ID: "products",
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{Paths: []string{"/category"}},
		UniqueKeyPolicy:        &azcosmos.UniqueKeyPolicy{UniqueKeys: []azcosmos.UniqueKey{{Paths: []string{"/sku"}}}}}
	must(db.CreateContainer(ctx, spec, &azcosmos.CreateContainerOptions{ThroughputProperties: &tp}))
	if _, err := db.CreateContainer(ctx, spec, nil); status(err) != 409 {
		t.Fatalf("duplicate container: %v", err)
	}
	ct := must(db.NewContainer("products"))
	if p := must(ct.Read(ctx, nil)).ContainerProperties; p.PartitionKeyDefinition.Paths[0] != "/category" ||
		p.PartitionKeyDefinition.Version != 2 || p.IndexingPolicy == nil || len(p.UniqueKeyPolicy.UniqueKeys) != 1 {
		t.Fatalf("container = %+v", p)
	}

	// Throughput.
	if n, _ := must(ct.ReadThroughput(ctx, nil)).ThroughputProperties.ManualThroughput(); n != 400 {
		t.Fatalf("throughput = %d", n)
	}
	must(ct.ReplaceThroughput(ctx, azcosmos.NewManualThroughputProperties(1000), nil))
	if n, _ := must(ct.ReadThroughput(ctx, nil)).ThroughputProperties.ManualThroughput(); n != 1000 {
		t.Fatalf("replaced throughput = %d", n)
	}

	// Items: create, read, conditional replace and delete, upsert.
	tools := azcosmos.NewPartitionKeyString("tools")
	hammer := map[string]any{"id": "hammer", "category": "tools", "sku": "H1", "name": "Hammer", "price": 12.5, "tags": []string{"steel"}}
	created := must(ct.CreateItem(ctx, tools, doc(hammer), nil))
	if created.RawResponse.StatusCode != 201 || created.ETag == "" || len(created.Value) != 0 {
		t.Fatalf("create: %d etag=%q body=%s", created.RawResponse.StatusCode, created.ETag, created.Value)
	}
	for _, bad := range []struct {
		item map[string]any
		want int
	}{
		{hammer, 409}, // same id
		{map[string]any{"id": "mallet", "category": "tools", "sku": "H1"}, 409}, // unique sku
		{map[string]any{"category": "tools"}, 400},                              // no id
		{map[string]any{"id": "x", "category": "garden"}, 400},                  // body and header disagree
	} {
		if _, err := ct.CreateItem(ctx, tools, doc(bad.item), nil); status(err) != bad.want {
			t.Fatalf("create %v: want %d, got %v", bad.item, bad.want, err)
		}
	}
	var got map[string]any
	json.Unmarshal(must(ct.ReadItem(ctx, tools, "hammer", nil)).Value, &got)
	if got["price"] != 12.5 || got["_etag"] != string(created.ETag) || got["_rid"] == nil || got["_ts"] == nil {
		t.Fatalf("read = %v", got)
	}
	stale := created.ETag
	hammer["price"] = 14
	if r := must(ct.ReplaceItem(ctx, tools, "hammer", doc(hammer), &azcosmos.ItemOptions{IfMatchEtag: &stale})); r.ETag == stale {
		t.Fatal("replace kept the etag")
	}
	if _, err := ct.ReplaceItem(ctx, tools, "hammer", doc(hammer), &azcosmos.ItemOptions{IfMatchEtag: &stale}); status(err) != 412 {
		t.Fatalf("stale replace: %v", err)
	}
	if _, err := ct.DeleteItem(ctx, tools, "hammer", &azcosmos.ItemOptions{IfMatchEtag: &stale}); status(err) != 412 {
		t.Fatalf("stale delete: %v", err)
	}
	saw := map[string]any{"id": "saw", "category": "tools", "sku": "S1", "name": "Saw", "price": 20}
	if r := must(ct.UpsertItem(ctx, tools, doc(saw), nil)); r.RawResponse.StatusCode != 201 {
		t.Fatalf("upsert new = %d", r.RawResponse.StatusCode)
	}
	saw["price"] = 22
	if r := must(ct.UpsertItem(ctx, tools, doc(saw), nil)); r.RawResponse.StatusCode != 200 {
		t.Fatalf("upsert existing = %d", r.RawResponse.StatusCode)
	}
	if _, err := ct.ReadItem(ctx, tools, "nope", nil); status(err) != 404 {
		t.Fatalf("missing item: %v", err)
	}

	// Partial updates.
	var ops azcosmos.PatchOperations
	ops.AppendSet("/price", 15)
	ops.AppendIncrement("/stock", 3)
	ops.AppendAdd("/tags/-", "new")
	ops.AppendRemove("/name")
	got = nil
	json.Unmarshal(must(ct.PatchItem(ctx, tools, "hammer", ops, &azcosmos.ItemOptions{EnableContentResponseOnWrite: true})).Value, &got)
	if got["price"] != 15.0 || got["stock"] != 3.0 || asJSON(got["tags"].([]any)) != `["steel","new"]` || got["name"] != nil {
		t.Fatalf("patched = %v", got)
	}
	var cond azcosmos.PatchOperations
	cond.SetCondition("from c where c.price > 100")
	cond.AppendSet("/price", 1)
	if _, err := ct.PatchItem(ctx, tools, "hammer", cond, nil); status(err) != 412 {
		t.Fatalf("patch with a false condition: %v", err)
	}

	// Queries.
	for _, it := range []map[string]any{
		{"id": "drill", "category": "tools", "sku": "D1", "name": "Drill", "price": 99, "tags": []string{"power", "steel"}},
		{"id": "rake", "category": "garden", "sku": "R1", "name": "Garden rake", "price": 18, "tags": []string{"wood"}},
		{"id": "hose", "category": "garden", "sku": "G1", "name": "Hose", "price": 25},
		{"id": "pan", "category": "kitchen", "sku": "P1", "name": "Pan", "price": 30, "tags": []string{"steel"}},
	} {
		must(ct.CreateItem(ctx, azcosmos.NewPartitionKeyString(it["category"].(string)), doc(it), nil))
	}
	all := azcosmos.NewPartitionKey()
	for _, tc := range []struct {
		sql    string
		pk     azcosmos.PartitionKey
		params []azcosmos.QueryParameter
		want   string
	}{
		{"SELECT VALUE c.id FROM c WHERE c.category = @cat AND c.price < @max ORDER BY c.id", all,
			[]azcosmos.QueryParameter{{Name: "@cat", Value: "tools"}, {Name: "@max", Value: 50}}, `["hammer","saw"]`},
		{"SELECT VALUE c.id FROM c WHERE c.category IN ('garden', 'kitchen') OR c.price > 90 ORDER BY c.price DESC", all, nil, `["drill","pan","hose","rake"]`},
		{"SELECT VALUE c.id FROM c WHERE c.name LIKE 'Ga%' OR c.price BETWEEN 20 AND 25", all, nil, `["saw","rake","hose"]`},
		{"SELECT TOP 2 VALUE c.id FROM c ORDER BY c.price DESC", all, nil, `["drill","pan"]`},
		{"SELECT VALUE c.id FROM c ORDER BY c.id OFFSET 1 LIMIT 2", all, nil, `["hammer","hose"]`},
		{"SELECT VALUE COUNT(1) FROM c", all, nil, `[6]`},
		{"SELECT VALUE COUNT(1) FROM c", tools, nil, `[3]`},
		{"SELECT * FROM c WHERE c.price > 1000", all, nil, `[]`},
		{"SELECT c.id AS item, c.price * 2 AS double FROM c WHERE c.id = 'saw'", all, nil, `[{"double":44,"item":"saw"}]`},
		{"SELECT c.id, t AS tag FROM c JOIN t IN c.tags WHERE t = 'steel' ORDER BY c.id", all, nil,
			`[{"id":"drill","tag":"steel"},{"id":"hammer","tag":"steel"},{"id":"pan","tag":"steel"}]`},
		{"SELECT VALUE c.id FROM c WHERE CONTAINS(c.name, 'RAKE', true) OR STARTSWITH(c.id, 'dr')", all, nil, `["drill","rake"]`},
		{"SELECT VALUE c.id FROM c WHERE ARRAY_CONTAINS(c.tags, 'steel') AND IS_DEFINED(c.stock)", all, nil, `["hammer"]`},
		{"SELECT VALUE c.id FROM c WHERE NOT IS_DEFINED(c.tags)", all, nil, `["saw","hose"]`},
	} {
		got, _ := query(t, ct, tc.sql, tc.pk, &azcosmos.QueryOptions{QueryParameters: tc.params})
		if s := asJSON(got); s != tc.want {
			t.Errorf("%s = %s, want %s", tc.sql, s, tc.want)
		}
	}
	if ids, pages := query(t, ct, "SELECT VALUE c.id FROM c", all, &azcosmos.QueryOptions{PageSizeHint: 2}); len(ids) != 6 || pages != 3 {
		t.Fatalf("paging: %d items in %d pages", len(ids), pages)
	}
	if _, err := ct.NewQueryItemsPager("SELECT FROM c", all, nil).NextPage(ctx); status(err) != 400 {
		t.Fatalf("bad query: %v", err)
	}

	// Transactional batch: success, then a conflict rolls everything back.
	bpk := azcosmos.NewPartitionKeyString("batch")
	b := ct.NewTransactionalBatch(bpk)
	b.CreateItem(doc(map[string]any{"id": "b1", "category": "batch", "sku": "B1"}), nil)
	b.UpsertItem(doc(map[string]any{"id": "b2", "category": "batch", "sku": "B2"}), nil)
	b.ReadItem("b1", nil)
	res := must(ct.ExecuteTransactionalBatch(ctx, b, nil))
	if !res.Success || len(res.OperationResults) != 3 || res.OperationResults[0].StatusCode != 201 ||
		res.OperationResults[2].StatusCode != 200 || len(res.OperationResults[2].ResourceBody) == 0 {
		t.Fatalf("batch = %+v", res)
	}
	b = ct.NewTransactionalBatch(bpk)
	b.CreateItem(doc(map[string]any{"id": "b3", "category": "batch", "sku": "B3"}), nil)
	b.CreateItem(doc(map[string]any{"id": "b1", "category": "batch", "sku": "B9"}), nil)
	res = must(ct.ExecuteTransactionalBatch(ctx, b, nil))
	if res.Success || res.OperationResults[0].StatusCode != 424 || res.OperationResults[1].StatusCode != 409 {
		t.Fatalf("failed batch = %+v", res)
	}
	if _, err := ct.ReadItem(ctx, bpk, "b3", nil); status(err) != 404 {
		t.Fatalf("b3 survived the rollback: %v", err)
	}

	// Change feed: everything from the beginning, then only later changes.
	ranges := must(ct.ReadFeedRanges(ctx, nil))
	feed := must(ct.ReadChangeFeed(ctx, &azcosmos.ChangeFeedOptions{FeedRange: &ranges[0]}))
	if len(feed.Items) != 8 {
		t.Fatalf("change feed has %d items", len(feed.Items))
	}
	token := feed.ContinuationToken
	if feed = must(ct.ReadChangeFeed(ctx, &azcosmos.ChangeFeedOptions{Continuation: &token})); len(feed.Items) != 0 {
		t.Fatalf("change feed without changes has %d items", len(feed.Items))
	}
	must(ct.UpsertItem(ctx, tools, doc(saw), nil))
	if feed = must(ct.ReadChangeFeed(ctx, &azcosmos.ChangeFeedOptions{Continuation: &token})); len(feed.Items) != 1 {
		t.Fatalf("change feed after one write has %d items", len(feed.Items))
	}

	// TTL: items expire after the container's defaultTtl unless they say ttl -1.
	ttl := int32(1)
	must(db.CreateContainer(ctx, azcosmos.ContainerProperties{ID: "sessions", DefaultTimeToLive: &ttl,
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{Paths: []string{"/id"}}}, nil))
	sessions := must(db.NewContainer("sessions"))
	must(sessions.CreateItem(ctx, azcosmos.NewPartitionKeyString("s1"), doc(map[string]any{"id": "s1"}), nil))
	must(sessions.CreateItem(ctx, azcosmos.NewPartitionKeyString("keep"), doc(map[string]any{"id": "keep", "ttl": -1}), nil))
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		_, err := sessions.ReadItem(ctx, azcosmos.NewPartitionKeyString("s1"), "s1", nil)
		if status(err) == 404 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("s1 did not expire: %v", err)
		}
	}
	must(sessions.ReadItem(ctx, azcosmos.NewPartitionKeyString("keep"), "keep", nil))

	// Deletes.
	must(ct.DeleteItem(ctx, tools, "saw", nil))
	if _, err := ct.ReadItem(ctx, tools, "saw", nil); status(err) != 404 {
		t.Fatalf("deleted item: %v", err)
	}
	must(ct.Delete(ctx, nil))
	if _, err := ct.Read(ctx, nil); status(err) != 404 {
		t.Fatalf("deleted container: %v", err)
	}
	must(db.Delete(ctx, nil))
	if _, err := db.Read(ctx, nil); status(err) != 404 {
		t.Fatalf("deleted database: %v", err)
	}
}

// TestSQL covers the query language and the console helpers, plus the REST
// calls the Go SDK does not make.
func TestSQL(t *testing.T) {
	c, svc, raw := start(t)
	must(c.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: "lab"}, nil))
	db := must(c.NewDatabase("lab"))
	for name, path := range map[string]string{"t": "/g", "mixed": "/id"} {
		must(db.CreateContainer(ctx, azcosmos.ContainerProperties{ID: name, PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{Paths: []string{path}}}, nil))
	}
	for _, it := range []map[string]any{
		{"id": "1", "g": "x", "n": 1, "s": "abc", "tags": []any{"a", "b"}},
		{"id": "2", "g": "x", "n": 2, "s": "b%c", "tags": []any{"c"}},
		{"id": "3", "g": "y", "n": 3, "s": "xyz", "nested": map[string]any{"k": "v"}},
	} {
		if err := svc.Upsert("lab", "t", it); err != nil {
			t.Fatal(err)
		}
	}
	for i, v := range []any{"s", 5, nil, true, []any{1}, map[string]any{"k": 1}} {
		svc.Upsert("lab", "mixed", map[string]any{"id": strconv.Itoa(i), "v": v})
	}
	svc.Upsert("lab", "mixed", map[string]any{"id": "u"})

	for _, tc := range []struct{ coll, sql, want string }{
		{"t", "SELECT VALUE c.id FROM c WHERE c.n > 1 ORDER BY c.n DESC", `["3","2"]`},
		{"t", "SELECT VALUE c.id FROM c WHERE c.nested.k = 'v' OR c.missing = 1", `["3"]`},
		{"t", "SELECT VALUE c.id FROM c WHERE c.missing != 1 OR c.n = '1' OR NOT (c.missing = 1)", `[]`},
		{"t", "SELECT VALUE c.id FROM c WHERE NOT IS_DEFINED(c.nested) AND c.g IN ('x', 'z')", `["1","2"]`},
		{"t", "SELECT VALUE c.id FROM c WHERE c.s LIKE 'a_c' OR c.s LIKE 'b!%%' ESCAPE '!'", `["1","2"]`},
		{"t", "SELECT VALUE c.id FROM c WHERE c.s BETWEEN 'abc' AND 'b~'", `["1","2"]`},
		{"t", "SELECT c.id, c.n * 10 AS tens, c.n + 1 FROM c WHERE c.id = '2'", `[{"$1":3,"id":"2","tens":20}]`},
		{"t", `SELECT VALUE {"id": c.id, "first": c.tags[0], "none": c.missing} FROM c WHERE c.id = "1"`, `[{"first":"a","id":"1"}]`},
		{"t", "SELECT VALUE [LOWER('AbC'), UPPER('x'), LENGTH('héllo'), CONCAT('a', 'b', 'c'), SUBSTRING('hello', 1, 3), INDEX_OF('hello', 'l'), " +
			"REPLACE('aXa', 'X', '-'), TRIM('  t '), LEFT('hello', 2), RIGHT('hello', 3), TOSTRING(42), REVERSE('abc')]",
			`[["abc","X",5,"abc","ell",2,"a-a","t","he","llo","42","cba"]]`},
		{"t", "SELECT VALUE [ABS(-2), CEILING(1.2), FLOOR(1.8), ROUND(2.5), POWER(2, 10), SQRT(16), 7 % 3, 1 / 0 ?? 'inf']", `[[2,2,1,3,1024,4,1,"inf"]]`},
		{"t", "SELECT VALUE [IS_STRING(c.s), IS_NUMBER(c.n), IS_ARRAY(c.tags), IS_OBJECT(c.nested), IS_NULL(null), IS_BOOL(true), IS_DEFINED(c.nope)] FROM c WHERE c.id = '1'",
			`[[true,true,true,false,true,true,false]]`},
		{"t", `SELECT VALUE [CONTAINS(c.s, 'B', true), STARTSWITH(c.s, 'ab'), ENDSWITH(c.s, 'z'), ARRAY_CONTAINS(c.tags, 'b'), ARRAY_LENGTH(c.tags), ` +
			`ARRAY_SLICE(c.tags, 1), ARRAY_CONTAINS([{"a": 1, "b": 2}], {"a": 1}, true)] FROM c WHERE c.id = '1'`, `[[true,true,false,true,2,["b"],true]]`},
		{"t", "SELECT c.g, COUNT(1) AS n, SUM(c.n) AS total, AVG(c.n) AS mean FROM c GROUP BY c.g",
			`[{"g":"x","mean":1.5,"n":2,"total":3},{"g":"y","mean":3,"n":1,"total":3}]`},
		{"t", "SELECT VALUE COUNT(1) FROM c WHERE c.n > 100", `[0]`},
		{"t", "SELECT MIN(c.n) AS lo, MAX(c.s) AS hi FROM c", `[{"hi":"xyz","lo":1}]`},
		{"t", "SELECT DISTINCT VALUE c.g FROM c", `["x","y"]`},
		{"t", "SELECT VALUE c.id FROM c WHERE EXISTS(SELECT VALUE t FROM t IN c.tags WHERE t = 'c')", `["2"]`},
		{"t", "SELECT VALUE ARRAY(SELECT VALUE UPPER(t) FROM t IN c.tags) FROM c WHERE c.id = '1'", `[["A","B"]]`},
		{"t", "SELECT VALUE c.missing ?? (c.n > 1 ? 'big' : 'small') FROM c", `["small","big","big"]`},
		{"t", "SELECT VALUE t FROM c JOIN t IN c.tags WHERE c.g = 'x'", `["a","b","c"]`},
		{"t", "SELECT VALUE c.id FROM c JOIN (SELECT VALUE t FROM t IN c.tags WHERE t IN ('b', 'c')) AS hit", `["1","2"]`},
		{"t", `SELECT VALUE c.nested["k"] FROM c`, `["v"]`},
		{"t", "SELECT VALUE 1 + 2", `[3]`},
		{"t", "SELECT TOP 1 VALUE c.id FROM c ORDER BY c.id DESC", `["3"]`},
		{"mixed", "SELECT VALUE c.id FROM c ORDER BY c.v", `["u","2","3","1","0","4","5"]`},
	} {
		got, err := svc.Query("lab", tc.coll, tc.sql)
		if s := asJSON(got); err != nil || s != tc.want {
			t.Errorf("%s = %s, %v; want %s", tc.sql, s, err, tc.want)
		}
	}
	for _, bad := range []string{"SELECT FROM c", "SELECT VALUE FOO(1) FROM c", "SELECT * FROM c JOIN t IN c.tags", "SELECT VALUE c.x FROM c WHERE", "SELECT VALUE d.x FROM c",
		"SELECT * FROM c WHERE " + strings.Repeat("(", 100000) + "1=1", // too deep: an error, not a crash
		"SELECT * FROM c WHERE " + strings.Repeat("NOT ", 100000) + "true"} {
		if _, err := svc.Query("lab", "t", bad); err == nil {
			t.Errorf("%s: want an error", bad)
		}
	}

	// Console, persistence.
	if got := svc.Containers(); len(got) != 2 || got[1] != [2]string{"lab", "t"} {
		t.Fatalf("containers = %v", got)
	}
	if tabs := svc.Resources(); len(tabs) != 3 || tabs[2].Rows[1][2] != "3" || svc.Inspect("Containers", "t") == nil {
		t.Fatalf("resources = %+v", tabs)
	}
	state := must(svc.Save())
	svc.Reset()
	if err := svc.Load(state); err != nil {
		t.Fatal(err)
	}
	if got, _ := svc.Query("lab", "t", "SELECT VALUE COUNT(1) FROM c"); asJSON(got) != "[3]" {
		t.Fatalf("after load: %v", got)
	}

	// Stored procedures are stored but not run; query plans; routing.
	if code, _ := raw("POST", "/dbs/lab/colls/t/sprocs", `{"id":"sp","body":"function () {}"}`); code != 201 {
		t.Fatalf("create sproc = %d", code)
	}
	if code, m := raw("GET", "/dbs/lab/colls/t/sprocs", ""); code != 200 || m["_count"] != 1.0 {
		t.Fatalf("list sprocs = %d %v", code, m)
	}
	if code, _ := raw("POST", "/dbs/lab/colls/t/sprocs/sp", "[]"); code != 400 {
		t.Fatalf("execute sproc = %d", code)
	}
	if code, m := raw("POST", "/dbs/lab/colls/t/docs", `{"query":"SELECT c.g, COUNT(1) FROM c GROUP BY c.g"}`,
		"Content-Type", "application/query+json", "x-ms-cosmos-is-query-plan-request", "True"); code != 200 || m["queryRanges"] == nil {
		t.Fatalf("query plan = %d %v", code, m)
	}
	if code, _ := raw("GET", "/?comp=list", ""); code != 404 {
		t.Fatalf("Azure Storage traffic was claimed: %d", code)
	}
	if code, _ := raw("GET", "/dbs", "", "x-ms-version", ""); code != 404 {
		t.Fatalf("traffic without x-ms-version was claimed: %d", code)
	}
}

func TestBadPathAndPartitionKey(t *testing.T) {
	_, _, raw := start(t)
	raw("POST", "/dbs", `{"id":"db"}`)
	raw("POST", "/dbs/db/colls", `{"id":"c","partitionKey":{"paths":["/pk"],"kind":"Hash"}}`)
	for _, tc := range []struct {
		method, path string
		hdr          []string
	}{
		{"PUT", "/dbs//colls", nil},
		{"DELETE", "/dbs//colls", nil},
		{"GET", "/dbs/db/colls/c/docs/x", []string{"x-ms-documentdb-partitionkey", `["a","b"]`}},
		{"GET", "/dbs/db/colls/c/docs/x", []string{"x-ms-documentdb-partitionkey", `["` + strings.Repeat("a", 3000) + `"]`}},
	} {
		if st, m := raw(tc.method, tc.path, "", tc.hdr...); st != 400 || m["code"] != "BadRequest" {
			t.Errorf("%s %s %v = %d %v, want 400 BadRequest", tc.method, tc.path, tc.hdr, st, m)
		}
	}
}
