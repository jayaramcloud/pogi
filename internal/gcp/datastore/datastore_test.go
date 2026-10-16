package datastore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type Task struct {
	Desc     string
	Priority int
	Done     bool
	Tags     []string
	Score    float64
	Created  time.Time
}

// setup runs pogi and points the official client at it with DATASTORE_EMULATOR_HOST.
func setup(t *testing.T) (*pogi.Running, *datastore.Client) {
	t.Helper()
	srv := pogi.Start(t, pogi.Config{Services: []string{"datastore"}})
	t.Setenv("DATASTORE_EMULATOR_HOST", core.Hostport(srv.URL))
	c := must(datastore.NewClient(ctx, "demo"))
	t.Cleanup(func() { c.Close() })
	return srv, c
}

func TestEnv(t *testing.T) {
	srv := pogi.Start(t)
	if !slices.Contains(srv.Env(), "DATASTORE_EMULATOR_HOST="+core.Hostport(srv.URL)) {
		t.Errorf("pogi env lacks DATASTORE_EMULATOR_HOST: %v", srv.Env())
	}
}

func TestCRUD(t *testing.T) {
	_, c := setup(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	k := datastore.NameKey("Task", "write", nil)
	in := Task{Desc: "write tests", Priority: 3, Tags: []string{"a", "b"}, Score: 1.5, Created: now}
	if got, err := c.Put(ctx, k, &in); err != nil || !got.Equal(k) {
		t.Fatalf("put: %v %v", got, err)
	}
	var out Task
	if err := c.Get(ctx, k, &out); err != nil || out.Desc != in.Desc || out.Priority != 3 || !slices.Equal(out.Tags, in.Tags) || out.Score != 1.5 || !out.Created.Equal(now) {
		t.Fatalf("get: %+v %v", out, err)
	}
	if err := c.Get(ctx, datastore.NameKey("Task", "missing", nil), &out); !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("a missing entity: %v", err)
	}

	// GetMulti reports each key: found ones load, missing ones are ErrNoSuchEntity
	k2 := datastore.IDKey("Task", 42, nil)
	must(c.Put(ctx, k2, &Task{Desc: "by id"}))
	keys := []*datastore.Key{k, datastore.NameKey("Task", "nope", nil), k2}
	tasks := make([]Task, 3)
	err := c.GetMulti(ctx, keys, tasks)
	me, ok := err.(datastore.MultiError)
	if !ok || me[0] != nil || !errors.Is(me[1], datastore.ErrNoSuchEntity) || me[2] != nil || tasks[0].Desc != "write tests" || tasks[2].Desc != "by id" {
		t.Errorf("GetMulti: %v %+v", err, tasks)
	}
	if _, err := c.PutMulti(ctx, []*datastore.Key{k, k2}, []Task{{Desc: "one"}, {Desc: "two"}}); err != nil {
		t.Fatal(err)
	}
	c.Get(ctx, k2, &out)
	if out.Desc != "two" {
		t.Errorf("PutMulti: %+v", out)
	}

	if err := c.Delete(ctx, k); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, k, &out); !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("deleted: %v", err)
	}
	if err := c.DeleteMulti(ctx, []*datastore.Key{k, k2}); err != nil { // deleting a missing key is fine
		t.Errorf("DeleteMulti: %v", err)
	}

	// Insert and Update through mutations
	mk := datastore.NameKey("Task", "mut", nil)
	if _, err := c.Mutate(ctx, datastore.NewInsert(mk, &Task{Desc: "first"})); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Mutate(ctx, datastore.NewInsert(mk, &Task{Desc: "again"})); status.Code(err) != codes.AlreadyExists {
		t.Errorf("insert of an existing entity: %v", err)
	}
	if _, err := c.Mutate(ctx, datastore.NewUpdate(datastore.NameKey("Task", "ghost", nil), &Task{})); status.Code(err) != codes.NotFound {
		t.Errorf("update of a missing entity: %v", err)
	}
	if _, err := c.Mutate(ctx, datastore.NewUpdate(mk, &Task{Desc: "second"})); err != nil {
		t.Fatal(err)
	}
	c.Get(ctx, mk, &out)
	if out.Desc != "second" {
		t.Errorf("update: %+v", out)
	}

	// server-side transforms, applied atomically
	type Counter struct{ N int }
	ck := datastore.NameKey("Counter", "c", nil)
	must(c.Put(ctx, ck, &Counter{N: 1}))
	for i := 0; i < 3; i++ { // a transform-only upsert: the empty mask keeps the stored entity
		must(c.Mutate(ctx, datastore.NewUpsert(ck, &Counter{}).WithPropertyMask().WithTransforms(datastore.Increment("N", 5))))
	}
	// a mask writes only the properties it names
	pk := datastore.NameKey("Task", "masked", nil)
	must(c.Put(ctx, pk, &Task{Desc: "old", Priority: 9}))
	must(c.Mutate(ctx, datastore.NewUpsert(pk, &Task{Desc: "new", Priority: 1}).WithPropertyMask("Desc")))
	var pm Task
	if err := c.Get(ctx, pk, &pm); err != nil || pm.Desc != "new" || pm.Priority != 9 {
		t.Errorf("property mask: %+v %v", pm, err)
	}
	var cn Counter
	if err := c.Get(ctx, ck, &cn); err != nil {
		t.Fatal(err)
	}
	if cn.N != 16 {
		t.Errorf("three increments of 5 on 1: %d", cn.N)
	}
}

func TestAllocateIDs(t *testing.T) {
	_, c := setup(t)
	parent := datastore.NameKey("List", "home", nil)
	keys := must(c.AllocateIDs(ctx, []*datastore.Key{datastore.IncompleteKey("Task", parent), datastore.IncompleteKey("Task", parent), datastore.IncompleteKey("Other", nil)}))
	seen := map[string]bool{}
	for _, k := range keys {
		if k.Incomplete() || k.ID == 0 {
			t.Fatalf("not allocated: %v", k)
		}
		seen[k.String()] = true
	}
	if len(seen) != 3 || !keys[0].Parent.Equal(parent) {
		t.Errorf("keys: %v", keys)
	}
	// an incomplete key on Put gets an id too, and does not collide with the allocated ones
	k := must(c.Put(ctx, datastore.IncompleteKey("Task", parent), &Task{Desc: "auto"}))
	if k.Incomplete() || seen[k.String()] {
		t.Errorf("put: %v", k)
	}
	var out Task
	if err := c.Get(ctx, k, &out); err != nil || out.Desc != "auto" {
		t.Errorf("get: %+v %v", out, err)
	}
}

func seed(t *testing.T, c *datastore.Client) []*datastore.Key {
	t.Helper()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	var keys []*datastore.Key
	for i := 1; i <= 10; i++ {
		k := datastore.IDKey("Task", int64(i), nil)
		tags := []string{"odd"}
		if i%2 == 0 {
			tags = []string{"even", "all"}
		}
		must(c.Put(ctx, k, &Task{Desc: fmt.Sprintf("task %d", i), Priority: i, Done: i > 6, Tags: tags, Score: float64(i) / 2, Created: base.Add(time.Duration(i) * time.Hour)}))
		keys = append(keys, k)
	}
	return keys
}

func descs(ts []Task) []string {
	var out []string
	for _, t := range ts {
		out = append(out, t.Desc)
	}
	return out
}

func TestQueries(t *testing.T) {
	_, c := setup(t)
	seed(t, c)
	all := func(q *datastore.Query) []Task {
		t.Helper()
		var out []Task
		if _, err := c.GetAll(ctx, q, &out); err != nil {
			t.Fatalf("%v: %v", q, err)
		}
		return out
	}
	// filters, order
	if got := descs(all(datastore.NewQuery("Task").FilterField("Done", "=", false).FilterField("Priority", ">", 3).Order("-Priority"))); !slices.Equal(got, []string{"task 6", "task 5", "task 4"}) {
		t.Errorf("done=false, priority>3, descending: %v", got)
	}
	if got := descs(all(datastore.NewQuery("Task").FilterField("Priority", ">=", 9).Order("Priority"))); !slices.Equal(got, []string{"task 9", "task 10"}) {
		t.Errorf(">=: %v", got)
	}
	if got := descs(all(datastore.NewQuery("Task").FilterField("Created", "<", time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC)).Order("Created"))); !slices.Equal(got, []string{"task 1", "task 2"}) {
		t.Errorf("time filter: %v", got)
	}
	if got := all(datastore.NewQuery("Task").FilterField("Tags", "=", "all")); len(got) != 5 {
		t.Errorf("an array property matches when any element does: %d", len(got))
	}
	if got := all(datastore.NewQuery("Task").FilterField("Tags", "=", "odd").FilterField("Tags", "=", "even")); len(got) != 0 {
		t.Errorf("no task is both: %d", len(got))
	}
	if got := all(datastore.NewQuery("Task").FilterField("Priority", "in", []any{1, 2, 99})); len(got) != 2 {
		t.Errorf("in: %d", len(got))
	}
	if got := all(datastore.NewQuery("Task").FilterField("Priority", "not-in", []any{1, 2})); len(got) != 8 {
		t.Errorf("not-in: %d", len(got))
	}
	if got := all(datastore.NewQuery("Task").FilterField("Priority", "!=", 5)); len(got) != 9 {
		t.Errorf("!=: %d", len(got))
	}
	or := datastore.OrFilter{Filters: []datastore.EntityFilter{
		datastore.PropertyFilter{FieldName: "Priority", Operator: "=", Value: 1}, datastore.PropertyFilter{FieldName: "Priority", Operator: ">", Value: 9}}}
	if got := descs(all(datastore.NewQuery("Task").FilterEntity(or).Order("Priority"))); !slices.Equal(got, []string{"task 1", "task 10"}) {
		t.Errorf("or: %v", got)
	}
	and := datastore.AndFilter{Filters: []datastore.EntityFilter{
		datastore.PropertyFilter{FieldName: "Done", Operator: "=", Value: true}, or}}
	if got := descs(all(datastore.NewQuery("Task").FilterEntity(and))); !slices.Equal(got, []string{"task 10"}) {
		t.Errorf("and of or: %v", got)
	}
	// without an order, the key's
	if got := all(datastore.NewQuery("Task")); len(got) != 10 || got[0].Desc != "task 1" || got[9].Desc != "task 10" {
		t.Errorf("default order: %v", descs(got))
	}
	// a property in the order that an entity lacks leaves it out
	must(c.Put(ctx, datastore.NameKey("Task", "bare", nil), &datastore.PropertyList{{Name: "Desc", Value: "bare"}}))
	if got := all(datastore.NewQuery("Task").Order("Priority")); len(got) != 10 {
		t.Errorf("an entity without the ordered property: %d", len(got))
	}
	if got := all(datastore.NewQuery("Task")); len(got) != 11 {
		t.Errorf("all: %d", len(got))
	}

	// limit, offset
	if got := descs(all(datastore.NewQuery("Task").Order("Priority").Offset(2).Limit(3))); !slices.Equal(got, []string{"task 3", "task 4", "task 5"}) {
		t.Errorf("offset 2 limit 3: %v", got)
	}

	// keys only, projection, distinct
	keys := must(c.GetAll(ctx, datastore.NewQuery("Task").KeysOnly().FilterField("Priority", "<=", 3).Order("Priority"), nil))
	if len(keys) != 3 || keys[0].ID != 1 || keys[2].ID != 3 {
		t.Errorf("keys only: %v", keys)
	}
	var proj []Task
	must(c.GetAll(ctx, datastore.NewQuery("Task").Project("Priority", "Done").FilterField("Priority", "<=", 2).Order("Priority"), &proj))
	if len(proj) != 2 || proj[0].Priority != 1 || proj[1].Priority != 2 || proj[0].Desc != "" || proj[0].Done {
		t.Errorf("projection: %+v", proj)
	}
	var dist []Task
	must(c.GetAll(ctx, datastore.NewQuery("Task").Project("Done").Distinct().Order("Done"), &dist))
	if len(dist) != 2 || dist[0].Done || !dist[1].Done {
		t.Errorf("distinct: %+v", dist)
	}
	var tags []Task
	must(c.GetAll(ctx, datastore.NewQuery("Task").Project("Tags").Distinct().Order("Tags"), &tags))
	if len(tags) != 3 {
		t.Errorf("distinct values of an array property: %+v", tags)
	}
	var on []Task
	must(c.GetAll(ctx, datastore.NewQuery("Task").Project("Done", "Priority").DistinctOn("Done").Order("Done").Order("Priority"), &on))
	if len(on) != 2 || on[0].Priority != 1 || on[1].Priority != 7 {
		t.Errorf("distinct on: %+v", on)
	}

	// cursors: pages continue where the last one ended, and an end cursor stops a query
	var seen []string
	var cur datastore.Cursor
	for pages := 0; pages < 10; pages++ {
		q := datastore.NewQuery("Task").Order("-Priority").Limit(4)
		if pages > 0 {
			q = q.Start(cur)
		}
		it, n := c.Run(ctx, q), 0
		for {
			var x Task
			_, err := it.Next(&x)
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			seen = append(seen, x.Desc)
			n++
		}
		cur = must(it.Cursor())
		if n < 4 {
			break
		}
	}
	if !slices.Equal(seen, []string{"task 10", "task 9", "task 8", "task 7", "task 6", "task 5", "task 4", "task 3", "task 2", "task 1"}) {
		t.Errorf("pages: %v", seen)
	}
	it := c.Run(ctx, datastore.NewQuery("Task").Order("Priority").Limit(3))
	for {
		if _, err := it.Next(&Task{}); err != nil {
			break
		}
	}
	end := must(it.Cursor())
	if got := descs(all(datastore.NewQuery("Task").Order("Priority").Start(end).Limit(2))); !slices.Equal(got, []string{"task 4", "task 5"}) {
		t.Errorf("start cursor: %v", got)
	}
	if got := descs(all(datastore.NewQuery("Task").Order("Priority").End(end))); !slices.Equal(got, []string{"task 1", "task 2", "task 3"}) {
		t.Errorf("end cursor: %v", got)
	}
	if got := descs(all(datastore.NewQuery("Task").Order("Priority").Offset(1).Limit(2).Start(end))); !slices.Equal(got, []string{"task 5", "task 6"}) {
		t.Errorf("cursor and offset: %v", got)
	}
	// a cursor from another query is refused
	if _, err := c.GetAll(ctx, datastore.NewQuery("Task").Order("Desc").Start(end), &[]Task{}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("a foreign cursor: %v", err)
	}

	// aggregations
	ar := must(c.RunAggregationQuery(ctx, datastore.NewQuery("Task").FilterField("Done", "=", false).NewAggregationQuery().
		WithCount("n").WithSum("Priority", "sum").WithAvg("Priority", "avg").WithSum("Score", "score")))
	num := func(r datastore.AggregationResult, alias string) (int64, float64) {
		v := r[alias].(*pb.Value)
		return v.GetIntegerValue(), v.GetDoubleValue()
	}
	if n, _ := num(ar, "n"); n != 6 {
		t.Errorf("count: %v", ar)
	}
	if n, _ := num(ar, "sum"); n != 21 {
		t.Errorf("sum of integers is an integer: %v", ar)
	}
	if _, f := num(ar, "avg"); f != 3.5 {
		t.Errorf("avg: %v", ar)
	}
	if _, f := num(ar, "score"); f != 10.5 {
		t.Errorf("sum of doubles is a double: %v", ar)
	}
	empty := must(c.RunAggregationQuery(ctx, datastore.NewQuery("Task").FilterField("Priority", ">", 100).NewAggregationQuery().WithCount("n").WithAvg("Priority", "avg")))
	if n, _ := num(empty, "n"); n != 0 || empty["avg"].(*pb.Value).GetValueType() == nil {
		t.Errorf("an empty aggregation: %v", empty)
	}
	if _, isNull := empty["avg"].(*pb.Value).GetValueType().(*pb.Value_NullValue); !isNull {
		t.Errorf("the average of nothing is null: %v", empty["avg"])
	}
	upTo := must(c.RunAggregationQuery(ctx, datastore.NewQuery("Task").NewAggregationQuery().WithCount("n")))
	if n, _ := num(upTo, "n"); n != 11 {
		t.Errorf("count of all: %v", upTo)
	}
}

func TestAncestorsAndNamespaces(t *testing.T) {
	_, c := setup(t)
	alice, bob := datastore.NameKey("User", "alice", nil), datastore.NameKey("User", "bob", nil)
	for i, p := range []*datastore.Key{alice, alice, bob} {
		must(c.Put(ctx, datastore.IDKey("Note", int64(i+1), p), &Task{Desc: fmt.Sprint("note ", i+1)}))
	}
	must(c.Put(ctx, alice, &Task{Desc: "alice herself"}))
	// the same id under another parent is another entity
	must(c.Put(ctx, datastore.IDKey("Note", 1, bob), &Task{Desc: "bob's note 1"}))

	var out []Task
	keys := must(c.GetAll(ctx, datastore.NewQuery("Note").Ancestor(alice).Order("__key__"), &out))
	if len(keys) != 2 || out[0].Desc != "note 1" || out[1].Desc != "note 2" || !keys[0].Parent.Equal(alice) {
		t.Errorf("alice's notes: %v %v", keys, descs(out))
	}
	out = nil
	must(c.GetAll(ctx, datastore.NewQuery("").Ancestor(alice).Order("__key__"), &out))
	if len(out) != 3 { // a kindless ancestor query includes the ancestor
		t.Errorf("everything under alice: %v", descs(out))
	}
	out = nil
	must(c.GetAll(ctx, datastore.NewQuery("Note").FilterField("__key__", ">", datastore.IDKey("Note", 1, bob)).Ancestor(bob), &out))
	if len(out) != 1 || out[0].Desc != "note 3" {
		t.Errorf("a key filter: %v", descs(out))
	}

	// namespaces keep entities apart, with the same keys
	k := datastore.NameKey("Task", "same", nil)
	k2 := datastore.NameKey("Task", "same", nil)
	k2.Namespace = "tenant-2"
	must(c.Put(ctx, k, &Task{Desc: "default"}))
	must(c.Put(ctx, k2, &Task{Desc: "tenant two"}))
	var a, b Task
	c.Get(ctx, k, &a)
	c.Get(ctx, k2, &b)
	if a.Desc != "default" || b.Desc != "tenant two" {
		t.Errorf("namespaces: %q %q", a.Desc, b.Desc)
	}
	out = nil
	must(c.GetAll(ctx, datastore.NewQuery("Task").Namespace("tenant-2"), &out))
	if len(out) != 1 || out[0].Desc != "tenant two" {
		t.Errorf("a query in a namespace: %v", descs(out))
	}
	out = nil
	must(c.GetAll(ctx, datastore.NewQuery("Task"), &out))
	if len(out) != 1 || out[0].Desc != "default" {
		t.Errorf("the default namespace: %v", descs(out))
	}
}

func TestTransactions(t *testing.T) {
	_, c := setup(t)
	k := datastore.NameKey("Counter", "hits", nil)
	type Counter struct{ N int }
	must(c.Put(ctx, k, &Counter{}))

	// two transactions read the same entity; the one that commits second is told so
	tx1, tx2 := must(c.NewTransaction(ctx)), must(c.NewTransaction(ctx))
	var a, b Counter
	must0(tx1.Get(k, &a))
	must0(tx2.Get(k, &b))
	must(tx1.Put(k, &Counter{N: a.N + 1}))
	must(tx2.Put(k, &Counter{N: b.N + 10}))
	must(tx1.Commit())
	if _, err := tx2.Commit(); !errors.Is(err, datastore.ErrConcurrentTransaction) {
		t.Fatalf("the second commit: %v", err)
	}
	var got Counter
	must0(c.Get(ctx, k, &got))
	if got.N != 1 {
		t.Errorf("the committed value: %d", got.N)
	}

	// RunInTransaction retries a conflict, so concurrent increments all count
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
				var n Counter
				if err := tx.Get(k, &n); err != nil {
					return err
				}
				n.N++
				_, err := tx.Put(k, &n)
				return err
			}, datastore.MaxAttempts(20))
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	must0(c.Get(ctx, k, &got))
	if got.N != 9 {
		t.Errorf("1 + 8 increments: %d", got.N)
	}

	// a transaction that only writes does not conflict; a rollback writes nothing
	rb := must(c.NewTransaction(ctx))
	must(rb.Put(datastore.NameKey("Counter", "ghost", nil), &Counter{N: 1}))
	must0(rb.Rollback())
	if err := c.Get(ctx, datastore.NameKey("Counter", "ghost", nil), &got); !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("after a rollback: %v", err)
	}
	// a transaction sees a missing entity as a conflict when it appears before the commit
	tx3 := must(c.NewTransaction(ctx))
	gk := datastore.NameKey("Counter", "late", nil)
	if err := tx3.Get(gk, &got); !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Fatal(err)
	}
	must(c.Put(ctx, gk, &Counter{N: 5}))
	must(tx3.Put(gk, &Counter{N: 6}))
	if _, err := tx3.Commit(); !errors.Is(err, datastore.ErrConcurrentTransaction) {
		t.Errorf("an entity created behind the transaction's back: %v", err)
	}
	// read-only transactions cannot write
	ro := must(c.NewTransaction(ctx, datastore.ReadOnly))
	must0(ro.Get(k, &got))
	must(ro.Put(k, &Counter{}))
	if _, err := ro.Commit(); err == nil {
		t.Error("a read-only transaction wrote")
	}
	// queries and aggregations inside a transaction
	_, err := c.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var out []Counter
		if _, err := c.GetAll(ctx, datastore.NewQuery("Counter").Transaction(tx), &out); err != nil || len(out) != 2 {
			return fmt.Errorf("query in a transaction: %v %v", out, err)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func must0(err error) {
	if err != nil {
		panic(err)
	}
}

func TestResetAndREST(t *testing.T) {
	srv, c := setup(t)
	k := datastore.NameKey("Task", "x", nil)
	must(c.Put(ctx, k, &Task{Desc: "x"}))

	// the same API over REST
	post := func(path, body string) (int, string) {
		resp := must(http.Post(srv.URL+path, "application/json", strings.NewReader(body)))
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	key := `{"partitionId":{"projectId":"demo"},"path":[{"kind":"Task","name":"x"}]}`
	if code, body := post("/v1/projects/demo:lookup", `{"keys":[`+key+`]}`); code != 200 || !strings.Contains(body, `"stringValue":"x"`) || !strings.Contains(body, `"found"`) {
		t.Errorf("lookup: %d %s", code, body)
	}
	code, body := post("/v1/projects/demo:commit", `{"mode":"NON_TRANSACTIONAL","mutations":[{"upsert":{"key":{"partitionId":{"projectId":"demo"},"path":[{"kind":"Task"}]},"properties":{"Desc":{"stringValue":"rest"}}}}]}`)
	if code != 200 || !strings.Contains(body, `"id"`) {
		t.Errorf("commit: %d %s", code, body)
	}
	if code, body := post("/v1/projects/demo:runQuery", `{"query":{"kind":[{"name":"Task"}],"filter":{"propertyFilter":{"property":{"name":"Desc"},"op":"EQUAL","value":{"stringValue":"rest"}}}}}`); code != 200 || !strings.Contains(body, `"entityResults"`) || !strings.Contains(body, `"FULL"`) {
		t.Errorf("runQuery: %d %s", code, body)
	}
	if code, body := post("/v1/projects/demo:beginTransaction", `{}`); code != 200 || !strings.Contains(body, `"transaction"`) {
		t.Errorf("beginTransaction: %d %s", code, body)
	}
	if code, _ := post("/v1/projects/demo:lookup", `{"keys":[{"path":[{"kind":"Task"}]}]}`); code != 400 {
		t.Errorf("an incomplete key: %d", code)
	}
	if code, _ := post("/v1/projects/demo:nothing", `{}`); code == 200 {
		t.Error("an unknown method succeeded")
	}

	// POST /reset empties everything
	if code, _ := post("/reset", ""); code != 200 {
		t.Fatalf("reset: %d", code)
	}
	if err := c.Get(ctx, k, &Task{}); !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("after the reset: %v", err)
	}
	if code, _ := post("/reset", ""); code != 200 {
		t.Errorf("a second reset: %d", code)
	}
}

func TestPersistenceAndConsole(t *testing.T) {
	srv, c := setup(t)
	seed(t, c)
	k := datastore.NameKey("Task", "named", datastore.IDKey("List", 7, nil))
	must(c.Put(ctx, k, &Task{Desc: "kept", Tags: []string{"x"}}))
	svc := srv.Hub.Service("datastore")
	b := must(svc.(core.Stateful).Save())
	srv.Reset("datastore")
	if err := c.Get(ctx, k, &Task{}); !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Fatalf("after reset: %v", err)
	}
	if err := svc.(core.Stateful).Load(b); err != nil {
		t.Fatal(err)
	}
	var out Task
	if err := c.Get(ctx, k, &out); err != nil || out.Desc != "kept" || len(out.Tags) != 1 {
		t.Errorf("after load: %+v %v", out, err)
	}
	// ids allocated before the save are not handed out again
	if ids := must(c.AllocateIDs(ctx, []*datastore.Key{datastore.IncompleteKey("Task", nil)})); ids[0].ID <= 0 {
		t.Errorf("id: %v", ids)
	}
	rows := svc.(core.Browsable).Resources()[0].Rows
	if len(rows) != 1 || rows[0][0] != "Task" || rows[0][4] != "11" {
		t.Fatalf("console: %v", rows)
	}
	r := rows[0]
	if v, ok := svc.(core.Inspectable).Inspect("Kinds", r[1]+"/"+r[2]+"/"+r[3]+"/"+r[0]).([]json.RawMessage); !ok || len(v) != 11 {
		t.Errorf("inspect: %v", v)
	}
}
