package firestore_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/nrndev/pogi/internal/core"
	emu "github.com/nrndev/pogi/internal/gcp/firestore"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func start(t *testing.T) (*firestore.Client, *emu.Service, string) {
	t.Helper()
	var svc *emu.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = emu.New(h); return svc })
	t.Setenv("FIRESTORE_EMULATOR_HOST", strings.TrimPrefix(url, "http://"))
	client := must(firestore.NewClient(ctx, "proj"))
	t.Cleanup(func() { client.Close() })
	return client, svc, url
}

func wantCode(t *testing.T, what string, err error, c codes.Code) {
	t.Helper()
	if status.Code(err) != c {
		t.Fatalf("%s: got %v, want %s", what, err, c)
	}
}

func ids(t *testing.T, q firestore.Query) string {
	t.Helper()
	docs, err := q.Documents(ctx).GetAll()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, d := range docs {
		out = append(out, d.Ref.ID)
	}
	return strings.Join(out, ",")
}

func TestDocuments(t *testing.T) {
	client, _, _ := start(t)
	ref := client.Doc("users/alice")
	data := map[string]any{"name": "Alice", "age": 30, "address": map[string]any{"city": "Paris", "zip": "75001"}, "tags": []string{"a", "b"}}
	wr := must(ref.Set(ctx, data))
	if again := must(ref.Set(ctx, data)); !again.UpdateTime.Equal(wr.UpdateTime) {
		t.Fatal("a write that changes nothing must keep the update time")
	}
	_, err := ref.Create(ctx, map[string]any{"x": 1})
	wantCode(t, "create existing", err, codes.AlreadyExists)

	must(ref.Update(ctx, []firestore.Update{
		{Path: "address.city", Value: "Lyon"},
		{Path: "address.zip", Value: firestore.Delete},
		{Path: "age", Value: firestore.Increment(2)},
		{Path: "tags", Value: firestore.ArrayUnion("b", "c")},
		{Path: "seen", Value: firestore.ServerTimestamp},
		{FieldPath: firestore.FieldPath{"odd.key"}, Value: true},
	}))
	got := must(ref.Get(ctx)).Data()
	seen, _ := got["seen"].(time.Time)
	delete(got, "seen")
	want := map[string]any{"name": "Alice", "age": int64(32), "address": map[string]any{"city": "Lyon"}, "tags": []any{"a", "b", "c"}, "odd.key": true}
	if !reflect.DeepEqual(got, want) || time.Since(seen) > time.Minute {
		t.Fatalf("after update: %v (seen %v)", got, seen)
	}
	must(ref.Update(ctx, []firestore.Update{{Path: "tags", Value: firestore.ArrayRemove("a", "c")}}))
	must(ref.Set(ctx, map[string]any{"address": map[string]any{"country": "FR"}}, firestore.MergeAll))
	snap := must(ref.Get(ctx))
	if tags, addr := snap.Data()["tags"], snap.Data()["address"]; !reflect.DeepEqual(tags, []any{"b"}) ||
		!reflect.DeepEqual(addr, map[string]any{"city": "Lyon", "country": "FR"}) {
		t.Fatalf("tags %v address %v", tags, addr)
	}

	_, err = client.Doc("users/nobody").Update(ctx, []firestore.Update{{Path: "a", Value: 1}})
	wantCode(t, "update missing", err, codes.NotFound)
	_, err = ref.Delete(ctx, firestore.LastUpdateTime(wr.UpdateTime))
	wantCode(t, "delete with stale update time", err, codes.FailedPrecondition)
	must(ref.Delete(ctx, firestore.LastUpdateTime(snap.UpdateTime)))
	_, err = ref.Get(ctx)
	wantCode(t, "get deleted", err, codes.NotFound)
	_, err = ref.Delete(ctx, firestore.Exists)
	wantCode(t, "delete missing with exists", err, codes.NotFound)

	// Reads at a past read time see the old version.
	old := must(client.Doc("users/carol").Set(ctx, map[string]any{"v": "old"}))
	must(client.Doc("users/carol").Set(ctx, map[string]any{"v": "new"}))
	if v := must(client.Doc("users/carol").WithReadOptions(firestore.ReadTime(old.UpdateTime)).Get(ctx)).Data()["v"]; v != "old" {
		t.Fatalf("read at old time = %v", v)
	}
}

func TestTypes(t *testing.T) {
	client, _, _ := start(t)
	type record struct {
		When  time.Time
		Tags  []string
		Attrs map[string]string
		N     int64
		F     float64
		OK    bool
		Nil   *string
		Geo   *latlng.LatLng
		Ref   *firestore.DocumentRef
		Blob  []byte
	}
	in := record{When: time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC), Tags: []string{"x", "y"}, Attrs: map[string]string{"k": "v"},
		N: -7, F: 2.5, OK: true, Geo: &latlng.LatLng{Latitude: 48.85, Longitude: 2.35}, Ref: client.Doc("users/bob"), Blob: []byte{1, 2}}
	must(client.Doc("types/one").Set(ctx, in))
	var out record
	if err := must(client.Doc("types/one").Get(ctx)).DataTo(&out); err != nil {
		t.Fatal(err)
	}
	if !out.When.Equal(in.When) || !slices.Equal(out.Tags, in.Tags) || out.Attrs["k"] != "v" || out.N != -7 || out.F != 2.5 || !out.OK ||
		out.Nil != nil || out.Geo.GetLatitude() != 48.85 || out.Geo.GetLongitude() != 2.35 || out.Ref.Path != in.Ref.Path || !bytes.Equal(out.Blob, in.Blob) {
		t.Fatalf("round trip = %+v", out)
	}
}

func TestQueries(t *testing.T) {
	client, _, _ := start(t)
	cities := client.Collection("cities")
	for id, m := range map[string]map[string]any{
		"SF":  {"name": "San Francisco", "state": "CA", "pop": 860000, "tags": []any{"west", "coast"}, "capital": false},
		"LA":  {"name": "Los Angeles", "state": "CA", "pop": 3900000, "tags": []any{"west"}, "capital": false},
		"DC":  {"name": "Washington", "state": "DC", "pop": 680000, "tags": []any{"east"}, "capital": true},
		"TOK": {"name": "Tokyo", "state": nil, "pop": 9000000, "tags": []any{"asia"}, "capital": true},
		"BJ":  {"name": "Beijing", "pop": 21500000, "tags": []any{"asia"}, "capital": true},
	} {
		must(cities.Doc(id).Set(ctx, m))
	}
	sf := must(cities.Doc("SF").Get(ctx))
	for i, c := range []struct {
		q    firestore.Query
		want string
	}{
		{cities.Where("state", "==", "CA"), "LA,SF"},
		{cities.Where("pop", "<", 1000000), "DC,SF"}, // implicitly ordered by the inequality field
		{cities.Where("pop", ">=", 3900000), "LA,TOK,BJ"},
		{cities.Where("state", "!=", "CA"), "DC"}, // null and missing never match !=
		{cities.Where("state", "in", []string{"CA", "DC"}), "DC,LA,SF"},
		{cities.Where("state", "not-in", []string{"CA"}), "DC"},
		{cities.Where("state", "==", nil), "TOK"},
		{cities.Where("tags", "array-contains", "west"), "LA,SF"},
		{cities.Where("tags", "array-contains-any", []string{"east", "asia"}), "BJ,DC,TOK"},
		{cities.Where("pop", ">", 1000000).Where("capital", "==", true), "TOK,BJ"},
		{cities.OrderBy("pop", firestore.Desc).Limit(2), "BJ,TOK"},
		{cities.OrderBy("pop", firestore.Asc).Offset(1).Limit(2), "SF,LA"},
		{cities.OrderBy("pop", firestore.Asc).StartAfter(680000).EndBefore(9000000), "SF,LA"},
		{cities.OrderBy("pop", firestore.Asc).StartAt(sf).EndAt(9000000), "SF,LA,TOK"},
		{cities.OrderBy("pop", firestore.Asc).LimitToLast(2), "TOK,BJ"},
		{cities.OrderBy("pop", firestore.Desc).StartAfter(sf), "DC"},
		{cities.WhereEntity(firestore.OrFilter{Filters: []firestore.EntityFilter{
			firestore.PropertyFilter{Path: "state", Operator: "==", Value: "CA"},
			firestore.PropertyFilter{Path: "pop", Operator: ">", Value: 10000000},
		}}), "SF,LA,BJ"},
	} {
		if got := ids(t, c.q); got != c.want {
			t.Errorf("query %d: got %s, want %s", i, got, c.want)
		}
	}
	if d := must(cities.Select("name").Where("state", "==", "DC").Documents(ctx).GetAll())[0].Data(); !reflect.DeepEqual(d, map[string]any{"name": "Washington"}) {
		t.Fatalf("select = %v", d)
	}
	_, err := cities.Where("state", "in", []string{}).Documents(ctx).GetAll()
	if err == nil {
		t.Fatal("an empty in-list must be rejected")
	}

	// Collection groups and subcollections.
	must(client.Doc("cities/SF/landmarks/golden-gate").Set(ctx, map[string]any{"type": "bridge"}))
	must(client.Doc("cities/LA/landmarks/getty").Set(ctx, map[string]any{"type": "museum"}))
	must(client.Doc("parks/yosemite/landmarks/half-dome").Set(ctx, map[string]any{"type": "rock"}))
	if got := ids(t, client.CollectionGroup("landmarks").Where("type", "!=", "museum")); got != "golden-gate,half-dome" {
		t.Fatalf("collection group = %s", got)
	}
	if got := ids(t, cities.Doc("SF").Collection("landmarks").Query); got != "golden-gate" {
		t.Fatalf("subcollection = %s", got)
	}

	// Aggregations.
	capitals := cities.Where("capital", "==", true)
	res := must(capitals.NewAggregationQuery().WithCount("n").WithSum("pop", "total").WithAvg("pop", "avg").Get(ctx))
	n, total, avg := res["n"].(*pb.Value).GetIntegerValue(), res["total"].(*pb.Value).GetIntegerValue(), res["avg"].(*pb.Value).GetDoubleValue()
	if n != 3 || total != 31180000 || math.Abs(avg-31180000.0/3) > 1e-6 {
		t.Fatalf("aggregation = %d %d %f", n, total, avg)
	}
}

// TestOrdering checks Firestore's cross-type value order end to end.
func TestOrdering(t *testing.T) {
	client, _, _ := start(t)
	vals := []any{nil, false, true, math.NaN(), math.Inf(-1), -5, 1.5, 2, time.Unix(100, 0), "a", "b", []byte("z"),
		client.Doc("a/b"), &latlng.LatLng{Latitude: 1}, []any{1}, []any{1, 2}, map[string]any{"a": 1}, map[string]any{"b": 0}}
	var want []string
	for i, v := range vals {
		id := fmt.Sprintf("%02d", len(vals)-i) // ids sort opposite to values
		must(client.Collection("mixed").Doc(id).Set(ctx, map[string]any{"v": v}))
		want = append(want, id)
	}
	if got := ids(t, client.Collection("mixed").OrderBy("v", firestore.Asc)); got != strings.Join(want, ",") {
		t.Fatalf("order = %s\nwant    %s", got, strings.Join(want, ","))
	}
	if got := ids(t, client.Collection("mixed").Where("v", ">", 1)); got != "12,11" { // 1.5 and 2: numbers only
		t.Fatalf("v > 1 = %s", got)
	}
	if got := ids(t, client.Collection("mixed").Where("v", "==", 2.0)); got != "11" { // int 2 == double 2.0
		t.Fatalf("v == 2.0 = %s", got)
	}
}

func TestTransactions(t *testing.T) {
	client, _, _ := start(t)
	ref := client.Doc("counters/c")
	must(ref.Set(ctx, map[string]any{"n": 1}))
	n := func() int64 { return must(ref.Get(ctx)).Data()["n"].(int64) }

	err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		snap, err := tx.Get(ref)
		if err != nil {
			return err
		}
		return tx.Update(ref, []firestore.Update{{Path: "n", Value: snap.Data()["n"].(int64) + 1}})
	})
	if err != nil || n() != 2 {
		t.Fatalf("read-modify-write: %v, n=%d", err, n())
	}

	// A write behind the transaction's back aborts its commit; the retry sees it.
	attempts := 0
	err = client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		attempts++
		snap, err := tx.Get(ref)
		if err != nil {
			return err
		}
		if attempts == 1 {
			if _, err := ref.Set(ctx, map[string]any{"n": 100}); err != nil {
				return err
			}
		}
		return tx.Set(ref, map[string]any{"n": snap.Data()["n"].(int64) + 1})
	})
	if err != nil || attempts != 2 || n() != 101 {
		t.Fatalf("contention: %v, attempts=%d, n=%d", err, attempts, n())
	}

	err = client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		docs, err := tx.Documents(client.Collection("counters")).GetAll()
		if err != nil || len(docs) != 1 {
			return fmt.Errorf("read-only query: %v %d", err, len(docs))
		}
		return nil
	}, firestore.ReadOnly)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBulkWriter(t *testing.T) {
	client, _, _ := start(t)
	must(client.Doc("bulk/exists").Set(ctx, map[string]any{"x": 0}))
	bw := client.BulkWriter(ctx)
	set := must(bw.Set(client.Doc("bulk/a"), map[string]any{"x": 1}))
	create := must(bw.Create(client.Doc("bulk/exists"), map[string]any{"x": 2}))
	del := must(bw.Delete(client.Doc("bulk/gone")))
	bw.End()
	if _, err := set.Results(); err != nil {
		t.Fatal(err)
	}
	_, err := create.Results()
	wantCode(t, "bulk create existing", err, codes.AlreadyExists)
	if _, err := del.Results(); err != nil {
		t.Fatal(err)
	}
	if x := must(client.Doc("bulk/a").Get(ctx)).Data()["x"]; x != int64(1) {
		t.Fatalf("bulk set = %v", x)
	}
}

func TestListen(t *testing.T) {
	client, _, _ := start(t)
	lctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	rooms := client.Collection("rooms")
	must(rooms.Doc("a").Set(ctx, map[string]any{"open": true, "n": 1}))
	must(rooms.Doc("closed").Set(ctx, map[string]any{"open": false}))

	it := rooms.Where("open", "==", true).Snapshots(lctx)
	defer it.Stop()
	next := func(size int, kind firestore.DocumentChangeKind, id string) {
		t.Helper()
		snap := must(it.Next())
		if snap.Size != size || len(snap.Changes) != 1 || snap.Changes[0].Kind != kind || snap.Changes[0].Doc.Ref.ID != id {
			t.Fatalf("snapshot: size %d, changes %+v; want %d, %v %s", snap.Size, snap.Changes, size, kind, id)
		}
	}
	next(1, firestore.DocumentAdded, "a")
	must(rooms.Doc("b").Set(ctx, map[string]any{"open": true}))
	next(2, firestore.DocumentAdded, "b")
	must(rooms.Doc("a").Update(ctx, []firestore.Update{{Path: "n", Value: 2}}))
	next(2, firestore.DocumentModified, "a")
	must(rooms.Doc("b").Update(ctx, []firestore.Update{{Path: "open", Value: false}}))
	next(1, firestore.DocumentRemoved, "b")
	must(rooms.Doc("a").Delete(ctx))
	next(0, firestore.DocumentRemoved, "a")
	it.Stop()
	if _, err := it.Next(); err != iterator.Done {
		t.Fatalf("after stop: %v", err)
	}

	dit := client.Doc("rooms/z").Snapshots(lctx)
	defer dit.Stop()
	if snap := must(dit.Next()); snap.Exists() {
		t.Fatal("rooms/z should not exist yet")
	}
	must(client.Doc("rooms/z").Set(ctx, map[string]any{"x": 1}))
	if snap := must(dit.Next()); !snap.Exists() || snap.Data()["x"] != int64(1) {
		t.Fatalf("document listener: %v", snap.Data())
	}
}

func TestCollectionsAndConsole(t *testing.T) {
	client, svc, _ := start(t)
	must(client.Doc("users/alice/orders/o1").Set(ctx, map[string]any{"total": 5}))
	must(client.Doc("users/bob").Set(ctx, map[string]any{"x": 1}))
	must(client.Doc("teams/t1").Set(ctx, map[string]any{"x": 1}))
	collIDs := func(it *firestore.CollectionIterator) string {
		var out []string
		for _, c := range must(it.GetAll()) {
			out = append(out, c.ID)
		}
		return strings.Join(out, ",")
	}
	if got := collIDs(client.Collections(ctx)); got != "teams,users" {
		t.Fatalf("root collections = %s", got)
	}
	if got := collIDs(client.Doc("users/alice").Collections(ctx)); got != "orders" {
		t.Fatalf("subcollections = %s", got)
	}
	var refs []string
	for _, r := range must(client.Collection("users").DocumentRefs(ctx).GetAll()) {
		refs = append(refs, r.ID)
	}
	if strings.Join(refs, ",") != "alice,bob" { // alice only has a subcollection
		t.Fatalf("document refs = %v", refs)
	}

	if got := svc.Collections("proj", ""); !slices.Equal(got, []string{"teams", "users", "users/alice/orders"}) {
		t.Fatalf("console collections = %v", got)
	}
	if got := svc.Documents("proj", "", "users", 10); len(got) != 1 || got[0]["__name__"] != "projects/proj/databases/(default)/documents/users/bob" {
		t.Fatalf("console documents = %v", got)
	}
	if err := svc.SetDocument("proj", "", "teams/t2", map[string]any{"name": "Two", "n": 2.0}); err != nil {
		t.Fatal(err)
	}
	if d := must(client.Doc("teams/t2").Get(ctx)).Data(); d["name"] != "Two" || d["n"] != int64(2) {
		t.Fatalf("console set = %v", d)
	}
	if err := svc.DeleteDocument("proj", "", "teams/t2"); err != nil {
		t.Fatal(err)
	}
	tables := svc.Resources()
	if len(tables) != 2 || len(tables[0].Rows) != 3 || len(tables[1].Rows) != 3 {
		t.Fatalf("resources = %+v", tables)
	}
	if d, _ := svc.Inspect("Documents", "projects/proj/databases/(default)/documents/teams/t1").(map[string]any); d["x"] != int64(1) {
		t.Fatalf("inspect = %v", d)
	}
}

func TestEmulatorExtras(t *testing.T) {
	client, svc, url := start(t)
	other := must(firestore.NewClientWithDatabase(ctx, "proj", "other"))
	defer other.Close()
	must(client.Doc("a/1").Set(ctx, map[string]any{"x": 1}))
	must(other.Doc("a/1").Set(ctx, map[string]any{"x": 2}))

	// Save, reset, load.
	saved := must(svc.Save())
	svc.Reset()
	_, err := client.Doc("a/1").Get(ctx)
	wantCode(t, "get after reset", err, codes.NotFound)
	if err := svc.Load(saved); err != nil {
		t.Fatal(err)
	}
	if x := must(client.Doc("a/1").Get(ctx)).Data()["x"]; x != int64(1) {
		t.Fatalf("after load = %v", x)
	}

	// The emulator's clear endpoint empties one database only.
	req := must(http.NewRequest(http.MethodDelete, url+"/emulator/v1/projects/proj/databases/(default)/documents", nil))
	if resp := must(http.DefaultClient.Do(req)); resp.StatusCode != 200 {
		t.Fatalf("clear: %s", resp.Status)
	}
	_, err = client.Doc("a/1").Get(ctx)
	wantCode(t, "get after clear", err, codes.NotFound)
	if x := must(other.Doc("a/1").Get(ctx)).Data()["x"]; x != int64(2) {
		t.Fatalf("other database = %v", x)
	}

	// Raw RPCs the Go client does not use: CreateDocument with an auto id and the legacy Write stream.
	conn := must(grpc.NewClient(strings.TrimPrefix(url, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials())))
	defer conn.Close()
	fc := pb.NewFirestoreClient(conn)
	root := "projects/proj/databases/(default)/documents"
	doc := must(fc.CreateDocument(ctx, &pb.CreateDocumentRequest{Parent: root, CollectionId: "auto",
		Document: &pb.Document{Fields: map[string]*pb.Value{"x": {ValueType: &pb.Value_IntegerValue{IntegerValue: 3}}}}}))
	if id := doc.Name[strings.LastIndex(doc.Name, "/")+1:]; len(id) != 20 {
		t.Fatalf("auto id %q", id)
	}
	ws := must(fc.Write(ctx))
	if err := ws.Send(&pb.WriteRequest{Database: "projects/proj/databases/(default)"}); err != nil {
		t.Fatal(err)
	}
	hs := must(ws.Recv())
	err = ws.Send(&pb.WriteRequest{StreamToken: hs.StreamToken, Writes: []*pb.Write{{Operation: &pb.Write_Update{Update: &pb.Document{
		Name: root + "/w/1", Fields: map[string]*pb.Value{"y": {ValueType: &pb.Value_StringValue{StringValue: "z"}}}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if r := must(ws.Recv()); hs.StreamId == "" || len(r.WriteResults) != 1 || r.CommitTime == nil {
		t.Fatalf("write stream: %v %v", hs, r)
	}
	ws.CloseSend()
	if y := must(client.Doc("w/1").Get(ctx)).Data()["y"]; y != "z" {
		t.Fatalf("write stream doc = %v", y)
	}

	// Listen protocol: a resumed target is reset and resent, then removable.
	ls := must(fc.Listen(ctx))
	send := func(r *pb.ListenRequest) {
		r.Database = "projects/proj/databases/(default)"
		if err := ls.Send(r); err != nil {
			t.Fatal(err)
		}
	}
	recv := func(n int) string {
		var got []string
		for range n {
			r := must(ls.Recv())
			if tc := r.GetTargetChange(); tc != nil {
				got = append(got, tc.TargetChangeType.String()+fmt.Sprint(tc.TargetIds))
			} else {
				got = append(got, "DOC")
			}
		}
		return strings.Join(got, " ")
	}
	send(&pb.ListenRequest{TargetChange: &pb.ListenRequest_AddTarget{AddTarget: &pb.Target{TargetId: 7,
		ResumeType: &pb.Target_ResumeToken{ResumeToken: []byte("old")},
		TargetType: &pb.Target_Documents{Documents: &pb.Target_DocumentsTarget{Documents: []string{root + "/w/1"}}}}}})
	if got := recv(5); got != "ADD[7] RESET[7] DOC CURRENT[7] NO_CHANGE[]" {
		t.Fatalf("listen = %s", got)
	}
	send(&pb.ListenRequest{TargetChange: &pb.ListenRequest_RemoveTarget{RemoveTarget: 7}})
	if got := recv(1); got != "REMOVE[7]" {
		t.Fatalf("remove = %s", got)
	}
	ls.CloseSend()
}
