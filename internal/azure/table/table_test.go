package table_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/nrndev/pogi/internal/azure/table"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

// Azurite's well-known development account key.
const key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func wantCode(t *testing.T, err error, code aztables.TableErrorCode) {
	t.Helper()
	var re *azcore.ResponseError
	if !errors.As(err, &re) || re.ErrorCode != string(code) {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func js(v any) []byte { return must(json.Marshal(v)) }

func row(pk, rk string, kv ...any) []byte {
	m := map[string]any{"PartitionKey": pk, "RowKey": rk}
	for i := 0; i < len(kv); i += 2 {
		m[kv[i].(string)] = kv[i+1]
	}
	return js(m)
}

func props(t *testing.T, c *aztables.Client, pk, rk string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(must(c.GetEntity(ctx, pk, rk, nil)).Value, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestOfficialSDK(t *testing.T) {
	var impl *table.Service
	_, url := core.ServePort(t, func(h *core.Hub) core.Service { impl = table.New(h); return impl })
	conn := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=" + key + ";TableEndpoint=" + url + "/devstoreaccount1;"
	svc := must(aztables.NewServiceClientFromConnectionString(conn, nil))

	// Tables: names are case-insensitive but keep their casing.
	for _, name := range []string{"Inventory", "orders", "otherTable"} {
		must(svc.CreateTable(ctx, name, nil))
	}
	_, err := svc.CreateTable(ctx, "inventory", nil)
	wantCode(t, err, aztables.TableAlreadyExists)
	listTables := func(o *aztables.ListTablesOptions) string {
		var names []string
		for p := svc.NewListTablesPager(o); p.More(); {
			for _, tp := range must(p.NextPage(ctx)).Tables {
				names = append(names, *tp.Name)
			}
		}
		return strings.Join(names, ",")
	}
	if got := listTables(&aztables.ListTablesOptions{Top: to.Ptr(int32(2))}); got != "Inventory,orders,otherTable" {
		t.Fatalf("tables = %s", got)
	}
	if got := listTables(&aztables.ListTablesOptions{Filter: to.Ptr("TableName ge 'o' and TableName lt 'p'")}); got != "orders,otherTable" {
		t.Fatalf("prefix query = %s", got)
	}
	must(svc.DeleteTable(ctx, "otherTable", nil))
	_, err = svc.DeleteTable(ctx, "otherTable", nil)
	wantCode(t, err, aztables.ResourceNotFound)

	// Typed (EDM) properties round-trip exactly.
	c := svc.NewClient("inventory")
	when := time.Date(2026, 10, 11, 6, 31, 0, 123456700, time.UTC)
	ent := aztables.EDMEntity{Entity: aztables.Entity{PartitionKey: "tools", RowKey: "hammer"}, Properties: map[string]any{
		"Count": aztables.EDMInt64(9007199254740993), "Small": int32(42), "Price": 12.75, "InStock": true, "Name": "Claw hammer",
		"Added": aztables.EDMDateTime(when), "ID": aztables.EDMGUID("4185404a-5818-48c3-b9be-f217df0dba6f"), "Blob": aztables.EDMBinary("\x00\x01bin"),
	}}
	added := must(c.AddEntity(ctx, js(ent), nil))
	_, err = c.AddEntity(ctx, js(ent), nil)
	wantCode(t, err, aztables.EntityAlreadyExists)
	got := must(c.GetEntity(ctx, "tools", "hammer", nil))
	var back aztables.EDMEntity
	if err := json.Unmarshal(got.Value, &back); err != nil {
		t.Fatal(err)
	}
	p := back.Properties
	if p["Count"] != aztables.EDMInt64(9007199254740993) || p["Small"] != int32(42) || p["Price"] != 12.75 || p["InStock"] != true ||
		p["Name"] != "Claw hammer" || p["ID"] != aztables.EDMGUID("4185404a-5818-48c3-b9be-f217df0dba6f") ||
		!time.Time(p["Added"].(aztables.EDMDateTime)).Equal(when) || string(p["Blob"].(aztables.EDMBinary)) != "\x00\x01bin" {
		t.Fatalf("round trip = %#v", p)
	}
	if got.ETag != added.ETag || back.ETag != string(got.ETag) || time.Time(back.Timestamp).IsZero() {
		t.Fatalf("etags %s %s %s, timestamp %v", added.ETag, got.ETag, back.ETag, back.Timestamp)
	}
	full := must(c.GetEntity(ctx, "tools", "hammer", &aztables.GetEntityOptions{Format: to.Ptr(aztables.MetadataFormatFull)}))
	none := must(c.GetEntity(ctx, "tools", "hammer", &aztables.GetEntityOptions{Format: to.Ptr(aztables.MetadataFormatNone)}))
	if !strings.Contains(string(full.Value), `"odata.type":"devstoreaccount1.Inventory"`) || !strings.Contains(string(full.Value), `"Count@odata.type":"Edm.Int64"`) ||
		strings.Contains(string(none.Value), "odata") {
		t.Fatalf("full = %s\nnone = %s", full.Value, none.Value)
	}

	// Typed filters: Int64 (L suffix), datetime, guid and numbers across Int32/Double.
	count := func(o *aztables.ListEntitiesOptions) (n, pages int) {
		for pg := c.NewListEntitiesPager(o); pg.More(); pages++ {
			n += len(must(pg.NextPage(ctx)).Entities)
		}
		return n, pages
	}
	f := "Count eq 9007199254740993L and Added lt datetime'2027-01-01T00:00:00Z' and ID eq guid'4185404A-5818-48C3-B9BE-F217DF0DBA6F' and Price gt 12 and not (Name eq 'x')"
	if n, _ := count(&aztables.ListEntitiesOptions{Filter: &f}); n != 1 {
		t.Fatalf("typed filter matched %d", n)
	}

	// Merge keeps other properties, replace drops them, stale ETags fail.
	merged := must(c.UpdateEntity(ctx, row("tools", "hammer", "Name", "Big hammer"), &aztables.UpdateEntityOptions{UpdateMode: aztables.UpdateModeMerge, IfMatch: &got.ETag}))
	_, err = c.UpdateEntity(ctx, row("tools", "hammer", "Name", "x"), &aztables.UpdateEntityOptions{UpdateMode: aztables.UpdateModeReplace, IfMatch: &got.ETag})
	wantCode(t, err, aztables.UpdateConditionNotSatisfied)
	if m := props(t, c, "tools", "hammer"); m["Name"] != "Big hammer" || m["Small"] != float64(42) {
		t.Fatalf("after merge = %v", m)
	}
	must(c.UpdateEntity(ctx, row("tools", "hammer", "Name", "Sledge"), &aztables.UpdateEntityOptions{UpdateMode: aztables.UpdateModeReplace, IfMatch: &merged.ETag}))
	if m := props(t, c, "tools", "hammer"); m["Name"] != "Sledge" || m["Small"] != nil {
		t.Fatalf("after replace = %v", m)
	}
	_, err = c.UpdateEntity(ctx, row("tools", "nope"), nil)
	wantCode(t, err, aztables.ResourceNotFound)

	// Upsert inserts, then merges.
	must(c.UpsertEntity(ctx, row("tools", "saw", "Teeth", 24), nil))
	must(c.UpsertEntity(ctx, row("tools", "saw", "Blade", "steel"), &aztables.UpsertEntityOptions{UpdateMode: aztables.UpdateModeMerge}))
	if m := props(t, c, "tools", "saw"); m["Teeth"] != float64(24) || m["Blade"] != "steel" {
		t.Fatalf("upserted = %v", m)
	}

	// Deletes need a matching ETag (or *).
	_, err = c.DeleteEntity(ctx, "tools", "hammer", &aztables.DeleteEntityOptions{IfMatch: &got.ETag})
	wantCode(t, err, aztables.UpdateConditionNotSatisfied)
	must(c.DeleteEntity(ctx, "tools", "hammer", nil))
	_, err = c.GetEntity(ctx, "tools", "hammer", nil)
	wantCode(t, err, aztables.ResourceNotFound)
	_, err = c.DeleteEntity(ctx, "tools", "hammer", nil)
	wantCode(t, err, aztables.ResourceNotFound)

	// Keys with quotes and URL-reserved characters.
	must(c.AddEntity(ctx, row("it's", "a b&c%d(e)"), nil))
	if m := props(t, c, "it's", "a b&c%d(e)"); m["RowKey"] != "a b&c%d(e)" {
		t.Fatalf("odd keys = %v", m)
	}

	// Transactions: 1050 entities in batches of 100, then paging.
	for i := 0; i < 1050; i += 100 {
		var actions []aztables.TransactionAction
		for j := i; j < min(i+100, 1050); j++ {
			actions = append(actions, aztables.TransactionAction{ActionType: aztables.TransactionTypeAdd,
				Entity: row("bulk", fmt.Sprintf("%04d", j), "N", j, "Even", j%2 == 0)})
		}
		must(c.SubmitTransaction(ctx, actions, nil))
	}
	if n, pages := count(&aztables.ListEntitiesOptions{Filter: to.Ptr("PartitionKey eq 'bulk'")}); n != 1050 || pages != 2 {
		t.Fatalf("bulk = %d entities in %d pages", n, pages)
	}
	if n, pages := count(&aztables.ListEntitiesOptions{Filter: to.Ptr("PartitionKey eq 'bulk' and N ge 1000 and not (Even eq true)"), Top: to.Ptr(int32(10))}); n != 25 || pages != 3 {
		t.Fatalf("top 10 = %d entities in %d pages", n, pages)
	}
	page := must(c.NewListEntitiesPager(&aztables.ListEntitiesOptions{Filter: to.Ptr("RowKey eq '0007'"), Select: to.Ptr("RowKey, N")}).NextPage(ctx))
	var sel map[string]any
	json.Unmarshal(page.Entities[0], &sel)
	if len(page.Entities) != 1 || sel["N"] != float64(7) || sel["RowKey"] != "0007" || sel["Even"] != nil || sel["PartitionKey"] != nil {
		t.Fatalf("select = %v", sel)
	}

	// A failing transaction changes nothing and names the failing operation.
	_, err = c.SubmitTransaction(ctx, []aztables.TransactionAction{
		{ActionType: aztables.TransactionTypeUpdateMerge, Entity: row("bulk", "0001", "N", -1)},
		{ActionType: aztables.TransactionTypeDelete, Entity: row("bulk", "0002")},
		{ActionType: aztables.TransactionTypeAdd, Entity: row("bulk", "0003")},
	}, nil)
	wantCode(t, err, aztables.EntityAlreadyExists)
	if !strings.Contains(err.Error(), "2:The specified entity already exists.") {
		t.Fatalf("error does not name the operation: %v", err)
	}
	if m := props(t, c, "bulk", "0001"); m["N"] != float64(1) {
		t.Fatalf("merge not rolled back: %v", m)
	}
	must(c.GetEntity(ctx, "bulk", "0002", nil)) // delete rolled back
	_, err = c.SubmitTransaction(ctx, []aztables.TransactionAction{
		{ActionType: aztables.TransactionTypeAdd, Entity: row("bulk", "x1")},
		{ActionType: aztables.TransactionTypeAdd, Entity: row("bulk", "x1")},
	}, nil)
	wantCode(t, err, aztables.InvalidDuplicateRow)

	// A successful mixed transaction.
	must(c.SubmitTransaction(ctx, []aztables.TransactionAction{
		{ActionType: aztables.TransactionTypeUpdateMerge, Entity: row("bulk", "0001", "Tag", "merged")},
		{ActionType: aztables.TransactionTypeUpdateReplace, Entity: row("bulk", "0002", "Tag", "replaced")},
		{ActionType: aztables.TransactionTypeDelete, Entity: row("bulk", "0004")},
		{ActionType: aztables.TransactionTypeInsertReplace, Entity: row("bulk", "9999", "Tag", "new")},
		{ActionType: aztables.TransactionTypeInsertMerge, Entity: row("bulk", "0005", "Tag", "merged")},
	}, nil))
	if m := props(t, c, "bulk", "0001"); m["N"] != float64(1) || m["Tag"] != "merged" {
		t.Fatalf("batch merge = %v", m)
	}
	if m := props(t, c, "bulk", "0002"); m["N"] != nil || m["Tag"] != "replaced" {
		t.Fatalf("batch replace = %v", m)
	}
	_, err = c.GetEntity(ctx, "bulk", "0004", nil)
	wantCode(t, err, aztables.ResourceNotFound)

	// Access policies, service properties and stats.
	start, expiry := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	must(c.SetAccessPolicy(ctx, &aztables.SetAccessPolicyOptions{TableACL: []*aztables.SignedIdentifier{
		{ID: to.Ptr("read"), AccessPolicy: &aztables.AccessPolicy{Start: &start, Expiry: &expiry, Permission: to.Ptr("r")}}}}))
	if acl := must(c.GetAccessPolicy(ctx, nil)); len(acl.SignedIdentifiers) != 1 || *acl.SignedIdentifiers[0].ID != "read" {
		t.Fatalf("acl = %+v", acl.SignedIdentifiers)
	}
	if p := must(svc.GetProperties(ctx, nil)); p.Logging == nil {
		t.Fatalf("service properties = %+v", p)
	}
	if st := must(svc.GetStatistics(ctx, nil)); st.GeoReplication == nil {
		t.Fatalf("stats = %+v", st)
	}

	// Errors on missing tables.
	_, err = svc.NewClient("missing").AddEntity(ctx, row("p", "r"), nil)
	wantCode(t, err, aztables.TableNotFound)

	// Console helpers and persistence keep ETags.
	before := must(c.GetEntity(ctx, "tools", "saw", nil))
	state := must(impl.Save())
	impl.Reset()
	if err := impl.Load(state); err != nil {
		t.Fatal(err)
	}
	if after := must(c.GetEntity(ctx, "tools", "saw", nil)); after.ETag != before.ETag {
		t.Fatalf("etag changed across save/load: %s != %s", after.ETag, before.ETag)
	}
	if es := impl.Entities("devstoreaccount1", "INVENTORY", 2); len(es) != 2 || es[0]["PartitionKey"] != "bulk" || es[0]["RowKey"] != "0000" {
		t.Fatalf("console entities = %v", es)
	}
	if rows := impl.Resources()[0].Rows; len(rows) != 2 || rows[0][0] != "Inventory" || rows[0][2] != "1052" {
		t.Fatalf("resources = %v", rows)
	}
}
