package web

// DynamoDB items in the Query tab: add, edit and delete one item. Writes go through
// the hub as the console, so they show in Activity.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

const ddbItemMax = 400 << 10 // DynamoDB's item size limit

// ddbRow is one result row's item: Edit is its JSON for the edit box, Item the same
// as typed attribute values (what Delete sends back).
type ddbRow struct{ Edit, Item string }

// itemFlash is the outcome of an item write, shown on the page that re-runs the query.
type itemFlash struct{ Err, ItemErr, Item string }

// editAV is plainAV, except that values plain JSON cannot hold (binary and sets of
// strings or binary) stay typed, so an edit does not change their type.
func editAV(v any) any {
	m, _ := v.(map[string]any)
	for k, x := range m {
		switch k {
		case "B", "SS", "BS":
			return m
		case "L":
			l := []any{}
			for _, e := range x.([]any) {
				l = append(l, editAV(e))
			}
			return l
		case "M":
			o := map[string]any{}
			for n, e := range x.(map[string]any) {
				o[n] = editAV(e)
			}
			return o
		}
	}
	return plainAV(v)
}

func ddbRows(items []map[string]any) []ddbRow {
	out := make([]ddbRow, len(items))
	for i, it := range items {
		plain := map[string]any{}
		for k, v := range it {
			plain[k] = editAV(v)
		}
		e, _ := json.MarshalIndent(plain, "", "  ")
		t, _ := json.Marshal(it)
		out[i] = ddbRow{string(e), string(t)}
	}
	return out
}

// Vals is the extra form values of an Edit or Delete button: the table, the query to re-run and, for Delete, the typed item.
func (d *queryData) Vals(item string) string {
	m := map[string]string{"target": d.V.Get("target"), "q": d.V.Encode()}
	if item != "" {
		m["item"] = item
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// ddbKeys is the names of a table's key attributes.
func (ui *UI) ddbKeys(account, region, table string) ([]string, error) {
	var out struct {
		Table struct {
			KeySchema []struct{ AttributeName string }
		}
	}
	if err := ui.hub.Call(account, region, "DynamoDB_20120810", "DescribeTable", map[string]any{"TableName": table}, &out); err != nil {
		return nil, err
	}
	var keys []string
	for _, k := range out.Table.KeySchema {
		keys = append(keys, k.AttributeName)
	}
	return keys, nil
}

// ddbPutItem turns the box's plain JSON into an item and puts it. The item must hold every key attribute.
func (ui *UI) ddbPutItem(account, region, table, text string) error {
	var plain map[string]any
	if err := decodeJSON(text, &plain); err != nil || plain == nil {
		return errors.New(`Item is not a JSON object. Write it like {"id": "u3", "name": "Kim"}.`)
	}
	keys, err := ui.ddbKeys(account, region, table)
	if err != nil {
		return err
	}
	item := map[string]any{}
	for k, v := range plain {
		item[k] = av(v)
	}
	for _, k := range keys {
		if _, ok := item[k]; !ok {
			return fmt.Errorf("Item is missing the key attribute %q. Add it and save again.", k)
		}
	}
	if b, _ := json.Marshal(item); len(b) > ddbItemMax {
		return errors.New("Item is larger than 400 KB. Make it smaller.")
	}
	return ui.hub.ConsoleCall(account, region, "DynamoDB_20120810", "PutItem", map[string]any{"TableName": table, "Item": item}, nil)
}

// ddbDeleteItem deletes by the key attributes of a typed item.
func (ui *UI) ddbDeleteItem(account, region, table, text string) error {
	var item map[string]any
	if err := json.Unmarshal([]byte(text), &item); err != nil || item == nil {
		return errors.New("Choose an item to delete.")
	}
	keys, err := ui.ddbKeys(account, region, table)
	if err != nil {
		return err
	}
	key := map[string]any{}
	for _, k := range keys {
		v, ok := item[k]
		if !ok {
			return fmt.Errorf("This row has no key attribute %q (the query left it out). Run the query without a projection and try again.", k)
		}
		key[k] = v
	}
	return ui.hub.ConsoleCall(account, region, "DynamoDB_20120810", "DeleteItem", map[string]any{"TableName": table, "Key": key}, nil)
}

// ddbItem is POST /_pogi/query/dynamodb/{act} (put or delete): it writes, then shows the query again.
func (ui *UI) ddbItem(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "The form is too large or unreadable.", http.StatusBadRequest)
		return
	}
	q, _ := url.ParseQuery(r.PostForm.Get("q"))
	target := r.PostForm.Get("target")
	q.Set("target", target)
	q.Set("run", "1")
	t := strings.SplitN(target, "/", 3)
	fl := &itemFlash{}
	var err error
	switch {
	case len(t) != 3:
		err = errors.New("Choose a table first.")
	case r.PathValue("act") == "put":
		err = ui.ddbPutItem(t[0], t[1], t[2], r.PostForm.Get("item"))
	case r.PathValue("act") == "delete":
		err = ui.ddbDeleteItem(t[0], t[1], t[2], r.PostForm.Get("item"))
	default:
		http.NotFound(w, r)
		return
	}
	switch {
	case err != nil && r.PostForm.Get("from") == "add":
		fl.ItemErr, fl.Item = err.Error(), r.PostForm.Get("item")
	case err != nil:
		fl.Err = err.Error()
	default:
		msg := "Item saved in " + t[2]
		if r.PathValue("act") == "delete" {
			msg = "Item deleted from " + t[2]
		}
		trigger(w, "pogi-toast", map[string]string{"msg": msg})
	}
	ui.queryRender(w, r, "dynamodb", q, fl)
}
