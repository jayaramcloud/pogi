package web_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
)

func gcpPost(t *testing.T, srv *pogi.Running, verb, path, body string) {
	t.Helper()
	req, _ := http.NewRequest(verb, srv.URL+path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil || res.StatusCode >= 300 {
		t.Fatalf("%s %s: %v %v", verb, path, err, res)
	}
	res.Body.Close()
}

func TestDynamoDBItems(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"dynamodb"}})
	call(t, srv, "DynamoDB_20120810", "CreateTable", map[string]any{"TableName": "Users", "BillingMode": "PAY_PER_REQUEST",
		"AttributeDefinitions": []any{map[string]any{"AttributeName": "id", "AttributeType": "S"}},
		"KeySchema":            []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}})
	target := core.DefaultAccount + "/" + core.DefaultRegion + "/Users"
	q := url.Values{"target": {target}, "mode": {"scan"}, "run": {"1"}}
	put := func(item string) string {
		return form(t, srv, "/_pogi/query/dynamodb/put", url.Values{"target": {target}, "q": {q.Encode()}, "item": {item}, "from": {"add"}})
	}

	// add: plain JSON becomes an item and the results show it
	p := put(`{"id": "u3", "name": "Kim", "age": 31, "tags": {"SS": ["a", "b"]}}`)
	has(t, p, "1 items", ">u3<", ">Kim<", "Delete")
	got := call(t, srv, "DynamoDB_20120810", "GetItem", map[string]any{"TableName": "Users", "Key": map[string]any{"id": map[string]any{"S": "u3"}}})
	if !strings.Contains(strings.ReplaceAll(strings.ReplaceAll(toJSON(got), " ", ""), `"`, ""), "age:{N:31}") {
		t.Errorf("age is not a number: %v", got)
	}
	// the edit box keeps a string set typed
	has(t, p, "&#34;SS&#34;")

	// failures show in the Add item form and write nothing
	for _, c := range []struct{ item, msg string }{
		{`not json`, "not a JSON object"},
		{`{"name": "x"}`, `missing the key attribute &#34;id&#34;`},
		{`{"id": "big", "x": "` + strings.Repeat("a", 401<<10) + `"}`, "larger than 400 KB"},
	} {
		has(t, put(c.item), c.msg)
	}
	has(t, put(`{"id": "u4"}`), "2 items")

	// edit saves over the same key
	has(t, put(`{"id": "u3", "name": "Kimberly"}`), ">Kimberly<")

	// delete by key; a row without its key is refused
	typed := `{"id":{"S":"u3"},"name":{"S":"Kimberly"}}`
	p = form(t, srv, "/_pogi/query/dynamodb/delete", url.Values{"target": {target}, "q": {q.Encode()}, "item": {typed}})
	has(t, p, "1 items")
	wantNot(t, p, ">u3<")
	p = form(t, srv, "/_pogi/query/dynamodb/delete", url.Values{"target": {target}, "q": {q.Encode()}, "item": {`{"name":{"S":"x"}}`}})
	has(t, p, "has no key attribute")
}

func TestPubSubMessages(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"pubsub"}})
	gcpPost(t, srv, "PUT", "/v1/projects/pogi/topics/jobs", `{}`)
	gcpPost(t, srv, "PUT", "/v1/projects/pogi/subscriptions/worker", `{"topic":"projects/pogi/topics/jobs","ackDeadlineSeconds":60}`)
	gcpPost(t, srv, "POST", "/v1/projects/pogi/topics/jobs:publish", `{"messages":[{"data":"`+base64.StdEncoding.EncodeToString([]byte("hello"))+`","attributes":{"k":"v"}},{"data":"/wA="}]}`)
	sub := "projects/pogi/subscriptions/worker"

	has(t, page(t, srv, "/_pogi/pubsub/messages"), `value="`+sub+`"`)
	do := func(v url.Values) string {
		v.Set("sub", sub)
		return form(t, srv, "/_pogi/pubsub/messages", v)
	}
	p := do(url.Values{"act": {"pull"}, "n": {"10"}})
	has(t, p, ">hello<", "k=v", ">base64<", "/wA=", `name="ack"`)
	// pulled but not acked: the backlog is leased, not gone
	has(t, do(url.Values{"act": {"pull"}, "n": {"10"}}), "No messages to pull")

	// ack by ack ID, then bad input
	i := strings.Index(p, `name="ack" value="`) + len(`name="ack" value="`)
	id := p[i : i+strings.Index(p[i:], `"`)]
	has(t, do(url.Values{"act": {"ack"}, "n": {"10"}, "ack": {id}}), "1 message acked")
	has(t, do(url.Values{"act": {"ack"}, "n": {"10"}}), "Tick the messages")
	has(t, do(url.Values{"act": {"pull"}, "n": {"0"}}), "from 1 to 100")
	has(t, form(t, srv, "/_pogi/pubsub/messages", url.Values{"act": {"pull"}, "n": {"5"}, "sub": {"../x"}}), "Choose a subscription")
}

func TestKinesisRecords(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"kinesis"}})
	call(t, srv, "Kinesis_20131202", "CreateStream", map[string]any{"StreamName": "events", "ShardCount": 1})
	stream := core.DefaultAccount + "/" + core.DefaultRegion + "/events"
	do := func(v url.Values) string {
		v.Set("stream", stream)
		return form(t, srv, "/_pogi/kinesis/records", v)
	}
	has(t, page(t, srv, "/_pogi/kinesis/records"), `value="`+stream+`"`)
	has(t, do(url.Values{"act": {"put"}, "key": {"k1"}, "data": {"hello"}}), "Record put on events")
	has(t, do(url.Values{"act": {"get"}, "shard": {"shardId-000000000000"}, "from": {"oldest"}, "limit": {"10"}}), ">hello<", ">k1<")
	has(t, do(url.Values{"act": {"get"}, "shard": {"shardId-000000000000"}, "from": {"latest"}, "limit": {"10"}}), "No records")
	has(t, do(url.Values{"act": {"put"}, "data": {"x"}}), "Partition key is required")
	has(t, do(url.Values{"act": {"get"}, "shard": {"nope"}, "from": {"oldest"}, "limit": {"10"}}), "Shard must look like")
	has(t, do(url.Values{"act": {"get"}, "shard": {"shardId-000000000000"}, "from": {"oldest"}, "limit": {"0"}}), "Limit must be")
}

func toJSON(v any) string { b, _ := json.Marshal(v); return string(b) }
