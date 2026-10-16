package pubsub_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const avroOrder = `{"type":"record","name":"Order","namespace":"shop","fields":[
  {"name":"id","type":"long"},
  {"name":"name","type":"string"},
  {"name":"kind","type":{"type":"enum","name":"Kind","symbols":["A","B"]}},
  {"name":"tags","type":{"type":"array","items":"string"}},
  {"name":"counts","type":{"type":"map","values":"int"}},
  {"name":"note","type":["null","string"],"default":null},
  {"name":"next","type":["null","Order"],"default":null}]}`

const protoOrder = `
syntax = "proto3";
package shop;
// An order.
message Order {
  enum Kind { KIND_UNSPECIFIED = 0; A = 1; B = 2; }
  message Line { string sku = 1; int32 qty = 2; }
  int64 id = 1;
  string name = 2;
  Kind kind = 3;
  repeated Line lines = 4;
  map<string, int32> counts = 5;
  optional string note = 6;
  oneof payment { string card = 7; string cash = 8; }
}`

func schemaClient(t *testing.T, url string) pb.SchemaServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(strings.TrimPrefix(url, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewSchemaServiceClient(conn)
}

func TestSchemaValidationOnPublish(t *testing.T) {
	_, c, url := setup(t)
	sc := schemaClient(t, url)
	mk := func(id string, typ pb.Schema_Type, def string, enc pb.Encoding) string {
		t.Helper()
		s := must(sc.CreateSchema(ctx, &pb.CreateSchemaRequest{Parent: proj, SchemaId: id, Schema: &pb.Schema{Type: typ, Definition: def}}))
		return must(c.TopicAdminClient.CreateTopic(ctx, &pb.Topic{Name: proj + "/topics/" + id, SchemaSettings: &pb.SchemaSettings{Schema: s.Name, Encoding: enc}})).Name
	}
	refused := func(topic string, data []byte) {
		t.Helper()
		_, err := c.TopicAdminClient.Publish(ctx, &pb.PublishRequest{Topic: topic, Messages: []*pb.PubsubMessage{{Data: data}}})
		wantCode(t, err, codes.InvalidArgument)
		if want := "Invalid data in message: Message failed schema validation."; !strings.Contains(err.Error(), want) {
			t.Fatalf("message %q, want %q", err, want)
		}
	}
	accepted := func(topic string, data []byte) {
		t.Helper()
		if len(publish(t, c, topic, &pb.PubsubMessage{Data: data})) != 1 {
			t.Fatal("not published")
		}
	}

	avroJSON := mk("avro-json", pb.Schema_AVRO, avroOrder, pb.Encoding_JSON)
	accepted(avroJSON, []byte(`{"id":1,"name":"x","kind":"A","tags":["a","b"],"counts":{"n":3},"note":{"string":"hi"},"next":{"shop.Order":{"id":2,"name":"y","kind":"B","tags":[],"counts":{}}}}`))
	accepted(avroJSON, []byte(`{"id":1,"name":"x","kind":"B","tags":[],"counts":{},"extra":true}`)) // note and next have defaults; unknown fields are ignored
	for _, bad := range []string{
		`not json`,
		`{"id":1,"name":"x","kind":"A","tags":[],"counts":{}`,                                 // truncated
		`{"name":"x","kind":"A","tags":[],"counts":{}}`,                                       // id is missing
		`{"id":"1","name":"x","kind":"A","tags":[],"counts":{}}`,                              // id is a string
		`{"id":1.5,"name":"x","kind":"A","tags":[],"counts":{}}`,                              // id is not whole
		`{"id":1,"name":"x","kind":"C","tags":[],"counts":{}}`,                                // not an enum symbol
		`{"id":1,"name":"x","kind":"A","tags":[1],"counts":{}}`,                               // array item type
		`{"id":1,"name":"x","kind":"A","tags":[],"counts":{"n":"3"}}`,                         // map value type
		`{"id":1,"name":"x","kind":"A","tags":[],"counts":{"n":4294967296}}`,                  // too big for int
		`{"id":1,"name":"x","kind":"A","tags":[],"counts":{},"note":"hi"}`,                    // a union needs its branch name
		`{"id":1,"name":"x","kind":"A","tags":[],"counts":{},"note":{"int":1}}`,               // not a branch
		`{"id":1,"name":"x","kind":"A","tags":[],"counts":{},"next":{"shop.Order":{"id":2}}}`, // nested record is missing fields
	} {
		refused(avroJSON, []byte(bad))
	}

	// {id long, name string}: 1 is 0x02, the 2-byte string "ab" is 0x04 'a' 'b'
	avroBin := mk("avro-bin", pb.Schema_AVRO, `{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"xs","type":{"type":"array","items":"int"}}]}`, pb.Encoding_BINARY)
	accepted(avroBin, []byte{0x02, 0x04, 'a', 'b', 0x04, 0x02, 0x04, 0x00}) // two ints in one block, then the end
	refused(avroBin, []byte{0x02, 0x04, 'a', 'b', 0x04, 0x02, 0x04})        // the array has no end
	refused(avroBin, []byte{0x02, 0x08, 'a', 'b', 0x00})                    // the string is longer than the data
	refused(avroBin, []byte{0x02, 0x04, 'a', 'b', 0x00, 0x00})              // data left over
	refused(avroBin, []byte(`{"id":1}`))                                    // JSON is not binary

	protoJSON := mk("proto-json", pb.Schema_PROTOCOL_BUFFER, protoOrder, pb.Encoding_JSON)
	accepted(protoJSON, []byte(`{"id":"7","name":"x","kind":"B","lines":[{"sku":"a","qty":2}],"counts":{"k":1},"note":"n","card":"visa"}`))
	accepted(protoJSON, []byte(`{}`))
	for _, bad := range []string{`nope`, `{"id":"x"}`, `{"kind":"Z"}`, `{"lines":[{"qty":"many"}]}`, `{"unknown":1}`, `{"card":"a","cash":"b"}`, `{"counts":{"k":"v"}}`} {
		refused(protoJSON, []byte(bad))
	}

	protoBin := mk("proto-bin", pb.Schema_PROTOCOL_BUFFER, `syntax = "proto3"; message M { int32 id = 1; string name = 2; }`, pb.Encoding_BINARY)
	accepted(protoBin, []byte{0x08, 0x01, 0x12, 0x02, 'a', 'b'})
	refused(protoBin, []byte{0x08})                   // a cut varint
	refused(protoBin, []byte{0x12, 0x05, 'a'})        // the string is longer than the data
	refused(protoBin, []byte{0x12, 0x02, 0xff, 0xfe}) // a string must be UTF-8

	// a topic that names a schema that does not exist is refused
	_, err := c.TopicAdminClient.CreateTopic(ctx, &pb.Topic{Name: proj + "/topics/lost", SchemaSettings: &pb.SchemaSettings{Schema: proj + "/schemas/none", Encoding: pb.Encoding_JSON}})
	wantCode(t, err, codes.NotFound)

	// REST answers 400 INVALID_ARGUMENT
	req, _ := http.NewRequest("POST", url+"/v1/"+avroJSON+":publish", strings.NewReader(`{"messages":[{"data":"bm9wZQ=="}]}`))
	req.Header.Set("Content-Type", "application/json")
	resp := must(http.DefaultClient.Do(req))
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("REST status %d", resp.StatusCode)
	}
}

func TestValidateMessageAndSchema(t *testing.T) {
	_, c, url := setup(t)
	sc := schemaClient(t, url)
	_ = c
	bg := context.Background()
	avro := &pb.Schema{Type: pb.Schema_AVRO, Definition: `{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}`}
	must(sc.CreateSchema(bg, &pb.CreateSchemaRequest{Parent: proj, SchemaId: "rec", Schema: avro}))

	validate := func(req *pb.ValidateMessageRequest) error {
		req.Parent = proj
		_, err := sc.ValidateMessage(bg, req)
		return err
	}
	if err := validate(&pb.ValidateMessageRequest{SchemaSpec: &pb.ValidateMessageRequest_Name{Name: proj + "/schemas/rec"}, Message: []byte(`{"n":1}`), Encoding: pb.Encoding_JSON}); err != nil {
		t.Fatalf("valid message: %v", err)
	}
	err := validate(&pb.ValidateMessageRequest{SchemaSpec: &pb.ValidateMessageRequest_Name{Name: proj + "/schemas/rec"}, Message: []byte(`{"n":"1"}`), Encoding: pb.Encoding_JSON})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "Message failed schema validation") {
		t.Fatalf("invalid message: %v", err)
	}
	if err := validate(&pb.ValidateMessageRequest{SchemaSpec: &pb.ValidateMessageRequest_Schema{Schema: avro}, Message: []byte{0x02}, Encoding: pb.Encoding_BINARY}); err != nil {
		t.Fatalf("inline schema, binary: %v", err)
	}
	wantCode(t, validate(&pb.ValidateMessageRequest{SchemaSpec: &pb.ValidateMessageRequest_Name{Name: proj + "/schemas/none"}, Message: []byte(`{}`)}), codes.NotFound)

	// bad definitions are refused when a schema is created or validated
	for _, bad := range []*pb.Schema{
		{Type: pb.Schema_AVRO, Definition: `{"type":"record","name":"R"}`},
		{Type: pb.Schema_AVRO, Definition: `{"type":"nope"}`},
		{Type: pb.Schema_PROTOCOL_BUFFER, Definition: `message M { int32 id = ; }`},
		{Type: pb.Schema_PROTOCOL_BUFFER, Definition: `syntax = "proto3"; message M { Other o = 1; }`},
		{Type: pb.Schema_PROTOCOL_BUFFER, Definition: `syntax = "proto3"; enum E { A = 1; }`},
	} {
		_, err := sc.ValidateSchema(bg, &pb.ValidateSchemaRequest{Parent: proj, Schema: bad})
		wantCode(t, err, codes.InvalidArgument)
		_, err = sc.CreateSchema(bg, &pb.CreateSchemaRequest{Parent: proj, SchemaId: "bad", Schema: bad})
		wantCode(t, err, codes.InvalidArgument)
	}
	if _, err := sc.ValidateSchema(bg, &pb.ValidateSchemaRequest{Parent: proj, Schema: &pb.Schema{Type: pb.Schema_PROTOCOL_BUFFER, Definition: protoOrder}}); err != nil {
		t.Fatalf("valid proto: %v", err)
	}
}
