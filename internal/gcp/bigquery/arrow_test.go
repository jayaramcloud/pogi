package bigquery

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func mustBatch(t *testing.T, s *readSession, rows []row) []byte {
	t.Helper()
	b, err := encodeBatch(s, rows)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func allTypeFields() []*Field {
	return []*Field{
		{Name: "i", Type: "INTEGER", Mode: "REQUIRED"}, {Name: "f", Type: "FLOAT"}, {Name: "b", Type: "BOOLEAN"}, {Name: "by", Type: "BYTES"},
		{Name: "s", Type: "STRING"}, {Name: "ts", Type: "TIMESTAMP"}, {Name: "dt", Type: "DATETIME"}, {Name: "d", Type: "DATE"}, {Name: "t", Type: "TIME"},
		{Name: "n", Type: "NUMERIC"}, {Name: "bn", Type: "BIGNUMERIC"},
		{Name: "r", Type: "RECORD", Fields: []*Field{{Name: "x", Type: "INTEGER", Mode: "REQUIRED"}, {Name: "y", Type: "STRING"}, {Name: "ys", Type: "STRING", Mode: "REPEATED"}}},
		{Name: "li", Type: "INTEGER", Mode: "REPEATED"},
		{Name: "lr", Type: "RECORD", Mode: "REPEATED", Fields: []*Field{{Name: "a", Type: "DATE"}}},
	}
}

// What arrow-go's own writer made of the schema, before pogi had one of its own.
const allTypesSchema = `schema:
  fields: 14
    - i: type=int64
    - f: type=float64, nullable
    - b: type=bool, nullable
    - by: type=binary, nullable
    - s: type=utf8, nullable
    - ts: type=timestamp[us, tz=UTC], nullable
    - dt: type=timestamp[us], nullable
    - d: type=date32, nullable
    - t: type=time64[us], nullable
    - n: type=decimal(38, 9), nullable
    - bn: type=decimal256(76, 38), nullable
    - r: type=struct<x: int64, y: utf8 nullable, ys: list<item: utf8>>, nullable
    - li: type=list<item: int64>
    - lr: type=list<item: struct<a: date32 nullable>>`

// read decodes a schema message and batch messages with arrow-go and shows every row as text.
func readBack(t *testing.T, schema []byte, batches ...[]byte) (string, []string) {
	t.Helper()
	var rows []string
	var sc string
	for _, b := range batches {
		r, err := ipc.NewReader(bytes.NewReader(slices.Concat(schema, b)))
		if err != nil {
			t.Fatal(err)
		}
		sc = r.Schema().String()
		for r.Next() {
			rec := r.RecordBatch()
			for i := 0; i < int(rec.NumRows()); i++ {
				var cells []string
				for _, col := range rec.Columns() {
					cells = append(cells, col.ValueStr(i))
				}
				rows = append(rows, strings.Join(cells, " | "))
			}
		}
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
		r.Release()
	}
	return sc, rows
}

func TestArrowWriter(t *testing.T) {
	fields := allTypeFields()
	rows := []row{
		{"i": "-7", "f": 1.5, "b": true, "by": "aGk=", "s": "héllo", "ts": "2026-03-01T10:00:00.000005Z", "dt": "2026-03-01T10:00:00.000005", "d": "2026-03-01",
			"t": "01:02:03.456", "n": "-0.333333333", "bn": "12345678901234567890.12345678901234567890123456789012345678",
			"r": map[string]any{"x": "1", "y": "yy", "ys": []any{"a", "b"}}, "li": []any{"1", "2", "3"}, "lr": []any{map[string]any{"a": "1969-12-31"}, map[string]any{"a": nil}}},
		{"i": "2"}, // every other field NULL, the lists empty
		{"i": "9223372036854775807", "f": "NaN", "b": false, "by": "", "s": "", "d": "1970-01-01", "r": map[string]any{"x": "5"}, "li": []any{}, "lr": nil},
	}
	want := []string{
		`-7 | 1.5 | true | aGk= | héllo | 2026-03-01T10:00:00.000005Z | 2026-03-01T10:00:00.000005Z | 2026-03-01 | 01:02:03.456000 | -0.333333333 | 12345678901234567890.12345678901234567890123456789012345678 | {"x":1,"y":"yy","ys":["a","b"]} | [1,2,3] | [{"a":"1969-12-31"},{"a":null}]`,
		`2 | (null) | (null) | (null) | (null) | (null) | (null) | (null) | (null) | (null) | (null) | (null) | [] | []`,
		`9223372036854775807 | NaN | false |  |  | (null) | (null) | 1970-01-01 | (null) | (null) | (null) | {"x":5,"y":null,"ys":[]} | [] | []`,
	}
	schema := arrowSchemaBytes(fields)
	for _, comp := range []bufferCompression{noCompression, lz4Frame, zstdCompression} {
		sess := &readSession{fields: fields, comp: comp}
		b, err := encodeBatch(sess, rows)
		if err != nil {
			t.Fatal(err)
		}
		sc, got := readBack(t, schema, b)
		if sc != allTypesSchema {
			t.Fatalf("compression %d: schema\n%s", comp, sc)
		}
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("compression %d: rows\n%s\nwant\n%s", comp, strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
		// an empty batch
		eb, err := encodeBatch(sess, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, got := readBack(t, schema, eb); len(got) != 0 {
			t.Fatalf("empty batch gave %v", got)
		}
	}
}

// Compressible data really shrinks, and data that does not compress is sent as it is.
func TestArrowCompression(t *testing.T) {
	fields := []*Field{{Name: "s", Type: "STRING"}}
	var rows []row
	for range 500 {
		rows = append(rows, row{"s": strings.Repeat("pogi", 20)})
	}
	plain := mustBatch(t, &readSession{fields: fields}, rows)
	for _, comp := range []bufferCompression{lz4Frame, zstdCompression} {
		b := mustBatch(t, &readSession{fields: fields, comp: comp}, rows)
		if len(b) > len(plain)/5 {
			t.Errorf("compression %d: %d bytes, uncompressed %d", comp, len(b), len(plain))
		}
		if _, got := readBack(t, arrowSchemaBytes(fields), b); len(got) != 500 || got[499] != strings.Repeat("pogi", 20) {
			t.Errorf("compression %d: read back %d rows", comp, len(got))
		}
	}
	tiny := []row{{"s": "x"}}
	if _, got := readBack(t, arrowSchemaBytes(fields), mustBatch(t, &readSession{fields: fields, comp: zstdCompression}, tiny)); len(got) != 1 || got[0] != "x" {
		t.Errorf("incompressible: %v", got)
	}
}

// A stored value that is not valid for its type is an error, not a wrong number.
func TestArrowBadValues(t *testing.T) {
	for typ, v := range map[string]any{"INTEGER": "x", "FLOAT": "x", "BOOLEAN": "x", "BYTES": "!!", "TIMESTAMP": "x", "DATE": "x", "TIME": "x", "NUMERIC": "x",
		"BIGNUMERIC": "1e80", "RECORD": "x"} {
		_, err := encodeBatch(&readSession{fields: []*Field{{Name: "c", Type: typ}}}, []row{{"c": v}})
		if err == nil {
			t.Errorf("%s %v: no error", typ, v)
		}
	}
	if _, err := encodeBatch(&readSession{fields: []*Field{{Name: "c", Type: "NUMERIC"}}}, []row{{"c": "1e30"}}); err == nil {
		t.Error("a NUMERIC with more than 29 whole digits was accepted")
	}
}
