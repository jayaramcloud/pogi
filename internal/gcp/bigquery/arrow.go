package bigquery

// A small Arrow IPC writer for the Storage Read API: the schema message and record batch messages for
// the types pogi sends, and nothing else. Arrow IPC is the format Arrow uses to send tables between
// programs; the metadata is FlatBuffers, built with the flatbuffers library. The layout follows
// format/Message.fbs and format/Schema.fbs in the Arrow project.

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"math"
	"math/big"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Values of the Arrow enums and unions used below.
const (
	headerSchema      = 1 // MessageHeader
	headerRecordBatch = 3
	metadataV5        = 4 // MetadataVersion

	typeInt       = 2 // Type union
	typeFloat     = 3
	typeBinary    = 4
	typeUtf8      = 5
	typeBool      = 6
	typeDecimal   = 7
	typeDate      = 8
	typeTime      = 9
	typeTimestamp = 10
	typeList      = 12
	typeStruct    = 13

	unitDay         = 0 // DateUnit
	unitMicro       = 2 // TimeUnit
	precisionDouble = 2 // FloatingPoint precision

	codecLZ4  = 0 // CompressionType
	codecZstd = 1
)

// arrowTypeOf writes the Type of a BigQuery field (without its REPEATED mode) and returns the union
// tag, the table and the child fields.
func arrowTypeOf(b *flatbuffers.Builder, f *Field) (tag byte, tbl flatbuffers.UOffsetT, kids []flatbuffers.UOffsetT) {
	empty := func(t byte) (byte, flatbuffers.UOffsetT, []flatbuffers.UOffsetT) {
		b.StartObject(0)
		return t, b.EndObject(), nil
	}
	decimal := func(precision, scale, bits int32) (byte, flatbuffers.UOffsetT, []flatbuffers.UOffsetT) {
		b.StartObject(3)
		b.PrependInt32Slot(0, precision, 0)
		b.PrependInt32Slot(1, scale, 0)
		b.PrependInt32Slot(2, bits, 128)
		return typeDecimal, b.EndObject(), nil
	}
	timestamp := func(zone string) (byte, flatbuffers.UOffsetT, []flatbuffers.UOffsetT) {
		var z flatbuffers.UOffsetT
		if zone != "" {
			z = b.CreateString(zone)
		}
		b.StartObject(2)
		b.PrependInt16Slot(0, unitMicro, 0)
		if zone != "" {
			b.PrependUOffsetTSlot(1, z, 0)
		}
		return typeTimestamp, b.EndObject(), nil
	}
	switch f.Type {
	case "INTEGER":
		b.StartObject(2)
		b.PrependInt32Slot(0, 64, 0)
		b.PrependBoolSlot(1, true, false)
		return typeInt, b.EndObject(), nil
	case "FLOAT":
		b.StartObject(1)
		b.PrependInt16Slot(0, precisionDouble, 0)
		return typeFloat, b.EndObject(), nil
	case "BOOLEAN":
		return empty(typeBool)
	case "BYTES":
		return empty(typeBinary)
	case "TIMESTAMP":
		return timestamp("UTC")
	case "DATETIME":
		return timestamp("")
	case "DATE":
		b.StartObject(1)
		b.PrependInt16Slot(0, unitDay, 1) // the default is milliseconds
		return typeDate, b.EndObject(), nil
	case "TIME":
		b.StartObject(2)
		b.PrependInt16Slot(0, unitMicro, 1)
		b.PrependInt32Slot(1, 64, 32)
		return typeTime, b.EndObject(), nil
	case "NUMERIC":
		return decimal(38, 9, 128)
	case "BIGNUMERIC":
		return decimal(76, 38, 256)
	case "RECORD":
		for _, c := range f.Fields {
			kids = append(kids, arrowFieldOf(b, c))
		}
		b.StartObject(0)
		return typeStruct, b.EndObject(), kids
	}
	return empty(typeUtf8) // STRING, JSON, GEOGRAPHY (WKT) and INTERVAL
}

// arrowFieldOf writes a field. A repeated field is a list, never NULL, of values that are never NULL.
func arrowFieldOf(b *flatbuffers.Builder, f *Field) flatbuffers.UOffsetT {
	if !f.repeated() {
		return arrowField(b, f.Name, f.Mode != "REQUIRED", f)
	}
	item := *f
	item.Mode = "REQUIRED"
	child := arrowField(b, "item", false, &item)
	b.StartObject(0)
	return arrowFieldWith(b, f.Name, false, typeList, b.EndObject(), []flatbuffers.UOffsetT{child})
}

func arrowField(b *flatbuffers.Builder, name string, nullable bool, f *Field) flatbuffers.UOffsetT {
	tag, tbl, kids := arrowTypeOf(b, f)
	return arrowFieldWith(b, name, nullable, tag, tbl, kids)
}

func arrowFieldWith(b *flatbuffers.Builder, name string, nullable bool, tag byte, tbl flatbuffers.UOffsetT, kids []flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	n := b.CreateString(name)
	var children flatbuffers.UOffsetT
	if len(kids) > 0 {
		children = offsetVector(b, kids)
	}
	b.StartObject(7)
	b.PrependUOffsetTSlot(0, n, 0)
	b.PrependBoolSlot(1, nullable, false)
	b.PrependByteSlot(2, tag, 0)
	b.PrependUOffsetTSlot(3, tbl, 0)
	if len(kids) > 0 {
		b.PrependUOffsetTSlot(5, children, 0)
	}
	return b.EndObject()
}

func offsetVector(b *flatbuffers.Builder, offs []flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	b.StartVector(4, len(offs), 4)
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(offs))
}

// arrowMessage is an encapsulated IPC message: the continuation marker, the length of the metadata
// (padded so the body starts on 8 bytes), the metadata and the body.
func arrowMessage(header byte, hdr func(b *flatbuffers.Builder) flatbuffers.UOffsetT, body []byte) []byte {
	b := flatbuffers.NewBuilder(512)
	h := hdr(b)
	b.StartObject(5)
	b.PrependInt16Slot(0, metadataV5, 0)
	b.PrependByteSlot(1, header, 0)
	b.PrependUOffsetTSlot(2, h, 0)
	b.PrependInt64Slot(3, int64(len(body)), 0)
	b.Finish(b.EndObject())
	meta := b.FinishedBytes()
	pad := (8 - (8+len(meta))%8) % 8
	out := make([]byte, 0, 8+len(meta)+pad+len(body))
	out = binary.LittleEndian.AppendUint32(out, 0xFFFFFFFF)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(meta)+pad))
	out = append(out, meta...)
	out = append(out, make([]byte, pad)...)
	return append(out, body...)
}

// arrowSchemaBytes is the schema of the fields as an IPC message.
func arrowSchemaBytes(fields []*Field) []byte {
	return arrowMessage(headerSchema, func(b *flatbuffers.Builder) flatbuffers.UOffsetT {
		var offs []flatbuffers.UOffsetT
		for _, f := range fields {
			offs = append(offs, arrowFieldOf(b, f))
		}
		vec := offsetVector(b, offs)
		b.StartObject(4)
		b.PrependUOffsetTSlot(1, vec, 0) // little endian is the default
		return b.EndObject()
	}, nil)
}

// bufferCompression is the compression a client asked for.
type bufferCompression int

const (
	noCompression bufferCompression = iota
	lz4Frame
	zstdCompression
)

func compressionOf(c storagepb.ArrowSerializationOptions_CompressionCodec) bufferCompression {
	switch c {
	case storagepb.ArrowSerializationOptions_LZ4_FRAME:
		return lz4Frame
	case storagepb.ArrowSerializationOptions_ZSTD:
		return zstdCompression
	}
	return noCompression
}

var zstdEncoder = sync.OnceValue(func() *zstd.Encoder {
	e, _ := zstd.NewWriter(nil) // only fails on a bad option
	return e
})

// batch collects the nodes, buffers and body of one record batch.
type batch struct {
	comp  bufferCompression
	nodes [][2]int64 // length, null count
	bufs  [][2]int64 // offset, length
	body  []byte
}

// add appends a buffer to the body, compressed if asked, and pads it to 8 bytes.
func (e *batch) add(data []byte) {
	if e.comp != noCompression {
		data = e.compress(data)
	}
	e.bufs = append(e.bufs, [2]int64{int64(len(e.body)), int64(len(data))})
	e.body = append(e.body, data...)
	e.body = append(e.body, make([]byte, (8-len(e.body)%8)%8)...)
}

// compress prefixes the buffer with its size, or with -1 and the data as it was when compression saves nothing.
// An empty buffer stays empty.
func (e *batch) compress(data []byte) []byte {
	if len(data) == 0 { // readers pass an empty buffer through as it is
		return nil
	}
	var c []byte
	if e.comp == zstdCompression {
		c = zstdEncoder().EncodeAll(data, nil)
	} else {
		var buf bytes.Buffer
		w := lz4.NewWriter(&buf)
		w.Write(data) // a bytes.Buffer does not fail
		w.Close()
		c = buf.Bytes()
	}
	if len(c) >= len(data) {
		return append(binary.LittleEndian.AppendUint64(nil, math.MaxUint64), data...)
	}
	return append(binary.LittleEndian.AppendUint64(nil, uint64(len(data))), c...)
}

// encodeBatch makes rows one record batch message.
func encodeBatch(sess *readSession, rows []row) ([]byte, error) {
	e := &batch{comp: sess.comp}
	for _, f := range sess.fields {
		vals := make([]any, len(rows))
		for i, r := range rows {
			vals[i] = r[f.Name]
		}
		if err := e.column(f, vals); err != nil {
			return nil, err
		}
	}
	return arrowMessage(headerRecordBatch, func(b *flatbuffers.Builder) flatbuffers.UOffsetT {
		b.StartVector(16, len(e.bufs), 8)
		for i := len(e.bufs) - 1; i >= 0; i-- {
			b.Prep(8, 16)
			b.PrependInt64(e.bufs[i][1])
			b.PrependInt64(e.bufs[i][0])
		}
		bufs := b.EndVector(len(e.bufs))
		b.StartVector(16, len(e.nodes), 8)
		for i := len(e.nodes) - 1; i >= 0; i-- {
			b.Prep(8, 16)
			b.PrependInt64(e.nodes[i][1])
			b.PrependInt64(e.nodes[i][0])
		}
		nodes := b.EndVector(len(e.nodes))
		var comp flatbuffers.UOffsetT
		if e.comp != noCompression {
			b.StartObject(2)
			b.PrependByteSlot(0, map[bufferCompression]byte{lz4Frame: codecLZ4, zstdCompression: codecZstd}[e.comp], 0)
			comp = b.EndObject()
		}
		b.StartObject(5)
		b.PrependInt64Slot(0, int64(len(rows)), 0)
		b.PrependUOffsetTSlot(1, nodes, 0)
		b.PrependUOffsetTSlot(2, bufs, 0)
		if e.comp != noCompression {
			b.PrependUOffsetTSlot(3, comp, 0)
		}
		return b.EndObject()
	}, e.body), nil
}

// column adds the nodes and buffers of one field, in the order of the Arrow format: a node, then its
// buffers, then its children. vals holds one canonical value (see values.go) per row, nil for NULL.
func (e *batch) column(f *Field, vals []any) error {
	if f.repeated() {
		e.nodes = append(e.nodes, [2]int64{int64(len(vals)), 0})
		e.add(nil) // never NULL
		offsets := make([]byte, 0, 4*(len(vals)+1))
		offsets = binary.LittleEndian.AppendUint32(offsets, 0)
		var items []any
		for _, v := range vals {
			l, _ := v.([]any)
			items = append(items, l...)
			offsets = binary.LittleEndian.AppendUint32(offsets, uint32(len(items)))
		}
		e.add(offsets)
		item := *f
		item.Mode = "REQUIRED"
		return e.column(&item, items)
	}
	nulls := 0
	for _, v := range vals {
		if v == nil {
			nulls++
		}
	}
	e.nodes = append(e.nodes, [2]int64{int64(len(vals)), int64(nulls)})
	if nulls == 0 {
		e.add(nil)
	} else {
		e.add(bitmap(vals, func(v any) bool { return v != nil }))
	}
	bad := func(v any, err error) error {
		return errf(500, "backendError", "pogi: the stored value %v of field %s is not a valid %s: %v", v, f.Name, f.Type, err)
	}
	var data []byte // the values buffer
	var offs []byte // the offsets buffer of a variable-size type
	switch f.Type {
	case "RECORD":
		for _, v := range vals {
			if _, ok := v.(map[string]any); !ok && v != nil {
				return bad(v, strconv.ErrSyntax)
			}
		}
		for _, c := range f.Fields {
			cv := make([]any, len(vals))
			for i, v := range vals {
				if m, ok := v.(map[string]any); ok {
					cv[i] = m[c.Name]
				}
			}
			if err := e.column(c, cv); err != nil {
				return err
			}
		}
		return nil
	case "BOOLEAN":
		for _, v := range vals {
			if v != nil {
				if _, ok := v.(bool); !ok {
					return bad(v, strconv.ErrSyntax)
				}
			}
		}
		data = bitmap(vals, func(v any) bool { b, _ := v.(bool); return b })
	case "BYTES", "STRING", "JSON", "GEOGRAPHY", "INTERVAL":
		offs = binary.LittleEndian.AppendUint32(offs, 0)
		for _, v := range vals {
			s, _ := v.(string)
			if f.Type == "BYTES" && v != nil {
				raw, err := base64.StdEncoding.DecodeString(s)
				if err != nil {
					return bad(v, err)
				}
				s = string(raw)
			}
			data = append(data, s...)
			offs = binary.LittleEndian.AppendUint32(offs, uint32(len(data)))
		}
	default:
		for _, v := range vals {
			var err error
			if v == nil {
				data = append(data, make([]byte, fixedWidth(f.Type))...)
			} else if data, err = appendFixed(data, f.Type, v); err != nil {
				return bad(v, err)
			}
		}
	}
	if offs != nil {
		e.add(offs)
	}
	e.add(data)
	return nil
}

// bitmap packs one bit per value, the first value in the lowest bit.
func bitmap(vals []any, set func(any) bool) []byte {
	out := make([]byte, (len(vals)+7)/8)
	for i, v := range vals {
		if set(v) {
			out[i/8] |= 1 << (i % 8)
		}
	}
	return out
}

func fixedWidth(typ string) int {
	switch typ {
	case "DATE":
		return 4
	case "NUMERIC":
		return 16
	case "BIGNUMERIC":
		return 32
	}
	return 8
}

// appendFixed adds the little-endian bytes of one non-NULL value of a fixed-width type.
func appendFixed(out []byte, typ string, v any) ([]byte, error) {
	str, _ := v.(string)
	le := binary.LittleEndian
	switch typ {
	case "INTEGER":
		n, err := strconv.ParseInt(str, 10, 64)
		return le.AppendUint64(out, uint64(n)), err
	case "FLOAT":
		x, ok := v.(float64)
		if !ok {
			x, ok = map[string]float64{"NaN": math.NaN(), "Infinity": math.Inf(1), "-Infinity": math.Inf(-1)}[str]
		}
		if !ok {
			return out, strconv.ErrSyntax
		}
		return le.AppendUint64(out, math.Float64bits(x)), nil
	case "TIMESTAMP", "DATETIME":
		layout := time.RFC3339Nano
		if typ == "DATETIME" {
			layout = "2006-01-02T15:04:05.999999999"
		}
		t, err := time.Parse(layout, str)
		return le.AppendUint64(out, uint64(t.UnixMicro())), err
	case "DATE":
		t, err := time.Parse(time.DateOnly, str)
		return le.AppendUint32(out, uint32(int32(t.Unix()/86400))), err
	case "TIME":
		t, err := time.Parse("15:04:05.999999999", str)
		return le.AppendUint64(out, uint64((t.Hour()*3600+t.Minute()*60+t.Second())*1e6+t.Nanosecond()/1e3)), err
	case "NUMERIC":
		return appendDecimal(out, str, 38, 9, 16)
	case "BIGNUMERIC":
		return appendDecimal(out, str, 76, 38, 32)
	}
	return out, strconv.ErrSyntax
}

// appendDecimal adds s as a two's complement little-endian integer of width bytes: s times 10^scale,
// rounded half away from zero, which must have at most precision digits.
func appendDecimal(out []byte, s string, precision, scale, width int) ([]byte, error) {
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return out, strconv.ErrSyntax
	}
	ten := big.NewInt(10)
	r.Mul(r, new(big.Rat).SetInt(new(big.Int).Exp(ten, big.NewInt(int64(scale)), nil)))
	n, rem := new(big.Int).QuoRem(r.Num(), r.Denom(), new(big.Int))
	if new(big.Int).Abs(rem).Lsh(new(big.Int).Abs(rem), 1).Cmp(r.Denom()) >= 0 {
		n.Add(n, big.NewInt(int64(r.Sign())))
	}
	if new(big.Int).Abs(n).Cmp(new(big.Int).Exp(ten, big.NewInt(int64(precision)), nil)) >= 0 {
		return out, strconv.ErrRange
	}
	if n.Sign() < 0 {
		n.Add(n, new(big.Int).Lsh(big.NewInt(1), uint(8*width)))
	}
	be := n.FillBytes(make([]byte, width))
	for i := width - 1; i >= 0; i-- {
		out = append(out, be[i])
	}
	return out, nil
}
