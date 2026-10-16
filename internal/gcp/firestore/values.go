package firestore

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var null = &pb.Value{ValueType: &pb.Value_NullValue{}}

func intValue(i int64) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: i}}
}
func doubleValue(f float64) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: f}}
}
func refValue(s string) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: s}}
}
func mapValue(f map[string]*pb.Value) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: f}}}
}
func arrayValue(vs []*pb.Value) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: vs}}}
}

// typeOrder ranks value types in Firestore's cross-type sort order.
func typeOrder(v *pb.Value) int {
	switch v.GetValueType().(type) {
	case *pb.Value_NullValue:
		return 0
	case *pb.Value_BooleanValue:
		return 1
	case *pb.Value_IntegerValue, *pb.Value_DoubleValue:
		return 2
	case *pb.Value_TimestampValue:
		return 3
	case *pb.Value_StringValue:
		return 4
	case *pb.Value_BytesValue:
		return 5
	case *pb.Value_ReferenceValue:
		return 6
	case *pb.Value_GeoPointValue:
		return 7
	case *pb.Value_ArrayValue:
		return 8
	case *pb.Value_MapValue:
		return 9
	}
	return 10
}

// compare orders values like Firestore: by type, then numbers numerically (int
// and double mixed, NaN first), strings by UTF-8 bytes, references segment by
// segment, arrays element-wise, maps by sorted keys then values.
func compare(a, b *pb.Value) int {
	if c := cmp.Compare(typeOrder(a), typeOrder(b)); c != 0 {
		return c
	}
	switch x := a.GetValueType().(type) {
	case *pb.Value_BooleanValue:
		return cmp.Compare(b2i(x.BooleanValue), b2i(b.GetBooleanValue()))
	case *pb.Value_IntegerValue, *pb.Value_DoubleValue:
		return compareNumbers(a, b)
	case *pb.Value_TimestampValue:
		return x.TimestampValue.AsTime().Compare(b.GetTimestampValue().AsTime())
	case *pb.Value_StringValue:
		return strings.Compare(x.StringValue, b.GetStringValue())
	case *pb.Value_BytesValue:
		return bytes.Compare(x.BytesValue, b.GetBytesValue())
	case *pb.Value_ReferenceValue:
		return slices.Compare(strings.Split(x.ReferenceValue, "/"), strings.Split(b.GetReferenceValue(), "/"))
	case *pb.Value_GeoPointValue:
		y := b.GetGeoPointValue()
		return cmp.Or(cmp.Compare(x.GeoPointValue.GetLatitude(), y.GetLatitude()), cmp.Compare(x.GeoPointValue.GetLongitude(), y.GetLongitude()))
	case *pb.Value_ArrayValue:
		return slices.CompareFunc(x.ArrayValue.GetValues(), b.GetArrayValue().GetValues(), compare)
	case *pb.Value_MapValue:
		am, bm := x.MapValue.GetFields(), b.GetMapValue().GetFields()
		ak, bk := slices.Sorted(maps.Keys(am)), slices.Sorted(maps.Keys(bm))
		for i := 0; i < len(ak) && i < len(bk); i++ {
			if c := strings.Compare(ak[i], bk[i]); c != 0 {
				return c
			}
			if c := compare(am[ak[i]], bm[bk[i]]); c != 0 {
				return c
			}
		}
		return cmp.Compare(len(ak), len(bk))
	}
	return 0
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func compareNumbers(a, b *pb.Value) int {
	ai, aInt := a.ValueType.(*pb.Value_IntegerValue)
	bi, bInt := b.ValueType.(*pb.Value_IntegerValue)
	switch {
	case aInt && bInt:
		return cmp.Compare(ai.IntegerValue, bi.IntegerValue)
	case aInt:
		return compareIntDouble(ai.IntegerValue, b.GetDoubleValue())
	case bInt:
		return -compareIntDouble(bi.IntegerValue, a.GetDoubleValue())
	}
	return cmp.Compare(a.GetDoubleValue(), b.GetDoubleValue()) // NaN first, -0 == 0
}

// compareIntDouble compares exactly, without rounding big integers to doubles.
func compareIntDouble(i int64, f float64) int {
	switch {
	case math.IsNaN(f) || f < -0x1p63:
		return 1
	case f >= 0x1p63:
		return -1
	}
	t := math.Trunc(f)
	return cmp.Or(cmp.Compare(i, int64(t)), cmp.Compare(0, f-t))
}

func isNaN(v *pb.Value) bool { return math.IsNaN(v.GetDoubleValue()) }

func contains(vs []*pb.Value, v *pb.Value) bool {
	return slices.ContainsFunc(vs, func(x *pb.Value) bool { return compare(x, v) == 0 })
}

// splitPath parses a field path such as a.b or `odd.key`.c into segments.
func splitPath(p string) []string {
	var segs []string
	var cur []byte
	quoted := false
	for i := 0; i < len(p); i++ {
		switch c := p[i]; {
		case c == '`':
			quoted = !quoted
		case c == '\\' && quoted && i+1 < len(p):
			i++
			cur = append(cur, p[i])
		case c == '.' && !quoted:
			segs, cur = append(segs, string(cur)), cur[:0]
		default:
			cur = append(cur, c)
		}
	}
	return append(segs, string(cur))
}

// get returns the value at path inside fields.
func get(fields map[string]*pb.Value, path []string) (*pb.Value, bool) {
	for i, seg := range path {
		v, ok := fields[seg]
		if !ok || i == len(path)-1 {
			return v, ok
		}
		fields = v.GetMapValue().GetFields()
	}
	return nil, false
}

// set returns a copy of fields with path set to v (removed when v is nil).
// Stored documents are shared and never mutated, so maps on the path are copied.
func set(fields map[string]*pb.Value, path []string, v *pb.Value) map[string]*pb.Value {
	out := maps.Clone(fields)
	if out == nil {
		out = map[string]*pb.Value{}
	}
	k := path[0]
	if len(path) == 1 {
		if v == nil {
			delete(out, k)
		} else {
			out[k] = v
		}
		return out
	}
	child := out[k].GetMapValue().GetFields()
	if v == nil && child == nil {
		return out
	}
	out[k] = mapValue(set(child, path[1:], v))
	return out
}

// field returns a document field; __name__ is the document's reference.
func field(d *pb.Document, path []string) (*pb.Value, bool) {
	if len(path) == 1 && path[0] == "__name__" {
		return refValue(d.Name), true
	}
	return get(d.Fields, path)
}

// project returns d with only the given field paths (a mask or a select).
func project(d *pb.Document, paths []string) *pb.Document {
	out := &pb.Document{Name: d.Name, CreateTime: d.CreateTime, UpdateTime: d.UpdateTime}
	for _, p := range paths {
		sp := splitPath(p)
		if v, ok := get(d.Fields, sp); ok {
			out.Fields = set(out.Fields, sp, v)
		}
	}
	return out
}

func mask(d *pb.Document, m *pb.DocumentMask) *pb.Document {
	if m == nil {
		return d
	}
	return project(d, m.FieldPaths)
}

// transform applies a field transform to the current value (nil when unset)
// and returns the new value and the transform result.
func transform(cur *pb.Value, t *pb.DocumentTransform_FieldTransform, now time.Time) (*pb.Value, *pb.Value, error) {
	var v *pb.Value
	switch x := t.GetTransformType().(type) {
	case *pb.DocumentTransform_FieldTransform_SetToServerValue:
		if x.SetToServerValue != pb.DocumentTransform_FieldTransform_REQUEST_TIME {
			return nil, nil, status.Errorf(codes.InvalidArgument, "unknown server value for %s", t.FieldPath)
		}
		v = &pb.Value{ValueType: &pb.Value_TimestampValue{TimestampValue: timestamppb.New(now)}}
	case *pb.DocumentTransform_FieldTransform_Increment:
		v = increment(cur, x.Increment)
	case *pb.DocumentTransform_FieldTransform_Maximum:
		v = extreme(cur, x.Maximum, 1)
	case *pb.DocumentTransform_FieldTransform_Minimum:
		v = extreme(cur, x.Minimum, -1)
	case *pb.DocumentTransform_FieldTransform_AppendMissingElements:
		out := slices.Clone(cur.GetArrayValue().GetValues())
		for _, e := range x.AppendMissingElements.GetValues() {
			if !contains(out, e) {
				out = append(out, e)
			}
		}
		return arrayValue(out), null, nil
	case *pb.DocumentTransform_FieldTransform_RemoveAllFromArray:
		out := slices.DeleteFunc(slices.Clone(cur.GetArrayValue().GetValues()), func(e *pb.Value) bool {
			return contains(x.RemoveAllFromArray.GetValues(), e)
		})
		return arrayValue(out), null, nil
	default:
		return nil, nil, status.Errorf(codes.InvalidArgument, "unsupported transform for %s", t.FieldPath)
	}
	return v, v, nil
}

// increment adds numerically; integers saturate instead of overflowing, and a
// field that is not a number is replaced by the operand.
func increment(cur, by *pb.Value) *pb.Value {
	if typeOrder(cur) != 2 {
		return by
	}
	a, aInt := cur.ValueType.(*pb.Value_IntegerValue)
	b, bInt := by.ValueType.(*pb.Value_IntegerValue)
	if aInt && bInt {
		s := a.IntegerValue + b.IntegerValue
		if (s > a.IntegerValue) != (b.IntegerValue > 0) {
			s = math.MaxInt64
			if b.IntegerValue < 0 {
				s = math.MinInt64
			}
		}
		return intValue(s)
	}
	return doubleValue(toFloat(cur) + toFloat(by))
}

func toFloat(v *pb.Value) float64 {
	if i, ok := v.ValueType.(*pb.Value_IntegerValue); ok {
		return float64(i.IntegerValue)
	}
	return v.GetDoubleValue()
}

// extreme is maximum (sign 1) or minimum (sign -1): NaN wins, equal numbers
// keep the stored value, a non-number is replaced by the operand.
func extreme(cur, v *pb.Value, sign int) *pb.Value {
	switch {
	case typeOrder(cur) != 2, isNaN(v):
		return v
	case isNaN(cur) || compareNumbers(v, cur)*sign <= 0:
		return cur
	}
	return v
}

// toJSON renders a value as natural JSON for the console.
func toJSON(v *pb.Value) any {
	switch x := v.GetValueType().(type) {
	case *pb.Value_BooleanValue:
		return x.BooleanValue
	case *pb.Value_IntegerValue:
		return x.IntegerValue
	case *pb.Value_DoubleValue:
		if math.IsNaN(x.DoubleValue) || math.IsInf(x.DoubleValue, 0) {
			return fmt.Sprint(x.DoubleValue) // JSON has no NaN
		}
		return x.DoubleValue
	case *pb.Value_TimestampValue:
		return x.TimestampValue.AsTime().Format(time.RFC3339Nano)
	case *pb.Value_StringValue:
		return x.StringValue
	case *pb.Value_BytesValue:
		return x.BytesValue
	case *pb.Value_ReferenceValue:
		return x.ReferenceValue
	case *pb.Value_GeoPointValue:
		return map[string]any{"latitude": x.GeoPointValue.GetLatitude(), "longitude": x.GeoPointValue.GetLongitude()}
	case *pb.Value_ArrayValue:
		out := []any{}
		for _, e := range x.ArrayValue.GetValues() {
			out = append(out, toJSON(e))
		}
		return out
	case *pb.Value_MapValue:
		out := map[string]any{}
		for k, e := range x.MapValue.GetFields() {
			out[k] = toJSON(e)
		}
		return out
	}
	return nil
}

// fromJSON converts decoded JSON (whole numbers become integers) to a value.
func fromJSON(x any) *pb.Value {
	switch v := x.(type) {
	case nil:
		return null
	case bool:
		return &pb.Value{ValueType: &pb.Value_BooleanValue{BooleanValue: v}}
	case int:
		return intValue(int64(v))
	case int64:
		return intValue(v)
	case float64:
		if v == math.Trunc(v) && math.Abs(v) < 1<<53 {
			return intValue(int64(v))
		}
		return doubleValue(v)
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return intValue(i)
		}
		f, _ := v.Float64()
		return doubleValue(f)
	case string:
		return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: v}}
	case []any:
		var out []*pb.Value
		for _, e := range v {
			out = append(out, fromJSON(e))
		}
		return arrayValue(out)
	case map[string]any:
		out := map[string]*pb.Value{}
		for k, e := range v {
			out[k] = fromJSON(e)
		}
		return mapValue(out)
	}
	return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: fmt.Sprint(x)}}
}
