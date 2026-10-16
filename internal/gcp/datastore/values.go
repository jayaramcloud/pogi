package datastore

import (
	"bytes"
	"cmp"
	"math"
	"slices"
	"strconv"
	"strings"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
)

// ---- keys ----

// skey is the identity of a key within a database: its namespace and path.
func skey(k *pb.Key) string {
	var b strings.Builder
	b.WriteString(k.GetPartitionId().GetNamespaceId())
	for _, p := range k.GetPath() {
		b.WriteByte(0)
		b.WriteString(p.Kind)
		b.WriteByte(1)
		if n, ok := p.IdType.(*pb.Key_PathElement_Name); ok {
			b.WriteString("n" + n.Name)
		} else {
			b.WriteString("i" + strconv.FormatInt(p.GetId(), 10))
		}
	}
	return b.String()
}

// complete reports whether the last path element has an id or a name.
func complete(k *pb.Key) bool {
	p := k.GetPath()
	return len(p) > 0 && p[len(p)-1].IdType != nil
}

// compareKeys orders keys by path, element by element: kind, then ids before names.
func compareKeys(a, b *pb.Key) int {
	if c := cmp.Compare(a.GetPartitionId().GetNamespaceId(), b.GetPartitionId().GetNamespaceId()); c != 0 {
		return c
	}
	ap, bp := a.GetPath(), b.GetPath()
	for i := 0; i < len(ap) && i < len(bp); i++ {
		if c := cmp.Compare(ap[i].Kind, bp[i].Kind); c != 0 {
			return c
		}
		an, aname := ap[i].IdType.(*pb.Key_PathElement_Name)
		bn, bname := bp[i].IdType.(*pb.Key_PathElement_Name)
		switch {
		case aname && bname:
			if c := cmp.Compare(an.Name, bn.Name); c != 0 {
				return c
			}
		case aname != bname:
			if aname {
				return 1 // ids sort before names
			}
			return -1
		default:
			if c := cmp.Compare(ap[i].GetId(), bp[i].GetId()); c != 0 {
				return c
			}
		}
	}
	return cmp.Compare(len(ap), len(bp))
}

// hasAncestor reports whether anc is k or one of its ancestors.
func hasAncestor(k, anc *pb.Key) bool {
	kp, ap := k.GetPath(), anc.GetPath()
	if k.GetPartitionId().GetNamespaceId() != anc.GetPartitionId().GetNamespaceId() || len(ap) > len(kp) {
		return false
	}
	for i := range ap {
		if !proto.Equal(kp[i], ap[i]) {
			return false
		}
	}
	return true
}

// ---- values ----

// rank orders the types of values among themselves, as Datastore does:
// null, booleans, numbers (integers and doubles together), timestamps,
// strings, blobs, geo points, keys, entities.
func rank(v *pb.Value) int {
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
	case *pb.Value_BlobValue:
		return 5
	case *pb.Value_GeoPointValue:
		return 6
	case *pb.Value_KeyValue:
		return 7
	case *pb.Value_EntityValue:
		return 8
	}
	return 9 // arrays and the unset
}

func number(v *pb.Value) (f float64, isInt bool, ok bool) {
	switch x := v.GetValueType().(type) {
	case *pb.Value_IntegerValue:
		return float64(x.IntegerValue), true, true
	case *pb.Value_DoubleValue:
		return x.DoubleValue, false, true
	}
	return 0, false, false
}

// compareValues orders two scalar values; values of different types order by rank.
func compareValues(a, b *pb.Value) int {
	if c := cmp.Compare(rank(a), rank(b)); c != 0 {
		return c
	}
	switch x := a.GetValueType().(type) {
	case *pb.Value_BooleanValue:
		return cmp.Compare(b2i(x.BooleanValue), b2i(b.GetBooleanValue()))
	case *pb.Value_IntegerValue, *pb.Value_DoubleValue:
		if ai, ok := a.GetValueType().(*pb.Value_IntegerValue); ok {
			if bi, ok := b.GetValueType().(*pb.Value_IntegerValue); ok {
				return cmp.Compare(ai.IntegerValue, bi.IntegerValue)
			}
		}
		af, _, _ := number(a)
		bf, _, _ := number(b)
		if math.IsNaN(af) || math.IsNaN(bf) {
			return cmp.Compare(b2i(!math.IsNaN(af)), b2i(!math.IsNaN(bf))) // NaN is the smallest
		}
		return cmp.Compare(af, bf)
	case *pb.Value_TimestampValue:
		return a.GetTimestampValue().AsTime().Compare(b.GetTimestampValue().AsTime())
	case *pb.Value_StringValue:
		return strings.Compare(x.StringValue, b.GetStringValue())
	case *pb.Value_BlobValue:
		return bytes.Compare(x.BlobValue, b.GetBlobValue())
	case *pb.Value_GeoPointValue:
		if c := cmp.Compare(x.GeoPointValue.GetLatitude(), b.GetGeoPointValue().GetLatitude()); c != 0 {
			return c
		}
		return cmp.Compare(x.GeoPointValue.GetLongitude(), b.GetGeoPointValue().GetLongitude())
	case *pb.Value_KeyValue:
		return compareKeys(x.KeyValue, b.GetKeyValue())
	case *pb.Value_EntityValue:
		o := proto.MarshalOptions{Deterministic: true}
		ab, _ := o.Marshal(x.EntityValue)
		bb, _ := o.Marshal(b.GetEntityValue())
		return bytes.Compare(ab, bb)
	}
	return 0
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// indexed lists the values a property can be filtered and ordered by: the
// elements of an array, or the value itself, without those excluded from indexes.
func indexed(v *pb.Value) []*pb.Value {
	if v == nil {
		return nil
	}
	if a, ok := v.GetValueType().(*pb.Value_ArrayValue); ok {
		var out []*pb.Value
		for _, e := range a.ArrayValue.Values {
			if !e.ExcludeFromIndexes {
				out = append(out, e)
			}
		}
		return out
	}
	if v.ExcludeFromIndexes {
		return nil
	}
	return []*pb.Value{v}
}

// property finds a property of an entity: by name, or as a dotted path into
// embedded entities ("address.city"); __key__ is the entity's key.
func property(e *pb.Entity, name string) *pb.Value {
	if name == "__key__" {
		return &pb.Value{ValueType: &pb.Value_KeyValue{KeyValue: e.Key}}
	}
	if v, ok := e.Properties[name]; ok {
		return v
	}
	head, rest, nested := strings.Cut(name, ".")
	if !nested {
		return nil
	}
	if sub := e.Properties[head].GetEntityValue(); sub != nil {
		return property(sub, rest)
	}
	return nil
}

// minMax is the element an ordering uses: the smallest ascending, the largest descending.
func minMax(vals []*pb.Value, desc bool) *pb.Value {
	if len(vals) == 0 {
		return nil
	}
	best := vals[0]
	for _, v := range vals[1:] {
		if c := compareValues(v, best); desc && c > 0 || !desc && c < 0 {
			best = v
		}
	}
	return best
}

func equalAny(vals []*pb.Value, want ...*pb.Value) bool {
	for _, v := range vals {
		for _, w := range want {
			if rank(v) == rank(w) && compareValues(v, w) == 0 {
				return true
			}
		}
	}
	return false
}

// holds reports whether an entity's property satisfies a property filter.
// Arrays match when any element does.
func holds(e *pb.Entity, f *pb.PropertyFilter) bool {
	name := f.GetProperty().GetName()
	if f.Op == pb.PropertyFilter_HAS_ANCESTOR {
		return hasAncestor(e.Key, f.Value.GetKeyValue())
	}
	vals := indexed(property(e, name))
	if len(vals) == 0 {
		return false // not in the index
	}
	want := f.Value
	switch f.Op {
	case pb.PropertyFilter_EQUAL:
		return equalAny(vals, want)
	case pb.PropertyFilter_IN:
		return equalAny(vals, want.GetArrayValue().GetValues()...)
	case pb.PropertyFilter_NOT_EQUAL:
		return slices.ContainsFunc(vals, func(v *pb.Value) bool { return !equalAny([]*pb.Value{v}, want) })
	case pb.PropertyFilter_NOT_IN:
		return !equalAny(vals, want.GetArrayValue().GetValues()...)
	}
	for _, v := range vals {
		if rank(v) != rank(want) {
			continue // inequalities compare values of one type
		}
		c := compareValues(v, want)
		if f.Op == pb.PropertyFilter_LESS_THAN && c < 0 || f.Op == pb.PropertyFilter_LESS_THAN_OR_EQUAL && c <= 0 ||
			f.Op == pb.PropertyFilter_GREATER_THAN && c > 0 || f.Op == pb.PropertyFilter_GREATER_THAN_OR_EQUAL && c >= 0 {
			return true
		}
	}
	return false
}

func matches(e *pb.Entity, f *pb.Filter) bool {
	switch t := f.GetFilterType().(type) {
	case *pb.Filter_PropertyFilter:
		return holds(e, t.PropertyFilter)
	case *pb.Filter_CompositeFilter:
		or := t.CompositeFilter.Op == pb.CompositeFilter_OR
		for _, sub := range t.CompositeFilter.Filters {
			if m := matches(e, sub); m == or {
				return or
			}
		}
		return !or
	}
	return true
}
