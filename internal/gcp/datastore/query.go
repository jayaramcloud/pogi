package datastore

import (
	"cmp"
	"slices"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// row is one result of a query: an entity, the values it is ordered by (one
// per order, then its key) and, for projections, the projected values.
type row struct {
	ent   *ent
	order []*pb.Value
	proj  map[string]*pb.Value
}

// ordering is the sort of a query: its orders and a trailing key order.
type ordering []*pb.PropertyOrder

func (o ordering) desc(i int) bool {
	return i < len(o) && o[i].Direction == pb.PropertyOrder_DESCENDING
}

// compare orders two rows, then by key as the query does.
func (o ordering) compare(a, b *row) int {
	for i := range o {
		if c := compareValues(a.order[i], b.order[i]); c != 0 {
			if o.desc(i) {
				return -c
			}
			return c
		}
	}
	return compareKeys(a.ent.e.Key, b.ent.e.Key)
}

// cursor encodes the position after a row: its order values and key.
func (o ordering) cursor(r *row, sig string) []byte {
	e := &pb.Entity{Key: r.ent.e.Key, Properties: map[string]*pb.Value{"sig": {ValueType: &pb.Value_StringValue{StringValue: sig}}}}
	for i, v := range r.order {
		e.Properties[string(rune('a'+i))] = v
	}
	b, _ := proto.Marshal(e)
	return b
}

func (o ordering) fromCursor(b []byte, sig string) (*row, error) {
	e := &pb.Entity{}
	if err := proto.Unmarshal(b, e); err != nil || e.Key == nil || len(e.Properties) != len(o)+1 || e.Properties["sig"].GetStringValue() != sig {
		return nil, status.Error(codes.InvalidArgument, "The cursor is not valid for this query.")
	}
	r := &row{ent: &ent{e: &pb.Entity{Key: e.Key}}}
	for i := range o {
		r.order = append(r.order, e.Properties[string(rune('a'+i))])
	}
	return r, nil
}

// result is what a query returned.
type result struct {
	rows     []*row
	skipped  int32
	typ      pb.EntityResult_ResultType
	more     pb.QueryResultBatch_MoreResultsType
	end      []byte
	skipCur  []byte
	cursors  [][]byte
	entities []*pb.Entity
}

// run evaluates a query over the entities of a namespace, in a database.
// ponytail: a scan of the namespace, no indexes (so no index errors, and
// queries cost what the namespace holds); a projection of several array
// properties is their product; a dotted property path goes into embedded
// entities only.
func (d *db) run(ns string, q *pb.Query) (*result, error) {
	if len(q.Kind) > 1 {
		return nil, status.Error(codes.InvalidArgument, "Only one kind may be queried at a time.")
	}
	if q.Offset < 0 || q.Limit != nil && q.Limit.Value < 0 {
		return nil, status.Error(codes.InvalidArgument, "The offset and limit must not be negative.")
	}
	var kind string
	if len(q.Kind) == 1 {
		kind = q.Kind[0].Name
	}
	ord := ordering(q.Order)
	sig := kind + "\x00" // a cursor only continues the query it came from: same kind and orders
	for _, o := range q.Order {
		sig += o.Property.Name + "\x00" + o.Direction.String() + "\x00"
	}
	keysOnly := len(q.Projection) > 0
	var props []string
	for _, p := range q.Projection {
		if p.Property.Name != "__key__" {
			props = append(props, p.Property.Name)
		}
		keysOnly = keysOnly && p.Property.Name == "__key__"
	}
	var rows []*row
scan:
	for _, e := range d.ents {
		k := e.e.Key
		if k.PartitionId.GetNamespaceId() != ns || kind != "" && k.Path[len(k.Path)-1].Kind != kind || q.Filter != nil && !matches(e.e, q.Filter) {
			continue
		}
		r := &row{ent: e}
		for _, o := range ord {
			v := minMax(indexed(property(e.e, o.Property.Name)), o.Direction == pb.PropertyOrder_DESCENDING)
			if v == nil {
				continue scan // an entity without the property is not in its index
			}
			r.order = append(r.order, v)
		}
		if len(props) == 0 {
			rows = append(rows, r)
			continue
		}
		rows = append(rows, project(r, props)...) // none when a property is missing
	}
	slices.SortStableFunc(rows, ord.compare)
	if len(q.DistinctOn) > 0 {
		seen := map[string]bool{}
		rows = slices.DeleteFunc(rows, func(r *row) bool {
			var key []byte
			for _, p := range q.DistinctOn {
				v := r.proj[p.Name]
				if v == nil {
					v = minMax(indexed(property(r.ent.e, p.Name)), false)
				}
				b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(v)
				key = append(key, byte(len(b)), byte(len(b)>>8))
				key = append(key, b...)
			}
			dup := seen[string(key)]
			seen[string(key)] = true
			return dup
		})
	}

	res := &result{typ: pb.EntityResult_FULL, more: pb.QueryResultBatch_NO_MORE_RESULTS}
	switch {
	case keysOnly:
		res.typ = pb.EntityResult_KEY_ONLY
	case len(props) > 0:
		res.typ = pb.EntityResult_PROJECTION
	}
	if len(q.StartCursor) > 0 {
		c, err := ord.fromCursor(q.StartCursor, sig)
		if err != nil {
			return nil, err
		}
		rows = slices.DeleteFunc(rows, func(r *row) bool { return ord.compare(r, c) <= 0 })
	}
	if len(q.EndCursor) > 0 {
		c, err := ord.fromCursor(q.EndCursor, sig)
		if err != nil {
			return nil, err
		}
		n := len(rows)
		rows = slices.DeleteFunc(rows, func(r *row) bool { return ord.compare(r, c) > 0 })
		if len(rows) < n {
			res.more = pb.QueryResultBatch_MORE_RESULTS_AFTER_CURSOR
		}
	}
	if skip := min(int(q.Offset), len(rows)); skip > 0 {
		res.skipped = int32(skip)
		res.skipCur = ord.cursor(rows[skip-1], sig)
		rows = rows[skip:]
	}
	if q.Limit != nil && int(q.Limit.Value) <= len(rows) {
		rows = rows[:q.Limit.Value]
		res.more = pb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT
	}
	res.rows = rows
	for _, r := range rows {
		res.cursors = append(res.cursors, ord.cursor(r, sig))
		res.entities = append(res.entities, r.output(res.typ))
	}
	switch {
	case len(rows) > 0:
		res.end = res.cursors[len(rows)-1]
	case res.skipCur != nil:
		res.end = res.skipCur
	default:
		res.end = q.StartCursor
	}
	return res, nil
}

// project expands a row into one row per combination of the values of the
// projected properties: an entity with an array there gives one per element.
func project(r *row, props []string) []*row {
	out := []*row{{ent: r.ent, order: r.order, proj: map[string]*pb.Value{}}}
	for _, p := range props {
		vals := indexed(property(r.ent.e, p))
		var next []*row
		for _, o := range out {
			for _, v := range vals {
				m := map[string]*pb.Value{p: v}
				for k, x := range o.proj {
					m[k] = x
				}
				next = append(next, &row{ent: o.ent, order: o.order, proj: m})
			}
		}
		out = next
	}
	return out
}

// output is the entity a row returns: all of it, its key, or the projected properties.
func (r *row) output(t pb.EntityResult_ResultType) *pb.Entity {
	switch t {
	case pb.EntityResult_KEY_ONLY:
		return &pb.Entity{Key: r.ent.e.Key}
	case pb.EntityResult_PROJECTION:
		e := &pb.Entity{Key: r.ent.e.Key, Properties: map[string]*pb.Value{}}
		for k, v := range r.proj {
			e.Properties[k] = &pb.Value{ValueType: v.ValueType, Meaning: v.Meaning}
		}
		return e
	}
	return r.ent.e
}

// aggregate runs an aggregation query over its nested query.
func (d *db) aggregate(ns string, q *pb.AggregationQuery) (*pb.AggregationResult, error) {
	nested := q.GetNestedQuery()
	if nested == nil {
		return nil, status.Error(codes.InvalidArgument, "An aggregation query needs a nested query.")
	}
	if len(q.Aggregations) == 0 || len(q.Aggregations) > 5 {
		return nil, status.Error(codes.InvalidArgument, "An aggregation query has one to five aggregations.")
	}
	base := proto.Clone(nested).(*pb.Query)
	base.Projection = nil
	res, err := d.run(ns, base)
	if err != nil {
		return nil, err
	}
	out := &pb.AggregationResult{AggregateProperties: map[string]*pb.Value{}}
	for i, a := range q.Aggregations {
		alias := cmp.Or(a.Alias, "property_"+string(rune('1'+i)))
		switch op := a.Operator.(type) {
		case *pb.AggregationQuery_Aggregation_Count_:
			n := int64(len(res.rows))
			if up := op.Count.GetUpTo(); up != nil && up.Value < n {
				n = up.Value
			}
			out.AggregateProperties[alias] = &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: n}}
		case *pb.AggregationQuery_Aggregation_Sum_:
			sum, _, _ := total(res.rows, op.Sum.Property.GetName())
			out.AggregateProperties[alias] = sum
		case *pb.AggregationQuery_Aggregation_Avg_:
			_, n, avg := total(res.rows, op.Avg.Property.GetName())
			if n == 0 {
				avg = &pb.Value{ValueType: &pb.Value_NullValue{}}
			}
			out.AggregateProperties[alias] = avg
		default:
			return nil, status.Error(codes.InvalidArgument, "An aggregation needs count, sum or avg.")
		}
	}
	return out, nil
}

// total sums a numeric property over rows: an integer when all values are
// integers, else a double (the value of an entity without a number is left out), and the mean.
func total(rows []*row, name string) (sum *pb.Value, n int, avg *pb.Value) {
	var is int64
	var fs float64
	allInt := true
	for _, r := range rows {
		f, isInt, ok := number(property(r.ent.e, name))
		if !ok {
			continue
		}
		n++
		fs += f
		if isInt {
			is += property(r.ent.e, name).GetIntegerValue()
		} else {
			allInt = false
		}
	}
	sum = &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: is}}
	if !allInt {
		sum = &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: fs}}
	}
	if n > 0 {
		avg = &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: fs / float64(n)}}
	}
	return sum, n, avg
}
