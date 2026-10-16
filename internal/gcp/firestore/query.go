package firestore

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// order is one sort key of a query.
type order struct {
	path []string
	desc bool
}

type hit struct {
	doc  *pb.Document
	keys []*pb.Value
}

// query runs a structured query at instant t and returns the matching
// documents (unprojected), recording them as reads of tx. Callers hold s.mu.
// ponytail: scans every document of the database; add per-collection indexes if it gets slow.
func (s *Service) query(db, parent string, q *pb.StructuredQuery, t time.Time, tx *txn) ([]*pb.Document, error) {
	if len(q.GetFrom()) != 1 {
		return nil, invalid("a query must have exactly one collection selector")
	}
	if q.FindNearest != nil {
		return nil, status.Error(codes.Unimplemented, "vector search is not supported by the emulator")
	}
	if q.Offset < 0 || q.GetLimit().GetValue() < 0 {
		return nil, invalid("offset and limit must not be negative")
	}
	where, err := compile(q.Where)
	if err != nil {
		return nil, err
	}
	orders := normalize(q)
	if len(q.GetStartAt().GetValues()) > len(orders) || len(q.GetEndAt().GetValues()) > len(orders) {
		return nil, invalid("too many cursor values: the query orders by %d fields", len(orders))
	}
	from, prefix := q.From[0], parent+"/"
	var hits []hit
	for name, revs := range s.dbs[db] {
		rest, ok := strings.CutPrefix(name, prefix)
		seg := strings.Split(rest, "/")
		if !ok || !from.AllDescendants && len(seg) != 2 || from.CollectionId != "" && seg[len(seg)-2] != from.CollectionId {
			continue
		}
		if d := at(revs, t); d != nil && where(d) {
			if k, ok := keys(d, orders); ok {
				hits = append(hits, hit{d, k})
			}
		}
	}
	slices.SortFunc(hits, func(a, b hit) int { return cmpKeys(a.keys, b.keys, orders) })
	var docs []*pb.Document
	for _, h := range hits {
		if c := q.StartAt; len(c.GetValues()) > 0 {
			if x := cmpKeys(h.keys, c.Values, orders); x < 0 || x == 0 && !c.Before {
				continue
			}
		}
		if c := q.EndAt; len(c.GetValues()) > 0 {
			if x := cmpKeys(h.keys, c.Values, orders); x > 0 || x == 0 && c.Before {
				continue
			}
		}
		docs = append(docs, h.doc)
	}
	docs = docs[min(int(q.Offset), len(docs)):]
	if l := q.GetLimit(); l != nil && int(l.Value) < len(docs) {
		docs = docs[:l.Value]
	}
	for _, d := range docs {
		tx.saw(d.Name, d)
	}
	return docs, nil
}

// normalize returns the sort keys: the explicit orders, then inequality fields
// not ordered yet (lexicographically), then __name__; the implicit ones take
// the direction of the last explicit order.
func normalize(q *pb.StructuredQuery) []order {
	var out []order
	seen := map[string]bool{}
	add := func(path []string, desc bool) {
		if k := strings.Join(path, "\x00"); !seen[k] {
			seen[k] = true
			out = append(out, order{path, desc})
		}
	}
	for _, o := range q.OrderBy {
		add(splitPath(o.GetField().GetFieldPath()), o.Direction == pb.StructuredQuery_DESCENDING)
	}
	desc := len(out) > 0 && out[len(out)-1].desc
	ineq := inequalities(q.Where)
	slices.SortFunc(ineq, slices.Compare[[]string])
	for _, p := range ineq {
		add(p, desc)
	}
	add([]string{"__name__"}, desc)
	return out
}

// inequalities lists the fields with inequality filters (!= and not-in included).
func inequalities(f *pb.StructuredQuery_Filter) [][]string {
	var path string
	switch x := f.GetFilterType().(type) {
	case *pb.StructuredQuery_Filter_CompositeFilter:
		var out [][]string
		for _, sub := range x.CompositeFilter.Filters {
			out = append(out, inequalities(sub)...)
		}
		return out
	case *pb.StructuredQuery_Filter_FieldFilter:
		switch x.FieldFilter.Op {
		case pb.StructuredQuery_FieldFilter_LESS_THAN, pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
			pb.StructuredQuery_FieldFilter_GREATER_THAN, pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL,
			pb.StructuredQuery_FieldFilter_NOT_EQUAL, pb.StructuredQuery_FieldFilter_NOT_IN:
			path = x.FieldFilter.GetField().GetFieldPath()
		}
	case *pb.StructuredQuery_Filter_UnaryFilter:
		if op := x.UnaryFilter.Op; op == pb.StructuredQuery_UnaryFilter_IS_NOT_NAN || op == pb.StructuredQuery_UnaryFilter_IS_NOT_NULL {
			path = x.UnaryFilter.GetField().GetFieldPath()
		}
	}
	if path == "" || path == "__name__" {
		return nil
	}
	return [][]string{splitPath(path)}
}

// keys returns d's sort keys; false when d lacks an ordered field (ordering implies existence).
func keys(d *pb.Document, orders []order) ([]*pb.Value, bool) {
	k := make([]*pb.Value, len(orders))
	for i, o := range orders {
		v, ok := field(d, o.path)
		if !ok {
			return nil, false
		}
		k[i] = v
	}
	return k, true
}

// cmpKeys compares sort keys over the shorter list (cursors may be partial).
func cmpKeys(a, b []*pb.Value, orders []order) int {
	for i := range min(len(a), len(b)) {
		c := compare(a[i], b[i])
		if orders[i].desc {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return 0
}

// compile turns a filter into a predicate, rejecting invalid filters up front.
// Field filters only match documents that have the field; range filters only
// match values of the filter value's type.
func compile(f *pb.StructuredQuery_Filter) (func(*pb.Document) bool, error) {
	switch x := f.GetFilterType().(type) {
	case nil:
		return func(*pb.Document) bool { return true }, nil
	case *pb.StructuredQuery_Filter_CompositeFilter:
		or := x.CompositeFilter.Op == pb.StructuredQuery_CompositeFilter_OR
		if !or && x.CompositeFilter.Op != pb.StructuredQuery_CompositeFilter_AND {
			return nil, invalid("unknown composite filter operator %s", x.CompositeFilter.Op)
		}
		var subs []func(*pb.Document) bool
		for _, sf := range x.CompositeFilter.Filters {
			p, err := compile(sf)
			if err != nil {
				return nil, err
			}
			subs = append(subs, p)
		}
		return func(d *pb.Document) bool {
			for _, p := range subs {
				if p(d) == or {
					return or
				}
			}
			return !or
		}, nil
	case *pb.StructuredQuery_Filter_UnaryFilter:
		op, path := x.UnaryFilter.Op, splitPath(x.UnaryFilter.GetField().GetFieldPath())
		if op < pb.StructuredQuery_UnaryFilter_IS_NAN || op > pb.StructuredQuery_UnaryFilter_IS_NOT_NULL {
			return nil, invalid("unknown unary filter operator %s", op)
		}
		return func(d *pb.Document) bool {
			v, ok := field(d, path)
			nan, isNull := isNaN(v), typeOrder(v) == 0
			switch op {
			case pb.StructuredQuery_UnaryFilter_IS_NAN:
				return ok && nan
			case pb.StructuredQuery_UnaryFilter_IS_NULL:
				return ok && isNull
			case pb.StructuredQuery_UnaryFilter_IS_NOT_NAN:
				return ok && !nan && !isNull
			}
			return ok && !isNull
		}, nil
	case *pb.StructuredQuery_Filter_FieldFilter:
		op, want, path := x.FieldFilter.Op, x.FieldFilter.Value, splitPath(x.FieldFilter.GetField().GetFieldPath())
		list := want.GetArrayValue().GetValues()
		switch op {
		case pb.StructuredQuery_FieldFilter_IN, pb.StructuredQuery_FieldFilter_NOT_IN, pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY:
			if len(list) == 0 {
				return nil, invalid("%s requires a non-empty array value", op)
			}
		case pb.StructuredQuery_FieldFilter_OPERATOR_UNSPECIFIED:
			return nil, invalid("unknown field filter operator %s", op)
		}
		if op > pb.StructuredQuery_FieldFilter_NOT_IN {
			return nil, invalid("unknown field filter operator %s", op)
		}
		return func(d *pb.Document) bool {
			v, ok := field(d, path)
			if !ok {
				return false
			}
			c, sameType := compare(v, want), typeOrder(v) == typeOrder(want)
			switch op {
			case pb.StructuredQuery_FieldFilter_LESS_THAN:
				return sameType && c < 0
			case pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:
				return sameType && c <= 0
			case pb.StructuredQuery_FieldFilter_GREATER_THAN:
				return sameType && c > 0
			case pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
				return sameType && c >= 0
			case pb.StructuredQuery_FieldFilter_EQUAL:
				return c == 0
			case pb.StructuredQuery_FieldFilter_NOT_EQUAL:
				return typeOrder(v) != 0 && c != 0
			case pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS:
				return contains(v.GetArrayValue().GetValues(), want)
			case pb.StructuredQuery_FieldFilter_IN:
				return contains(list, v)
			case pb.StructuredQuery_FieldFilter_NOT_IN:
				return typeOrder(v) != 0 && !contains(list, v) && !contains(list, null)
			case pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY:
				return slices.ContainsFunc(v.GetArrayValue().GetValues(), func(e *pb.Value) bool { return contains(list, e) })
			}
			return false
		}, nil
	}
	return nil, invalid("unknown filter type")
}

// sel applies a query's projection.
func sel(d *pb.Document, p *pb.StructuredQuery_Projection) *pb.Document {
	if p == nil {
		return d
	}
	var paths []string
	for _, f := range p.Fields {
		paths = append(paths, f.GetFieldPath())
	}
	return project(d, paths)
}

// page cuts a listing at an offset page token.
func page[T any](items []T, size int32, token string) ([]T, string) {
	off, _ := strconv.Atoi(token)
	off = max(0, min(off, len(items)))
	items = items[off:]
	if size > 0 && int(size) < len(items) {
		return items[:size], strconv.Itoa(off + int(size))
	}
	return items, ""
}

// ---- gRPC: queries and listings ----

func (s server) RunQuery(req *pb.RunQueryRequest, stream pb.Firestore_RunQueryServer) error {
	out, err := locked(s.Service, func() ([]*pb.RunQueryResponse, error) {
		db, err := dbOf(req.Parent, false)
		if err != nil {
			return nil, err
		}
		t, tx, id, err := s.snapshot(db, req.GetTransaction(), req.GetNewTransaction(), req.GetReadTime())
		if err != nil {
			return nil, err
		}
		q := req.GetStructuredQuery()
		docs, err := s.query(db, req.Parent, q, t, tx)
		if err != nil {
			return nil, err
		}
		rt := timestamppb.New(t)
		out := []*pb.RunQueryResponse{{Transaction: id, ReadTime: rt}} // also the answer to an empty result
		for _, d := range docs {
			out = append(out, &pb.RunQueryResponse{Document: sel(d, q.Select), ReadTime: rt})
		}
		return out, nil
	})
	if err != nil {
		return err
	}
	return sendAll(stream.Send, out)
}

func (s server) RunAggregationQuery(req *pb.RunAggregationQueryRequest, stream pb.Firestore_RunAggregationQueryServer) error {
	out, err := locked(s.Service, func() (*pb.RunAggregationQueryResponse, error) {
		db, err := dbOf(req.Parent, false)
		if err != nil {
			return nil, err
		}
		t, tx, id, err := s.snapshot(db, req.GetTransaction(), req.GetNewTransaction(), req.GetReadTime())
		if err != nil {
			return nil, err
		}
		aq := req.GetStructuredAggregationQuery()
		docs, err := s.query(db, req.Parent, aq.GetStructuredQuery(), t, tx)
		if err != nil {
			return nil, err
		}
		fields := map[string]*pb.Value{}
		for i, a := range aq.GetAggregations() {
			alias := cmp.Or(a.Alias, fmt.Sprintf("field_%d", i+1))
			switch op := a.Operator.(type) {
			case *pb.StructuredAggregationQuery_Aggregation_Count_:
				n := int64(len(docs))
				if up := op.Count.GetUpTo(); up != nil {
					n = min(n, up.Value)
				}
				fields[alias] = intValue(n)
			case *pb.StructuredAggregationQuery_Aggregation_Sum_:
				fields[alias] = aggregate(docs, op.Sum.GetField().GetFieldPath(), false)
			case *pb.StructuredAggregationQuery_Aggregation_Avg_:
				fields[alias] = aggregate(docs, op.Avg.GetField().GetFieldPath(), true)
			default:
				return nil, invalid("unknown aggregation %q", alias)
			}
		}
		return &pb.RunAggregationQueryResponse{Result: &pb.AggregationResult{AggregateFields: fields}, Transaction: id, ReadTime: timestamppb.New(t)}, nil
	})
	if err != nil {
		return err
	}
	return stream.Send(out)
}

// aggregate sums or averages a field's numbers: a sum stays an integer until
// a double or an overflow shows up; the average of nothing is null.
func aggregate(docs []*pb.Document, path string, avg bool) *pb.Value {
	sp := splitPath(path)
	var i int64
	var f float64
	n, dbl := 0, false
	for _, d := range docs {
		v, _ := get(d.Fields, sp)
		switch x := v.GetValueType().(type) {
		case *pb.Value_IntegerValue:
			n, f = n+1, f+float64(x.IntegerValue)
			if s := i + x.IntegerValue; (s > i) != (x.IntegerValue > 0) {
				dbl = true
			} else {
				i = s
			}
		case *pb.Value_DoubleValue:
			n, f, dbl = n+1, f+x.DoubleValue, true
		}
	}
	switch {
	case avg && n == 0:
		return null
	case avg:
		return doubleValue(f / float64(n))
	case dbl:
		return doubleValue(f)
	}
	return intValue(i)
}

// PartitionQuery returns no split points: the whole query is one partition.
func (s server) PartitionQuery(context.Context, *pb.PartitionQueryRequest) (*pb.PartitionQueryResponse, error) {
	return &pb.PartitionQueryResponse{}, nil
}

func (s server) ListDocuments(_ context.Context, req *pb.ListDocumentsRequest) (*pb.ListDocumentsResponse, error) {
	return locked(s.Service, func() (*pb.ListDocumentsResponse, error) {
		db, err := dbOf(req.Parent, false)
		if err != nil {
			return nil, err
		}
		t, tx, _, err := s.snapshot(db, req.GetTransaction(), nil, req.GetReadTime())
		if err != nil {
			return nil, err
		}
		q := &pb.StructuredQuery{From: []*pb.StructuredQuery_CollectionSelector{{CollectionId: req.CollectionId}}}
		for _, o := range strings.Split(req.OrderBy, ",") { // "field [desc], ..."
			if f := strings.Fields(o); len(f) > 0 {
				dir := pb.StructuredQuery_ASCENDING
				if len(f) > 1 && strings.EqualFold(f[1], "desc") {
					dir = pb.StructuredQuery_DESCENDING
				}
				q.OrderBy = append(q.OrderBy, &pb.StructuredQuery_Order{Field: &pb.StructuredQuery_FieldReference{FieldPath: f[0]}, Direction: dir})
			}
		}
		docs, err := s.query(db, req.Parent, q, t, tx)
		if err != nil {
			return nil, err
		}
		if req.ShowMissing {
			docs = append(docs, s.missing(db, req.Parent, req.CollectionId, t)...)
			slices.SortFunc(docs, func(a, b *pb.Document) int { return compare(refValue(a.Name), refValue(b.Name)) })
		}
		resp := &pb.ListDocumentsResponse{}
		docs, resp.NextPageToken = page(docs, req.PageSize, req.PageToken)
		for _, d := range docs {
			resp.Documents = append(resp.Documents, mask(d, req.Mask))
		}
		return resp, nil
	})
}

// missing lists documents that do not exist but have subcollections (show_missing).
func (s *Service) missing(db, parent, coll string, t time.Time) []*pb.Document {
	var out []*pb.Document
	seen := map[string]bool{}
	for name, revs := range s.dbs[db] {
		rest, ok := strings.CutPrefix(name, parent+"/")
		p := strings.Split(rest, "/")
		if !ok || len(p) <= 2 || coll != "" && p[0] != coll || at(revs, t) == nil {
			continue
		}
		if doc := parent + "/" + p[0] + "/" + p[1]; !seen[doc] && at(s.dbs[db][doc], t) == nil {
			seen[doc] = true
			out = append(out, &pb.Document{Name: doc})
		}
	}
	return out
}

func (s server) ListCollectionIds(_ context.Context, req *pb.ListCollectionIdsRequest) (*pb.ListCollectionIdsResponse, error) {
	return locked(s.Service, func() (*pb.ListCollectionIdsResponse, error) {
		db, err := dbOf(req.Parent, false)
		if err != nil {
			return nil, err
		}
		t, _, _, _ := s.snapshot(db, nil, nil, req.GetReadTime())
		ids := map[string]bool{}
		for name, revs := range s.dbs[db] {
			if rest, ok := strings.CutPrefix(name, req.Parent+"/"); ok && at(revs, t) != nil {
				id, _, _ := strings.Cut(rest, "/")
				ids[id] = true
			}
		}
		resp := &pb.ListCollectionIdsResponse{}
		resp.CollectionIds, resp.NextPageToken = page(slices.Sorted(maps.Keys(ids)), req.PageSize, req.PageToken)
		return resp, nil
	})
}
