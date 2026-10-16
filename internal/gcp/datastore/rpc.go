package datastore

import (
	"context"
	"math"
	"strconv"
	"time"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *Service) Lookup(_ context.Context, req *pb.LookupRequest) (*pb.LookupResponse, error) {
	for _, k := range req.Keys {
		if err := checkKey(k, true); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, name := s.database(req.ProjectId, req.DatabaseId)
	t, newID, err := s.readTxn(name, req.ReadOptions)
	if err != nil {
		return nil, err
	}
	res := &pb.LookupResponse{Transaction: newID, ReadTime: ts(time.Now())}
	for _, k := range req.Keys {
		sk := skey(k)
		e := d.ents[sk]
		if e == nil {
			t.saw(sk, 0)
			res.Missing = append(res.Missing, &pb.EntityResult{Entity: &pb.Entity{Key: qualify(req.ProjectId, req.DatabaseId, k)}, Version: s.last})
			continue
		}
		t.saw(sk, e.version)
		res.Found = append(res.Found, entityResult(e, e.e, nil))
	}
	return res, nil
}

func entityResult(e *ent, out *pb.Entity, cursor []byte) *pb.EntityResult {
	return &pb.EntityResult{Entity: proto.Clone(out).(*pb.Entity), Version: e.version, CreateTime: ts(e.created), UpdateTime: ts(e.updated), Cursor: cursor}
}

func (s *Service) RunQuery(_ context.Context, req *pb.RunQueryRequest) (*pb.RunQueryResponse, error) {
	q := req.GetQuery()
	if q == nil {
		return nil, status.Error(codes.Unimplemented, "pogi runs query messages, not GQL.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, name := s.database(req.ProjectId, req.DatabaseId)
	t, newID, err := s.readTxn(name, req.ReadOptions)
	if err != nil {
		return nil, err
	}
	res, err := d.run(req.GetPartitionId().GetNamespaceId(), q)
	if err != nil {
		return nil, err
	}
	batch := &pb.QueryResultBatch{SkippedResults: res.skipped, SkippedCursor: res.skipCur, EntityResultType: res.typ, EndCursor: res.end,
		MoreResults: res.more, SnapshotVersion: s.last, ReadTime: ts(time.Now())}
	for i, r := range res.rows {
		t.saw(skey(r.ent.e.Key), r.ent.version)
		batch.EntityResults = append(batch.EntityResults, entityResult(r.ent, res.entities[i], res.cursors[i]))
	}
	return &pb.RunQueryResponse{Batch: batch, Query: q, Transaction: newID}, nil
}

func (s *Service) RunAggregationQuery(_ context.Context, req *pb.RunAggregationQueryRequest) (*pb.RunAggregationQueryResponse, error) {
	q := req.GetAggregationQuery()
	if q == nil {
		return nil, status.Error(codes.Unimplemented, "pogi runs aggregation query messages, not GQL.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, name := s.database(req.ProjectId, req.DatabaseId)
	_, newID, err := s.readTxn(name, req.ReadOptions)
	if err != nil {
		return nil, err
	}
	r, err := d.aggregate(req.GetPartitionId().GetNamespaceId(), q)
	if err != nil {
		return nil, err
	}
	return &pb.RunAggregationQueryResponse{Transaction: newID, Query: q, Batch: &pb.AggregationResultBatch{
		AggregationResults: []*pb.AggregationResult{r}, MoreResults: pb.QueryResultBatch_NO_MORE_RESULTS, ReadTime: ts(time.Now())}}, nil
}

func (s *Service) BeginTransaction(_ context.Context, req *pb.BeginTransactionRequest) (*pb.BeginTransactionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, name := s.database(req.ProjectId, req.DatabaseId)
	return &pb.BeginTransactionResponse{Transaction: s.begin(name, req.TransactionOptions)}, nil
}

func (s *Service) Rollback(_ context.Context, req *pb.RollbackRequest) (*pb.RollbackResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, name := s.database(req.ProjectId, req.DatabaseId)
	if t := s.txns[string(req.Transaction)]; t != nil && t.db == name { // one a failed commit ended is already gone: fine
		delete(s.txns, string(req.Transaction))
	}
	return &pb.RollbackResponse{}, nil
}

// allocate gives an incomplete key an id: the next one nothing uses or reserves. Lock held.
func allocate(d *db, k *pb.Key, taken func(string) bool) {
	last := k.Path[len(k.Path)-1]
	for {
		d.next++
		last.IdType = &pb.Key_PathElement_Id{Id: d.next}
		if sk := skey(k); !d.reserved[sk] && !taken(sk) {
			return
		}
	}
}

func (s *Service) AllocateIds(_ context.Context, req *pb.AllocateIdsRequest) (*pb.AllocateIdsResponse, error) {
	for _, k := range req.Keys {
		if err := checkKey(k, false); err != nil {
			return nil, err
		}
		if complete(k) {
			return nil, invalid("Invalid key: only incomplete keys can be given ids.")
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, _ := s.database(req.ProjectId, req.DatabaseId)
	res := &pb.AllocateIdsResponse{}
	for _, k := range req.Keys {
		k = qualify(req.ProjectId, req.DatabaseId, k)
		allocate(d, k, func(sk string) bool { return d.ents[sk] != nil })
		res.Keys = append(res.Keys, k)
	}
	s.dirty()
	return res, nil
}

func (s *Service) ReserveIds(_ context.Context, req *pb.ReserveIdsRequest) (*pb.ReserveIdsResponse, error) {
	for _, k := range req.Keys {
		if err := checkKey(k, true); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, _ := s.database(req.ProjectId, req.DatabaseId)
	for _, k := range req.Keys {
		d.reserved[skey(k)] = true
	}
	s.dirty()
	return &pb.ReserveIdsResponse{}, nil
}

var errContention = status.Error(codes.Aborted, "too much contention on these datastore entities. please try again.")

func (s *Service) Commit(_ context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, name := s.database(req.ProjectId, req.DatabaseId)
	var t *txn
	if id, ok := req.TransactionSelector.(*pb.CommitRequest_Transaction); ok {
		var err error
		if t, err = s.txn(name, id.Transaction); err != nil {
			return nil, err
		}
		defer delete(s.txns, string(id.Transaction)) // committed or not, the transaction is over
	}
	if t != nil {
		if t.readOnly && len(req.Mutations) > 0 {
			return nil, invalid("Cannot modify entities in a read-only transaction.")
		}
		for sk, seen := range t.reads {
			var cur int64
			if e := d.ents[sk]; e != nil {
				cur = e.version
			}
			if cur != seen {
				return nil, errContention
			}
		}
	}
	results, now, err := s.apply(d, req.ProjectId, req.DatabaseId, req.Mutations)
	if err != nil {
		return nil, err
	}
	s.dirty()
	return &pb.CommitResponse{MutationResults: results, IndexUpdates: int32(len(req.Mutations)), CommitTime: ts(now)}, nil
}

// apply runs mutations atomically: they are staged, and stored only if all succeed. Lock held.
func (s *Service) apply(d *db, project, database string, muts []*pb.Mutation) ([]*pb.MutationResult, time.Time, error) {
	staged := map[string]*ent{} // a nil entity is a delete
	get := func(sk string) *ent {
		if e, ok := staged[sk]; ok {
			return e
		}
		return d.ents[sk]
	}
	version, now := s.tick(), time.Now().UTC().Truncate(time.Microsecond)
	results := make([]*pb.MutationResult, len(muts))
	for i, m := range muts {
		r := &pb.MutationResult{Version: version, CreateTime: ts(now), UpdateTime: ts(now)}
		results[i] = r
		var src *pb.Entity
		switch op := m.Operation.(type) {
		case *pb.Mutation_Insert:
			src = op.Insert
		case *pb.Mutation_Update:
			src = op.Update
		case *pb.Mutation_Upsert:
			src = op.Upsert
		case *pb.Mutation_Delete:
			k := qualify(project, database, op.Delete)
			if err := checkKey(k, true); err != nil {
				return nil, now, err
			}
			sk := skey(k)
			if skip, err := conflicts(m, get(sk)); err != nil || skip {
				r.ConflictDetected = skip
				if err != nil {
					return nil, now, err
				}
				continue
			}
			staged[sk] = nil
			continue
		default:
			return nil, now, invalid("A mutation needs an insert, update, upsert or delete.")
		}
		if src == nil {
			return nil, now, invalid("A mutation has no entity.")
		}
		key := qualify(project, database, src.Key)
		_, isUpdate := m.Operation.(*pb.Mutation_Update)
		if err := checkKey(key, isUpdate); err != nil {
			return nil, now, err
		}
		if !complete(key) {
			allocate(d, key, func(sk string) bool { return get(sk) != nil })
			r.Key = key
		}
		sk := skey(key)
		cur := get(sk)
		switch m.Operation.(type) {
		case *pb.Mutation_Insert:
			if cur != nil {
				return nil, now, status.Errorf(codes.AlreadyExists, "entity already exists: %s", keyString(key))
			}
		case *pb.Mutation_Update:
			if cur == nil {
				return nil, now, status.Errorf(codes.NotFound, "no entity to update: %s", keyString(key))
			}
		}
		if skip, err := conflicts(m, cur); err != nil || skip {
			r.ConflictDetected = skip
			if err != nil {
				return nil, now, err
			}
			continue
		}
		props := map[string]*pb.Value{}
		if m.PropertyMask != nil && cur != nil { // only the masked properties change
			for k, v := range cur.e.Properties {
				props[k] = v
			}
		}
		if m.PropertyMask == nil {
			for k, v := range src.Properties {
				props[k] = proto.Clone(v).(*pb.Value)
			}
		} else {
			for _, path := range m.PropertyMask.Paths {
				if v, ok := src.Properties[path]; ok {
					props[path] = proto.Clone(v).(*pb.Value)
				} else {
					delete(props, path)
				}
			}
		}
		for _, t := range m.PropertyTransforms {
			v, err := transform(props, t, now)
			if err != nil {
				return nil, now, err
			}
			props[t.Property] = v
			r.TransformResults = append(r.TransformResults, v)
		}
		created := now
		if cur != nil {
			created = cur.created
			r.CreateTime = ts(created)
		}
		staged[sk] = &ent{e: &pb.Entity{Key: key, Properties: props}, version: version, created: created, updated: now}
	}
	for sk, e := range staged {
		if e == nil {
			delete(d.ents, sk)
		} else {
			d.ents[sk] = e
		}
	}
	return results, now, nil
}

// conflicts checks a mutation's base version or update time against the
// entity: skip means the server's entity is kept, an error that the commit fails.
func conflicts(m *pb.Mutation, cur *ent) (skip bool, err error) {
	var ok bool
	switch c := m.ConflictDetectionStrategy.(type) {
	case *pb.Mutation_BaseVersion:
		ok = cur != nil && cur.version == c.BaseVersion || cur == nil && c.BaseVersion == 0
	case *pb.Mutation_UpdateTime:
		ok = cur != nil && cur.updated.Equal(c.UpdateTime.AsTime())
	default:
		return false, nil
	}
	switch {
	case ok:
		return false, nil
	case m.ConflictResolutionStrategy == pb.Mutation_FAIL:
		return false, errContention
	}
	return true, nil
}

func keyString(k *pb.Key) string {
	s := ""
	for _, p := range k.Path {
		if s != "" {
			s += ", "
		}
		if p.GetName() != "" {
			s += p.Kind + ", \"" + p.GetName() + "\""
		} else {
			s += p.Kind + ", " + strconv.FormatInt(p.GetId(), 10)
		}
	}
	return "Key(" + s + ")"
}

// transform applies a property transform and returns the property's new value.
func transform(props map[string]*pb.Value, t *pb.PropertyTransform, now time.Time) (*pb.Value, error) {
	cur := props[t.Property]
	switch op := t.TransformType.(type) {
	case *pb.PropertyTransform_SetToServerValue:
		return &pb.Value{ValueType: &pb.Value_TimestampValue{TimestampValue: ts(now)}}, nil
	case *pb.PropertyTransform_Increment:
		return arithmetic(cur, op.Increment, func(a, b int64) int64 { return a + b }, func(a, b float64) float64 { return a + b })
	case *pb.PropertyTransform_Maximum:
		return arithmetic(cur, op.Maximum, func(a, b int64) int64 { return max(a, b) }, math.Max)
	case *pb.PropertyTransform_Minimum:
		return arithmetic(cur, op.Minimum, func(a, b int64) int64 { return min(a, b) }, math.Min)
	case *pb.PropertyTransform_AppendMissingElements:
		vals := cur.GetArrayValue().GetValues()
		for _, v := range op.AppendMissingElements.Values {
			if !equalAny(vals, v) {
				vals = append(vals, proto.Clone(v).(*pb.Value))
			}
		}
		return &pb.Value{ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: vals}}}, nil
	case *pb.PropertyTransform_RemoveAllFromArray:
		var vals []*pb.Value
		for _, v := range cur.GetArrayValue().GetValues() {
			if !equalAny([]*pb.Value{v}, op.RemoveAllFromArray.Values...) {
				vals = append(vals, v)
			}
		}
		return &pb.Value{ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: vals}}}, nil
	}
	return nil, invalid("A property transform needs a type.")
}

// arithmetic combines a numeric property with an operand: integers stay
// integers, and anything else (or a missing property, which takes the operand) is a double.
func arithmetic(cur, operand *pb.Value, fi func(a, b int64) int64, ff func(a, b float64) float64) (*pb.Value, error) {
	b, bInt, ok := number(operand)
	if !ok {
		return nil, invalid("A numeric transform needs a number.")
	}
	a, aInt, ok := number(cur)
	if !ok {
		return operand, nil
	}
	if aInt && bInt {
		return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: fi(cur.GetIntegerValue(), operand.GetIntegerValue())}}, nil
	}
	return &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: ff(a, b)}}, nil
}
