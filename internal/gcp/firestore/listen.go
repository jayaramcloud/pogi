package firestore

import (
	"io"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/nrndev/pogi/internal/core"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// listener is one Listen stream; wake is signalled after commits to its database.
type listener struct {
	db   string
	wake chan struct{}
}

// target is a query or documents target and the documents last sent for it.
type target struct {
	id    int32
	query *pb.Target_QueryTarget
	names []string
	sent  map[string]*pb.Document
}

// Listen serves realtime listeners. An added target gets ADD, its documents,
// CURRENT and then a NO_CHANGE without target ids carrying the snapshot's
// read time (the consistency marker clients wait for). After every commit the
// targets are re-run and the differences are sent, again followed by NO_CHANGE.
// A resumed target gets RESET and its full state instead of a delta.
func (s server) Listen(stream pb.Firestore_ListenServer) error {
	ctx := stream.Context()
	l := &listener{wake: make(chan struct{}, 1)}
	defer func() {
		s.mu.Lock()
		delete(s.subs, l)
		s.mu.Unlock()
	}()
	reqs, errc := make(chan *pb.ListenRequest), make(chan error, 1)
	core.Go("firestore.Listen", func() {
		defer func() {
			if r := recover(); r != nil {
				errc <- status.Error(codes.Internal, "internal error") // end the stream, then let core.Go log it
				panic(r)
			}
		}()
		for {
			r, err := stream.Recv()
			if err != nil {
				errc <- err
				return
			}
			select {
			case reqs <- r:
			case <-ctx.Done():
				return
			}
		}
	})
	targets := map[int32]*target{}
	for {
		var out []*pb.ListenResponse
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case err := <-errc:
			if err == io.EOF {
				return nil
			}
			return err
		case r := <-reqs:
			out = s.listen(l, targets, r)
		case <-l.wake:
			out = s.changes(l, targets)
		}
		if err := sendAll(stream.Send, out); err != nil {
			return err
		}
	}
}

// listen handles an add_target or remove_target request.
func (s *Service) listen(l *listener, targets map[int32]*target, r *pb.ListenRequest) []*pb.ListenResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.db == "" {
		l.db = r.Database
		s.subs[l] = true
	}
	if rm, ok := r.TargetChange.(*pb.ListenRequest_RemoveTarget); ok {
		delete(targets, rm.RemoveTarget)
		return []*pb.ListenResponse{targetChange(pb.TargetChange_REMOVE, time.Time{}, rm.RemoveTarget)}
	}
	add := r.GetAddTarget()
	if add == nil {
		return nil
	}
	t := &target{id: add.TargetId, query: add.GetQuery(), names: add.GetDocuments().GetDocuments(), sent: map[string]*pb.Document{}}
	if t.id == 0 { // the server picks the id
		for t.id = 1; targets[t.id] != nil; t.id++ {
		}
	}
	out := []*pb.ListenResponse{targetChange(pb.TargetChange_ADD, time.Time{}, t.id)}
	if add.ResumeType != nil { // no change log to resume from: resend everything
		out = append(out, targetChange(pb.TargetChange_RESET, time.Time{}, t.id))
	}
	var docs []*pb.ListenResponse
	err := t.check(l.db)
	if err == nil {
		docs, err = s.diff(l.db, t)
	}
	if err != nil {
		rm := targetChange(pb.TargetChange_REMOVE, time.Time{}, t.id)
		rm.GetTargetChange().Cause = status.Convert(err).Proto()
		return append(out, rm)
	}
	targets[t.id] = t
	now := s.tick()
	out = append(append(out, docs...), targetChange(pb.TargetChange_CURRENT, now, t.id))
	// The NO_CHANGE below vouches for every target on the stream: catch the others up first.
	return append(append(out, s.sync(l.db, targets)...), targetChange(pb.TargetChange_NO_CHANGE, now))
}

// check validates that a target's names belong to db.
func (t *target) check(db string) error {
	names, doc := t.names, true
	if t.query != nil {
		names, doc = []string{t.query.Parent}, false
	}
	for _, n := range names {
		if d, err := dbOf(n, doc); err != nil {
			return err
		} else if d != db {
			return invalid("%s is not in database %s", n, db)
		}
	}
	return nil
}

// changes re-runs every target after a commit and returns what changed,
// closed by a NO_CHANGE marking the new consistent snapshot.
func (s *Service) changes(l *listener, targets map[int32]*target) []*pb.ListenResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	if out := s.sync(l.db, targets); len(out) > 0 {
		return append(out, targetChange(pb.TargetChange_NO_CHANGE, s.tick()))
	}
	return nil
}

// sync returns the document changes of every target since its last snapshot.
func (s *Service) sync(db string, targets map[int32]*target) []*pb.ListenResponse {
	var out []*pb.ListenResponse
	for _, t := range targets {
		docs, _ := s.diff(db, t) // the target ran fine when added
		out = append(out, docs...)
	}
	return out
}

// diff re-runs a target and returns the document changes since the last
// snapshot sent for it.
// ponytail: re-runs whole queries per commit; match only the changed documents if listeners get slow.
func (s *Service) diff(db string, t *target) ([]*pb.ListenResponse, error) {
	now := map[string]*pb.Document{}
	if q := t.query; q != nil {
		docs, err := s.query(db, q.Parent, q.GetStructuredQuery(), s.last, nil)
		if err != nil {
			return nil, err
		}
		for _, d := range docs {
			now[d.Name] = sel(d, q.GetStructuredQuery().GetSelect())
		}
	}
	for _, n := range t.names {
		if d := at(s.dbs[db][n], s.last); d != nil {
			now[n] = d
		}
	}
	var out []*pb.ListenResponse
	ids := []int32{t.id}
	for n, d := range now {
		if old := t.sent[n]; old == nil || !proto.Equal(old.UpdateTime, d.UpdateTime) {
			out = append(out, &pb.ListenResponse{ResponseType: &pb.ListenResponse_DocumentChange{
				DocumentChange: &pb.DocumentChange{Document: d, TargetIds: ids}}})
		}
	}
	for n := range t.sent {
		switch {
		case now[n] != nil:
		case at(s.dbs[db][n], s.last) == nil:
			out = append(out, &pb.ListenResponse{ResponseType: &pb.ListenResponse_DocumentDelete{
				DocumentDelete: &pb.DocumentDelete{Document: n, RemovedTargetIds: ids}}})
		default: // still exists, no longer matches
			out = append(out, &pb.ListenResponse{ResponseType: &pb.ListenResponse_DocumentRemove{
				DocumentRemove: &pb.DocumentRemove{Document: n, RemovedTargetIds: ids}}})
		}
	}
	t.sent = now
	return out, nil
}

func targetChange(kind pb.TargetChange_TargetChangeType, at time.Time, ids ...int32) *pb.ListenResponse {
	tc := &pb.TargetChange{TargetChangeType: kind, TargetIds: ids}
	if !at.IsZero() {
		tc.ReadTime, tc.ResumeToken = timestamppb.New(at), []byte(at.Format(time.RFC3339Nano))
	}
	return &pb.ListenResponse{ResponseType: &pb.ListenResponse_TargetChange{TargetChange: tc}}
}

// wake tells the Listen streams on db ("" for all) to re-run their targets.
func (s *Service) wake(db string) {
	for l := range s.subs {
		if db == "" || l.db == db {
			select {
			case l.wake <- struct{}{}:
			default:
			}
		}
	}
}
