package run

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"github.com/nrndev/pogi/internal/gcp/serverless"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	latestType   = runpb.TrafficTargetAllocationType_TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST
	revisionType = runpb.TrafficTargetAllocationType_TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION
	defaultSA    = "pogi-compute@developer.gserviceaccount.com"
)

// defaults fills what Cloud Run fills in a revision template.
func defaults(t *runpb.RevisionTemplate) {
	if t.Timeout == nil {
		t.Timeout = durationpb.New(300 * time.Second)
	}
	if t.MaxInstanceRequestConcurrency == 0 {
		t.MaxInstanceRequestConcurrency = 80
	}
	if t.ServiceAccount == "" {
		t.ServiceAccount = defaultSA
	}
	for _, c := range t.Containers {
		if len(c.Ports) == 0 && len(t.Containers) == 1 {
			c.Ports = []*runpb.ContainerPort{{Name: "http1", ContainerPort: 8080}}
		}
	}
}

func checkContainers(cs []*runpb.Container, field string) error {
	if len(cs) == 0 {
		return gapi.Invalid("Invalid value for %s.containers: at least one container is required", field)
	}
	for i, c := range cs {
		if c.Image == "" {
			return gapi.Invalid("Invalid value for %s.containers[%d].image: image is required", field, i)
		}
	}
	return nil
}

// mkRev is the revision a template makes; name, uid and times are the caller's.
func mkRev(svc *runpb.Service, t *runpb.RevisionTemplate) *runpb.Revision {
	r := &runpb.Revision{Service: svc.Name, LaunchStage: svc.LaunchStage, Labels: t.Labels, Annotations: t.Annotations,
		VpcAccess: t.VpcAccess, MaxInstanceRequestConcurrency: t.MaxInstanceRequestConcurrency, Timeout: t.Timeout,
		ServiceAccount: t.ServiceAccount, Containers: t.Containers, Volumes: t.Volumes, ExecutionEnvironment: t.ExecutionEnvironment,
		SessionAffinity: t.SessionAffinity, EncryptionKey: t.EncryptionKey, Creator: svc.Creator}
	if t.Scaling != nil {
		r.Scaling = &runpb.RevisionScaling{MinInstanceCount: t.Scaling.MinInstanceCount, MaxInstanceCount: t.Scaling.MaxInstanceCount}
	}
	return r
}

// same reports whether two revisions run the same configuration.
func same(a, b *runpb.Revision) bool {
	n := func(r *runpb.Revision) *runpb.Revision {
		r = proto.Clone(r).(*runpb.Revision)
		r.Name, r.Uid, r.Generation, r.CreateTime, r.UpdateTime, r.Conditions, r.ObservedGeneration, r.Etag, r.Reconciling = "", "", 0, nil, nil, nil, 0, "", false
		return r
	}
	return proto.Equal(n(a), n(b))
}

// spec is a service without what pogi computes.
func spec(s *runpb.Service) *runpb.Service {
	s = proto.Clone(s).(*runpb.Service)
	s.Generation, s.ObservedGeneration, s.CreateTime, s.UpdateTime, s.Etag, s.Uri, s.Urls = 0, 0, nil, nil, "", "", nil
	s.Conditions, s.TerminalCondition, s.TrafficStatuses, s.LatestReadyRevision, s.LatestCreatedRevision, s.Reconciling = nil, nil, nil, "", "", false
	return s
}

// traffic checks the allocations and returns them, the default being all on the latest revision.
func (s *Service) traffic(svc string, in []*runpb.TrafficTarget) ([]*runpb.TrafficTarget, error) {
	if len(in) == 0 {
		return []*runpb.TrafficTarget{{Type: latestType, Percent: 100}}, nil
	}
	total := 0
	out := make([]*runpb.TrafficTarget, len(in))
	for i, t := range in {
		t = proto.Clone(t).(*runpb.TrafficTarget)
		out[i] = t
		if t.Type == runpb.TrafficTargetAllocationType_TRAFFIC_TARGET_ALLOCATION_TYPE_UNSPECIFIED {
			t.Type = map[bool]runpb.TrafficTargetAllocationType{true: revisionType, false: latestType}[t.Revision != ""]
		}
		if t.Percent < 0 || t.Percent > 100 {
			return nil, gapi.Invalid("Invalid value for traffic[%d].percent: must be between 0 and 100", i)
		}
		if t.Type == revisionType && s.revs[svc+"/revisions/"+t.Revision] == nil {
			return nil, gapi.Invalid("Revision '%s' referenced in traffic[%d] was not found.", t.Revision, i)
		}
		total += int(t.Percent)
	}
	if total != 100 {
		return nil, gapi.Invalid("The sum of all traffic target percents must equal 100, got %d.", total)
	}
	return out, nil
}

// uri is the URL a service or tag answers at.
func (s *Service) uri(label string) string {
	return "http://" + label + ".run.localhost:" + serverless.Port(s.hub)
}

// settle fills the status of a service from its revisions; lock held.
func (s *Service) settle(svc *runpb.Service) {
	svc.ObservedGeneration, svc.Reconciling = svc.Generation, false
	svc.Uri = s.uri(last(svc.Name))
	svc.Urls = []string{svc.Uri}
	svc.TrafficStatuses = nil
	for _, t := range svc.Traffic {
		st := &runpb.TrafficTargetStatus{Type: t.Type, Revision: t.Revision, Percent: t.Percent, Tag: t.Tag}
		if t.Type == latestType {
			st.Revision = last(svc.LatestReadyRevision)
		}
		if t.Tag != "" {
			st.Uri = s.uri(t.Tag + "---" + last(svc.Name))
		}
		svc.TrafficStatuses = append(svc.TrafficStatuses, st)
	}
	svc.Conditions = []*runpb.Condition{condition("Ready", true, ""), condition("RoutesReady", true, ""), condition("ConfigurationsReady", true, "")}
	svc.TerminalCondition = svc.Conditions[0]
}

// newRevision stores a revision of svc made from its template; lock held.
func (s *Service) newRevision(svc *runpb.Service, name string) *runpb.Revision {
	if name == "" {
		s.seq[svc.Name]++
		name = fmt.Sprintf("%s-%05d-%s", last(svc.Name), s.seq[svc.Name], core.RandString(3, "abcdefghijklmnopqrstuvwxyz"))
	} else {
		s.seq[svc.Name]++
	}
	r := mkRev(svc, svc.Template)
	r.Name, r.Uid, r.Generation, r.CreateTime, r.UpdateTime, r.Etag = svc.Name+"/revisions/"+name, core.UUID(), svc.Generation, timestamppb.Now(), timestamppb.Now(), etag()
	r.ObservedGeneration = r.Generation
	r.Conditions = []*runpb.Condition{condition("Ready", true, ""), condition("ResourcesAvailable", true, ""), condition("ContainerReady", true, "")}
	s.revs[r.Name] = r
	svc.LatestCreatedRevision, svc.LatestReadyRevision = r.Name, r.Name
	return r
}

// revisionFor decides what a template change does to svc's revisions: it
// returns the name of a new revision to make ("" and false: none), or an error.
func (s *Service) revisionFor(svc *runpb.Service) (name string, make bool, err error) {
	cand := mkRev(svc, svc.Template)
	if n := svc.Template.Revision; n != "" {
		if !strings.HasPrefix(n, last(svc.Name)+"-") {
			return "", false, gapi.Invalid("Invalid value for template.revision: %q must start with %q", n, last(svc.Name)+"-")
		}
		if old := s.revs[svc.Name+"/revisions/"+n]; old != nil {
			if !same(old, cand) {
				return "", false, gapi.Exists("Revision named '%s' with different configuration already exists.", n)
			}
			return "", false, nil
		}
		return n, true, nil
	}
	if old := s.revs[svc.LatestCreatedRevision]; old != nil && same(old, cand) {
		return "", false, nil
	}
	return "", true, nil
}

func (s *Service) CreateService(_ context.Context, req *runpb.CreateServiceRequest) (*longrunningpb.Operation, error) {
	if err := checkParent(req.Parent); err != nil {
		return nil, err
	}
	if err := checkID("service", req.ServiceId); err != nil {
		return nil, err
	}
	if req.Service == nil || req.Service.Template == nil {
		return nil, gapi.Invalid("Invalid value for service.template: required")
	}
	svc := proto.Clone(req.Service).(*runpb.Service)
	if err := checkContainers(svc.Template.Containers, "template"); err != nil {
		return nil, err
	}
	defaults(svc.Template)
	svc.Name = req.Parent + "/services/" + req.ServiceId
	svc.Uid, svc.Generation, svc.CreateTime, svc.UpdateTime, svc.Etag = core.UUID(), 1, timestamppb.Now(), timestamppb.Now(), etag()
	svc.Creator = "pogi@localhost"
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.svcs[svc.Name] != nil {
		return nil, gapi.Exists("Resource '%s' already exists.", svc.Name)
	}
	name, _, err := s.revisionFor(svc)
	if err != nil {
		return nil, err
	}
	s.newRevision(svc, name) // traffic may name it
	svc.Traffic, err = s.traffic(svc.Name, req.Service.Traffic)
	if err != nil || req.ValidateOnly {
		s.dropRevisions(svc.Name)
		delete(s.seq, svc.Name)
		if err != nil {
			return nil, err
		}
	}
	s.settle(svc)
	if !req.ValidateOnly {
		s.svcs[svc.Name] = svc
	}
	return s.done(req.Parent, proto.Clone(svc)), nil
}

func (s *Service) GetService(_ context.Context, req *runpb.GetServiceRequest) (*runpb.Service, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v := s.svcs[req.Name]; v != nil {
		return proto.Clone(v).(*runpb.Service), nil
	}
	return nil, notFound("SERVICE", req.Name)
}

func (s *Service) ListServices(_ context.Context, req *runpb.ListServicesRequest) (*runpb.ListServicesResponse, error) {
	if err := checkParent(req.Parent); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*runpb.Service
	for _, n := range core.SortedKeys(s.svcs) {
		if parent(n) == req.Parent+"/services" {
			all = append(all, s.svcs[n])
		}
	}
	lo, hi, next := page(len(all), req.PageSize, req.PageToken)
	out := &runpb.ListServicesResponse{NextPageToken: next}
	for _, v := range all[lo:hi] {
		out.Services = append(out.Services, proto.Clone(v).(*runpb.Service))
	}
	return out, nil
}

// masked reports whether an update touches field: all of them when the mask is empty.
func masked(paths []string, field string) bool {
	return len(paths) == 0 || slices.ContainsFunc(paths, func(p string) bool { return p == field || strings.HasPrefix(p, field+".") })
}

func (s *Service) UpdateService(ctx context.Context, req *runpb.UpdateServiceRequest) (*longrunningpb.Operation, error) {
	in := req.Service
	if in == nil || !strings.Contains(in.Name, "/services/") {
		return nil, gapi.Invalid("Invalid value for service.name: %q", in.GetName())
	}
	s.mu.Lock()
	cur := s.svcs[in.Name]
	s.mu.Unlock()
	if cur == nil {
		if !req.AllowMissing {
			return nil, notFound("SERVICE", in.Name)
		}
		return s.CreateService(ctx, &runpb.CreateServiceRequest{Parent: parent(parent(in.Name)), ServiceId: last(in.Name), Service: in, ValidateOnly: req.ValidateOnly})
	}
	paths := req.UpdateMask.GetPaths()
	s.mu.Lock()
	defer s.mu.Unlock()
	cur = s.svcs[in.Name]
	if cur == nil {
		return nil, notFound("SERVICE", in.Name)
	}
	next := proto.Clone(cur).(*runpb.Service)
	if in.Template != nil && masked(paths, "template") {
		next.Template = proto.Clone(in.Template).(*runpb.RevisionTemplate)
		if err := checkContainers(next.Template.Containers, "template"); err != nil {
			return nil, err
		}
		defaults(next.Template)
	}
	if masked(paths, "traffic") {
		next.Traffic = in.Traffic
	}
	if masked(paths, "labels") {
		next.Labels = in.Labels
	}
	if masked(paths, "annotations") {
		next.Annotations = in.Annotations
	}
	if masked(paths, "description") {
		next.Description = in.Description
	}
	if masked(paths, "ingress") {
		next.Ingress = in.Ingress
	}
	if masked(paths, "scaling") {
		next.Scaling = in.Scaling
	}
	name, mk, err := s.revisionFor(next)
	if err != nil {
		return nil, err
	}
	var made *runpb.Revision
	if mk {
		made = s.newRevision(next, name)
	}
	next.Traffic, err = s.traffic(next.Name, next.Traffic)
	if err != nil || req.ValidateOnly {
		if made != nil {
			delete(s.revs, made.Name)
			s.seq[next.Name]--
		}
		if err != nil {
			return nil, err
		}
	}
	if mk || !proto.Equal(spec(cur), spec(next)) {
		next.Generation++
		next.UpdateTime, next.Etag, next.LastModifier = timestamppb.Now(), etag(), "pogi@localhost"
	}
	s.settle(next)
	if !req.ValidateOnly {
		s.svcs[next.Name] = next
	}
	return s.done(parent(parent(next.Name)), proto.Clone(next)), nil
}

func (s *Service) DeleteService(_ context.Context, req *runpb.DeleteServiceRequest) (*longrunningpb.Operation, error) {
	s.mu.Lock()
	cur := s.svcs[req.Name]
	if cur == nil {
		s.mu.Unlock()
		return nil, notFound("SERVICE", req.Name)
	}
	if req.Etag != "" && req.Etag != cur.Etag {
		s.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "The etag %s does not match the service's current etag %s.", req.Etag, cur.Etag)
	}
	if req.ValidateOnly {
		s.mu.Unlock()
		return s.done(parent(parent(req.Name)), proto.Clone(cur)), nil
	}
	stop := s.dropRevisions(req.Name)
	delete(s.svcs, req.Name)
	delete(s.iam, req.Name)
	delete(s.seq, req.Name)
	s.mu.Unlock()
	for _, x := range stop {
		x.stop()
	}
	out := proto.Clone(cur).(*runpb.Service)
	out.DeleteTime = timestamppb.Now()
	return s.done(parent(parent(req.Name)), out), nil
}

// dropRevisions forgets the revisions of a service and returns their containers to stop; lock held.
func (s *Service) dropRevisions(svc string) (stop []*inst) {
	for n, r := range s.revs {
		if r.Service == svc {
			delete(s.revs, n)
			if x := s.insts[n]; x != nil {
				stop = append(stop, x)
				delete(s.insts, n)
			}
		}
	}
	return stop
}

func (s *Service) GetRevision(_ context.Context, req *runpb.GetRevisionRequest) (*runpb.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v := s.revs[req.Name]; v != nil {
		return proto.Clone(v).(*runpb.Revision), nil
	}
	return nil, notFound("REVISION", req.Name)
}

func (s *Service) ListRevisions(_ context.Context, req *runpb.ListRevisionsRequest) (*runpb.ListRevisionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.svcs[req.Parent] == nil {
		return nil, notFound("SERVICE", req.Parent)
	}
	var all []*runpb.Revision
	for _, n := range core.SortedKeys(s.revs) {
		if s.revs[n].Service == req.Parent {
			all = append(all, s.revs[n])
		}
	}
	slices.Reverse(all) // newest first
	lo, hi, next := page(len(all), req.PageSize, req.PageToken)
	out := &runpb.ListRevisionsResponse{NextPageToken: next}
	for _, v := range all[lo:hi] {
		out.Revisions = append(out.Revisions, proto.Clone(v).(*runpb.Revision))
	}
	return out, nil
}

func (s *Service) DeleteRevision(_ context.Context, req *runpb.DeleteRevisionRequest) (*longrunningpb.Operation, error) {
	s.mu.Lock()
	cur := s.revs[req.Name]
	if cur == nil {
		s.mu.Unlock()
		return nil, notFound("REVISION", req.Name)
	}
	if svc := s.svcs[cur.Service]; svc != nil {
		for _, t := range svc.Traffic {
			if t.Percent > 0 && (t.Type == revisionType && cur.Service+"/revisions/"+t.Revision == req.Name || t.Type == latestType && svc.LatestReadyRevision == req.Name) {
				s.mu.Unlock()
				return nil, status.Errorf(codes.FailedPrecondition, "Revision '%s' is serving traffic and cannot be deleted.", last(req.Name))
			}
		}
	}
	delete(s.revs, req.Name)
	x := s.insts[req.Name]
	delete(s.insts, req.Name)
	s.mu.Unlock()
	if x != nil {
		x.stop()
	}
	out := proto.Clone(cur).(*runpb.Revision)
	out.DeleteTime = timestamppb.Now()
	return s.done(parent(parent(parent(req.Name))), out), nil
}
