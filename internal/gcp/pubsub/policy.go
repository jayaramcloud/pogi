package pubsub

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// IAM policies of topics, subscriptions, snapshots and schemas, over REST (:getIamPolicy,
// :setIamPolicy, :testIamPermissions). The IAM authorizer in internal/gcp/iam reads them.
// ponytail: gRPC clients reach google.iam.v1.IAMPolicy on whichever service registered it first (Cloud KMS).

// policyTarget is the resource a policy hangs on; the resource must exist. Lock held.
func (s *server) policyTarget(name string) error {
	ok := s.topics[name] != nil || s.subs[name] != nil || s.snaps[name] != nil || s.schemas[name] != nil
	if !ok {
		return gapi.NotFound("Resource not found (resource=%s).", name)
	}
	return nil
}

func (s *server) GetIamPolicy(_ context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.policyTarget(req.Resource); err != nil {
		return nil, err
	}
	return gapi.Policy(s.policies[req.Resource]), nil
}

func (s *server) SetIamPolicy(_ context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.policyTarget(req.Resource); err != nil {
		return nil, err
	}
	p, err := gapi.SetPolicy(s.policies[req.Resource], req.Policy)
	if err != nil {
		return nil, err
	}
	s.policies[req.Resource] = p
	return proto.Clone(p).(*iampb.Policy), nil
}

// TestIamPermissions grants every permission asked for.
func (s *server) TestIamPermissions(_ context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.policyTarget(req.Resource); err != nil {
		return nil, err
	}
	return &iampb.TestIamPermissionsResponse{Permissions: req.Permissions}, nil
}

func savePolicies(m map[string]*iampb.Policy) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for k, p := range m {
		out[k] = pj(p)
	}
	return out
}

func loadPolicies(in map[string]json.RawMessage) map[string]*iampb.Policy {
	out := map[string]*iampb.Policy{}
	for k, raw := range in {
		p := &iampb.Policy{}
		if protojson.Unmarshal(raw, p) == nil {
			out[k] = p
		}
	}
	return out
}
