package storage

import (
	"context"
	"encoding/json"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
)

// A bucket's IAM policy through Google's IAM types, for the console and the IAM authorizer. The JSON
// API keeps serving the stored document as the client sent it.

type jsonPolicy struct {
	Kind       string        `json:"kind"`
	ResourceID string        `json:"resourceId"`
	Version    int32         `json:"version"`
	Etag       string        `json:"etag"`
	Bindings   []jsonBinding `json:"bindings"`
}

type jsonBinding struct {
	Role    string   `json:"role"`
	Members []string `json:"members"`
}

func bucketOf(resource string) string { return resource[strings.LastIndex(resource, "/")+1:] }

// BucketProject is the project that owns a bucket, "" when there is no such bucket.
func (s *Service) BucketProject(name string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if b := s.st.Buckets[name]; b != nil {
		if b.Project == "" {
			return core.DefaultProject
		}
		return b.Project
	}
	return ""
}

func (b *Bucket) policy() *iampb.Policy {
	if b.IAM == nil {
		return gapi.Policy(nil)
	}
	var j jsonPolicy
	json.Unmarshal(b.IAM, &j)
	p := &iampb.Policy{Version: j.Version, Etag: []byte(j.Etag)}
	for _, x := range j.Bindings {
		p.Bindings = append(p.Bindings, &iampb.Binding{Role: x.Role, Members: x.Members})
	}
	return p
}

// GetIamPolicy reads projects/_/buckets/<bucket>.
func (s *Service) GetIamPolicy(_ context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := s.bucket(bucketOf(req.Resource))
	if err != nil {
		return nil, err
	}
	return b.policy(), nil
}

// SetIamPolicy replaces the bucket's policy (a stale etag is refused).
func (s *Service) SetIamPolicy(_ context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := bucketOf(req.Resource)
	b, err := s.bucket(name)
	if err != nil {
		return nil, err
	}
	var cur *iampb.Policy
	if b.IAM != nil {
		cur = b.policy()
	}
	p, err := gapi.SetPolicy(cur, req.Policy)
	if err != nil {
		return nil, err
	}
	j := jsonPolicy{Kind: "storage#policy", ResourceID: "projects/_/buckets/" + name, Version: max(p.Version, 1), Etag: string(p.Etag)}
	for _, x := range p.Bindings {
		j.Bindings = append(j.Bindings, jsonBinding{Role: x.Role, Members: x.Members})
	}
	b.IAM, _ = json.Marshal(j)
	return b.policy(), nil
}
