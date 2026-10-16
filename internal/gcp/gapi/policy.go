package gapi

import (
	"bytes"
	"slices"

	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/nrndev/pogi/internal/core"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// EmptyEtag is the etag of a policy that was never set.
var EmptyEtag = []byte{0, 0, 1}

// Policy returns a copy of the stored policy, or the empty one when none was set.
func Policy(cur *iampb.Policy) *iampb.Policy {
	if cur == nil {
		return &iampb.Policy{Etag: EmptyEtag}
	}
	return proto.Clone(cur).(*iampb.Policy)
}

// SetPolicy returns the policy to store when in replaces cur (nil if never
// set). A non-empty etag that isn't cur's is a concurrent change, as at Google.
// Bindings without members are dropped.
func SetPolicy(cur, in *iampb.Policy) (*iampb.Policy, error) {
	if in == nil {
		return nil, Invalid("The policy is required")
	}
	if len(in.Etag) > 0 && !bytes.Equal(in.Etag, Policy(cur).Etag) {
		return nil, status.Error(codes.Aborted, "There were concurrent policy changes. Please retry the whole read-modify-write with exponential backoff.")
	}
	p := proto.Clone(in).(*iampb.Policy)
	p.Bindings = slices.DeleteFunc(p.Bindings, func(b *iampb.Binding) bool { return len(b.Members) == 0 })
	p.Etag = []byte(core.RandHex(6))
	return p, nil
}
