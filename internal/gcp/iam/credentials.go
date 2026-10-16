package iam

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	credpb "cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Credentials is IAM Credentials v1 ("iamcredentials"). It shares IAM
// Admin's accounts: access tokens and ID tokens come from gapi, and signBlob
// and signJwt use the account's own system key, published at /service_accounts/v1/metadata.
// ponytail: delegates are accepted and ignored.
type Credentials struct {
	credpb.UnimplementedIAMCredentialsServer
	s    *Service
	rest *gapi.Mux
}

// NewCredentials creates IAM Credentials on top of the hub's IAM Admin, or
// on an empty one of its own when that service is off.
func NewCredentials(h *core.Hub) *Credentials {
	s, ok := h.Service("gcpiam").(*Service)
	if !ok {
		s = New(h)
	}
	c := &Credentials{s: s}
	var rules []gapi.Rule
	for _, m := range []string{"GenerateAccessToken", "GenerateIdToken", "SignBlob", "SignJwt"} {
		rules = append(rules, gapi.Rule{HTTP: "POST /v1/{name=projects/*/serviceAccounts/*}:" + strings.ToLower(m[:1]) + m[1:], Method: m})
	}
	c.rest = gapi.NewMux(c, rules...)
	return c
}

func (c *Credentials) Info() core.Info {
	return core.Info{Name: "iamcredentials", Cloud: core.GCP, Title: "IAM Credentials", Match: c.rest.Match,
		Targets: []string{"google.iam.credentials.v1.IAMCredentials"}}
}

func (c *Credentials) RegisterGRPC(g *grpc.Server) { credpb.RegisterIAMCredentialsServer(g, c) }

func (c *Credentials) ServeHTTP(w http.ResponseWriter, r *http.Request) { c.rest.ServeHTTP(w, r) }

// email resolves a service account name to the email of an enabled account.
func (c *Credentials) email(name string) (string, error) {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	a, err := c.s.account(name)
	if err != nil {
		return "", status.Errorf(codes.NotFound, "Not found; Gaia id not found for email %s", name)
	}
	if a.Disabled {
		return "", status.Errorf(codes.FailedPrecondition, "Service account %s is disabled.", a.Email)
	}
	return a.Email, nil
}

func (c *Credentials) GenerateAccessToken(_ context.Context, req *credpb.GenerateAccessTokenRequest) (*credpb.GenerateAccessTokenResponse, error) {
	email, err := c.email(req.Name)
	if err != nil {
		return nil, err
	}
	if len(req.Scope) == 0 {
		return nil, gapi.Invalid("Request contains an invalid argument: scope is required.")
	}
	ttl := time.Hour
	if req.Lifetime != nil {
		ttl = req.Lifetime.AsDuration()
	}
	if ttl <= 0 || ttl > time.Hour {
		return nil, gapi.Invalid("The requested lifetime %s is not valid: it must be at most 3600s.", ttl)
	}
	scope := ""
	for i, s := range req.Scope {
		if i > 0 {
			scope += " "
		}
		scope += s
	}
	return &credpb.GenerateAccessTokenResponse{AccessToken: gapi.AccessToken(email, scope, ttl), ExpireTime: timestamppb.New(time.Now().Add(ttl))}, nil
}

func (c *Credentials) GenerateIdToken(_ context.Context, req *credpb.GenerateIdTokenRequest) (*credpb.GenerateIdTokenResponse, error) {
	email, err := c.email(req.Name)
	if err != nil {
		return nil, err
	}
	if req.Audience == "" {
		return nil, gapi.Invalid("Request contains an invalid argument: audience is required.")
	}
	return &credpb.GenerateIdTokenResponse{Token: gapi.OIDCToken(email, req.Audience)}, nil
}

func (c *Credentials) SignBlob(_ context.Context, req *credpb.SignBlobRequest) (*credpb.SignBlobResponse, error) {
	email, err := c.email(req.Name)
	if err != nil {
		return nil, err
	}
	k, priv, err := c.s.systemKey(email)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(req.Payload)
	sig, _ := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, sum[:])
	return &credpb.SignBlobResponse{KeyId: k.ID, SignedBlob: sig}, nil
}

func (c *Credentials) SignJwt(_ context.Context, req *credpb.SignJwtRequest) (*credpb.SignJwtResponse, error) {
	email, err := c.email(req.Name)
	if err != nil {
		return nil, err
	}
	var claims map[string]any
	if json.Unmarshal([]byte(req.Payload), &claims) != nil || claims == nil {
		return nil, gapi.Invalid("Invalid JWT payload: it must be a JSON object holding a JWT Claims Set.")
	}
	k, priv, err := c.s.systemKey(email)
	if err != nil {
		return nil, err
	}
	return &credpb.SignJwtResponse{KeyId: k.ID, SignedJwt: gapi.SignJWT(k.ID, priv, claims)}, nil
}
