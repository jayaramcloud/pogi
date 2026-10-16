// Package iam emulates Google Cloud IAM over gRPC and REST. Service "gcpiam"
// is IAM Admin v1: service accounts, their keys (create returns a real
// credentials file whose key signs JWT assertions at pogi's token endpoint),
// predefined and custom roles, and IAM policies on service accounts. Service
// "iamcredentials" (credentials.go) is IAM Credentials v1: access and ID
// tokens, signBlob and signJwt. Public keys are published where Google does:
// /service_accounts/v1/metadata/{x509,jwk,raw}/<email>.
package iam

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service is IAM Admin.
type Service struct {
	adminpb.UnimplementedIAMServer
	hub  *core.Hub
	mu   sync.Mutex
	st   state
	rest *gapi.Mux
	rec  recorder // what each principal called, for the console (recorder.go)
}

// state is everything IAM keeps.
type state struct {
	Accounts map[string]*account `json:"accounts"`          // by email
	Deleted  map[string]*account `json:"deleted"`           // by unique id
	Roles    map[string]*role    `json:"roles"`             // custom roles, by full name
	Signing  []byte              `json:"signing,omitempty"` // gapi's shared ID-token key, PKCS#8
}

type account struct {
	Project     string          `json:"project"`
	UniqueID    string          `json:"uniqueId"`
	Email       string          `json:"email"`
	DisplayName string          `json:"displayName,omitempty"`
	Description string          `json:"description,omitempty"`
	Disabled    bool            `json:"disabled,omitempty"`
	Etag        []byte          `json:"etag"`
	Keys        []*key          `json:"keys,omitempty"`   // user-managed
	System      *key            `json:"system,omitempty"` // signs signBlob and signJwt; made on first use
	Policy      json.RawMessage `json:"policy,omitempty"`
}

type key struct {
	ID       string    `json:"id"`
	Created  time.Time `json:"created"`
	Expires  time.Time `json:"expires"`
	Disabled bool      `json:"disabled,omitempty"`
	Uploaded bool      `json:"uploaded,omitempty"`
	Public   []byte    `json:"public"`            // PKIX DER
	Cert     []byte    `json:"cert,omitempty"`    // X.509 DER
	Private  []byte    `json:"private,omitempty"` // PKCS#8 DER, system keys only: users hold their own
}

// New creates the IAM Admin service.
func New(h *core.Hub) *Service {
	s := &Service{hub: h}
	s.Reset()
	s.rest = gapi.NewMux(s, rules...)
	h.Authorize[core.GCP] = s.authorize
	return s
}

const (
	metaPrefix = "/service_accounts/v1/metadata/"
	maxKeys    = 10
)

func (s *Service) Info() core.Info {
	return core.Info{Name: "gcpiam", Cloud: core.GCP, Title: "IAM", Targets: []string{"google.iam.admin.v1.IAM"},
		Match: func(r *http.Request) bool { return s.rest.Match(r) || strings.HasPrefix(r.URL.Path, metaPrefix) }}
}

func (s *Service) RegisterGRPC(g *grpc.Server) { adminpb.RegisterIAMServer(g, s) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Accounts: map[string]*account{}, Deleted: map[string]*account{}, Roles: map[string]*role{}}
	s.mu.Unlock()
	s.rec.mu.Lock()
	s.rec.m = nil
	s.rec.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.st
	st.Signing, _ = x509.MarshalPKCS8PrivateKey(gapi.SigningKey())
	return json.Marshal(st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Accounts == nil {
		st.Accounts = map[string]*account{}
	}
	if st.Deleted == nil {
		st.Deleted = map[string]*account{}
	}
	if st.Roles == nil {
		st.Roles = map[string]*role{}
	}
	if k, err := x509.ParsePKCS8PrivateKey(st.Signing); err == nil {
		if rk, ok := k.(*rsa.PrivateKey); ok {
			gapi.SetSigningKey(rk)
		}
	}
	st.Signing = nil
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, metaPrefix) {
		s.metadata(w, r)
		return
	}
	s.rest.ServeHTTP(w, r)
}

// baseURL is where clients reach pogi: the token_uri and certificate URLs in key files.
func (s *Service) baseURL() string {
	if s.hub.URL != "" {
		return s.hub.URL
	}
	return "http://localhost:7644"
}

// ---- REST ----

var rules = []gapi.Rule{
	{HTTP: "GET /v1/{name=projects/*}/serviceAccounts", Method: "ListServiceAccounts"},
	{HTTP: "POST /v1/{name=projects/*}/serviceAccounts", Method: "CreateServiceAccount"},
	{HTTP: "GET /v1/{name=projects/*/serviceAccounts/*}", Method: "GetServiceAccount"},
	{HTTP: "PUT /v1/{name=projects/*/serviceAccounts/*}", Method: "UpdateServiceAccount"},
	{HTTP: "PATCH /v1/{serviceAccount.name=projects/*/serviceAccounts/*}", Method: "PatchServiceAccount"},
	{HTTP: "DELETE /v1/{name=projects/*/serviceAccounts/*}", Method: "DeleteServiceAccount"},
	{HTTP: "POST /v1/{name=projects/*/serviceAccounts/*}:undelete", Method: "UndeleteServiceAccount"},
	{HTTP: "POST /v1/{name=projects/*/serviceAccounts/*}:enable", Method: "EnableServiceAccount"},
	{HTTP: "POST /v1/{name=projects/*/serviceAccounts/*}:disable", Method: "DisableServiceAccount"},
	{HTTP: "GET /v1/{name=projects/*/serviceAccounts/*}/keys", Method: "ListServiceAccountKeys"},
	{HTTP: "POST /v1/{name=projects/*/serviceAccounts/*}/keys", Method: "CreateServiceAccountKey"},
	{HTTP: "POST /v1/{name=projects/*/serviceAccounts/*}/keys:upload", Method: "UploadServiceAccountKey"},
	{HTTP: "GET /v1/{name=projects/*/serviceAccounts/*/keys/*}", Method: "GetServiceAccountKey"},
	{HTTP: "DELETE /v1/{name=projects/*/serviceAccounts/*/keys/*}", Method: "DeleteServiceAccountKey"},
	{HTTP: "POST /v1/{name=projects/*/serviceAccounts/*/keys/*}:disable", Method: "DisableServiceAccountKey"},
	{HTTP: "POST /v1/{name=projects/*/serviceAccounts/*/keys/*}:enable", Method: "EnableServiceAccountKey"},
	{HTTP: "POST /v1/{resource=projects/*/serviceAccounts/*}:getIamPolicy", Method: "GetIamPolicy"},
	{HTTP: "GET /v1/{resource=projects/*/serviceAccounts/*}:getIamPolicy", Method: "GetIamPolicy"},
	{HTTP: "POST /v1/{resource=projects/*/serviceAccounts/*}:setIamPolicy", Method: "SetIamPolicy"},
	{HTTP: "POST /v1/{resource=projects/*/serviceAccounts/*}:testIamPermissions", Method: "TestIamPermissions"},

	{HTTP: "GET /v1/roles", Method: "ListRoles"},
	{HTTP: "GET /v1/{name=roles/*}", Method: "GetRole"},
	{HTTP: "GET /v1/{parent=projects/*}/roles", Method: "ListRoles"},
	{HTTP: "POST /v1/{parent=projects/*}/roles", Method: "CreateRole"},
	{HTTP: "GET /v1/{name=projects/*/roles/*}", Method: "GetRole"},
	{HTTP: "PATCH /v1/{name=projects/*/roles/*}", Method: "UpdateRole", Body: "role"},
	{HTTP: "DELETE /v1/{name=projects/*/roles/*}", Method: "DeleteRole"},
	{HTTP: "POST /v1/{name=projects/*/roles/*}:undelete", Method: "UndeleteRole"},
	{HTTP: "GET /v1/{parent=organizations/*}/roles", Method: "ListRoles"},
	{HTTP: "POST /v1/{parent=organizations/*}/roles", Method: "CreateRole"},
	{HTTP: "GET /v1/{name=organizations/*/roles/*}", Method: "GetRole"},
	{HTTP: "PATCH /v1/{name=organizations/*/roles/*}", Method: "UpdateRole", Body: "role"},
	{HTTP: "DELETE /v1/{name=organizations/*/roles/*}", Method: "DeleteRole"},
	{HTTP: "POST /v1/{name=organizations/*/roles/*}:undelete", Method: "UndeleteRole"},
}

// ---- names ----

var (
	projectRE = regexp.MustCompile(`^projects/[^/]+$`)
	accountRE = regexp.MustCompile(`^projects/([^/]+)/serviceAccounts/([^/]+)$`)
	keyRE     = regexp.MustCompile(`^(projects/[^/]+/serviceAccounts/[^/]+)/keys/([^/]+)$`)
	accountID = regexp.MustCompile(`^[a-z]([-a-z0-9]*[a-z0-9])$`)
)

func (a *account) name() string { return "projects/" + a.Project + "/serviceAccounts/" + a.Email }

// account finds a service account by projects/<p|->/serviceAccounts/<email|unique id>. Lock held.
func (s *Service) account(name string) (*account, error) {
	m := accountRE.FindStringSubmatch(name)
	if m == nil {
		return nil, gapi.Invalid("Invalid service account name %q: want projects/{project}/serviceAccounts/{email or unique id}", name)
	}
	a := s.st.Accounts[m[2]]
	if a == nil {
		for _, x := range s.st.Accounts {
			if x.UniqueID == m[2] {
				a = x
			}
		}
	}
	if a == nil || m[1] != "-" && m[1] != a.Project {
		return nil, gapi.NotFound("Service account %s does not exist.", name)
	}
	return a, nil
}

func (a *account) proto() *adminpb.ServiceAccount {
	return &adminpb.ServiceAccount{Name: a.name(), ProjectId: a.Project, UniqueId: a.UniqueID, Email: a.Email,
		DisplayName: a.DisplayName, Description: a.Description, Etag: a.Etag, Oauth2ClientId: a.UniqueID, Disabled: a.Disabled}
}

func newEtag() []byte { return []byte(core.RandHex(4)) }

// uniqueID is 21 digits, like Google's.
func uniqueID() string {
	b := make([]byte, 21)
	rand.Read(b)
	for i := range b {
		b[i] = '0' + b[i]%10
	}
	b[0] = '1'
	return string(b)
}

// ---- service accounts ----

func (s *Service) CreateServiceAccount(_ context.Context, req *adminpb.CreateServiceAccountRequest) (*adminpb.ServiceAccount, error) {
	if !projectRE.MatchString(req.Name) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/{project}", req.Name)
	}
	if n := len(req.AccountId); n < 6 || n > 30 || !accountID.MatchString(req.AccountId) {
		return nil, gapi.Invalid("Invalid service account ID '%s'. Service Account ID must be 6-30 characters long, and match the regular expression [a-z]([-a-z0-9]*[a-z0-9]) to comply with RFC1035.", req.AccountId)
	}
	project := strings.TrimPrefix(req.Name, "projects/")
	a := &account{Project: project, UniqueID: uniqueID(), Email: req.AccountId + "@" + project + ".iam.gserviceaccount.com",
		DisplayName: req.GetServiceAccount().GetDisplayName(), Description: req.GetServiceAccount().GetDescription(), Etag: newEtag()}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Accounts[a.Email] != nil {
		return nil, gapi.Exists("Service account %s already exists within project projects/%s.", req.AccountId, project)
	}
	s.st.Accounts[a.Email] = a
	return a.proto(), nil
}

func (s *Service) GetServiceAccount(_ context.Context, req *adminpb.GetServiceAccountRequest) (*adminpb.ServiceAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.account(req.Name)
	if err != nil {
		return nil, err
	}
	return a.proto(), nil
}

func (s *Service) ListServiceAccounts(_ context.Context, req *adminpb.ListServiceAccountsRequest) (*adminpb.ListServiceAccountsResponse, error) {
	if !projectRE.MatchString(req.Name) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/{project}", req.Name)
	}
	project := strings.TrimPrefix(req.Name, "projects/")
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*adminpb.ServiceAccount
	for _, e := range core.SortedKeys(s.st.Accounts) {
		if a := s.st.Accounts[e]; a.Project == project {
			list = append(list, a.proto())
		}
	}
	items, next := gapi.Page(list, req.PageSize, req.PageToken)
	return &adminpb.ListServiceAccountsResponse{Accounts: items, NextPageToken: next}, nil
}

// UpdateServiceAccount is the old PUT: it replaces the display name and description.
func (s *Service) UpdateServiceAccount(_ context.Context, req *adminpb.ServiceAccount) (*adminpb.ServiceAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.account(req.Name)
	if err != nil {
		return nil, err
	}
	a.DisplayName, a.Description, a.Etag = req.DisplayName, req.Description, newEtag()
	return a.proto(), nil
}

func (s *Service) PatchServiceAccount(_ context.Context, req *adminpb.PatchServiceAccountRequest) (*adminpb.ServiceAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.account(req.GetServiceAccount().GetName())
	if err != nil {
		return nil, err
	}
	p := a.proto()
	if err := gapi.Update(p, req.ServiceAccount, req.GetUpdateMask().GetPaths(), "display_name", "description"); err != nil {
		return nil, err
	}
	a.DisplayName, a.Description, a.Etag = p.DisplayName, p.Description, newEtag()
	return a.proto(), nil
}

// DeleteServiceAccount keeps the account by unique id, so it can be undeleted.
func (s *Service) DeleteServiceAccount(_ context.Context, req *adminpb.DeleteServiceAccountRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.account(req.Name)
	if err != nil {
		return nil, err
	}
	delete(s.st.Accounts, a.Email)
	s.st.Deleted[a.UniqueID] = a
	return &emptypb.Empty{}, nil
}

// UndeleteServiceAccount takes projects/-/serviceAccounts/<unique id>.
func (s *Service) UndeleteServiceAccount(_ context.Context, req *adminpb.UndeleteServiceAccountRequest) (*adminpb.UndeleteServiceAccountResponse, error) {
	m := accountRE.FindStringSubmatch(req.Name)
	s.mu.Lock()
	defer s.mu.Unlock()
	var a *account
	if m != nil {
		a = s.st.Deleted[m[2]]
	}
	if a == nil {
		return nil, gapi.NotFound("Service account %s was not deleted or can no longer be restored.", req.Name)
	}
	if s.st.Accounts[a.Email] != nil {
		return nil, gapi.Exists("Service account %s already exists.", a.Email)
	}
	delete(s.st.Deleted, a.UniqueID)
	s.st.Accounts[a.Email] = a
	return &adminpb.UndeleteServiceAccountResponse{RestoredAccount: a.proto()}, nil
}

func (s *Service) setDisabled(name string, off bool) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.account(name)
	if err != nil {
		return nil, err
	}
	a.Disabled, a.Etag = off, newEtag()
	return &emptypb.Empty{}, nil
}

func (s *Service) EnableServiceAccount(_ context.Context, req *adminpb.EnableServiceAccountRequest) (*emptypb.Empty, error) {
	return s.setDisabled(req.Name, false)
}

func (s *Service) DisableServiceAccount(_ context.Context, req *adminpb.DisableServiceAccountRequest) (*emptypb.Empty, error) {
	return s.setDisabled(req.Name, true)
}

// ---- keys ----

func (a *account) keyName(k *key) string { return a.name() + "/keys/" + k.ID }

func (a *account) keyProto(k *key, pub adminpb.ServiceAccountPublicKeyType) *adminpb.ServiceAccountKey {
	p := &adminpb.ServiceAccountKey{Name: a.keyName(k), KeyAlgorithm: adminpb.ServiceAccountKeyAlgorithm_KEY_ALG_RSA_2048,
		ValidAfterTime: timestamppb.New(k.Created), ValidBeforeTime: timestamppb.New(k.Expires), Disabled: k.Disabled,
		KeyOrigin: adminpb.ServiceAccountKeyOrigin_GOOGLE_PROVIDED, KeyType: adminpb.ListServiceAccountKeysRequest_USER_MANAGED}
	switch {
	case k == a.System:
		p.KeyType = adminpb.ListServiceAccountKeysRequest_SYSTEM_MANAGED
	case k.Uploaded:
		p.KeyOrigin = adminpb.ServiceAccountKeyOrigin_USER_PROVIDED
	}
	switch pub {
	case adminpb.ServiceAccountPublicKeyType_TYPE_X509_PEM_FILE:
		if k.Cert != nil {
			p.PublicKeyData = gapi.PEM("CERTIFICATE", k.Cert)
		}
	case adminpb.ServiceAccountPublicKeyType_TYPE_RAW_PUBLIC_KEY:
		p.PublicKeyData = gapi.PEM("PUBLIC KEY", k.Public)
	}
	return p
}

func (s *Service) key(name string) (*account, *key, error) {
	m := keyRE.FindStringSubmatch(name)
	if m == nil {
		return nil, nil, gapi.Invalid("Invalid service account key name %q", name)
	}
	a, err := s.account(m[1])
	if err != nil {
		return nil, nil, err
	}
	for _, k := range append([]*key{a.System}, a.Keys...) {
		if k != nil && k.ID == m[2] {
			return a, k, nil
		}
	}
	return nil, nil, gapi.NotFound("Service account key %s does not exist.", name)
}

func newKey() (*key, *rsa.PrivateKey) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	pub, _ := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	now := time.Now()
	return &key{ID: core.RandHex(20), Created: now, Expires: now.AddDate(10, 0, 0), Public: pub}, priv
}

func (s *Service) CreateServiceAccountKey(_ context.Context, req *adminpb.CreateServiceAccountKeyRequest) (*adminpb.ServiceAccountKey, error) {
	s.mu.Lock()
	a, err := s.account(req.Name)
	if err == nil && len(a.Keys) >= maxKeys {
		err = status.Errorf(codes.FailedPrecondition, "Precondition check failed: service account %s already has the maximum of %d keys.", a.Email, maxKeys)
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	k, priv := newKey() // outside the lock: generating RSA takes a while
	k.Cert = gapi.Cert(priv, a.UniqueID)
	der, _ := x509.MarshalPKCS8PrivateKey(priv)
	file, _ := json.MarshalIndent(map[string]string{"type": "service_account", "project_id": a.Project, "private_key_id": k.ID,
		"private_key": string(gapi.PEM("PRIVATE KEY", der)), "client_email": a.Email, "client_id": a.UniqueID,
		"auth_uri": s.baseURL() + "/o/oauth2/auth", "token_uri": s.baseURL() + "/token",
		"auth_provider_x509_cert_url": s.baseURL() + "/oauth2/v1/certs",
		"client_x509_cert_url":        s.baseURL() + metaPrefix + "x509/" + strings.ReplaceAll(a.Email, "@", "%40"),
		"universe_domain":             "googleapis.com"}, "", "  ")
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, err = s.account(req.Name); err != nil { // deleted meanwhile
		return nil, err
	}
	a.Keys = append(a.Keys, k)
	p := a.keyProto(k, 0)
	p.PrivateKeyType, p.PrivateKeyData = adminpb.ServiceAccountPrivateKeyType_TYPE_GOOGLE_CREDENTIALS_FILE, file
	return p, nil
}

// UploadServiceAccountKey registers a public key (PEM: certificate or PKIX) the caller holds the private half of.
func (s *Service) UploadServiceAccountKey(_ context.Context, req *adminpb.UploadServiceAccountKeyRequest) (*adminpb.ServiceAccountKey, error) {
	var cert []byte
	var pk any
	der := pemBlock(req.PublicKeyData)
	if c, err := x509.ParseCertificate(der); err == nil {
		cert, pk = der, c.PublicKey
	} else {
		pk, _ = x509.ParsePKIXPublicKey(der)
	}
	rk, ok := pk.(*rsa.PublicKey)
	if !ok {
		return nil, gapi.Invalid("public_key_data must be a PEM-encoded X.509 certificate or RSA public key")
	}
	pub, _ := x509.MarshalPKIXPublicKey(rk)
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.account(req.Name)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	k := &key{ID: core.RandHex(20), Created: now, Expires: now.AddDate(10, 0, 0), Public: pub, Cert: cert, Uploaded: true}
	a.Keys = append(a.Keys, k)
	return a.keyProto(k, 0), nil
}

func (s *Service) GetServiceAccountKey(_ context.Context, req *adminpb.GetServiceAccountKeyRequest) (*adminpb.ServiceAccountKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, k, err := s.key(req.Name)
	if err != nil {
		return nil, err
	}
	return a.keyProto(k, req.PublicKeyType), nil
}

// ListServiceAccountKeys lists user-managed keys and, unless key_types says
// otherwise, the system key too.
func (s *Service) ListServiceAccountKeys(_ context.Context, req *adminpb.ListServiceAccountKeysRequest) (*adminpb.ListServiceAccountKeysResponse, error) {
	s.mu.Lock()
	a, err := s.account(req.Name)
	email := ""
	if err == nil {
		email = a.Email
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	want := func(t adminpb.ListServiceAccountKeysRequest_KeyType) bool {
		return len(req.KeyTypes) == 0 || slices.Contains(req.KeyTypes, t)
	}
	if want(adminpb.ListServiceAccountKeysRequest_SYSTEM_MANAGED) {
		s.systemKey(email)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, err = s.account(req.Name); err != nil {
		return nil, err
	}
	out := &adminpb.ListServiceAccountKeysResponse{}
	if want(adminpb.ListServiceAccountKeysRequest_USER_MANAGED) {
		for _, k := range a.Keys {
			out.Keys = append(out.Keys, a.keyProto(k, 0))
		}
	}
	if a.System != nil && want(adminpb.ListServiceAccountKeysRequest_SYSTEM_MANAGED) {
		out.Keys = append(out.Keys, a.keyProto(a.System, 0))
	}
	return out, nil
}

func (s *Service) DeleteServiceAccountKey(_ context.Context, req *adminpb.DeleteServiceAccountKeyRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, k, err := s.key(req.Name)
	if err != nil {
		return nil, err
	}
	if k == a.System {
		return nil, status.Error(codes.FailedPrecondition, "System-managed keys can't be deleted.")
	}
	a.Keys = slices.DeleteFunc(a.Keys, func(x *key) bool { return x == k })
	return &emptypb.Empty{}, nil
}

func (s *Service) setKeyDisabled(name string, off bool) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, k, err := s.key(name)
	if err != nil {
		return nil, err
	}
	if k == a.System {
		return nil, status.Error(codes.FailedPrecondition, "System-managed keys can't be disabled.")
	}
	k.Disabled = off
	return &emptypb.Empty{}, nil
}

func (s *Service) DisableServiceAccountKey(_ context.Context, req *adminpb.DisableServiceAccountKeyRequest) (*emptypb.Empty, error) {
	return s.setKeyDisabled(req.Name, true)
}

func (s *Service) EnableServiceAccountKey(_ context.Context, req *adminpb.EnableServiceAccountKeyRequest) (*emptypb.Empty, error) {
	return s.setKeyDisabled(req.Name, false)
}

// systemKey returns the account's own signing key, making it on first use.
func (s *Service) systemKey(email string) (*key, *rsa.PrivateKey, error) {
	s.mu.Lock()
	a := s.st.Accounts[email]
	if a == nil {
		s.mu.Unlock()
		return nil, nil, gapi.NotFound("Not found; Gaia id not found for email %s", email)
	}
	if a.System == nil {
		s.mu.Unlock()
		k, priv := newKey() // outside the lock: generating RSA takes a while
		k.Cert = gapi.Cert(priv, a.UniqueID)
		k.Private, _ = x509.MarshalPKCS8PrivateKey(priv)
		s.mu.Lock()
		if a = s.st.Accounts[email]; a == nil {
			s.mu.Unlock()
			return nil, nil, gapi.NotFound("Not found; Gaia id not found for email %s", email)
		}
		if a.System == nil {
			a.System = k
		}
	}
	k := a.System
	s.mu.Unlock()
	pk, err := x509.ParsePKCS8PrivateKey(k.Private)
	if err != nil {
		return nil, nil, err
	}
	return k, pk.(*rsa.PrivateKey), nil
}

// ServiceAccountKeys implements gapi.KeyDirectory for the token endpoint.
func (s *Service) ServiceAccountKeys(email string) map[string]*rsa.PublicKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.st.Accounts[email]
	if a == nil || a.Disabled {
		return nil
	}
	out := map[string]*rsa.PublicKey{}
	for _, k := range a.Keys {
		if pk, err := x509.ParsePKIXPublicKey(k.Public); err == nil && !k.Disabled {
			out[k.ID], _ = pk.(*rsa.PublicKey)
		}
	}
	return out
}

// ---- published public keys ----

// metadata serves /service_accounts/v1/metadata/{x509,jwk,raw}/<email>.
func (s *Service) metadata(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	format, email, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, metaPrefix), "/")
	c.Op = "Metadata/" + format
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		gapi.JSONErr(w, c, e.Status, e.Code, e.Msg)
		return
	}
	if _, _, err := s.systemKey(email); err != nil {
		gapi.Err(w, c, err)
		return
	}
	s.mu.Lock()
	a := s.st.Accounts[email]
	keys := append([]*key{a.System}, a.Keys...)
	s.mu.Unlock()
	var out any
	switch format {
	case "x509", "raw":
		m := map[string]string{}
		for _, k := range keys {
			if format == "raw" {
				m[k.ID] = string(gapi.PEM("PUBLIC KEY", k.Public))
			} else if k.Cert != nil {
				m[k.ID] = string(gapi.PEM("CERTIFICATE", k.Cert))
			}
		}
		out = m
	case "jwk":
		var list []map[string]any
		for _, k := range keys {
			if pk, err := x509.ParsePKIXPublicKey(k.Public); err == nil {
				list = append(list, gapi.JWK(k.ID, pk.(*rsa.PublicKey)))
			}
		}
		out = map[string]any{"keys": list}
	default:
		gapi.Err(w, c, status.Errorf(codes.Unimplemented, "pogi does not implement %s", r.URL.Path))
		return
	}
	gapi.JSON(w, out)
}

func pemBlock(b []byte) []byte {
	if blk, _ := pem.Decode(b); blk != nil {
		return blk.Bytes
	}
	return nil
}

// ---- IAM policy on service accounts (stored, not enforced) ----

func (a *account) policy() *iampb.Policy {
	var p *iampb.Policy
	if len(a.Policy) > 0 {
		p = &iampb.Policy{}
		protojson.Unmarshal(a.Policy, p)
	}
	return gapi.Policy(p)
}

func (s *Service) GetIamPolicy(_ context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.account(req.Resource)
	if err != nil {
		return nil, err
	}
	return a.policy(), nil
}

func (s *Service) SetIamPolicy(_ context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.account(req.Resource)
	if err != nil {
		return nil, err
	}
	var cur *iampb.Policy
	if len(a.Policy) > 0 {
		cur = a.policy()
	}
	p, err := gapi.SetPolicy(cur, req.Policy)
	if err != nil {
		return nil, err
	}
	a.Policy, _ = protojson.Marshal(p)
	return proto.Clone(p).(*iampb.Policy), nil
}

// TestIamPermissions grants every permission asked for.
func (s *Service) TestIamPermissions(_ context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.account(req.Resource); err != nil {
		return nil, err
	}
	return &iampb.TestIamPermissionsResponse{Permissions: req.Permissions}, nil
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	accts := core.Table{Name: "Service accounts", Columns: []string{"Email", "Project", "Display name", "Disabled", "Keys"}}
	for _, e := range core.SortedKeys(s.st.Accounts) {
		a := s.st.Accounts[e]
		accts.Rows = append(accts.Rows, []string{e, a.Project, a.DisplayName, fmt.Sprint(a.Disabled), fmt.Sprint(len(a.Keys))})
	}
	roles := core.Table{Name: "Custom roles", Columns: []string{"Name", "Title", "Stage", "Permissions", "Deleted"}}
	for _, n := range core.SortedKeys(s.st.Roles) {
		r := s.st.Roles[n]
		roles.Rows = append(roles.Rows, []string{n, r.Title, r.Stage.String(), fmt.Sprint(len(r.Perms)), fmt.Sprint(r.Deleted)})
	}
	return []core.Table{accts, roles}
}

// Inspect shows a service account with its keys and policy, or a custom role; never private keys.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Service accounts":
		a := s.st.Accounts[id]
		if a == nil {
			return nil
		}
		keys := []any{}
		for _, k := range a.Keys {
			keys = append(keys, map[string]any{"id": k.ID, "created": k.Created, "expires": k.Expires, "disabled": k.Disabled})
		}
		return map[string]any{"account": a.proto(), "keys": keys, "policy": a.policy()}
	case "Custom roles":
		if r := s.st.Roles[id]; r != nil {
			return r.proto(true)
		}
	}
	return nil
}
