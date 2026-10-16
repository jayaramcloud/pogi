// Package iam emulates AWS IAM over the Query protocol: users, access keys,
// groups, roles, managed and inline policies, instance profiles, identity
// providers, server certificates and account settings. IAM is global, so
// state is per account and regions are ignored. Access keys created here are
// registered with the hub: requests signed with them run as their user.
package iam

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the IAM emulator.
type Service struct {
	hub  *core.Hub
	rpc  *core.RPC
	mu   sync.Mutex
	st   state
	rec  recorder
	jobs map[string]lastJob // service last accessed jobs, by id
}

type state struct {
	Accounts map[string]*Account `json:"accounts"`
}

// Account is the IAM state of one AWS account. Entities are keyed by
// lower-case name (IAM names are case-insensitive), providers by ARN.
type Account struct {
	Users            map[string]*User            `json:"users"`
	Groups           map[string]*Group           `json:"groups"`
	Roles            map[string]*Role            `json:"roles"`
	Policies         map[string]*Policy          `json:"policies"`
	InstanceProfiles map[string]*InstanceProfile `json:"instanceProfiles"`
	OIDC             map[string]*OIDCProvider    `json:"oidc"`
	SAML             map[string]*SAMLProvider    `json:"saml"`
	Certs            map[string]*ServerCert      `json:"certs"`
	Aliases          []string                    `json:"aliases,omitempty"`
	PasswordPolicy   *PasswordPolicy             `json:"passwordPolicy,omitempty"`
}

// The types below use AWS member names, so they are both the persisted state
// (JSON) and the response shapes (Query XML). `query:"-"` fields are internal;
// `json:"-"` fields are only filled in responses.

// Tag is an IAM tag.
type Tag struct{ Key, Value string }

// Boundary is an attached permissions boundary.
type Boundary struct{ PermissionsBoundaryType, PermissionsBoundaryArn string }

// perms are the policies of a user, group or role. Embedded unexported, so
// the Query encoder skips them while JSON keeps them.
type perms struct {
	Inline   map[string]string // inline policy name → document
	Attached []string          // managed policy ARNs
}

// AttachedPolicy is a managed policy attached to an entity.
type AttachedPolicy struct{ PolicyName, PolicyArn string }

// PolicyDetail is an inline policy in GetAccountAuthorizationDetails.
type PolicyDetail struct{ PolicyName, PolicyDocument string }

// User is an IAM user.
type User struct {
	Path, UserName, UserId, Arn string
	CreateDate                  core.Time
	PermissionsBoundary         *Boundary
	Tags                        []Tag
	perms
	Groups                  []string         `query:"-"`
	Keys                    []AccessKey      `query:"-"`
	Login                   *LoginProfile    `query:"-"`
	SSHKeys                 []SSHKey         `query:"-"`
	UserPolicyList          []PolicyDetail   `json:"-" query:"UserPolicyList"`
	GroupList               []string         `json:"-" query:"GroupList"`
	AttachedManagedPolicies []AttachedPolicy `json:"-" query:"AttachedManagedPolicies"`
}

// AccessKey is a user's access key; the secret is kept for signature checks
// but only returned at creation.
type AccessKey struct {
	UserName, AccessKeyId, Status, SecretAccessKey string
	CreateDate                                     core.Time
}

// LoginProfile is a user's console password (the password is not kept).
type LoginProfile struct {
	UserName              string
	CreateDate            core.Time
	PasswordResetRequired bool
}

// Group is an IAM group; memberships live on the users.
type Group struct {
	Path, GroupName, GroupId, Arn string
	CreateDate                    core.Time
	perms
	GroupPolicyList         []PolicyDetail   `json:"-" query:"GroupPolicyList"`
	AttachedManagedPolicies []AttachedPolicy `json:"-" query:"AttachedManagedPolicies"`
}

// Role is an IAM role. AssumeRolePolicyDocument is kept raw and URL-encoded
// on output, like AWS returns it.
type Role struct {
	Path, RoleName, RoleId, Arn string
	CreateDate                  core.Time
	AssumeRolePolicyDocument    string
	Description                 string
	MaxSessionDuration          int
	PermissionsBoundary         *Boundary
	Tags                        []Tag
	RoleLastUsed                *struct{} `json:"-" query:"RoleLastUsed"`
	perms
	RolePolicyList          []PolicyDetail    `json:"-" query:"RolePolicyList"`
	AttachedManagedPolicies []AttachedPolicy  `json:"-" query:"AttachedManagedPolicies"`
	InstanceProfileList     []InstanceProfile `json:"-" query:"InstanceProfileList"`
}

// out is a response copy of the role.
func (r *Role) out() Role {
	o := *r
	o.AssumeRolePolicyDocument = enc(o.AssumeRolePolicyDocument)
	return o
}

// InstanceProfile holds at most one role, by name.
type InstanceProfile struct {
	Path, InstanceProfileName, InstanceProfileId, Arn string
	CreateDate                                        core.Time
	Roles                                             []Role `json:"-" query:"Roles"`
	Tags                                              []Tag
	RoleNames                                         []string `query:"-"`
}

// OIDCProvider is an OpenID Connect identity provider; Url has no scheme.
type OIDCProvider struct {
	Arn, Url                     string
	ClientIDList, ThumbprintList []string
	CreateDate                   core.Time
	Tags                         []Tag
}

// SAMLProvider is a SAML identity provider.
type SAMLProvider struct {
	Arn, SAMLMetadataDocument string
	CreateDate                core.Time
	Tags                      []Tag
}

// CertMeta is ServerCertificateMetadata.
type CertMeta struct {
	Path, ServerCertificateName, ServerCertificateId, Arn string
	UploadDate, Expiration                                core.Time
}

// ServerCert is an uploaded server certificate (the private key is not kept).
type ServerCert struct {
	ServerCertificateMetadata         CertMeta
	CertificateBody, CertificateChain string
	Tags                              []Tag
}

// PasswordPolicy is the account password policy (also the update input).
type PasswordPolicy struct {
	MinimumPasswordLength      int
	RequireSymbols             bool
	RequireNumbers             bool
	RequireUppercaseCharacters bool
	RequireLowercaseCharacters bool
	AllowUsersToChangePassword bool
	ExpirePasswords            bool
	MaxPasswordAge             int
	PasswordReusePrevention    int
	HardExpiry                 bool
}

// New creates the IAM service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	if hub != nil {
		hub.Authorize[core.AWS] = s.authorize
	}
	ops := map[string]core.Handler{
		"CreateUser":                              core.H(s.createUser),
		"GetUser":                                 core.H(s.getUser),
		"ListUsers":                               core.H(s.listUsers),
		"UpdateUser":                              core.H(s.updateUser),
		"DeleteUser":                              core.H(s.deleteUser),
		"CreateAccessKey":                         core.H(s.createAccessKey),
		"ListAccessKeys":                          core.H(s.listAccessKeys),
		"UpdateAccessKey":                         core.H(s.updateAccessKey),
		"DeleteAccessKey":                         core.H(s.deleteAccessKey),
		"GetAccessKeyLastUsed":                    core.H(s.getAccessKeyLastUsed),
		"CreateLoginProfile":                      core.H(s.loginProfile),
		"GetLoginProfile":                         core.H(s.loginProfile),
		"UpdateLoginProfile":                      core.H(s.loginProfile),
		"DeleteLoginProfile":                      core.H(s.loginProfile),
		"ListMFADevices":                          core.H(s.empty),
		"ListVirtualMFADevices":                   core.H(s.empty),
		"UploadSSHPublicKey":                      core.H(s.uploadSSHPublicKey),
		"GetSSHPublicKey":                         core.H(s.getSSHPublicKey),
		"ListSSHPublicKeys":                       core.H(s.listSSHPublicKeys),
		"UpdateSSHPublicKey":                      core.H(s.changeSSHPublicKey),
		"DeleteSSHPublicKey":                      core.H(s.changeSSHPublicKey),
		"ListSigningCertificates":                 core.H(s.empty),
		"ListServiceSpecificCredentials":          core.H(s.empty),
		"CreateGroup":                             core.H(s.createGroup),
		"GetGroup":                                core.H(s.getGroup),
		"ListGroups":                              core.H(s.listGroups),
		"DeleteGroup":                             core.H(s.deleteGroup),
		"AddUserToGroup":                          core.H(s.membership),
		"RemoveUserFromGroup":                     core.H(s.membership),
		"ListGroupsForUser":                       core.H(s.listGroupsForUser),
		"CreateRole":                              core.H(s.createRole),
		"GetRole":                                 core.H(s.getRole),
		"ListRoles":                               core.H(s.listRoles),
		"UpdateRole":                              core.H(s.updateRole),
		"UpdateAssumeRolePolicy":                  core.H(s.updateAssumeRolePolicy),
		"DeleteRole":                              core.H(s.deleteRole),
		"CreateServiceLinkedRole":                 core.H(s.createServiceLinkedRole),
		"DeleteServiceLinkedRole":                 core.H(s.deleteServiceLinkedRole),
		"GetServiceLinkedRoleDeletionStatus":      core.H(s.deletionStatus),
		"PutUserPermissionsBoundary":              core.H(s.boundary),
		"DeleteUserPermissionsBoundary":           core.H(s.boundary),
		"PutRolePermissionsBoundary":              core.H(s.boundary),
		"DeleteRolePermissionsBoundary":           core.H(s.boundary),
		"CreatePolicy":                            core.H(s.createPolicy),
		"GetPolicy":                               core.H(s.getPolicy),
		"ListPolicies":                            core.H(s.listPolicies),
		"DeletePolicy":                            core.H(s.deletePolicy),
		"CreatePolicyVersion":                     core.H(s.createPolicyVersion),
		"GetPolicyVersion":                        core.H(s.getPolicyVersion),
		"ListPolicyVersions":                      core.H(s.listPolicyVersions),
		"DeletePolicyVersion":                     core.H(s.deletePolicyVersion),
		"SetDefaultPolicyVersion":                 core.H(s.setDefaultPolicyVersion),
		"ListEntitiesForPolicy":                   core.H(s.listEntitiesForPolicy),
		"CreateInstanceProfile":                   core.H(s.createInstanceProfile),
		"GetInstanceProfile":                      core.H(s.getInstanceProfile),
		"DeleteInstanceProfile":                   core.H(s.deleteInstanceProfile),
		"ListInstanceProfiles":                    core.H(s.listInstanceProfiles),
		"AddRoleToInstanceProfile":                core.H(s.profileRole),
		"RemoveRoleFromInstanceProfile":           core.H(s.profileRole),
		"ListInstanceProfilesForRole":             core.H(s.listInstanceProfilesForRole),
		"CreateOpenIDConnectProvider":             core.H(s.createOIDC),
		"GetOpenIDConnectProvider":                core.H(s.getOIDC),
		"DeleteOpenIDConnectProvider":             core.H(s.updateOIDC),
		"AddClientIDToOpenIDConnectProvider":      core.H(s.updateOIDC),
		"RemoveClientIDFromOpenIDConnectProvider": core.H(s.updateOIDC),
		"UpdateOpenIDConnectProviderThumbprint":   core.H(s.updateOIDC),
		"ListOpenIDConnectProviders":              core.H(s.listOIDC),
		"CreateSAMLProvider":                      core.H(s.createSAML),
		"GetSAMLProvider":                         core.H(s.samlProvider),
		"UpdateSAMLProvider":                      core.H(s.samlProvider),
		"DeleteSAMLProvider":                      core.H(s.samlProvider),
		"ListSAMLProviders":                       core.H(s.listSAML),
		"UploadServerCertificate":                 core.H(s.uploadServerCertificate),
		"GetServerCertificate":                    core.H(s.serverCertificate),
		"DeleteServerCertificate":                 core.H(s.serverCertificate),
		"ListServerCertificates":                  core.H(s.listServerCertificates),
		"GetAccountSummary":                       core.H(s.accountSummary),
		"CreateAccountAlias":                      core.H(s.accountAlias),
		"DeleteAccountAlias":                      core.H(s.accountAlias),
		"ListAccountAliases":                      core.H(s.accountAlias),
		"GetAccountPasswordPolicy":                core.H(s.passwordPolicy),
		"UpdateAccountPasswordPolicy":             core.H(s.passwordPolicy),
		"DeleteAccountPasswordPolicy":             core.H(s.passwordPolicy),
		"GenerateServiceLastAccessedDetails":      core.H(s.generateServiceLastAccessedDetails),
		"GetServiceLastAccessedDetails":           core.H(s.getServiceLastAccessedDetails),
		"GetAccountAuthorizationDetails":          core.H(s.authorizationDetails),
		"SimulatePrincipalPolicy":                 core.H(s.simulate),
		"SimulateCustomPolicy":                    core.H(s.simulate),
	}
	for _, kind := range []string{"User", "Group", "Role"} {
		ops["Attach"+kind+"Policy"] = core.H(s.attach)
		ops["Detach"+kind+"Policy"] = core.H(s.detach)
		ops["ListAttached"+kind+"Policies"] = core.H(s.listAttached)
		ops["Put"+kind+"Policy"] = core.H(s.putInline)
		ops["Get"+kind+"Policy"] = core.H(s.getInline)
		ops["Delete"+kind+"Policy"] = core.H(s.deleteInline)
		ops["List"+kind+"Policies"] = core.H(s.listInline)
	}
	for _, kind := range []string{"User", "Role", "Policy", "InstanceProfile", "OpenIDConnectProvider", "SAMLProvider", "ServerCertificate"} {
		ops["Tag"+kind] = core.H(s.tags)
		ops["Untag"+kind] = core.H(s.tags)
		ops["List"+kind+"Tags"] = core.H(s.tags)
	}
	s.rpc = &core.RPC{XMLNS: "https://iam.amazonaws.com/doc/2010-05-08/", Ops: ops}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "iam", Cloud: core.AWS, Title: "IAM", Signing: []string{"iam"}, Targets: []string{"AWSIdentityManagementV20100508"}, Protocol: "query"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Accounts: map[string]*Account{}}
	s.jobs = nil
	s.mu.Unlock()
	s.rec.mu.Lock()
	s.rec.m = nil
	s.rec.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores state and re-registers the persisted access keys with the hub.
func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Accounts == nil {
		st.Accounts = map[string]*Account{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st = st
	for id, a := range st.Accounts {
		for _, u := range a.Users {
			s.register(id, u)
		}
	}
	return nil
}

// register maps the user's access keys to the user, so that requests signed
// with them run as the user.
func (s *Service) register(account string, u *User) {
	if s.hub == nil {
		return
	}
	for _, k := range u.Keys {
		if k.Status == "Active" {
			s.hub.AddIdentity(k.AccessKeyId, core.Identity{Account: account, ARN: u.Arn, UserID: u.UserId, Secret: k.SecretAccessKey})
		}
	}
}

// acct returns an account's state, creating it on first use; lock held.
func (s *Service) acct(id string) *Account {
	a := s.st.Accounts[id]
	if a == nil {
		a = &Account{}
		s.st.Accounts[id] = a
	}
	ensure(&a.Users)
	ensure(&a.Groups)
	ensure(&a.Roles)
	ensure(&a.Policies)
	ensure(&a.InstanceProfiles)
	ensure(&a.OIDC)
	ensure(&a.SAML)
	ensure(&a.Certs)
	return a
}

func ensure[T any](m *map[string]T) {
	if *m == nil {
		*m = map[string]T{}
	}
}

var lower = strings.ToLower

// get finds an entity by case-insensitive name.
func get[T any](m map[string]*T, kind, name string) (*T, error) {
	if v := m[lower(name)]; v != nil {
		return v, nil
	}
	return nil, core.Errorf(404, "NoSuchEntity", "The %s with name %s cannot be found.", kind, name)
}

func exists(kind, name string) error {
	return core.Errorf(409, "EntityAlreadyExists", "%s with name %s already exists.", kind, name)
}

func conflict(what string) error {
	return core.Errorf(409, "DeleteConflict", "Cannot delete entity, %s.", what)
}

func now() core.Time { return core.Time(time.Now().UTC().Truncate(time.Second)) }

func newID(prefix string) string { return prefix + core.RandString(17, core.UpperNum) }

func iamARN(c *core.Ctx, kind, path, name string) string {
	return c.GlobalARN("iam", kind+path+name)
}

// without returns s minus element i, never touching the original array.
func without[T any](s []T, i int) []T { return append(s[:i:i], s[i+1:]...) }

var namePattern = regexp.MustCompile(`^[\w+=,.@-]+$`)

func checkName(field, name string, max int) error {
	if len(name) == 0 || len(name) > max || !namePattern.MatchString(name) {
		return core.Errorf(400, "ValidationError", "1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]+ and have length between 1 and %d", name, field, max)
	}
	return nil
}

func checkPath(p string) (string, error) {
	if p == "" {
		return "/", nil
	}
	if !strings.HasPrefix(p, "/") || !strings.HasSuffix(p, "/") || len(p) > 512 {
		return "", core.Errorf(400, "ValidationError", "The specified value for path is invalid. It must begin and end with / and contain only alphanumeric characters and/or / characters.")
	}
	return p, nil
}

// page returns one page of items after an offset Marker.
func page[T any](key string, items []T, marker string, maxItems int) map[string]any {
	start, _ := strconv.Atoi(marker)
	start = min(max(start, 0), len(items))
	if maxItems <= 0 {
		maxItems = 100
	}
	end := min(start+maxItems, len(items))
	out := map[string]any{key: items[start:end], "IsTruncated": end < len(items)}
	if end < len(items) {
		out["Marker"] = strconv.Itoa(end)
	}
	return out
}

type listInput struct {
	PathPrefix, Marker string
	MaxItems           int
}

func (s *Service) empty(*core.Ctx, *struct{}) (any, error) {
	return map[string]any{"IsTruncated": false}, nil
}

// ---- users and access keys ----

func (s *Service) createUser(c *core.Ctx, in *struct {
	UserName, Path, PermissionsBoundary string
	Tags                                []Tag
}) (any, error) {
	path, err := checkPath(in.Path)
	if err != nil {
		return nil, err
	}
	if err := checkName("userName", in.UserName, 64); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	if a.Users[lower(in.UserName)] != nil {
		return nil, exists("User", in.UserName)
	}
	b, err := s.boundaryOf(in.PermissionsBoundary)
	if err != nil {
		return nil, err
	}
	u := &User{Path: path, UserName: in.UserName, UserId: newID("AIDA"), Arn: iamARN(c, "user", path, in.UserName),
		CreateDate: now(), PermissionsBoundary: b, Tags: in.Tags}
	a.Users[lower(u.UserName)] = u
	return map[string]any{"User": *u}, nil
}

// caller is the name of the calling IAM user, or "" for roots and roles.
func caller(c *core.Ctx) string {
	_, _, _, res, _ := core.ParseARN(c.Principal)
	if !strings.HasPrefix(res, "user/") {
		return ""
	}
	return res[strings.LastIndex(res, "/")+1:]
}

// userOrCaller resolves name, or the calling user when name is empty; lock held.
func (s *Service) userOrCaller(c *core.Ctx, name string) (*User, error) {
	if name == "" {
		if name = caller(c); name == "" {
			return nil, core.Errorf(400, "ValidationError", "Must specify userName when calling with non-User credentials")
		}
	}
	return get(s.acct(c.Account).Users, "user", name)
}

func (s *Service) getUser(c *core.Ctx, in *struct{ UserName string }) (any, error) {
	if in.UserName == "" && strings.HasSuffix(c.Principal, ":root") {
		return map[string]any{"User": User{UserId: c.Account, Arn: c.Principal, CreateDate: core.Time(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))}}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	u, err := s.userOrCaller(c, in.UserName)
	if err != nil {
		return nil, err
	}
	return map[string]any{"User": *u}, nil
}

func (s *Service) listUsers(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	var out []User
	for _, k := range core.SortedKeys(a.Users) {
		if u := a.Users[k]; strings.HasPrefix(u.Path, in.PathPrefix) {
			out = append(out, *u)
		}
	}
	return page("Users", out, in.Marker, in.MaxItems), nil
}

func (s *Service) updateUser(c *core.Ctx, in *struct{ UserName, NewUserName, NewPath string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	u, err := get(a.Users, "user", in.UserName)
	if err != nil {
		return nil, err
	}
	path, name := u.Path, u.UserName
	if in.NewPath != "" {
		if path, err = checkPath(in.NewPath); err != nil {
			return nil, err
		}
	}
	if in.NewUserName != "" {
		if err := checkName("newUserName", in.NewUserName, 64); err != nil {
			return nil, err
		}
		if o := a.Users[lower(in.NewUserName)]; o != nil && o != u {
			return nil, exists("User", in.NewUserName)
		}
		name = in.NewUserName
	}
	delete(a.Users, lower(u.UserName))
	u.UserName, u.Path, u.Arn = name, path, iamARN(c, "user", path, name)
	a.Users[lower(name)] = u
	s.register(c.Account, u)
	return nil, nil
}

func (s *Service) deleteUser(c *core.Ctx, in *struct{ UserName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	u, err := get(a.Users, "user", in.UserName)
	if err != nil {
		return nil, err
	}
	switch {
	case len(u.Keys) > 0:
		return nil, conflict("must delete access keys first")
	case len(u.Inline) > 0 || len(u.Attached) > 0:
		return nil, conflict("must delete policies first")
	case len(u.Groups) > 0:
		return nil, conflict("must remove users from group first")
	case u.Login != nil:
		return nil, conflict("must delete login profile first")
	case len(u.SSHKeys) > 0:
		return nil, conflict("must delete SSH public keys first")
	}
	delete(a.Users, lower(u.UserName))
	return nil, nil
}

func (s *Service) createAccessKey(c *core.Ctx, in *struct{ UserName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, err := s.userOrCaller(c, in.UserName)
	if err != nil {
		return nil, err
	}
	if len(u.Keys) >= 2 {
		return nil, core.Errorf(409, "LimitExceeded", "Cannot exceed quota for AccessKeysPerUser: 2")
	}
	k := AccessKey{AccessKeyId: "AKIA" + core.RandString(16, core.UpperNum), Status: "Active",
		SecretAccessKey: core.RandString(40, core.AlphaNum), CreateDate: now()}
	u.Keys = append(u.Keys, k)
	s.register(c.Account, u)
	k.UserName = u.UserName
	return map[string]any{"AccessKey": k}, nil
}

// ImportAccessKey gives a user a fixed access key (pogi.yaml identities). It
// is stored like CreateAccessKey stores keys and registered with the hub with
// its secret; importing a key the user already has only re-registers it.
func (s *Service) ImportAccessKey(account, user, akid, secret string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, err := get(s.acct(account).Users, "user", user)
	if err != nil {
		return err
	}
	if i := slices.IndexFunc(u.Keys, func(k AccessKey) bool { return k.AccessKeyId == akid }); i >= 0 {
		u.Keys[i].SecretAccessKey = secret
	} else {
		u.Keys = append(u.Keys, AccessKey{AccessKeyId: akid, Status: "Active", SecretAccessKey: secret, CreateDate: now()})
	}
	s.register(account, u)
	return nil
}

func (s *Service) listAccessKeys(c *core.Ctx, in *struct {
	UserName, Marker string
	MaxItems         int
}) (any, error) {
	if in.UserName == "" && caller(c) == "" { // root keys are not modeled
		return map[string]any{"IsTruncated": false}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	u, err := s.userOrCaller(c, in.UserName)
	if err != nil {
		return nil, err
	}
	keys := make([]AccessKey, len(u.Keys))
	for i, k := range u.Keys {
		k.UserName, k.SecretAccessKey = u.UserName, ""
		keys[i] = k
	}
	return page("AccessKeyMetadata", keys, in.Marker, in.MaxItems), nil
}

// key finds an access key of the account (of userName, when given); lock held.
func (s *Service) key(c *core.Ctx, userName, id string) (*User, int, error) {
	for _, u := range s.acct(c.Account).Users {
		if userName != "" && !strings.EqualFold(u.UserName, userName) {
			continue
		}
		for i, k := range u.Keys {
			if k.AccessKeyId == id {
				return u, i, nil
			}
		}
	}
	return nil, 0, core.Errorf(404, "NoSuchEntity", "The Access Key with id %s cannot be found.", id)
}

func (s *Service) updateAccessKey(c *core.Ctx, in *struct{ UserName, AccessKeyId, Status string }) (any, error) {
	if in.Status != "Active" && in.Status != "Inactive" {
		return nil, core.Errorf(400, "ValidationError", "Status must be Active or Inactive.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	u, i, err := s.key(c, in.UserName, in.AccessKeyId)
	if err != nil {
		return nil, err
	}
	u.Keys[i].Status = in.Status
	if in.Status == "Active" {
		s.register(c.Account, u)
	} else {
		s.hub.RemoveIdentity(in.AccessKeyId)
	}
	return nil, nil
}

func (s *Service) deleteAccessKey(c *core.Ctx, in *struct{ UserName, AccessKeyId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, i, err := s.key(c, in.UserName, in.AccessKeyId)
	if err != nil {
		return nil, err
	}
	u.Keys = without(u.Keys, i)
	s.hub.RemoveIdentity(in.AccessKeyId)
	return nil, nil
}

func (s *Service) getAccessKeyLastUsed(c *core.Ctx, in *struct{ AccessKeyId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, _, err := s.key(c, "", in.AccessKeyId)
	if err != nil {
		return nil, err
	}
	return map[string]any{"UserName": u.UserName, "AccessKeyLastUsed": struct{ ServiceName, Region string }{"N/A", "N/A"}}, nil
}

func (s *Service) loginProfile(c *core.Ctx, in *struct {
	UserName, Password    string
	PasswordResetRequired *bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, err := s.userOrCaller(c, in.UserName)
	if err != nil {
		return nil, err
	}
	switch {
	case c.Op == "CreateLoginProfile" && u.Login != nil:
		return nil, core.Errorf(409, "EntityAlreadyExists", "Login Profile for user %s already exists.", u.UserName)
	case c.Op == "CreateLoginProfile":
		u.Login = &LoginProfile{CreateDate: now(), PasswordResetRequired: in.PasswordResetRequired != nil && *in.PasswordResetRequired}
	case u.Login == nil:
		return nil, core.Errorf(404, "NoSuchEntity", "Login Profile for User %s cannot be found.", u.UserName)
	case c.Op == "UpdateLoginProfile":
		if in.PasswordResetRequired != nil {
			u.Login.PasswordResetRequired = *in.PasswordResetRequired
		}
		return nil, nil
	case c.Op == "DeleteLoginProfile":
		u.Login = nil
		return nil, nil
	}
	lp := *u.Login
	lp.UserName = u.UserName
	return map[string]any{"LoginProfile": lp}, nil
}

// boundaryOf validates a permissions boundary policy ARN.
func (s *Service) boundaryOf(arn string) (*Boundary, error) {
	if arn == "" {
		return nil, nil
	}
	if _, err := s.policy(arn); err != nil {
		return nil, err
	}
	return &Boundary{"PermissionsBoundaryPolicy", arn}, nil
}

func (s *Service) boundary(c *core.Ctx, in *struct{ UserName, RoleName, PermissionsBoundary string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	var b **Boundary
	if strings.Contains(c.Op, "User") {
		u, err := get(a.Users, "user", in.UserName)
		if err != nil {
			return nil, err
		}
		b = &u.PermissionsBoundary
	} else {
		r, err := get(a.Roles, "role", in.RoleName)
		if err != nil {
			return nil, err
		}
		b = &r.PermissionsBoundary
	}
	if strings.HasPrefix(c.Op, "Delete") {
		*b = nil
		return nil, nil
	}
	nb, err := s.boundaryOf(in.PermissionsBoundary)
	if err != nil {
		return nil, err
	}
	*b = nb
	return nil, nil
}

// ---- groups ----

func (s *Service) createGroup(c *core.Ctx, in *struct{ GroupName, Path string }) (any, error) {
	path, err := checkPath(in.Path)
	if err != nil {
		return nil, err
	}
	if err := checkName("groupName", in.GroupName, 128); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	if a.Groups[lower(in.GroupName)] != nil {
		return nil, exists("Group", in.GroupName)
	}
	g := &Group{Path: path, GroupName: in.GroupName, GroupId: newID("AGPA"), Arn: iamARN(c, "group", path, in.GroupName), CreateDate: now()}
	a.Groups[lower(g.GroupName)] = g
	return map[string]any{"Group": *g}, nil
}

func (s *Service) getGroup(c *core.Ctx, in *struct {
	GroupName, Marker string
	MaxItems          int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	g, err := get(a.Groups, "group", in.GroupName)
	if err != nil {
		return nil, err
	}
	var users []User
	for _, k := range core.SortedKeys(a.Users) {
		if u := a.Users[k]; slices.Contains(u.Groups, g.GroupName) {
			users = append(users, *u)
		}
	}
	out := page("Users", users, in.Marker, in.MaxItems)
	out["Group"] = *g
	return out, nil
}

func (s *Service) listGroups(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	var out []Group
	for _, k := range core.SortedKeys(a.Groups) {
		if g := a.Groups[k]; strings.HasPrefix(g.Path, in.PathPrefix) {
			out = append(out, *g)
		}
	}
	return page("Groups", out, in.Marker, in.MaxItems), nil
}

func (s *Service) deleteGroup(c *core.Ctx, in *struct{ GroupName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	g, err := get(a.Groups, "group", in.GroupName)
	if err != nil {
		return nil, err
	}
	for _, u := range a.Users {
		if slices.Contains(u.Groups, g.GroupName) {
			return nil, conflict("must remove users from group first")
		}
	}
	if len(g.Inline) > 0 || len(g.Attached) > 0 {
		return nil, conflict("must delete policies first")
	}
	delete(a.Groups, lower(g.GroupName))
	return nil, nil
}

func (s *Service) membership(c *core.Ctx, in *struct{ GroupName, UserName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	g, err := get(a.Groups, "group", in.GroupName)
	if err != nil {
		return nil, err
	}
	u, err := get(a.Users, "user", in.UserName)
	if err != nil {
		return nil, err
	}
	i := slices.Index(u.Groups, g.GroupName)
	switch {
	case c.Op == "AddUserToGroup" && i < 0:
		u.Groups = append(u.Groups, g.GroupName)
	case c.Op == "RemoveUserFromGroup" && i >= 0:
		u.Groups = without(u.Groups, i)
	}
	return nil, nil
}

func (s *Service) listGroupsForUser(c *core.Ctx, in *struct {
	UserName, Marker string
	MaxItems         int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	u, err := get(a.Users, "user", in.UserName)
	if err != nil {
		return nil, err
	}
	var out []Group
	for _, n := range u.Groups {
		if g := a.Groups[lower(n)]; g != nil {
			out = append(out, *g)
		}
	}
	return page("Groups", out, in.Marker, in.MaxItems), nil
}

// ---- roles ----

func checkDuration(d int) error {
	if d < 3600 || d > 43200 {
		return core.Errorf(400, "ValidationError", "1 validation error detected: Value '%d' at 'maxSessionDuration' failed to satisfy constraint: Member must have value between 3600 and 43200", d)
	}
	return nil
}

func (s *Service) createRole(c *core.Ctx, in *struct {
	RoleName, Path, AssumeRolePolicyDocument, Description, PermissionsBoundary string
	MaxSessionDuration                                                         int
	Tags                                                                       []Tag
}) (any, error) {
	path, err := checkPath(in.Path)
	if err != nil {
		return nil, err
	}
	if err := checkName("roleName", in.RoleName, 64); err != nil {
		return nil, err
	}
	if err := checkDoc(in.AssumeRolePolicyDocument, true); err != nil {
		return nil, err
	}
	if in.MaxSessionDuration == 0 {
		in.MaxSessionDuration = 3600
	}
	if err := checkDuration(in.MaxSessionDuration); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	if a.Roles[lower(in.RoleName)] != nil {
		return nil, exists("Role", in.RoleName)
	}
	b, err := s.boundaryOf(in.PermissionsBoundary)
	if err != nil {
		return nil, err
	}
	r := &Role{Path: path, RoleName: in.RoleName, RoleId: newID("AROA"), Arn: iamARN(c, "role", path, in.RoleName), CreateDate: now(),
		AssumeRolePolicyDocument: in.AssumeRolePolicyDocument, Description: in.Description, MaxSessionDuration: in.MaxSessionDuration,
		PermissionsBoundary: b, Tags: in.Tags}
	a.Roles[lower(r.RoleName)] = r
	return map[string]any{"Role": r.out()}, nil
}

// RoleExists tells STS whether a role exists in an account.
func (s *Service) RoleExists(account, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.st.Accounts[account]
	return a != nil && a.Roles[lower(name)] != nil
}

func (s *Service) getRole(c *core.Ctx, in *struct{ RoleName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := get(s.acct(c.Account).Roles, "role", in.RoleName)
	if err != nil {
		return nil, err
	}
	o := r.out()
	o.RoleLastUsed = &struct{}{}
	return map[string]any{"Role": o}, nil
}

func (s *Service) listRoles(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	var out []Role
	for _, k := range core.SortedKeys(a.Roles) {
		if r := a.Roles[k]; strings.HasPrefix(r.Path, in.PathPrefix) {
			out = append(out, r.out())
		}
	}
	return page("Roles", out, in.Marker, in.MaxItems), nil
}

func (s *Service) updateRole(c *core.Ctx, in *struct {
	RoleName           string
	Description        *string
	MaxSessionDuration int
}) (any, error) {
	if in.MaxSessionDuration != 0 {
		if err := checkDuration(in.MaxSessionDuration); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := get(s.acct(c.Account).Roles, "role", in.RoleName)
	if err != nil {
		return nil, err
	}
	if in.Description != nil {
		r.Description = *in.Description
	}
	if in.MaxSessionDuration != 0 {
		r.MaxSessionDuration = in.MaxSessionDuration
	}
	return nil, nil
}

func (s *Service) updateAssumeRolePolicy(c *core.Ctx, in *struct{ RoleName, PolicyDocument string }) (any, error) {
	if err := checkDoc(in.PolicyDocument, true); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := get(s.acct(c.Account).Roles, "role", in.RoleName)
	if err != nil {
		return nil, err
	}
	r.AssumeRolePolicyDocument = in.PolicyDocument
	return nil, nil
}

func (s *Service) deleteRole(c *core.Ctx, in *struct{ RoleName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	r, err := get(a.Roles, "role", in.RoleName)
	if err != nil {
		return nil, err
	}
	if len(r.Attached) > 0 {
		return nil, conflict("must detach all policies first")
	}
	if len(r.Inline) > 0 {
		return nil, conflict("must delete policies first")
	}
	for _, ip := range a.InstanceProfiles {
		if slices.Contains(ip.RoleNames, r.RoleName) {
			return nil, conflict("must remove roles from instance profile first")
		}
	}
	delete(a.Roles, lower(r.RoleName))
	return nil, nil
}

// slrNames are the service-linked role suffixes that do not follow the
// capitalized-service-name rule.
var slrNames = map[string]string{
	"autoscaling": "AutoScaling", "elasticloadbalancing": "ElasticLoadBalancing", "ecs": "ECS", "rds": "RDS",
	"eks": "AmazonEKS", "es": "AmazonElasticsearchService", "opensearchservice": "AmazonOpenSearchService",
	"spot": "EC2Spot", "elasticache": "ElastiCache", "sso": "SSO",
}

func (s *Service) createServiceLinkedRole(c *core.Ctx, in *struct{ AWSServiceName, Description, CustomSuffix string }) (any, error) {
	if err := checkName("AWSServiceName", in.AWSServiceName, 128); err != nil {
		return nil, err
	}
	base := strings.TrimSuffix(in.AWSServiceName, ".amazonaws.com")
	name := slrNames[base]
	if name == "" {
		for _, w := range strings.FieldsFunc(base, func(r rune) bool { return r == '.' || r == '-' }) {
			name += strings.ToUpper(w[:1]) + w[1:]
		}
	}
	name = "AWSServiceRoleFor" + name
	if in.CustomSuffix != "" {
		name += "_" + in.CustomSuffix
	}
	path := "/aws-service-role/" + in.AWSServiceName + "/"
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	if a.Roles[lower(name)] != nil {
		return nil, core.Errorf(400, "InvalidInput", "Service role name %s has been taken in this account, please try a different suffix.", name)
	}
	r := &Role{Path: path, RoleName: name, RoleId: newID("AROA"), Arn: iamARN(c, "role", path, name), CreateDate: now(),
		AssumeRolePolicyDocument: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":["` + in.AWSServiceName + `"]},"Action":["sts:AssumeRole"]}]}`,
		Description:              in.Description, MaxSessionDuration: 3600}
	a.Roles[lower(name)] = r
	return map[string]any{"Role": r.out()}, nil
}

func (s *Service) deleteServiceLinkedRole(c *core.Ctx, in *struct{ RoleName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	r, err := get(a.Roles, "role", in.RoleName)
	if err != nil {
		return nil, err
	}
	delete(a.Roles, lower(r.RoleName))
	return map[string]string{"DeletionTaskId": "task" + r.Path + r.RoleName + "/" + core.UUID()}, nil
}

func (s *Service) deletionStatus(*core.Ctx, *struct{ DeletionTaskId string }) (any, error) {
	return map[string]string{"Status": "SUCCEEDED"}, nil
}

// ---- instance profiles ----

// profileOut is a response copy of ip with its roles; lock held.
func (a *Account) profileOut(ip *InstanceProfile) InstanceProfile {
	o := *ip
	o.Roles = nil
	for _, n := range ip.RoleNames {
		if r := a.Roles[lower(n)]; r != nil {
			o.Roles = append(o.Roles, r.out())
		}
	}
	return o
}

func (s *Service) createInstanceProfile(c *core.Ctx, in *struct {
	InstanceProfileName, Path string
	Tags                      []Tag
}) (any, error) {
	path, err := checkPath(in.Path)
	if err != nil {
		return nil, err
	}
	if err := checkName("instanceProfileName", in.InstanceProfileName, 128); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	if a.InstanceProfiles[lower(in.InstanceProfileName)] != nil {
		return nil, exists("Instance Profile", in.InstanceProfileName)
	}
	ip := &InstanceProfile{Path: path, InstanceProfileName: in.InstanceProfileName, InstanceProfileId: newID("AIPA"),
		Arn: iamARN(c, "instance-profile", path, in.InstanceProfileName), CreateDate: now(), Tags: in.Tags}
	a.InstanceProfiles[lower(ip.InstanceProfileName)] = ip
	return map[string]any{"InstanceProfile": a.profileOut(ip)}, nil
}

func (s *Service) getInstanceProfile(c *core.Ctx, in *struct{ InstanceProfileName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	ip, err := get(a.InstanceProfiles, "instance profile", in.InstanceProfileName)
	if err != nil {
		return nil, err
	}
	return map[string]any{"InstanceProfile": a.profileOut(ip)}, nil
}

func (s *Service) deleteInstanceProfile(c *core.Ctx, in *struct{ InstanceProfileName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	ip, err := get(a.InstanceProfiles, "instance profile", in.InstanceProfileName)
	if err != nil {
		return nil, err
	}
	if len(ip.RoleNames) > 0 {
		return nil, conflict("must remove roles from instance profile first")
	}
	delete(a.InstanceProfiles, lower(ip.InstanceProfileName))
	return nil, nil
}

func (s *Service) listInstanceProfiles(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	var out []InstanceProfile
	for _, k := range core.SortedKeys(a.InstanceProfiles) {
		if ip := a.InstanceProfiles[k]; strings.HasPrefix(ip.Path, in.PathPrefix) {
			out = append(out, a.profileOut(ip))
		}
	}
	return page("InstanceProfiles", out, in.Marker, in.MaxItems), nil
}

func (s *Service) profileRole(c *core.Ctx, in *struct{ InstanceProfileName, RoleName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	ip, err := get(a.InstanceProfiles, "instance profile", in.InstanceProfileName)
	if err != nil {
		return nil, err
	}
	r, err := get(a.Roles, "role", in.RoleName)
	if err != nil {
		return nil, err
	}
	i := slices.Index(ip.RoleNames, r.RoleName)
	switch {
	case c.Op == "AddRoleToInstanceProfile" && len(ip.RoleNames) > 0:
		return nil, core.Errorf(409, "LimitExceeded", "Cannot exceed quota for InstanceSessionsPerInstanceProfile: 1")
	case c.Op == "AddRoleToInstanceProfile":
		ip.RoleNames = append(ip.RoleNames, r.RoleName)
	case i < 0:
		return nil, core.Errorf(404, "NoSuchEntity", "The role with name %s is not in instance profile %s.", r.RoleName, ip.InstanceProfileName)
	default:
		ip.RoleNames = without(ip.RoleNames, i)
	}
	return nil, nil
}

func (s *Service) listInstanceProfilesForRole(c *core.Ctx, in *struct {
	RoleName, Marker string
	MaxItems         int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	r, err := get(a.Roles, "role", in.RoleName)
	if err != nil {
		return nil, err
	}
	var out []InstanceProfile
	for _, k := range core.SortedKeys(a.InstanceProfiles) {
		if ip := a.InstanceProfiles[k]; slices.Contains(ip.RoleNames, r.RoleName) {
			out = append(out, a.profileOut(ip))
		}
	}
	return page("InstanceProfiles", out, in.Marker, in.MaxItems), nil
}

// ---- identity providers ----

func (s *Service) createOIDC(c *core.Ctx, in *struct {
	Url                          string
	ClientIDList, ThumbprintList []string
	Tags                         []Tag
}) (any, error) {
	host, ok := strings.CutPrefix(in.Url, "https://")
	if !ok || host == "" {
		return nil, core.Errorf(400, "InvalidInput", "Invalid OpenID Connect Provider URL %q: it must begin with https://.", in.Url)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	arn := c.GlobalARN("iam", "oidc-provider/"+host)
	if a.OIDC[arn] != nil {
		return nil, core.Errorf(409, "EntityAlreadyExists", "Provider with url %s already exists.", in.Url)
	}
	a.OIDC[arn] = &OIDCProvider{Arn: arn, Url: host, ClientIDList: in.ClientIDList, ThumbprintList: in.ThumbprintList, CreateDate: now(), Tags: in.Tags}
	return map[string]any{"OpenIDConnectProviderArn": arn, "Tags": in.Tags}, nil
}

// oidc finds an OIDC provider of the caller's account; lock held.
func (s *Service) oidc(c *core.Ctx, arn string) (*OIDCProvider, error) {
	if p := s.acct(c.Account).OIDC[arn]; p != nil {
		return p, nil
	}
	return nil, core.Errorf(404, "NoSuchEntity", "OpenIDConnect Provider not found for arn %s", arn)
}

func (s *Service) getOIDC(c *core.Ctx, in *struct{ OpenIDConnectProviderArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.oidc(c, in.OpenIDConnectProviderArn)
	if err != nil {
		return nil, err
	}
	return *p, nil
}

func (s *Service) updateOIDC(c *core.Ctx, in *struct {
	OpenIDConnectProviderArn, ClientID string
	ThumbprintList                     []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.oidc(c, in.OpenIDConnectProviderArn)
	if err != nil {
		return nil, err
	}
	i := slices.Index(p.ClientIDList, in.ClientID)
	switch {
	case c.Op == "DeleteOpenIDConnectProvider":
		delete(s.acct(c.Account).OIDC, p.Arn)
	case c.Op == "UpdateOpenIDConnectProviderThumbprint":
		p.ThumbprintList = in.ThumbprintList
	case c.Op == "AddClientIDToOpenIDConnectProvider" && i < 0:
		p.ClientIDList = append(p.ClientIDList, in.ClientID)
	case c.Op == "RemoveClientIDFromOpenIDConnectProvider" && i >= 0:
		p.ClientIDList = without(p.ClientIDList, i)
	}
	return nil, nil
}

type arnEntry struct{ Arn string }

func (s *Service) listOIDC(c *core.Ctx, _ *struct{}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []arnEntry
	for _, arn := range core.SortedKeys(s.acct(c.Account).OIDC) {
		out = append(out, arnEntry{arn})
	}
	return map[string]any{"OpenIDConnectProviderList": out}, nil
}

func (s *Service) createSAML(c *core.Ctx, in *struct {
	Name, SAMLMetadataDocument string
	Tags                       []Tag
}) (any, error) {
	if err := checkName("name", in.Name, 128); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	arn := c.GlobalARN("iam", "saml-provider/"+in.Name)
	if a.SAML[arn] != nil {
		return nil, exists("SAML provider", in.Name)
	}
	a.SAML[arn] = &SAMLProvider{Arn: arn, SAMLMetadataDocument: in.SAMLMetadataDocument, CreateDate: now(), Tags: in.Tags}
	return map[string]any{"SAMLProviderArn": arn, "Tags": in.Tags}, nil
}

func (s *Service) samlProvider(c *core.Ctx, in *struct{ SAMLProviderArn, SAMLMetadataDocument string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	p := a.SAML[in.SAMLProviderArn]
	if p == nil {
		return nil, core.Errorf(404, "NoSuchEntity", "SAMLProvider %s does not exist.", in.SAMLProviderArn)
	}
	switch c.Op {
	case "DeleteSAMLProvider":
		delete(a.SAML, p.Arn)
		return nil, nil
	case "UpdateSAMLProvider":
		p.SAMLMetadataDocument = in.SAMLMetadataDocument
		return map[string]string{"SAMLProviderArn": p.Arn}, nil
	}
	return *p, nil
}

func (s *Service) listSAML(c *core.Ctx, _ *struct{}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	var out []SAMLProvider
	for _, arn := range core.SortedKeys(a.SAML) {
		out = append(out, SAMLProvider{Arn: arn, CreateDate: a.SAML[arn].CreateDate})
	}
	return map[string]any{"SAMLProviderList": out}, nil
}

// ---- server certificates ----

func (s *Service) uploadServerCertificate(c *core.Ctx, in *struct {
	ServerCertificateName, Path, CertificateBody, PrivateKey, CertificateChain string
	Tags                                                                       []Tag
}) (any, error) {
	path, err := checkPath(in.Path)
	if err != nil {
		return nil, err
	}
	if err := checkName("serverCertificateName", in.ServerCertificateName, 128); err != nil {
		return nil, err
	}
	block, _ := pem.Decode([]byte(in.CertificateBody))
	if block == nil {
		return nil, core.Errorf(400, "MalformedCertificate", "Unable to parse certificate. Please ensure the certificate is in PEM format.")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, core.Errorf(400, "MalformedCertificate", "Unable to parse certificate: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	if a.Certs[lower(in.ServerCertificateName)] != nil {
		return nil, exists("Server Certificate", in.ServerCertificateName)
	}
	sc := &ServerCert{CertificateBody: in.CertificateBody, CertificateChain: in.CertificateChain, Tags: in.Tags,
		ServerCertificateMetadata: CertMeta{Path: path, ServerCertificateName: in.ServerCertificateName, ServerCertificateId: newID("ASCA"),
			Arn: iamARN(c, "server-certificate", path, in.ServerCertificateName), UploadDate: now(), Expiration: core.Time(cert.NotAfter)}}
	a.Certs[lower(in.ServerCertificateName)] = sc
	return map[string]any{"ServerCertificateMetadata": sc.ServerCertificateMetadata, "Tags": in.Tags}, nil
}

// ServerCertExists tells the load balancers whether IAM has a server certificate.
func (s *Service) ServerCertExists(arn string) bool {
	_, _, account, res, _ := core.ParseARN(arn)
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.st.Accounts[account]
	return a != nil && a.Certs[lower(res[strings.LastIndex(res, "/")+1:])] != nil
}

// ServerCertIDExists tells CloudFront whether an account has a server certificate with this ID (ASCA...).
func (s *Service) ServerCertIDExists(account, id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.st.Accounts[account]
	if a == nil {
		return false
	}
	for _, sc := range a.Certs {
		if sc.ServerCertificateMetadata.ServerCertificateId == id {
			return true
		}
	}
	return false
}

func (s *Service) serverCertificate(c *core.Ctx, in *struct{ ServerCertificateName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	sc, err := get(a.Certs, "server certificate", in.ServerCertificateName)
	if err != nil {
		return nil, err
	}
	if c.Op == "DeleteServerCertificate" {
		// Remove first, then ask the load balancers, without holding their lock
		// together with ours; put the certificate back if a listener uses it.
		// A listener made at the same moment cannot find the certificate.
		key := lower(in.ServerCertificateName)
		delete(a.Certs, key)
		s.mu.Unlock()
		inUse := false
		if elb, ok := c.Hub.Service("elbv2").(interface{ CertInUse(arn string) bool }); ok {
			inUse = elb.CertInUse(sc.ServerCertificateMetadata.Arn)
		}
		s.mu.Lock()
		if inUse {
			a.Certs[key] = sc
			return nil, conflict("certificate is in use by a load balancer listener")
		}
		return nil, nil
	}
	return map[string]any{"ServerCertificate": *sc}, nil
}

func (s *Service) listServerCertificates(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	var out []CertMeta
	for _, k := range core.SortedKeys(a.Certs) {
		if m := a.Certs[k].ServerCertificateMetadata; strings.HasPrefix(m.Path, in.PathPrefix) {
			out = append(out, m)
		}
	}
	return page("ServerCertificateMetadataList", out, in.Marker, in.MaxItems), nil
}

// ---- account ----

func (s *Service) accountSummary(c *core.Ctx, _ *struct{}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	versions := 0
	for _, p := range a.Policies {
		versions += len(p.Versions)
	}
	return map[string]any{"SummaryMap": map[string]int{
		"Users": len(a.Users), "UsersQuota": 5000, "Groups": len(a.Groups), "GroupsQuota": 300,
		"Roles": len(a.Roles), "RolesQuota": 1000, "Policies": len(a.Policies), "PoliciesQuota": 1500,
		"InstanceProfiles": len(a.InstanceProfiles), "InstanceProfilesQuota": 1000,
		"ServerCertificates": len(a.Certs), "ServerCertificatesQuota": 20, "Providers": len(a.OIDC) + len(a.SAML),
		"PolicyVersionsInUse": versions, "PolicyVersionsInUseQuota": 10000, "VersionsPerPolicyQuota": 5,
		"AccessKeysPerUserQuota": 2, "SigningCertificatesPerUserQuota": 2, "GroupsPerUserQuota": 10,
		"AttachedPoliciesPerUserQuota": 10, "AttachedPoliciesPerGroupQuota": 10, "AttachedPoliciesPerRoleQuota": 10,
		"PolicySizeQuota": 6144, "UserPolicySizeQuota": 2048, "GroupPolicySizeQuota": 5120, "RolePolicySizeQuota": 10240,
		"AssumeRolePolicySizeQuota": 2048, "MFADevices": 0, "MFADevicesInUse": 0, "AccountMFAEnabled": 0,
		"AccountAccessKeysPresent": 0, "AccountSigningCertificatesPresent": 0, "GlobalEndpointTokenVersion": 1,
	}}, nil
}

var aliasPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

func (s *Service) accountAlias(c *core.Ctx, in *struct {
	AccountAlias, Marker string
	MaxItems             int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	switch c.Op {
	case "CreateAccountAlias":
		if !aliasPattern.MatchString(in.AccountAlias) || strings.Contains(in.AccountAlias, "--") {
			return nil, core.Errorf(400, "ValidationError", "Account alias %q must be 3-63 lowercase letters, digits and single hyphens.", in.AccountAlias)
		}
		for id, o := range s.st.Accounts {
			if id != c.Account && slices.Contains(o.Aliases, in.AccountAlias) {
				return nil, core.Errorf(409, "EntityAlreadyExists", "The account alias %s already exists.", in.AccountAlias)
			}
		}
		a.Aliases = []string{in.AccountAlias}
	case "DeleteAccountAlias":
		if !slices.Contains(a.Aliases, in.AccountAlias) {
			return nil, core.Errorf(404, "NoSuchEntity", "The account alias %s cannot be found.", in.AccountAlias)
		}
		a.Aliases = nil
	default:
		return page("AccountAliases", a.Aliases, in.Marker, in.MaxItems), nil
	}
	return nil, nil
}

func (s *Service) passwordPolicy(c *core.Ctx, in *PasswordPolicy) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	switch {
	case c.Op == "UpdateAccountPasswordPolicy": // unspecified settings revert to defaults, like AWS
		p := *in
		if p.MinimumPasswordLength == 0 {
			p.MinimumPasswordLength = 6
		}
		if p.MinimumPasswordLength < 6 || p.MinimumPasswordLength > 128 {
			return nil, core.Errorf(400, "ValidationError", "MinimumPasswordLength must be between 6 and 128.")
		}
		p.ExpirePasswords = p.MaxPasswordAge > 0
		a.PasswordPolicy = &p
		return nil, nil
	case a.PasswordPolicy == nil:
		return nil, core.Errorf(404, "NoSuchEntity", "The Password Policy with domain name %s cannot be found.", c.Account)
	case c.Op == "DeleteAccountPasswordPolicy":
		a.PasswordPolicy = nil
		return nil, nil
	}
	return map[string]any{"PasswordPolicy": *a.PasswordPolicy}, nil
}

// ---- tags ----

type tagInput struct {
	UserName, RoleName, PolicyArn, InstanceProfileName string
	OpenIDConnectProviderArn, SAMLProviderArn          string
	ServerCertificateName, Marker                      string
	Tags                                               []Tag
	TagKeys                                            []string
	MaxItems                                           int
}

// tags serves Tag*, Untag* and List*Tags for every taggable resource.
func (s *Service) tags(c *core.Ctx, in *tagInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tagsOf(c, in)
	if err != nil {
		return nil, err
	}
	switch {
	case strings.HasPrefix(c.Op, "Tag"):
		if err := checkTags(in.Tags); err != nil {
			return nil, err
		}
		out := slices.Clone(*t)
		for _, nt := range in.Tags {
			if i := slices.IndexFunc(out, func(x Tag) bool { return strings.EqualFold(x.Key, nt.Key) }); i >= 0 {
				out[i] = nt
			} else {
				out = append(out, nt)
			}
		}
		if len(out) > 50 {
			return nil, core.Errorf(409, "LimitExceeded", "Cannot exceed quota for TagsPerResource: 50")
		}
		*t = out
	case strings.HasPrefix(c.Op, "Untag"):
		*t = slices.DeleteFunc(slices.Clone(*t), func(x Tag) bool {
			return slices.ContainsFunc(in.TagKeys, func(k string) bool { return strings.EqualFold(k, x.Key) })
		})
	default:
		return page("Tags", *t, in.Marker, in.MaxItems), nil
	}
	return nil, nil
}

var tagChars = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@]*$`)

// checkTags applies IAM's tag rules to one request: keys are 1-128 characters
// (case-insensitive, no duplicates, no "aws:" prefix), values at most 256.
func checkTags(tags []Tag) error {
	seen := map[string]bool{}
	for _, t := range tags {
		k := strings.ToLower(t.Key)
		switch {
		case t.Key == "" || len(t.Key) > 128 || !tagChars.MatchString(t.Key):
			return core.Errorf(400, "InvalidInput", "Tag key %q is invalid: it must be 1-128 characters of letters, numbers, spaces and _ . : / = + - @.", t.Key)
		case len(t.Value) > 256 || !tagChars.MatchString(t.Value):
			return core.Errorf(400, "InvalidInput", "Tag value for key %q is invalid: it must be at most 256 characters of letters, numbers, spaces and _ . : / = + - @.", t.Key)
		case strings.HasPrefix(k, "aws:"):
			return core.Errorf(400, "InvalidInput", "Tag key %q is invalid: the aws: prefix is reserved.", t.Key)
		case seen[k]:
			return core.Errorf(400, "InvalidInput", "Duplicate tag keys found. Please note that Tag keys are case insensitive.")
		}
		seen[k] = true
	}
	return nil
}

// tagsOf finds the tag list of the resource a tagging operation names; lock held.
func (s *Service) tagsOf(c *core.Ctx, in *tagInput) (*[]Tag, error) {
	a := s.acct(c.Account)
	switch {
	case in.UserName != "":
		u, err := get(a.Users, "user", in.UserName)
		if err != nil {
			return nil, err
		}
		return &u.Tags, nil
	case in.RoleName != "":
		r, err := get(a.Roles, "role", in.RoleName)
		if err != nil {
			return nil, err
		}
		return &r.Tags, nil
	case in.PolicyArn != "":
		_, p, err := s.localPolicy(c, in.PolicyArn)
		if err != nil {
			return nil, err
		}
		return &p.Tags, nil
	case in.InstanceProfileName != "":
		ip, err := get(a.InstanceProfiles, "instance profile", in.InstanceProfileName)
		if err != nil {
			return nil, err
		}
		return &ip.Tags, nil
	case in.OpenIDConnectProviderArn != "":
		p, err := s.oidc(c, in.OpenIDConnectProviderArn)
		if err != nil {
			return nil, err
		}
		return &p.Tags, nil
	case in.SAMLProviderArn != "":
		if p := a.SAML[in.SAMLProviderArn]; p != nil {
			return &p.Tags, nil
		}
		return nil, core.Errorf(404, "NoSuchEntity", "SAMLProvider %s does not exist.", in.SAMLProviderArn)
	case in.ServerCertificateName != "":
		sc, err := get(a.Certs, "server certificate", in.ServerCertificateName)
		if err != nil {
			return nil, err
		}
		return &sc.Tags, nil
	}
	return nil, core.Errorf(400, "ValidationError", "No resource specified for %s.", c.Op)
}

// ---- console ----

func ts(t core.Time) string { return time.Time(t).UTC().Format(time.RFC3339) }

// Resources lists users, groups, roles and customer managed policies.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	users := core.Table{Name: "Users", Columns: []string{"Name", "Account", "Path", "Created", "Groups", "Access keys"}}
	groups := core.Table{Name: "Groups", Columns: []string{"Name", "Account", "Path", "Created", "Policies"}}
	roles := core.Table{Name: "Roles", Columns: []string{"Name", "Account", "Path", "Created", "Policies"}}
	policies := core.Table{Name: "Policies", Columns: []string{"Name", "Account", "Path", "Created", "Default version", "Attachments"}}
	for _, id := range core.SortedKeys(s.st.Accounts) {
		a := s.st.Accounts[id]
		for _, k := range core.SortedKeys(a.Users) {
			u := a.Users[k]
			users.Rows = append(users.Rows, []string{u.UserName, id, u.Path, ts(u.CreateDate), strings.Join(u.Groups, ", "), strconv.Itoa(len(u.Keys))})
		}
		for _, k := range core.SortedKeys(a.Groups) {
			g := a.Groups[k]
			groups.Rows = append(groups.Rows, []string{g.GroupName, id, g.Path, ts(g.CreateDate), strconv.Itoa(len(g.Inline) + len(g.Attached))})
		}
		for _, k := range core.SortedKeys(a.Roles) {
			r := a.Roles[k]
			roles.Rows = append(roles.Rows, []string{r.RoleName, id, r.Path, ts(r.CreateDate), strconv.Itoa(len(r.Inline) + len(r.Attached))})
		}
		for _, k := range core.SortedKeys(a.Policies) {
			p := a.Policies[k]
			n, _ := a.usage(p.Arn)
			policies.Rows = append(policies.Rows, []string{p.PolicyName, id, p.Path, ts(p.CreateDate), p.DefaultVersionId, strconv.Itoa(n)})
		}
	}
	return []core.Table{users, groups, roles, policies}
}

// Inspect returns the stored form of a user, group, role or policy (a JSON
// snapshot taken under the lock, so callers can encode it safely).
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, acct := range core.SortedKeys(s.st.Accounts) {
		a := s.st.Accounts[acct]
		var v any
		switch table {
		case "Users":
			if u := a.Users[lower(id)]; u != nil {
				cp := *u // without the key secrets
				cp.Keys = slices.Clone(u.Keys)
				for i := range cp.Keys {
					cp.Keys[i].SecretAccessKey = ""
				}
				v = &cp
			}
		case "Groups":
			if g := a.Groups[lower(id)]; g != nil {
				v = g
			}
		case "Roles":
			if r := a.Roles[lower(id)]; r != nil {
				v = r
			}
		case "Policies":
			if p := a.Policies[lower(id)]; p != nil {
				o := a.policyOut(p)
				v = &o
			}
		}
		if v != nil {
			b, _ := json.Marshal(v)
			return json.RawMessage(b)
		}
	}
	return nil
}
