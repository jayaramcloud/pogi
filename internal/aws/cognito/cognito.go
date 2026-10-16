// Package cognito emulates Amazon Cognito user pools over JSON 1.1: pools,
// app clients, users, groups, domains and resource servers; sign-up,
// confirmation and recovery codes kept in an outbox; password, SRP, refresh,
// custom and admin auth with challenges; RS256 JWTs with JWKS and OIDC
// discovery; a minimal hosted login page and OAuth2 endpoints; and the user
// pool Lambda triggers.
package cognito

import (
	"cmp"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"maps"
	"math"
	"math/big"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the Cognito user pools emulator.
type Service struct {
	hub     *core.Hub
	rpc     *core.RPC
	mu      sync.Mutex
	st      state
	pending map[string]*challenge // sign-ins waiting for a challenge response
	grants  map[string]*authCode  // OAuth2 authorization codes
}

type state struct {
	Pools map[string]*Pool `json:"pools"` // by pool id
	Codes []Code           `json:"codes"` // outbox, oldest first
}

// Pool is one user pool and everything in it.
type Pool struct {
	ID        string                       `json:"id"`
	Name      string                       `json:"name"`
	Account   string                       `json:"account"`
	Region    string                       `json:"region"`
	Created   time.Time                    `json:"created"`
	Modified  time.Time                    `json:"modified"`
	Settings  Settings                     `json:"settings"`
	Schema    []SchemaAttribute            `json:"schema,omitempty"` // custom attributes and overridden standard ones
	Mfa       mfaConfig                    `json:"mfa"`
	Tags      map[string]string            `json:"tags,omitempty"`
	Domain    string                       `json:"domain,omitempty"`
	Custom    any                          `json:"customDomain,omitempty"` // CustomDomainConfig
	Clients   map[string]*Client           `json:"clients"`
	Users     map[string]*User             `json:"users"` // by key(username)
	Groups    map[string]*Group            `json:"groups"`
	Servers   map[string]*ResourceServer   `json:"resourceServers"`
	Providers map[string]*IdentityProvider `json:"identityProviders,omitempty"`
	Sessions  map[string]*Session          `json:"sessions"` // by origin_jti
	Key       []byte                       `json:"key"`      // PKCS#1 RSA signing key
	Kid       string                       `json:"kid"`
	priv      *rsa.PrivateKey
}

// Settings are the pool members CreateUserPool and UpdateUserPool take and
// DescribeUserPool returns, in the API shape.
type Settings struct {
	Policies                    *Policies                     `json:",omitempty"`
	DeletionProtection          string                        `json:",omitempty"`
	LambdaConfig                map[string]any                `json:",omitempty"`
	AutoVerifiedAttributes      []string                      `json:",omitempty"`
	AliasAttributes             []string                      `json:",omitempty"`
	UsernameAttributes          []string                      `json:",omitempty"`
	SmsVerificationMessage      string                        `json:",omitempty"`
	EmailVerificationMessage    string                        `json:",omitempty"`
	EmailVerificationSubject    string                        `json:",omitempty"`
	VerificationMessageTemplate any                           `json:",omitempty"`
	SmsAuthenticationMessage    string                        `json:",omitempty"`
	MfaConfiguration            string                        `json:",omitempty"`
	UserAttributeUpdateSettings any                           `json:",omitempty"`
	DeviceConfiguration         any                           `json:",omitempty"`
	EmailConfiguration          any                           `json:",omitempty"`
	SmsConfiguration            any                           `json:",omitempty"`
	AdminCreateUserConfig       *AdminCreateUserConfig        `json:",omitempty"`
	UserPoolAddOns              any                           `json:",omitempty"`
	UsernameConfiguration       *struct{ CaseSensitive bool } `json:",omitempty"`
	AccountRecoverySetting      any                           `json:",omitempty"`
	UserPoolTier                string                        `json:",omitempty"`
}

// Policies holds the password policy (API shape).
type Policies struct {
	PasswordPolicy PasswordPolicy
	SignInPolicy   any `json:",omitempty"`
}

// PasswordPolicy is enforced on every password set.
type PasswordPolicy struct {
	MinimumLength                 int
	RequireUppercase              bool
	RequireLowercase              bool
	RequireNumbers                bool
	RequireSymbols                bool
	PasswordHistorySize           int `json:",omitempty"`
	TemporaryPasswordValidityDays int `json:",omitempty"`
}

// AdminCreateUserConfig (API shape).
type AdminCreateUserConfig struct {
	AllowAdminCreateUserOnly  bool
	UnusedAccountValidityDays int `json:",omitempty"`
	InviteMessageTemplate     any `json:",omitempty"`
}

// SchemaAttribute is one pool attribute (API shape).
type SchemaAttribute struct {
	Name                       string
	AttributeDataType          string `json:",omitempty"`
	DeveloperOnlyAttribute     bool
	Mutable                    bool
	Required                   bool
	NumberAttributeConstraints any `json:",omitempty"`
	StringAttributeConstraints any `json:",omitempty"`
}

type mfaConfig struct {
	UserPoolId                    string `json:",omitempty"`
	SmsMfaConfiguration           any    `json:",omitempty"`
	SoftwareTokenMfaConfiguration any    `json:",omitempty"`
	EmailMfaConfiguration         any    `json:",omitempty"`
	WebAuthnConfiguration         any    `json:",omitempty"`
	MfaConfiguration              string `json:",omitempty"`
}

// Client is an app client (API shape). Updates replace the whole value, so a
// *Client read under the lock stays valid after it.
type Client struct {
	UserPoolId       string
	ClientName       string
	ClientId         string
	ClientSecret     string `json:",omitempty"`
	CreationDate     core.Time
	LastModifiedDate core.Time
	clientSettings
}

type clientSettings struct {
	RefreshTokenValidity                     int                 `json:",omitempty"`
	AccessTokenValidity                      int                 `json:",omitempty"`
	IdTokenValidity                          int                 `json:",omitempty"`
	TokenValidityUnits                       *TokenValidityUnits `json:",omitempty"`
	ReadAttributes                           []string            `json:",omitempty"`
	WriteAttributes                          []string            `json:",omitempty"`
	ExplicitAuthFlows                        []string            `json:",omitempty"`
	SupportedIdentityProviders               []string            `json:",omitempty"`
	CallbackURLs                             []string            `json:",omitempty"`
	LogoutURLs                               []string            `json:",omitempty"`
	DefaultRedirectURI                       string              `json:",omitempty"`
	AllowedOAuthFlows                        []string            `json:",omitempty"`
	AllowedOAuthScopes                       []string            `json:",omitempty"`
	AllowedOAuthFlowsUserPoolClient          bool                `json:",omitempty"`
	AnalyticsConfiguration                   any                 `json:",omitempty"`
	PreventUserExistenceErrors               string              `json:",omitempty"`
	EnableTokenRevocation                    *bool               `json:",omitempty"`
	EnablePropagateAdditionalUserContextData bool                `json:",omitempty"`
	AuthSessionValidity                      int                 `json:",omitempty"`
	RefreshTokenRotation                     any                 `json:",omitempty"`
}

// TokenValidityUnits (API shape): seconds, minutes, hours or days.
type TokenValidityUnits struct {
	AccessToken  string `json:",omitempty"`
	IdToken      string `json:",omitempty"`
	RefreshToken string `json:",omitempty"`
}

// Group is a user group (API shape).
type Group struct {
	GroupName        string
	UserPoolId       string
	Description      string `json:",omitempty"`
	RoleArn          string `json:",omitempty"`
	Precedence       *int   `json:",omitempty"`
	CreationDate     core.Time
	LastModifiedDate core.Time
}

// ResourceServer defines custom OAuth scopes (API shape).
type ResourceServer struct {
	UserPoolId string
	Identifier string
	Name       string
	Scopes     []Scope `json:",omitempty"`
}

// Scope is one resource server scope.
type Scope struct {
	ScopeName        string
	ScopeDescription string
}

// User is one user. Passwords are kept only as a PBKDF2 hash and an SRP verifier.
type User struct {
	Username  string              `json:"username"`
	Attrs     map[string]string   `json:"attrs"`
	Status    string              `json:"status"`
	Enabled   bool                `json:"enabled"`
	Created   time.Time           `json:"created"`
	Modified  time.Time           `json:"modified"`
	Salt      string              `json:"salt,omitempty"`     // hex
	Hash      string              `json:"hash,omitempty"`     // hex PBKDF2-SHA256
	Verifier  string              `json:"verifier,omitempty"` // hex SRP verifier
	History   []pwHash            `json:"history,omitempty"`  // earlier passwords, oldest first (PasswordHistorySize)
	Groups    []string            `json:"groups,omitempty"`
	MFA       []string            `json:"mfa,omitempty"` // enabled methods
	Preferred string              `json:"preferredMfa,omitempty"`
	Codes     map[string]*Pending `json:"codes,omitempty"`   // purpose → code
	TOTP      string              `json:"totp,omitempty"`    // base32 secret of the verified software token
	TOTPNew   string              `json:"totpNew,omitempty"` // secret from AssociateSoftwareToken, not yet verified
}

// pwHash is a salted password hash.
type pwHash struct{ Salt, Hash string }

// Pending is an issued confirmation or verification code.
type Pending struct {
	Code  string    `json:"code"`
	Attr  string    `json:"attr,omitempty"` // the attribute it verifies
	Exp   time.Time `json:"exp"`
	Tries int       `json:"tries,omitempty"`
}

// Session is one sign-in: its refresh token and every token derived from it
// share origin_jti, so revoking the session revokes them all.
type Session struct {
	Username string    `json:"username"`
	Client   string    `json:"client"`
	Scope    string    `json:"scope"`
	AuthTime time.Time `json:"authTime"`
	Exp      time.Time `json:"exp"`
	Rotated  time.Time `json:"rotated,omitempty"` // when a refresh token rotation replaced its token
	Grace    int       `json:"grace,omitempty"`   // seconds the replaced token still works
}

// Code is a message pogi would have sent. Type is the CustomMessage trigger
// source it belongs to: SignUp, ResendCode, ForgotPassword, AdminCreateUser
// (the code is the temporary password), UpdateUserAttribute,
// VerifyUserAttribute or Authentication (an SMS MFA code).
type Code struct {
	Pool        string    `json:"pool"`
	Username    string    `json:"username"`
	Destination string    `json:"destination"`
	From        string    `json:"from,omitempty"` // sender of an email: the pool's From, or Cognito's default
	Code        string    `json:"code"`
	Type        string    `json:"type"`
	Timestamp   time.Time `json:"timestamp"`
	Message     string    `json:"message,omitempty"` // from a CustomMessage trigger
}

// Attr is a user attribute (API shape).
type Attr struct{ Name, Value string }

const (
	lowerNum   = "abcdefghijklmnopqrstuvwxyz0123456789"
	digits     = "0123456789"
	outboxMax  = 500
	adminScope = "aws.cognito.signin.user.admin"
)

var standardAttrs = []string{"sub", "name", "given_name", "family_name", "middle_name", "nickname", "preferred_username",
	"profile", "picture", "website", "email", "email_verified", "gender", "birthdate", "zoneinfo", "locale",
	"phone_number", "phone_number_verified", "address", "updated_at", "identities"}

// New creates the Cognito user pools service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateUserPool":        core.H(s.createUserPool),
		"DescribeUserPool":      core.H(s.describeUserPool),
		"ListUserPools":         core.H(s.listUserPools),
		"UpdateUserPool":        core.H(s.updateUserPool),
		"DeleteUserPool":        core.H(s.deleteUserPool),
		"AddCustomAttributes":   core.H(s.addCustomAttributes),
		"SetUserPoolMfaConfig":  core.H(s.setUserPoolMfaConfig),
		"GetUserPoolMfaConfig":  core.H(s.getUserPoolMfaConfig),
		"GetSigningCertificate": core.H(s.getSigningCertificate),
		"TagResource":           core.H(s.tagResource),
		"UntagResource":         core.H(s.untagResource),
		"ListTagsForResource":   core.H(s.listTagsForResource),

		"CreateUserPoolClient":   core.H(s.createUserPoolClient),
		"DescribeUserPoolClient": core.H(s.describeUserPoolClient),
		"ListUserPoolClients":    core.H(s.listUserPoolClients),
		"UpdateUserPoolClient":   core.H(s.updateUserPoolClient),
		"DeleteUserPoolClient":   core.H(s.deleteUserPoolClient),
		"CreateUserPoolDomain":   core.H(s.createUserPoolDomain),
		"DescribeUserPoolDomain": core.H(s.describeUserPoolDomain),
		"DeleteUserPoolDomain":   core.H(s.deleteUserPoolDomain),
		"CreateResourceServer":   core.H(s.createResourceServer),
		"DescribeResourceServer": core.H(s.describeResourceServer),
		"ListResourceServers":    core.H(s.listResourceServers),
		"UpdateResourceServer":   core.H(s.updateResourceServer),
		"DeleteResourceServer":   core.H(s.deleteResourceServer),

		"CreateGroup":              core.H(s.createGroup),
		"GetGroup":                 core.H(s.getGroup),
		"ListGroups":               core.H(s.listGroups),
		"UpdateGroup":              core.H(s.updateGroup),
		"DeleteGroup":              core.H(s.deleteGroup),
		"AdminAddUserToGroup":      core.H(s.adminAddUserToGroup),
		"AdminRemoveUserFromGroup": core.H(s.adminRemoveUserFromGroup),
		"AdminListGroupsForUser":   core.H(s.adminListGroupsForUser),
		"ListUsersInGroup":         core.H(s.listUsersInGroup),

		"AdminCreateUser":           core.H(s.adminCreateUser),
		"AdminGetUser":              core.H(s.adminGetUser),
		"AdminDeleteUser":           core.H(s.adminDeleteUser),
		"AdminUpdateUserAttributes": core.H(s.adminUpdateUserAttributes),
		"AdminDeleteUserAttributes": core.H(s.adminDeleteUserAttributes),
		"AdminSetUserPassword":      core.H(s.adminSetUserPassword),
		"AdminConfirmSignUp":        core.H(s.adminConfirmSignUp),
		"AdminEnableUser":           core.H(s.adminEnableUser),
		"AdminDisableUser":          core.H(s.adminDisableUser),
		"AdminResetUserPassword":    core.H(s.adminResetUserPassword),
		"AdminUserGlobalSignOut":    core.H(s.adminUserGlobalSignOut),
		"AdminSetUserMFAPreference": core.H(s.adminSetUserMFAPreference),
		"ListUsers":                 core.H(s.listUsers),

		"SignUp":                           core.H(s.signUp),
		"ConfirmSignUp":                    core.H(s.confirmSignUp),
		"ResendConfirmationCode":           core.H(s.resendConfirmationCode),
		"ForgotPassword":                   core.H(s.forgotPassword),
		"ConfirmForgotPassword":            core.H(s.confirmForgotPassword),
		"ChangePassword":                   core.H(s.changePassword),
		"GetUser":                          core.H(s.getUser),
		"DeleteUser":                       core.H(s.deleteUser),
		"UpdateUserAttributes":             core.H(s.updateUserAttributes),
		"DeleteUserAttributes":             core.H(s.deleteUserAttributes),
		"GetUserAttributeVerificationCode": core.H(s.getUserAttributeVerificationCode),
		"VerifyUserAttribute":              core.H(s.verifyUserAttribute),
		"GlobalSignOut":                    core.H(s.globalSignOut),
		"RevokeToken":                      core.H(s.revokeToken),
		"SetUserMFAPreference":             core.H(s.setUserMFAPreference),

		"GetTokensFromRefreshToken": core.H(s.getTokensFromRefreshToken),

		"CreateIdentityProvider":          core.H(s.createIdentityProvider),
		"DescribeIdentityProvider":        core.H(s.describeIdentityProvider),
		"UpdateIdentityProvider":          core.H(s.updateIdentityProvider),
		"DeleteIdentityProvider":          core.H(s.deleteIdentityProvider),
		"ListIdentityProviders":           core.H(s.listIdentityProviders),
		"GetIdentityProviderByIdentifier": core.H(s.getIdentityProviderByIdentifier),

		"AssociateSoftwareToken": core.H(s.associateSoftwareToken),
		"VerifySoftwareToken":    core.H(s.verifySoftwareToken),

		"InitiateAuth":                core.H(s.initiateAuth),
		"AdminInitiateAuth":           core.H(s.adminInitiateAuth),
		"RespondToAuthChallenge":      core.H(s.respondToAuthChallenge),
		"AdminRespondToAuthChallenge": core.H(s.adminRespondToAuthChallenge),
	}}
	if hub != nil {
		hub.Mount("/_aws/cognito/codes", http.HandlerFunc(s.codesHandler))
		hub.Mount("/_aws/cognito-idp/", http.StripPrefix("/_aws/cognito-idp", http.HandlerFunc(s.hosted)))
	}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "cognito-idp", Cloud: core.AWS, Title: "Cognito User Pools", Targets: []string{"AWSCognitoIdentityProviderService"},
		Signing: []string{"cognito-idp"}, Match: hostedPath, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// ponytail: Ctx has no scheme, so Host carries it ("https://host") for issuer().
	c := core.C(r)
	c.Host = base(r)
	if o := r.Header.Get("Origin"); o != "" { // browser apps call Cognito directly; S3 answers the preflight of "/"
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", o)
		h.Set("Access-Control-Expose-Headers", "x-amzn-RequestId, x-amzn-ErrorType, x-amzn-ErrorMessage")
		h.Add("Vary", "Origin")
		if r.Method == http.MethodOptions {
			h.Set("Access-Control-Allow-Methods", "GET, POST")
			h.Set("Access-Control-Allow-Headers", r.Header.Get("Access-Control-Request-Headers"))
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	if r.Header.Get("X-Amz-Target") == "" {
		c.Op = strings.TrimPrefix(r.URL.Path, "/")
		s.hosted(w, r)
		return
	}
	s.rpc.ServeHTTP(w, r)
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Pools: map[string]*Pool{}}
	s.pending, s.grants = map[string]*challenge{}, map[string]*authCode{}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Pools == nil {
		st.Pools = map[string]*Pool{}
	}
	for _, p := range st.Pools {
		k, err := x509.ParsePKCS1PrivateKey(p.Key)
		if err != nil {
			return err
		}
		p.priv = k
		p.init()
	}
	s.mu.Lock()
	s.st = st
	s.pending, s.grants = map[string]*challenge{}, map[string]*authCode{}
	s.mu.Unlock()
	return nil
}

func (p *Pool) init() {
	p.Clients, p.Users, p.Groups, p.Servers, p.Sessions = orMap(p.Clients), orMap(p.Users), orMap(p.Groups), orMap(p.Servers), orMap(p.Sessions)
	p.Providers = orMap(p.Providers)
}

func orMap[V any](m map[string]V) map[string]V {
	if m == nil {
		return map[string]V{}
	}
	return m
}

// ---- errors and lookups (lock held) ----

func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterException", format, a...)
}

func errNoUser() error { return core.Errorf(400, "UserNotFoundException", "User does not exist.") }

func errAuth() error {
	return core.Errorf(400, "NotAuthorizedException", "Incorrect username or password.")
}

func (s *Service) pool(id string) (*Pool, error) {
	if p := s.st.Pools[id]; p != nil {
		return p, nil
	}
	return nil, core.Errorf(400, "ResourceNotFoundException", "User pool %s does not exist.", id)
}

func errNoClient(id string) error {
	return core.Errorf(400, "ResourceNotFoundException", "User pool client %s does not exist.", id)
}

// client finds an app client in any pool: client ids are globally unique.
func (s *Service) client(id string) (*Pool, *Client, error) {
	for _, p := range s.st.Pools {
		if cl := p.Clients[id]; cl != nil {
			return p, cl, nil
		}
	}
	return nil, nil, errNoClient(id)
}

func (s *Service) poolClient(poolID, clientID string) (*Pool, *Client, error) {
	p, err := s.pool(poolID)
	if err != nil {
		return nil, nil, err
	}
	if cl := p.Clients[clientID]; cl != nil {
		return p, cl, nil
	}
	return nil, nil, errNoClient(clientID)
}

func (s *Service) poolUser(poolID, name string) (*Pool, *User, error) {
	p, err := s.pool(poolID)
	if err != nil {
		return nil, nil, err
	}
	u := p.user(name)
	if u == nil { // the admin APIs also take the user's sub as Username
		u = p.owner("sub", name, nil, false)
	}
	if u == nil {
		return nil, nil, errNoUser()
	}
	return p, u, nil
}

func (p *Pool) arn() string { return core.ARN("cognito-idp", p.Region, p.Account, "userpool/"+p.ID) }

// key is the users-map key: usernames are case-insensitive when the pool says so.
func (p *Pool) key(name string) string {
	if c := p.Settings.UsernameConfiguration; c != nil && !c.CaseSensitive {
		return strings.ToLower(name)
	}
	return name
}

// user finds a user by username or by a sign-in alias (email, phone, preferred_username).
func (p *Pool) user(name string) *User {
	if u := p.Users[p.key(name)]; u != nil {
		return u
	}
	for _, a := range p.Settings.UsernameAttributes {
		if u := p.owner(a, name, nil, false); u != nil {
			return u
		}
	}
	for _, a := range p.Settings.AliasAttributes {
		if u := p.owner(a, name, nil, true); u != nil {
			return u
		}
	}
	return nil
}

// owner is the user other than except whose attr equals v (case-insensitively),
// verified when asked. ponytail: linear scan; index aliases if pools get big.
func (p *Pool) owner(attr, v string, except *User, verified bool) *User {
	if v == "" {
		return nil
	}
	for _, u := range p.Users {
		if u != except && strings.EqualFold(u.Attrs[attr], v) &&
			(!verified || attr == "preferred_username" || u.Attrs[attr+"_verified"] == "true") {
			return u
		}
	}
	return nil
}

// uniqueAttrs must not be shared by two users: sign-in attributes and a preferred_username alias.
func (p *Pool) uniqueAttrs() []string {
	a := slices.Clone(p.Settings.UsernameAttributes)
	if slices.Contains(p.Settings.AliasAttributes, "preferred_username") {
		a = append(a, "preferred_username")
	}
	return a
}

// page sorts keys and returns up to limit (default and max 60) starting at token, plus the next token.
func page(keys []string, token string, limit int) ([]string, string) {
	slices.Sort(keys)
	i, _ := slices.BinarySearch(keys, token)
	if limit <= 0 || limit > 60 {
		limit = 60
	}
	if i+limit >= len(keys) {
		return keys[i:], ""
	}
	return keys[i : i+limit], keys[i+limit]
}

func withNext(m map[string]any, key, next string) map[string]any {
	if next != "" {
		m[key] = next
	}
	return m
}

// ---- user pools ----

type poolInput struct {
	UserPoolId   string
	PoolName     string
	UserPoolTags map[string]string
	Schema       []SchemaAttribute
	Settings
}

type poolView struct {
	Id                     string
	Name                   string
	Arn                    string
	Domain                 string `json:",omitempty"`
	CreationDate           core.Time
	LastModifiedDate       core.Time
	EstimatedNumberOfUsers int
	SchemaAttributes       []SchemaAttribute
	UserPoolTags           map[string]string `json:",omitempty"`
	Settings
}

func (p *Pool) describe() poolView {
	return poolView{Id: p.ID, Name: p.Name, Arn: p.arn(), Domain: p.Domain, CreationDate: core.Time(p.Created),
		LastModifiedDate: core.Time(p.Modified), EstimatedNumberOfUsers: len(p.Users), SchemaAttributes: p.schema(),
		UserPoolTags: p.Tags, Settings: p.Settings}
}

// schema is the standard attributes (with overrides) followed by custom ones.
func (p *Pool) schema() []SchemaAttribute {
	var out []SchemaAttribute
	for _, n := range standardAttrs {
		a := SchemaAttribute{Name: n, AttributeDataType: "String", Mutable: n != "sub", Required: n == "sub"}
		switch n { // AWS's exact defaults: Terraform drops attributes that match them
		case "email_verified", "phone_number_verified":
			a.AttributeDataType = "Boolean"
		case "updated_at":
			a.AttributeDataType, a.NumberAttributeConstraints = "Number", map[string]string{"MinValue": "0"}
		case "sub":
			a.StringAttributeConstraints = map[string]string{"MinLength": "1", "MaxLength": "2048"}
		case "birthdate":
			a.StringAttributeConstraints = map[string]string{"MinLength": "10", "MaxLength": "10"}
		case "identities":
			a.StringAttributeConstraints = map[string]string{}
		default:
			a.StringAttributeConstraints = map[string]string{"MinLength": "0", "MaxLength": "2048"}
		}
		if d := p.def(n); d != nil {
			a.Mutable, a.Required = d.Mutable, d.Required
			a.StringAttributeConstraints = cmp.Or(d.StringAttributeConstraints, a.StringAttributeConstraints)
			a.NumberAttributeConstraints = cmp.Or(d.NumberAttributeConstraints, a.NumberAttributeConstraints)
		}
		out = append(out, a)
	}
	for _, a := range p.Schema {
		if !slices.Contains(standardAttrs, a.Name) {
			out = append(out, a)
		}
	}
	return out
}

func (p *Pool) def(name string) *SchemaAttribute {
	for i := range p.Schema {
		if p.Schema[i].Name == name {
			return &p.Schema[i]
		}
	}
	return nil
}

// addAttr adds a schema attribute: a standard name overrides its defaults, anything else is custom:name.
func (p *Pool) addAttr(a SchemaAttribute, custom bool) error {
	if custom || !slices.Contains(standardAttrs, a.Name) {
		a.Name = "custom:" + strings.TrimPrefix(a.Name, "custom:")
	}
	a.AttributeDataType = cmp.Or(a.AttributeDataType, "String")
	if p.def(a.Name) != nil {
		return invalid("Existing attribute already has name %s.", a.Name)
	}
	p.Schema = append(p.Schema, a)
	return nil
}

func (p *Pool) configure(st Settings) {
	if st.Policies == nil {
		st.Policies = &Policies{PasswordPolicy: PasswordPolicy{MinimumLength: 8, RequireUppercase: true, RequireLowercase: true,
			RequireNumbers: true, RequireSymbols: true, TemporaryPasswordValidityDays: 7}}
	}
	if st.Policies.PasswordPolicy.MinimumLength == 0 {
		st.Policies.PasswordPolicy.MinimumLength = 8
	}
	st.MfaConfiguration = cmp.Or(st.MfaConfiguration, "OFF")
	st.DeletionProtection = cmp.Or(st.DeletionProtection, "INACTIVE")
	p.Settings = st
}

func kidOf(k *rsa.PrivateKey) string {
	der, _ := x509.MarshalPKIXPublicKey(&k.PublicKey)
	sum := sha256.Sum256(der)
	return base64.StdEncoding.EncodeToString(sum[:])
}

func (s *Service) createUserPool(c *core.Ctx, in *poolInput) (any, error) {
	if in.PoolName == "" {
		return nil, invalid("PoolName is required.")
	}
	if len(in.UsernameAttributes) > 0 && len(in.AliasAttributes) > 0 {
		return nil, invalid("Only one of the aliasAttributes or usernameAttributes can be set in a User Pool.")
	}
	for _, a := range in.UsernameAttributes {
		if a != "email" && a != "phone_number" {
			return nil, invalid("Username attributes must be email or phone_number.")
		}
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	p := &Pool{ID: c.Region + "_" + core.RandString(9, core.AlphaNum), Name: in.PoolName, Account: c.Account, Region: c.Region,
		Created: now, Modified: now, Tags: in.UserPoolTags, Key: x509.MarshalPKCS1PrivateKey(key), Kid: kidOf(key), priv: key}
	p.init()
	p.configure(in.Settings)
	for _, a := range in.Schema {
		if err := p.addAttr(a, false); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st.Pools[p.ID] = p
	return map[string]any{"UserPool": p.describe()}, nil
}

func (s *Service) describeUserPool(c *core.Ctx, in *poolInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	return map[string]any{"UserPool": p.describe()}, nil
}

type listInput struct {
	UserPoolId, GroupName, Username, Filter string
	NextToken, PaginationToken              string
	MaxResults, Limit                       int
	AttributesToGet                         []string
}

func (s *Service) listUserPools(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for id, p := range s.st.Pools {
		if p.Account == c.Account && p.Region == c.Region {
			ids = append(ids, id)
		}
	}
	ids, next := page(ids, in.NextToken, in.MaxResults)
	out := []map[string]any{}
	for _, id := range ids {
		p := s.st.Pools[id]
		out = append(out, map[string]any{"Id": p.ID, "Name": p.Name, "LambdaConfig": orMap(p.Settings.LambdaConfig),
			"CreationDate": core.Time(p.Created), "LastModifiedDate": core.Time(p.Modified)})
	}
	return withNext(map[string]any{"UserPools": out}, "NextToken", next), nil
}

// updateUserPool replaces the settings: like AWS, anything left out returns to its default.
func (s *Service) updateUserPool(c *core.Ctx, in *poolInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	old := p.Settings
	st := in.Settings
	st.AliasAttributes, st.UsernameAttributes, st.UsernameConfiguration = old.AliasAttributes, old.UsernameAttributes, old.UsernameConfiguration
	p.configure(st)
	p.Name = cmp.Or(in.PoolName, p.Name)
	if in.UserPoolTags != nil {
		p.Tags = in.UserPoolTags
	}
	p.Modified = time.Now()
	return nil, nil
}

func (s *Service) deleteUserPool(c *core.Ctx, in *poolInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	switch {
	case err != nil:
		return nil, err
	case p.Settings.DeletionProtection == "ACTIVE":
		return nil, invalid("The user pool cannot be deleted because deletion protection is activated. Deletion protection must be inactivated first.")
	case p.Domain != "":
		return nil, invalid("User pool cannot be deleted. It has a domain configured that should be deleted first.")
	}
	delete(s.st.Pools, p.ID)
	return nil, nil
}

func (s *Service) addCustomAttributes(c *core.Ctx, in *struct {
	UserPoolId       string
	CustomAttributes []SchemaAttribute
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	for _, a := range in.CustomAttributes {
		if err := p.addAttr(a, true); err != nil {
			return nil, err
		}
	}
	p.Modified = time.Now()
	return nil, nil
}

func (s *Service) setUserPoolMfaConfig(c *core.Ctx, in *mfaConfig) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	p.Mfa = *in
	p.Mfa.UserPoolId, p.Mfa.MfaConfiguration = "", ""
	if in.MfaConfiguration != "" {
		p.Settings.MfaConfiguration = in.MfaConfiguration
	}
	return p.mfaView(), nil
}

func (s *Service) getUserPoolMfaConfig(c *core.Ctx, in *mfaConfig) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	return p.mfaView(), nil
}

func (p *Pool) mfaView() mfaConfig {
	v := p.Mfa
	v.MfaConfiguration = p.Settings.MfaConfiguration
	return v
}

// getSigningCertificate returns a self-signed certificate for the pool's signing key.
func (s *Service) getSigningCertificate(c *core.Ctx, in *poolInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: p.ID},
		NotBefore: p.Created, NotAfter: p.Created.AddDate(10, 0, 0)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &p.priv.PublicKey, p.priv)
	if err != nil {
		return nil, err
	}
	return map[string]string{"Certificate": base64.StdEncoding.EncodeToString(der)}, nil
}

type tagInput struct {
	ResourceArn string
	Tags        map[string]string
	TagKeys     []string
}

func (s *Service) tagged(arn string) (*Pool, error) {
	_, _, _, res, _ := core.ParseARN(arn)
	return s.pool(strings.TrimPrefix(res, "userpool/"))
}

// Tags are replaced, never edited in place: describe output may still be encoding the old map.
func (s *Service) tagResource(c *core.Ctx, in *tagInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.tagged(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	t := map[string]string{}
	maps.Copy(t, p.Tags)
	maps.Copy(t, in.Tags)
	p.Tags = t
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, in *tagInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.tagged(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	t := maps.Clone(p.Tags)
	for _, k := range in.TagKeys {
		delete(t, k)
	}
	p.Tags = t
	return nil, nil
}

func (s *Service) listTagsForResource(c *core.Ctx, in *tagInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.tagged(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Tags": orMap(p.Tags)}, nil
}

// ---- app clients ----

type clientInput struct {
	UserPoolId, ClientId, ClientName string
	GenerateSecret                   bool
	clientSettings
}

var stdScopes = []string{"openid", "email", "phone", "profile", adminScope}

// prepare validates a client's OAuth settings and fills in defaults.
func (p *Pool) prepare(cl *Client) error {
	for _, t := range []struct {
		kind     string
		v        int
		min, max time.Duration
	}{
		{"access", cl.AccessTokenValidity, 5 * time.Minute, 24 * time.Hour},
		{"id", cl.IdTokenValidity, 5 * time.Minute, 24 * time.Hour},
		{"refresh", cl.RefreshTokenValidity, time.Hour, 3650 * 24 * time.Hour},
	} {
		if d := cl.ttl(t.kind); t.v != 0 && (d < t.min || d > t.max) {
			return invalid("Invalid %s token validity: it must be between %s and %s.", t.kind, t.min, t.max)
		}
	}
	if f := cl.AllowedOAuthFlows; slices.Contains(f, "client_credentials") {
		if len(f) > 1 {
			return core.Errorf(400, "InvalidOAuthFlowException", "client_credentials flow can not be selected along with code flow or implicit flow.")
		}
		if cl.ClientSecret == "" {
			return core.Errorf(400, "InvalidOAuthFlowException", "client_credentials flow can not be selected if client does not have a client secret.")
		}
	}
	for _, sc := range cl.AllowedOAuthScopes {
		if slices.Contains(stdScopes, sc) {
			continue
		}
		i := strings.LastIndexByte(sc, '/')
		rs := p.Servers[sc[:max(i, 0)]]
		if i < 0 || rs == nil || !slices.ContainsFunc(rs.Scopes, func(x Scope) bool { return x.ScopeName == sc[i+1:] }) {
			return core.Errorf(400, "ScopeDoesNotExistException", "Invalid scope requested: %s", sc)
		}
	}
	if err := cl.checkRotation(); err != nil {
		return err
	}
	if err := p.checkProviders(cl); err != nil {
		return err
	}
	cl.RefreshTokenValidity = cmp.Or(cl.RefreshTokenValidity, 30)
	cl.AuthSessionValidity = cmp.Or(cl.AuthSessionValidity, 3)
	cl.PreventUserExistenceErrors = cmp.Or(cl.PreventUserExistenceErrors, "LEGACY")
	if cl.EnableTokenRevocation == nil {
		on := true
		cl.EnableTokenRevocation = &on
	}
	return nil
}

func (s *Service) createUserPoolClient(c *core.Ctx, in *clientInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	now := core.Time(time.Now())
	cl := &Client{UserPoolId: p.ID, ClientName: in.ClientName, ClientId: core.RandString(26, lowerNum),
		CreationDate: now, LastModifiedDate: now, clientSettings: in.clientSettings}
	if in.GenerateSecret {
		cl.ClientSecret = core.RandString(51, lowerNum)
	}
	if len(cl.ExplicitAuthFlows) == 0 { // AWS spells out the flows a new client gets
		cl.ExplicitAuthFlows = slices.Clone(defaultFlows)
	}
	if err := p.prepare(cl); err != nil {
		return nil, err
	}
	p.Clients[cl.ClientId] = cl
	return map[string]any{"UserPoolClient": cl}, nil
}

func (s *Service) describeUserPoolClient(c *core.Ctx, in *clientInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, cl, err := s.poolClient(in.UserPoolId, in.ClientId)
	if err != nil {
		return nil, err
	}
	return map[string]any{"UserPoolClient": cl}, nil
}

func (s *Service) listUserPoolClients(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	ids, next := page(slices.Collect(maps.Keys(p.Clients)), in.NextToken, in.MaxResults)
	out := []map[string]string{}
	for _, id := range ids {
		cl := p.Clients[id]
		out = append(out, map[string]string{"ClientId": cl.ClientId, "UserPoolId": p.ID, "ClientName": cl.ClientName})
	}
	return withNext(map[string]any{"UserPoolClients": out}, "NextToken", next), nil
}

// updateUserPoolClient replaces the client's settings, like AWS does.
func (s *Service) updateUserPoolClient(c *core.Ctx, in *clientInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, old, err := s.poolClient(in.UserPoolId, in.ClientId)
	if err != nil {
		return nil, err
	}
	cl := &Client{UserPoolId: p.ID, ClientName: cmp.Or(in.ClientName, old.ClientName), ClientId: old.ClientId, ClientSecret: old.ClientSecret,
		CreationDate: old.CreationDate, LastModifiedDate: core.Time(time.Now()), clientSettings: in.clientSettings}
	if err := p.prepare(cl); err != nil {
		return nil, err
	}
	p.Clients[cl.ClientId] = cl
	return map[string]any{"UserPoolClient": cl}, nil
}

func (s *Service) deleteUserPoolClient(c *core.Ctx, in *clientInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, cl, err := s.poolClient(in.UserPoolId, in.ClientId)
	if err != nil {
		return nil, err
	}
	delete(p.Clients, cl.ClientId)
	return nil, nil
}

// ---- domains ----

type domainInput struct {
	Domain, UserPoolId string
	CustomDomainConfig any
}

func (s *Service) createUserPoolDomain(c *core.Ctx, in *domainInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	if p.Domain != "" {
		return nil, invalid("User pool already has a domain configured.")
	}
	for _, o := range s.st.Pools {
		if o.Domain == in.Domain {
			return nil, invalid("Domain already associated with another user pool.")
		}
	}
	p.Domain, p.Custom = in.Domain, in.CustomDomainConfig
	if in.CustomDomainConfig != nil {
		return map[string]string{"CloudFrontDomain": "d" + core.RandString(13, lowerNum) + ".cloudfront.net"}, nil
	}
	return nil, nil
}

// describeUserPoolDomain returns an empty description for unknown domains, as AWS does.
func (s *Service) describeUserPoolDomain(c *core.Ctx, in *domainInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := map[string]any{}
	for _, p := range s.st.Pools {
		if in.Domain != "" && p.Domain == in.Domain {
			d = map[string]any{"UserPoolId": p.ID, "AWSAccountId": p.Account, "Domain": p.Domain, "Status": "ACTIVE", "Version": "1",
				"S3Bucket": "aws-cognito-prod-assets", "CloudFrontDistribution": p.Domain + ".auth." + p.Region + ".amazoncognito.com"}
			if p.Custom != nil {
				d["CustomDomainConfig"] = p.Custom
			}
		}
	}
	return map[string]any{"DomainDescription": d}, nil
}

func (s *Service) deleteUserPoolDomain(c *core.Ctx, in *domainInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	if p.Domain == "" || p.Domain != in.Domain {
		return nil, invalid("No such domain or user pool exists.")
	}
	p.Domain, p.Custom = "", nil
	return nil, nil
}

// ---- resource servers ----

func (s *Service) createResourceServer(c *core.Ctx, in *ResourceServer) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	if p.Servers[in.Identifier] != nil {
		return nil, invalid("%s already exists in user pool %s.", in.Identifier, p.ID)
	}
	rs := *in
	p.Servers[rs.Identifier] = &rs
	return map[string]any{"ResourceServer": rs}, nil
}

func (s *Service) server(in *ResourceServer) (*Pool, *ResourceServer, error) {
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, nil, err
	}
	if rs := p.Servers[in.Identifier]; rs != nil {
		return p, rs, nil
	}
	return nil, nil, core.Errorf(400, "ResourceNotFoundException", "Resource server %s does not exist.", in.Identifier)
}

func (s *Service) describeResourceServer(c *core.Ctx, in *ResourceServer) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, rs, err := s.server(in)
	if err != nil {
		return nil, err
	}
	return map[string]any{"ResourceServer": *rs}, nil
}

func (s *Service) updateResourceServer(c *core.Ctx, in *ResourceServer) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, _, err := s.server(in)
	if err != nil {
		return nil, err
	}
	rs := *in
	p.Servers[rs.Identifier] = &rs
	return map[string]any{"ResourceServer": rs}, nil
}

func (s *Service) deleteResourceServer(c *core.Ctx, in *ResourceServer) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, rs, err := s.server(in)
	if err != nil {
		return nil, err
	}
	delete(p.Servers, rs.Identifier)
	return nil, nil
}

func (s *Service) listResourceServers(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	ids, next := page(slices.Collect(maps.Keys(p.Servers)), in.NextToken, in.MaxResults)
	out := []ResourceServer{}
	for _, id := range ids {
		out = append(out, *p.Servers[id])
	}
	return withNext(map[string]any{"ResourceServers": out}, "NextToken", next), nil
}

// ---- groups ----

type groupInput struct {
	UserPoolId, GroupName, Description, RoleArn, Username string
	Precedence                                            *int
}

func errNoGroup() error { return core.Errorf(400, "ResourceNotFoundException", "Group not found.") }

func (s *Service) createGroup(c *core.Ctx, in *groupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	if p.Groups[in.GroupName] != nil {
		return nil, core.Errorf(400, "GroupExistsException", "A group with the name %s already exists.", in.GroupName)
	}
	now := core.Time(time.Now())
	g := &Group{GroupName: in.GroupName, UserPoolId: p.ID, Description: in.Description, RoleArn: in.RoleArn, Precedence: in.Precedence,
		CreationDate: now, LastModifiedDate: now}
	p.Groups[g.GroupName] = g
	return map[string]any{"Group": *g}, nil
}

func (s *Service) group(in *groupInput) (*Pool, *Group, error) {
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, nil, err
	}
	if g := p.Groups[in.GroupName]; g != nil {
		return p, g, nil
	}
	return nil, nil, errNoGroup()
}

func (s *Service) getGroup(c *core.Ctx, in *groupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, g, err := s.group(in)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Group": *g}, nil
}

func (s *Service) updateGroup(c *core.Ctx, in *groupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, old, err := s.group(in)
	if err != nil {
		return nil, err
	}
	g := *old
	g.Description, g.RoleArn = cmp.Or(in.Description, g.Description), cmp.Or(in.RoleArn, g.RoleArn)
	if in.Precedence != nil {
		g.Precedence = in.Precedence
	}
	g.LastModifiedDate = core.Time(time.Now())
	p.Groups[g.GroupName] = &g
	return map[string]any{"Group": g}, nil
}

func (s *Service) deleteGroup(c *core.Ctx, in *groupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, g, err := s.group(in)
	if err != nil {
		return nil, err
	}
	delete(p.Groups, g.GroupName)
	for _, u := range p.Users {
		u.Groups = slices.DeleteFunc(u.Groups, func(n string) bool { return n == g.GroupName })
	}
	return nil, nil
}

func (s *Service) listGroups(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	names, next := page(slices.Collect(maps.Keys(p.Groups)), in.NextToken, in.Limit)
	return withNext(map[string]any{"Groups": p.groupList(names)}, "NextToken", next), nil
}

func (p *Pool) groupList(names []string) []Group {
	out := []Group{}
	for _, n := range names {
		if g := p.Groups[n]; g != nil {
			out = append(out, *g)
		}
	}
	return out
}

// groupsOf lists a user's groups by precedence (lowest first), then name.
func (p *Pool) groupsOf(u *User) []*Group {
	var gs []*Group
	for _, n := range u.Groups {
		if g := p.Groups[n]; g != nil {
			gs = append(gs, g)
		}
	}
	prec := func(g *Group) int {
		if g.Precedence == nil {
			return math.MaxInt
		}
		return *g.Precedence
	}
	slices.SortFunc(gs, func(a, b *Group) int {
		return cmp.Or(cmp.Compare(prec(a), prec(b)), strings.Compare(a.GroupName, b.GroupName))
	})
	return gs
}

func (s *Service) adminAddUserToGroup(c *core.Ctx, in *groupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, g, err := s.group(in)
	if err != nil {
		return nil, err
	}
	u := p.user(in.Username)
	if u == nil {
		return nil, errNoUser()
	}
	if !slices.Contains(u.Groups, g.GroupName) {
		u.Groups = append(u.Groups, g.GroupName)
	}
	return nil, nil
}

func (s *Service) adminRemoveUserFromGroup(c *core.Ctx, in *groupInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, g, err := s.group(in)
	if err != nil {
		return nil, err
	}
	u := p.user(in.Username)
	if u == nil {
		return nil, errNoUser()
	}
	u.Groups = slices.DeleteFunc(u.Groups, func(n string) bool { return n == g.GroupName })
	return nil, nil
}

func (s *Service) adminListGroupsForUser(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, u, err := s.poolUser(in.UserPoolId, in.Username)
	if err != nil {
		return nil, err
	}
	names, next := page(slices.Clone(u.Groups), in.NextToken, in.Limit)
	return withNext(map[string]any{"Groups": p.groupList(names)}, "NextToken", next), nil
}

func (s *Service) listUsersInGroup(c *core.Ctx, in *listInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, g, err := s.group(&groupInput{UserPoolId: in.UserPoolId, GroupName: in.GroupName})
	if err != nil {
		return nil, err
	}
	var keys []string
	for k, u := range p.Users {
		if slices.Contains(u.Groups, g.GroupName) {
			keys = append(keys, k)
		}
	}
	keys, next := page(keys, in.NextToken, in.Limit)
	users := []map[string]any{}
	for _, k := range keys {
		users = append(users, p.Users[k].userType(nil))
	}
	return withNext(map[string]any{"Users": users}, "NextToken", next), nil
}

// ---- users (admin) ----

type userInput struct {
	UserPoolId, ClientId, Username, SecretHash, AccessToken         string
	Password, TemporaryPassword, PreviousPassword, ProposedPassword string
	ConfirmationCode, Code, AttributeName, MessageAction            string
	Token, ClientSecret                                             string
	UserAttributes, ValidationData                                  []Attr
	UserAttributeNames, DesiredDeliveryMediums                      []string
	ClientMetadata                                                  map[string]string
	ForceAliasCreation, Permanent                                   bool
	SMSMfaSettings, SoftwareTokenMfaSettings                        *mfaSetting
}

type mfaSetting struct{ Enabled, PreferredMfa bool }

// withUser runs fn under the lock on the user the admin request names.
func (s *Service) withUser(in *userInput, fn func(p *Pool, u *User) (any, error)) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, u, err := s.poolUser(in.UserPoolId, in.Username)
	if err != nil {
		return nil, err
	}
	return fn(p, u)
}

func attrList(m map[string]string, only []string) []Attr {
	out := []Attr{}
	for _, k := range core.SortedKeys(m) {
		if len(only) == 0 || slices.Contains(only, k) {
			out = append(out, Attr{k, m[k]})
		}
	}
	return out
}

func attrMap(list []Attr) map[string]string {
	m := map[string]string{}
	for _, a := range list {
		m[a.Name] = a.Value
	}
	return m
}

// userType is the UserType shape of ListUsers and AdminCreateUser.
func (u *User) userType(only []string) map[string]any {
	return map[string]any{"Username": u.Username, "Attributes": attrList(u.Attrs, only), "UserCreateDate": core.Time(u.Created),
		"UserLastModifiedDate": core.Time(u.Modified), "Enabled": u.Enabled, "UserStatus": u.Status}
}

// detail is the AdminGetUser (admin) or GetUser shape.
func (u *User) detail(admin bool) map[string]any {
	m := map[string]any{"Username": u.Username, "UserAttributes": attrList(u.Attrs, nil)}
	if len(u.MFA) > 0 {
		m["UserMFASettingList"] = slices.Clone(u.MFA)
	}
	if u.Preferred != "" {
		m["PreferredMfaSetting"] = u.Preferred
	}
	if admin {
		m["UserCreateDate"], m["UserLastModifiedDate"], m["Enabled"], m["UserStatus"] = core.Time(u.Created), core.Time(u.Modified), u.Enabled, u.Status
	}
	return m
}

var emailFormat = regexp.MustCompile(`^[^\s@]+@[^\s@]+$`)

// checkAttrs rejects attributes the schema does not know, sub, and an email
// that is not an address.
func (p *Pool) checkAttrs(attrs []Attr) error {
	for _, a := range attrs {
		switch {
		case a.Name == "sub":
			return invalid("Cannot modify the non-mutable attribute sub")
		case a.Name == "email" && a.Value != "" && !emailFormat.MatchString(a.Value):
			return invalid("Invalid email address format.")
		case !slices.Contains(standardAttrs, a.Name) && p.def(a.Name) == nil:
			return invalid("Attributes did not conform to the schema: %s: Attribute does not exist in the schema.", a.Name)
		}
	}
	return nil
}

// newUser validates a new user. Pools that sign in with email or phone take
// that as Username and give the user a UUID username (its sub).
func (p *Pool) newUser(name string, attrs []Attr) (*User, error) {
	if name == "" {
		return nil, invalid("Username cannot be empty.")
	}
	if err := p.checkAttrs(attrs); err != nil {
		return nil, err
	}
	m := attrMap(attrs)
	sub := core.UUID()
	username := name
	if ua := p.Settings.UsernameAttributes; len(ua) > 0 {
		switch {
		case strings.Contains(name, "@") && slices.Contains(ua, "email"):
			m["email"] = name
		case strings.HasPrefix(name, "+") && slices.Contains(ua, "phone_number"):
			m["phone_number"] = name
		case len(ua) == 1:
			return nil, invalid("Username should be an %s.", strings.ReplaceAll(ua[0], "_", " "))
		default:
			return nil, invalid("Username should be either an email or a phone number.")
		}
		username = sub
	}
	m["sub"] = sub
	for _, a := range p.Schema {
		if a.Required && m[a.Name] == "" {
			return nil, invalid("Attributes did not conform to the schema: %s: The attribute is required", a.Name)
		}
	}
	now := time.Now()
	u := &User{Username: username, Attrs: m, Enabled: true, Created: now, Modified: now}
	return u, p.unique(u)
}

// unique fails when the username or a unique attribute is taken.
func (p *Pool) unique(u *User) error {
	if o := p.Users[p.key(u.Username)]; o != nil && o != u {
		return core.Errorf(400, "UsernameExistsException", "User already exists")
	}
	for _, a := range p.uniqueAttrs() {
		if p.owner(a, u.Attrs[a], u, false) != nil {
			return core.Errorf(400, "UsernameExistsException", "An account with the given %s already exists.", a)
		}
	}
	return nil
}

// setAttrs applies attribute changes. A changed email or phone number becomes
// unverified unless the same call verifies it, and gets a code when the pool
// auto-verifies it.
func (p *Pool) setAttrs(u *User, attrs []Attr, client string, meta map[string]string) ([]send, error) {
	if err := p.checkAttrs(attrs); err != nil {
		return nil, err
	}
	given := attrMap(attrs)
	for _, a := range p.uniqueAttrs() {
		if v, ok := given[a]; ok && p.owner(a, v, u, false) != nil {
			return nil, core.Errorf(400, "AliasExistsException", "An account with the given %s already exists.", a)
		}
	}
	var sends []send
	for _, a := range attrs {
		old := u.Attrs[a.Name]
		if a.Value == "" {
			delete(u.Attrs, a.Name)
			continue
		}
		u.Attrs[a.Name] = a.Value
		if _, verified := given[a.Name+"_verified"]; (a.Name == "email" || a.Name == "phone_number") && old != a.Value && !verified {
			u.Attrs[a.Name+"_verified"] = "false"
			if slices.Contains(p.Settings.AutoVerifiedAttributes, a.Name) {
				sends = append(sends, p.code(u, "attr:"+a.Name, a.Name, "UpdateUserAttribute", client, meta))
			}
		}
	}
	u.Modified = time.Now()
	return sends, nil
}

func (p *Pool) deleteAttrs(u *User, names []string) error {
	for _, n := range names {
		if n == "sub" || p.def(n) != nil && p.def(n).Required {
			return invalid("Cannot delete the required attribute %s", n)
		}
	}
	for _, n := range names {
		delete(u.Attrs, n)
	}
	u.Modified = time.Now()
	return nil
}

func (p *Pool) tempPassword() string {
	n := max(p.Settings.Policies.PasswordPolicy.MinimumLength, 8)
	return "Aa1!" + core.RandString(n-4, core.AlphaNum)
}

// invite prepares the temporary-password messages of AdminCreateUser.
func (p *Pool) invite(u *User, pw string, mediums []string) []send {
	if len(mediums) == 0 {
		mediums = []string{"EMAIL"}
		if u.Attrs["phone_number"] != "" {
			mediums = []string{"SMS"}
		}
	}
	var out []send
	for _, m := range mediums {
		if attr := map[string]string{"EMAIL": "email", "SMS": "phone_number"}[m]; attr != "" && u.Attrs[attr] != "" {
			out = append(out, p.sendTo(u, attr, pw, "AdminCreateUser", "", nil))
		}
	}
	if len(out) == 0 { // nowhere to send it, but the developer still needs the password
		out = append(out, send{code: Code{Pool: p.ID, Username: u.Username, Code: pw, Type: "AdminCreateUser", Timestamp: time.Now()}})
	}
	return out
}

func (s *Service) adminCreateUser(c *core.Ctx, in *userInput) (any, error) {
	var sends []send
	if in.MessageAction == "RESEND" {
		out, err := s.withUser(in, func(p *Pool, u *User) (any, error) {
			if u.Status != "FORCE_CHANGE_PASSWORD" {
				return nil, core.Errorf(400, "UnsupportedUserStateException", "Resend not possible. %s status is not FORCE_CHANGE_PASSWORD", u.Username)
			}
			pw := cmp.Or(in.TemporaryPassword, p.tempPassword())
			p.setPassword(u, pw)
			sends = p.invite(u, pw, in.DesiredDeliveryMediums)
			return map[string]any{"User": u.userType(nil)}, nil
		})
		if err != nil {
			return nil, err
		}
		return out, s.deliver(sends...)
	}

	s.mu.Lock()
	p, err := s.pool(in.UserPoolId)
	var u *User
	if err == nil {
		u, err = p.newUser(in.Username, in.UserAttributes)
	}
	if err == nil && in.TemporaryPassword != "" {
		err = p.checkPolicy(in.TemporaryPassword)
	}
	var pre *trig
	if err == nil {
		pre = p.trig("PreSignUp_AdminCreateUser", u.Username, "", map[string]any{"userAttributes": maps.Clone(u.Attrs),
			"validationData": attrMap(in.ValidationData), "clientMetadata": in.ClientMetadata})
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if err := s.run(pre, nil); err != nil {
		return nil, err
	}

	s.mu.Lock()
	if err := p.unique(u); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	pw := cmp.Or(in.TemporaryPassword, p.tempPassword())
	p.setPassword(u, pw)
	u.Status = "FORCE_CHANGE_PASSWORD"
	p.Users[p.key(u.Username)] = u
	if in.MessageAction != "SUPPRESS" {
		sends = p.invite(u, pw, in.DesiredDeliveryMediums)
	}
	out := map[string]any{"User": u.userType(nil)}
	s.mu.Unlock()
	return out, s.deliver(sends...)
}

func (s *Service) adminGetUser(c *core.Ctx, in *userInput) (any, error) {
	return s.withUser(in, func(p *Pool, u *User) (any, error) { return u.detail(true), nil })
}

func (s *Service) adminDeleteUser(c *core.Ctx, in *userInput) (any, error) {
	return s.withUser(in, func(p *Pool, u *User) (any, error) {
		p.deleteUser(u)
		return nil, nil
	})
}

func (p *Pool) deleteUser(u *User) {
	delete(p.Users, p.key(u.Username))
	p.signOut(u.Username)
}

func (s *Service) adminUpdateUserAttributes(c *core.Ctx, in *userInput) (any, error) {
	var sends []send
	_, err := s.withUser(in, func(p *Pool, u *User) (any, error) {
		var err error
		sends, err = p.setAttrs(u, in.UserAttributes, "", in.ClientMetadata)
		return nil, err
	})
	if err != nil {
		return nil, err
	}
	return nil, s.deliver(sends...)
}

func (s *Service) adminDeleteUserAttributes(c *core.Ctx, in *userInput) (any, error) {
	return s.withUser(in, func(p *Pool, u *User) (any, error) { return nil, p.deleteAttrs(u, in.UserAttributeNames) })
}

func (s *Service) adminSetUserPassword(c *core.Ctx, in *userInput) (any, error) {
	return s.withUser(in, func(p *Pool, u *User) (any, error) {
		if err := p.checkPolicy(in.Password); err != nil {
			return nil, err
		}
		p.setPassword(u, in.Password)
		u.Status = map[bool]string{true: "CONFIRMED", false: "FORCE_CHANGE_PASSWORD"}[in.Permanent]
		return nil, nil
	})
}

func (s *Service) adminConfirmSignUp(c *core.Ctx, in *userInput) (any, error) {
	var post *trig
	_, err := s.withUser(in, func(p *Pool, u *User) (any, error) {
		if u.Status != "UNCONFIRMED" {
			return nil, core.Errorf(400, "NotAuthorizedException", "User cannot be confirmed. Current status is %s", u.Status)
		}
		u.Status, u.Modified = "CONFIRMED", time.Now()
		delete(u.Codes, "signup")
		post = p.trig("PostConfirmation_ConfirmSignUp", u.Username, "", map[string]any{"userAttributes": u.eventAttrs(), "clientMetadata": in.ClientMetadata})
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	return nil, s.run(post, nil)
}

func (s *Service) adminEnableUser(c *core.Ctx, in *userInput) (any, error) {
	return s.withUser(in, func(p *Pool, u *User) (any, error) {
		u.Enabled = true
		return nil, nil
	})
}

func (s *Service) adminDisableUser(c *core.Ctx, in *userInput) (any, error) {
	return s.withUser(in, func(p *Pool, u *User) (any, error) {
		u.Enabled = false
		p.signOut(u.Username)
		return nil, nil
	})
}

func (s *Service) adminResetUserPassword(c *core.Ctx, in *userInput) (any, error) {
	var sends []send
	_, err := s.withUser(in, func(p *Pool, u *User) (any, error) {
		x, err := p.recovery(u, "", in.ClientMetadata)
		if err != nil {
			return nil, err
		}
		u.Status, sends = "RESET_REQUIRED", []send{x}
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	return nil, s.deliver(sends...)
}

func (s *Service) adminUserGlobalSignOut(c *core.Ctx, in *userInput) (any, error) {
	return s.withUser(in, func(p *Pool, u *User) (any, error) {
		p.signOut(u.Username)
		return nil, nil
	})
}

func (s *Service) adminSetUserMFAPreference(c *core.Ctx, in *userInput) (any, error) {
	return s.withUser(in, func(p *Pool, u *User) (any, error) {
		u.setMFA(in)
		return nil, nil
	})
}

func (u *User) setMFA(in *userInput) {
	for name, m := range map[string]*mfaSetting{"SMS_MFA": in.SMSMfaSettings, "SOFTWARE_TOKEN_MFA": in.SoftwareTokenMfaSettings} {
		if m == nil {
			continue
		}
		u.MFA = slices.DeleteFunc(u.MFA, func(x string) bool { return x == name })
		if m.Enabled {
			u.MFA = append(u.MFA, name)
		}
		switch {
		case m.Enabled && m.PreferredMfa:
			u.Preferred = name
		case u.Preferred == name:
			u.Preferred = ""
		}
	}
	slices.Sort(u.MFA)
}

// signOut revokes every session (refresh token and the tokens derived from it) of a user.
func (p *Pool) signOut(username string) {
	for k, x := range p.Sessions {
		if x.Username == username {
			delete(p.Sessions, k)
		}
	}
}

var (
	filterRE   = regexp.MustCompile(`^\s*([\w:]+)\s*(\^?=)\s*"((?:[^"\\]|\\.)*)"\s*$`)
	filterable = []string{"username", "email", "phone_number", "name", "given_name", "family_name", "preferred_username", "cognito:user_status", "status", "sub"}
)

// userFilter parses a ListUsers filter: `attr = "value"` or `attr ^= "prefix"`.
func userFilter(f string) (func(*User) bool, error) {
	if strings.TrimSpace(f) == "" {
		return func(*User) bool { return true }, nil
	}
	m := filterRE.FindStringSubmatch(f)
	if m == nil {
		return nil, invalid("Error while parsing filter.")
	}
	if !slices.Contains(filterable, m[1]) {
		return nil, invalid("Invalid search attribute")
	}
	attr, prefix, want := m[1], m[2] == "^=", strings.ReplaceAll(m[3], `\"`, `"`)
	return func(u *User) bool {
		v := u.Attrs[attr]
		switch attr {
		case "username":
			v = u.Username
		case "cognito:user_status":
			v = u.Status
		case "status":
			v = map[bool]string{true: "Enabled", false: "Disabled"}[u.Enabled]
		}
		if prefix {
			return strings.HasPrefix(v, want)
		}
		return v == want
	}, nil
}

func (s *Service) listUsers(c *core.Ctx, in *listInput) (any, error) {
	match, err := userFilter(in.Filter)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	var keys []string
	for k, u := range p.Users {
		if match(u) {
			keys = append(keys, k)
		}
	}
	keys, next := page(keys, in.PaginationToken, in.Limit)
	users := []map[string]any{}
	for _, k := range keys {
		users = append(users, p.Users[k].userType(in.AttributesToGet))
	}
	return withNext(map[string]any{"Users": users}, "PaginationToken", next), nil
}

// ---- console ----

// Resources lists pools and users for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	pools := core.Table{Name: "User pools", Columns: []string{"Id", "Name", "Region", "Account", "Users", "Clients"}}
	users := core.Table{Name: "Users", Columns: []string{"Pool", "Username", "Status", "Enabled", "Email"}}
	for _, id := range core.SortedKeys(s.st.Pools) {
		p := s.st.Pools[id]
		pools.Rows = append(pools.Rows, []string{p.ID, p.Name, p.Region, p.Account, strconv.Itoa(len(p.Users)), strconv.Itoa(len(p.Clients))})
		for _, k := range core.SortedKeys(p.Users) {
			u := p.Users[k]
			users.Rows = append(users.Rows, []string{p.ID, u.Username, u.Status, strconv.FormatBool(u.Enabled), u.Attrs["email"]})
		}
	}
	return []core.Table{pools, users}
}

// Inspect returns a pool's settings, clients (without secrets), groups and users.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.st.Pools[id]
	if p == nil {
		return nil
	}
	clients := []Client{}
	for _, k := range core.SortedKeys(p.Clients) {
		cl := *p.Clients[k]
		cl.ClientSecret = ""
		clients = append(clients, cl)
	}
	users := []map[string]any{}
	for _, k := range core.SortedKeys(p.Users) {
		u := p.Users[k]
		v := u.userType(nil)
		v["Groups"] = slices.Clone(u.Groups)
		users = append(users, v)
	}
	return map[string]any{"pool": p.describe(), "clients": clients, "groups": p.groupList(core.SortedKeys(p.Groups)), "users": users}
}

// Codes returns the outbox: confirmation, recovery and MFA codes and temporary passwords, oldest first.
func (s *Service) Codes() []Code {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.st.Codes)
}

// codesHandler serves GET /_aws/cognito/codes[?pool=&username=] (username also matches the destination).
func (s *Service) codesHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	out := []Code{}
	for _, c := range s.Codes() {
		if (q.Get("pool") == "" || q.Get("pool") == c.Pool) && (q.Get("username") == "" || q.Get("username") == c.Username || q.Get("username") == c.Destination) {
			out = append(out, c)
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
