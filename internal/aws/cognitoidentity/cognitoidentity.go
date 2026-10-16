// Package cognitoidentity emulates Amazon Cognito identity pools (JSON 1.1):
// pools with their roles and role mappings; identities that sign in with
// Cognito user pool ID tokens (checked against pogi's pool JWKS), OIDC tokens
// (checked against the issuer's JWKS), developer logins, or none at all; STS
// credentials for the role the pool picks; and RS256 OpenID tokens, whose
// JWKS is served at /_aws/cognito-identity/.well-known/jwks_uri.
package cognitoidentity

import (
	"cmp"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"net/http"
	"path"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// issuer names the OpenID tokens, as AWS does.
const issuer = "https://cognito-identity.amazonaws.com"

var b64 = base64.RawURLEncoding

// Service is the Cognito identity pools emulator.
type Service struct {
	hub *core.Hub
	rpc *core.RPC
	mu  sync.Mutex
	st  state
	key *rsa.PrivateKey // signs the OpenID tokens; made on first use
	kid string
}

type state struct {
	Pools      map[string]*Pool     `json:"pools"`      // by pool id
	Identities map[string]*Identity `json:"identities"` // by identity id
	Key        []byte               `json:"key,omitempty"`
}

// Pool is an identity pool: its API shape, owner and roles. Updates replace
// maps and slices instead of changing them, so a copy taken under the lock
// stays valid after it.
type Pool struct {
	poolShape
	Account      string                  `json:"account"`
	Roles        map[string]string       `json:"roles,omitempty"`
	RoleMappings map[string]*RoleMapping `json:"roleMappings,omitempty"`
}

type poolShape struct {
	IdentityPoolId                 string
	IdentityPoolName               string
	AllowUnauthenticatedIdentities bool
	AllowClassicFlow               bool              `json:",omitempty"`
	SupportedLoginProviders        map[string]string `json:",omitempty"`
	DeveloperProviderName          string            `json:",omitempty"`
	OpenIdConnectProviderARNs      []string          `json:",omitempty"`
	CognitoIdentityProviders       []Provider        `json:",omitempty"`
	SamlProviderARNs               []string          `json:",omitempty"`
	IdentityPoolTags               map[string]string `json:",omitempty"`
}

// Provider is a Cognito user pool app client a pool accepts (API shape).
type Provider struct {
	ProviderName         string
	ClientId             string `json:",omitempty"`
	ServerSideTokenCheck bool
}

// RoleMapping picks the role of an authenticated identity from its token (API shape).
type RoleMapping struct {
	Type                    string
	AmbiguousRoleResolution string `json:",omitempty"`
	RulesConfiguration      *struct {
		Rules []struct{ Claim, MatchType, Value, RoleARN string }
	} `json:",omitempty"`
}

// Identity is an identity of a pool and the logins linked to it.
type Identity struct {
	Id       string    `json:"id"`
	Pool     string    `json:"pool"`
	Logins   []Login   `json:"logins,omitempty"`
	Created  time.Time `json:"created"`
	Modified time.Time `json:"modified"`
}

// Login is a provider's user linked to an identity: a token's subject, or a
// developer user identifier.
type Login struct {
	Provider string `json:"provider"`
	Sub      string `json:"sub"`
}

// New creates the identity pools service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateIdentityPool":                 core.H(s.createPool),
		"DescribeIdentityPool":               core.H(s.describePool),
		"ListIdentityPools":                  core.H(s.listPools),
		"UpdateIdentityPool":                 core.H(s.updatePool),
		"DeleteIdentityPool":                 core.H(s.deletePool),
		"SetIdentityPoolRoles":               core.H(s.setRoles),
		"GetIdentityPoolRoles":               core.H(s.getRoles),
		"GetId":                              core.H(s.getID),
		"GetCredentialsForIdentity":          core.H(s.getCredentials),
		"GetOpenIdToken":                     core.H(s.getOpenIDToken),
		"GetOpenIdTokenForDeveloperIdentity": core.H(s.developerToken),
		"LookupDeveloperIdentity":            core.H(s.lookupDeveloper),
		"MergeDeveloperIdentities":           core.H(s.mergeDevelopers),
		"UnlinkDeveloperIdentity":            core.H(s.unlinkDeveloper),
		"ListIdentities":                     core.H(s.listIdentities),
		"DescribeIdentity":                   core.H(s.describeIdentity),
		"DeleteIdentities":                   core.H(s.deleteIdentities),
		"TagResource":                        core.H(s.tagResource),
		"UntagResource":                      core.H(s.untagResource),
		"ListTagsForResource":                core.H(s.listTags),
	}}
	if hub != nil {
		hub.Mount("/_aws/cognito-identity/.well-known/", http.HandlerFunc(s.wellKnown))
	}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "cognito-identity", Cloud: core.AWS, Title: "Cognito Identity Pools", Targets: []string{"AWSCognitoIdentityService"},
		Signing: []string{"cognito-identity"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st, s.key = state{Pools: map[string]*Pool{}, Identities: map[string]*Identity{}}, nil
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := state{Pools: map[string]*Pool{}, Identities: map[string]*Identity{}}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	var key *rsa.PrivateKey
	if st.Key != nil {
		k, err := x509.ParsePKCS1PrivateKey(st.Key)
		if err != nil {
			return err
		}
		key = k
	}
	s.mu.Lock()
	s.st, s.key = st, key
	if key != nil {
		s.kid = kidOf(key)
	}
	s.mu.Unlock()
	return nil
}

func notFound(format string, a ...any) error {
	return core.Errorf(404, "ResourceNotFoundException", format, a...)
}

func notAuthorized(format string, a ...any) error {
	return core.Errorf(403, "NotAuthorizedException", format, a...)
}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterException", format, a...)
}

var errNoRole = core.Errorf(400, "InvalidIdentityPoolConfigurationException", "Invalid identity pool configuration. Check assigned IAM roles for this pool.")

// region is the region of a pool or identity id ("us-east-1:uuid").
func region(id string) string { r, _, _ := strings.Cut(id, ":"); return r }

// pool finds a pool; lock held.
func (s *Service) pool(id string) (*Pool, error) {
	if p := s.st.Pools[id]; p != nil {
		return p, nil
	}
	return nil, notFound("IdentityPool '%s' not found.", id)
}

// snapshot copies a pool under the lock.
func (s *Service) snapshot(id string) (Pool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(id)
	if err != nil {
		return Pool{}, err
	}
	return *p, nil
}

// ---- pools ----

var poolName = regexp.MustCompile(`^[\w\s+=,.@-]{1,128}$`)

func (s *Service) createPool(c *core.Ctx, in *poolShape) (any, error) {
	if !poolName.MatchString(in.IdentityPoolName) {
		return nil, invalid("1 validation error detected: Value '%s' at 'identityPoolName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w\\s+=,.@-]+", in.IdentityPoolName)
	}
	p := &Pool{poolShape: *in, Account: c.Account}
	p.IdentityPoolId = c.Region + ":" + core.UUID()
	s.mu.Lock()
	s.st.Pools[p.IdentityPoolId] = p
	s.mu.Unlock()
	return p.poolShape, nil
}

func (s *Service) describePool(c *core.Ctx, in *struct{ IdentityPoolId string }) (any, error) {
	p, err := s.snapshot(in.IdentityPoolId)
	return p.poolShape, err
}

func (s *Service) listPools(c *core.Ctx, in *struct {
	MaxResults int
	NextToken  string
}) (any, error) {
	type short struct{ IdentityPoolId, IdentityPoolName string }
	var all []short
	s.mu.Lock()
	for _, p := range s.st.Pools {
		if p.Account == c.Account && region(p.IdentityPoolId) == c.Region {
			all = append(all, short{p.IdentityPoolId, p.IdentityPoolName})
		}
	}
	s.mu.Unlock()
	items, next := page(all, func(p short) string { return p.IdentityPoolId }, in.NextToken, in.MaxResults)
	return map[string]any{"IdentityPools": items, "NextToken": next}, nil
}

func (s *Service) updatePool(c *core.Ctx, in *poolShape) (any, error) {
	if !poolName.MatchString(in.IdentityPoolName) {
		return nil, invalid("Invalid identity pool name %q.", in.IdentityPoolName)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.IdentityPoolId)
	if err != nil {
		return nil, err
	}
	tags := p.IdentityPoolTags
	p.poolShape = *in
	if in.IdentityPoolTags == nil {
		p.IdentityPoolTags = tags
	}
	return p.poolShape, nil
}

func (s *Service) deletePool(c *core.Ctx, in *struct{ IdentityPoolId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.pool(in.IdentityPoolId); err != nil {
		return nil, err
	}
	delete(s.st.Pools, in.IdentityPoolId)
	maps.DeleteFunc(s.st.Identities, func(_ string, x *Identity) bool { return x.Pool == in.IdentityPoolId })
	return nil, nil
}

type rolesShape struct {
	IdentityPoolId string
	Roles          map[string]string       `json:",omitempty"`
	RoleMappings   map[string]*RoleMapping `json:",omitempty"`
}

func (s *Service) setRoles(c *core.Ctx, in *rolesShape) (any, error) {
	for k := range in.Roles {
		if k != "authenticated" && k != "unauthenticated" {
			return nil, invalid("Invalid role type %q: roles are authenticated or unauthenticated.", k)
		}
	}
	for k, m := range in.RoleMappings {
		switch {
		case m.Type != "Token" && m.Type != "Rules":
			return nil, invalid("Invalid role mapping type %q for %s.", m.Type, k)
		case m.AmbiguousRoleResolution != "AuthenticatedRole" && m.AmbiguousRoleResolution != "Deny":
			return nil, invalid("AmbiguousRoleResolution must be AuthenticatedRole or Deny for %s.", k)
		case m.Type == "Rules" && (m.RulesConfiguration == nil || len(m.RulesConfiguration.Rules) == 0):
			return nil, invalid("RulesConfiguration is required for a Rules role mapping (%s).", k)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.IdentityPoolId)
	if err != nil {
		return nil, err
	}
	p.Roles, p.RoleMappings = in.Roles, in.RoleMappings
	return nil, nil
}

func (s *Service) getRoles(c *core.Ctx, in *struct{ IdentityPoolId string }) (any, error) {
	p, err := s.snapshot(in.IdentityPoolId)
	return rolesShape{p.IdentityPoolId, p.Roles, p.RoleMappings}, err
}

// ---- tokens and logins ----

// login is a verified login with the claims of its token.
type login struct {
	Login
	claims map[string]any
}

func (l login) mappingKey() string {
	if aud, _ := l.claims["aud"].(string); strings.HasPrefix(l.Provider, "cognito-idp.") && aud != "" {
		return l.Provider + ":" + aud // user pools map per app client
	}
	return l.Provider
}

// logins verifies the tokens of a request against pool p, without the lock:
// it may fetch the issuer's keys.
func (s *Service) logins(c *core.Ctx, p Pool, in map[string]string) ([]login, error) {
	var out []login
	for _, provider := range slices.Sorted(maps.Keys(in)) {
		claims, err := s.verify(c, p, provider, in[provider])
		if err != nil {
			return nil, err
		}
		sub, _ := claims["sub"].(string)
		out = append(out, login{Login{provider, sub}, claims})
	}
	return out, nil
}

var errToken = notAuthorized("Invalid login token. Not a valid OpenId Connect identity token.")

// verify checks a login token from provider and returns its claims.
func (s *Service) verify(c *core.Ctx, p Pool, provider, token string) (map[string]any, error) {
	switch {
	case provider == "cognito-identity.amazonaws.com": // our own OpenID tokens (developer flow)
		s.mu.Lock()
		key, kid := s.signer()
		s.mu.Unlock()
		claims, err := checkJWT(token, map[string]*rsa.PublicKey{kid: &key.PublicKey})
		if err != nil || claims["aud"] != p.IdentityPoolId {
			return nil, cmp.Or(err, errToken)
		}
		return claims, nil

	case strings.HasPrefix(provider, "cognito-idp."): // cognito-idp.<region>.amazonaws.com/<user pool>
		if !slices.ContainsFunc(p.CognitoIdentityProviders, func(x Provider) bool { return x.ProviderName == provider }) {
			return nil, notAuthorized("Invalid login token. Token is not from a supported provider of this identity pool.")
		}
		_, userPool, _ := strings.Cut(provider, "/")
		status, _, body := s.hub.Do(c.Account, c.Region, "cognito-idp", http.MethodGet, "/"+userPool+"/.well-known/jwks.json", nil, nil)
		if status != http.StatusOK {
			return nil, notAuthorized("Invalid login token. User pool %s does not exist.", userPool)
		}
		claims, err := checkJWT(token, jwks(body))
		if err != nil {
			return nil, err
		}
		// pogi's issuer is http://<host>/<pool>, not cognito-idp.<region>.amazonaws.com: the key decides
		iss, _ := claims["iss"].(string)
		aud, _ := claims["aud"].(string)
		switch {
		case path.Base(iss) != userPool:
			return nil, notAuthorized("Invalid login token. Issuer doesn't match providerName")
		case claims["token_use"] != "id":
			return nil, notAuthorized("Invalid login token. Not an id token.")
		case !slices.ContainsFunc(p.CognitoIdentityProviders, func(x Provider) bool { return x.ProviderName == provider && (x.ClientId == "" || x.ClientId == aud) }):
			return nil, notAuthorized("Invalid login token. Incorrect token audience.")
		}
		return claims, nil
	}

	// OIDC providers (IAM OIDC provider ARNs), the social providers and SAML
	_, social := p.SupportedLoginProviders[provider]
	oidc := slices.ContainsFunc(p.OpenIdConnectProviderARNs, func(arn string) bool { return strings.HasSuffix(arn, ":oidc-provider/"+provider) })
	if !social && !oidc && !slices.Contains(p.SamlProviderARNs, provider) {
		return nil, notAuthorized("Invalid login token. Token is not from a supported provider of this identity pool.")
	}
	if strings.Count(token, ".") != 2 {
		// ponytail: SAML assertions and opaque social tokens (Facebook, Amazon) are trusted as they are
		sum := sha256.Sum256([]byte(token))
		return map[string]any{"sub": hex.EncodeToString(sum[:16])}, nil
	}
	claims, err := checkJWT(token, nil) // unverified: only to read the issuer
	if err != errKeyless {
		return nil, cmp.Or(err, errToken)
	}
	iss, _ := claims["iss"].(string)
	if strings.TrimPrefix(strings.TrimPrefix(iss, "https://"), "http://") != provider {
		return nil, notAuthorized("Invalid login token. Issuer doesn't match providerName")
	}
	keys, err := fetchJWKS(iss)
	if err != nil {
		return nil, notAuthorized("Invalid login token. Couldn't verify signed token: %v", err)
	}
	return checkJWT(token, keys)
}

var errKeyless = errors.New("no keys")

// checkJWT verifies an RS256 JWT against keys (by kid) and its expiry, and
// returns its claims. With no keys it only decodes and returns errKeyless.
func checkJWT(token string, keys map[string]*rsa.PublicKey) (map[string]any, error) {
	part := strings.Split(token, ".")
	if len(part) != 3 {
		return nil, errToken
	}
	var h struct{ Alg, Kid string }
	var claims map[string]any
	hb, err1 := b64.DecodeString(part[0])
	cb, err2 := b64.DecodeString(part[1])
	sig, err3 := b64.DecodeString(part[2])
	if err1 != nil || err2 != nil || err3 != nil || json.Unmarshal(hb, &h) != nil || json.Unmarshal(cb, &claims) != nil {
		return nil, errToken
	}
	if keys == nil {
		return claims, errKeyless
	}
	key := keys[h.Kid]
	sum := sha256.Sum256([]byte(part[0] + "." + part[1]))
	if h.Alg != "RS256" || key == nil || rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig) != nil {
		return nil, notAuthorized("Invalid login token. Couldn't verify signed token.")
	}
	if exp, _ := claims["exp"].(float64); exp > 0 && float64(time.Now().Unix()) >= exp {
		return nil, notAuthorized("Invalid login token. Token expired: %s", time.Unix(int64(exp), 0).UTC().Format(time.RFC1123))
	}
	return claims, nil
}

// jwks reads the RSA keys of a JWKS document.
func jwks(b []byte) map[string]*rsa.PublicKey {
	var doc struct {
		Keys []struct{ Kty, Kid, N, E string }
	}
	json.Unmarshal(b, &doc)
	out := map[string]*rsa.PublicKey{}
	for _, k := range doc.Keys {
		n, err1 := b64.DecodeString(k.N)
		e, err2 := b64.DecodeString(k.E)
		if k.Kty == "RSA" && err1 == nil && err2 == nil {
			out[k.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
		}
	}
	return out
}

var web = &http.Client{Timeout: 10 * time.Second}

// fetchJWKS finds an OIDC issuer's keys through its discovery document.
// ponytail: fetched on every login and the token's audience is not checked
// against the IAM OIDC provider's client ids; cache keys per issuer if logins get hot.
func fetchJWKS(iss string) (map[string]*rsa.PublicKey, error) {
	get := func(url string, v any) error {
		res, err := web.Get(url)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("%s: %s", url, res.Status)
		}
		return json.NewDecoder(res.Body).Decode(v)
	}
	var conf struct {
		JwksURI string `json:"jwks_uri"`
	}
	if err := get(strings.TrimSuffix(iss, "/")+"/.well-known/openid-configuration", &conf); err != nil {
		return nil, err
	}
	var doc json.RawMessage
	if err := get(conf.JwksURI, &doc); err != nil {
		return nil, err
	}
	return jwks(doc), nil
}

// signer returns the key of the OpenID tokens, made on first use; lock held.
func (s *Service) signer() (*rsa.PrivateKey, string) {
	if s.key == nil {
		s.key, _ = rsa.GenerateKey(rand.Reader, 2048)
		s.kid, s.st.Key = kidOf(s.key), x509.MarshalPKCS1PrivateKey(s.key)
	}
	return s.key, s.kid
}

func kidOf(k *rsa.PrivateKey) string {
	sum := sha256.Sum256(k.N.Bytes())
	return "pogi-" + hex.EncodeToString(sum[:8])
}

// token issues an OpenID token for identity id of pool.
func (s *Service) token(pool, id string, ls []Login, ttl time.Duration) string {
	amr := []string{"unauthenticated"}
	if len(ls) > 0 {
		amr = []string{"authenticated"}
		for _, l := range ls {
			if !slices.Contains(amr, l.Provider) {
				amr = append(amr, l.Provider)
			}
		}
	}
	now := time.Now()
	s.mu.Lock()
	key, kid := s.signer()
	s.mu.Unlock()
	h, _ := json.Marshal(map[string]string{"kid": kid, "typ": "JWS", "alg": "RS256"})
	body, _ := json.Marshal(map[string]any{"iss": issuer, "aud": pool, "sub": id, "amr": amr, "iat": now.Unix(), "exp": now.Add(ttl).Unix()})
	msg := b64.EncodeToString(h) + "." + b64.EncodeToString(body)
	sum := sha256.Sum256([]byte(msg))
	sig, _ := rsa.SignPKCS1v15(nil, key, crypto.SHA256, sum[:])
	return msg + "." + b64.EncodeToString(sig)
}

// wellKnown serves the JWKS and OpenID configuration of the OpenID tokens
// (AWS: https://cognito-identity.amazonaws.com/.well-known/...).
func (s *Service) wellKnown(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	key, kid := s.signer()
	s.mu.Unlock()
	var out any
	switch path.Base(r.URL.Path) {
	case "jwks_uri":
		out = map[string]any{"keys": []map[string]string{{"kty": "RSA", "alg": "RS256", "use": "sig", "kid": kid,
			"n": b64.EncodeToString(key.N.Bytes()), "e": b64.EncodeToString(big.NewInt(int64(key.E)).Bytes())}}}
	case "openid-configuration":
		scheme := "http://"
		if r.TLS != nil {
			scheme = "https://"
		}
		out = map[string]any{"issuer": issuer, "jwks_uri": scheme + cmp.Or(r.Host, "localhost:7644") + "/_aws/cognito-identity/.well-known/jwks_uri",
			"response_types_supported": []string{"id_token"}, "subject_types_supported": []string{"public"},
			"id_token_signing_alg_values_supported": []string{"RS256"}}
	default:
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// ---- identities ----

// link returns the identity of pool that id or one of logins names, made if
// there is none, with every login linked to it; lock held.
// ponytail: logins are found by scanning the identities; index them if pools get big.
func (s *Service) link(pool, id string, ls []Login) *Identity {
	x := s.st.Identities[id]
	for _, y := range s.st.Identities {
		if x == nil && y.Pool == pool && slices.ContainsFunc(ls, func(l Login) bool { return slices.Contains(y.Logins, l) }) {
			x = y
		}
	}
	now := time.Now()
	if x == nil {
		x = &Identity{Id: region(pool) + ":" + core.UUID(), Pool: pool, Created: now}
		s.st.Identities[x.Id] = x
	}
	logins := slices.Clone(x.Logins)
	for _, l := range ls {
		if !slices.Contains(logins, l) {
			logins = append(logins, l)
		}
	}
	x.Logins, x.Modified = logins, now
	return x
}

func plain(ls []login) []Login {
	out := make([]Login, len(ls))
	for i, l := range ls {
		out[i] = l.Login
	}
	return out
}

func (s *Service) getID(c *core.Ctx, in *struct {
	IdentityPoolId string
	Logins         map[string]string
}) (any, error) {
	p, err := s.snapshot(in.IdentityPoolId)
	if err != nil {
		return nil, err
	}
	ls, err := s.logins(c, p, in.Logins)
	if err != nil {
		return nil, err
	}
	if len(ls) == 0 && !p.AllowUnauthenticatedIdentities {
		return nil, notAuthorized("Unauthenticated access is not supported for this identity pool.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]string{"IdentityId": s.link(p.IdentityPoolId, "", plain(ls)).Id}, nil
}

// authorize checks the logins of a request for identity id: they must be
// linked to it, and an identity without any must be allowed by its pool.
func (s *Service) authorize(c *core.Ctx, id string, logins map[string]string) (Pool, []login, error) {
	s.mu.Lock()
	var p Pool
	var linked []Login
	x := s.st.Identities[id]
	if x != nil && s.st.Pools[x.Pool] != nil {
		p, linked = *s.st.Pools[x.Pool], x.Logins
	}
	s.mu.Unlock()
	if p.IdentityPoolId == "" {
		return p, nil, notFound("Identity '%s' not found.", id)
	}
	ls, err := s.logins(c, p, logins)
	switch {
	case err != nil:
		return p, nil, err
	case len(ls) == 0 && len(linked) > 0:
		return p, nil, notAuthorized("Access to Identity '%s' is forbidden.", id)
	case len(ls) == 0 && !p.AllowUnauthenticatedIdentities:
		return p, nil, notAuthorized("Unauthenticated access is not supported for this identity pool.")
	}
	for _, l := range ls {
		own := l.Provider == "cognito-identity.amazonaws.com" && l.Sub == id // the identity's own OpenID token
		if !own && !slices.Contains(linked, l.Login) {
			return p, nil, notAuthorized("Logins don't match. Please include at least one valid login for this identity or identity pool.")
		}
	}
	return p, ls, nil
}

// role picks the role for an identity signed in with ls (none: unauthenticated).
func role(p Pool, ls []login, custom string) (string, error) {
	kind := "unauthenticated"
	if len(ls) > 0 {
		kind = "authenticated"
	}
	for _, l := range ls {
		m := p.RoleMappings[l.mappingKey()]
		if m == nil {
			continue
		}
		if r := m.pick(l.claims, custom); r != "" {
			return r, nil
		}
		if m.AmbiguousRoleResolution == "Deny" {
			return "", notAuthorized("No role matched the claims of the token, and the role mapping denies.")
		}
	}
	if r := p.Roles[kind]; r != "" {
		return r, nil
	}
	return "", errNoRole
}

// pick applies a role mapping to a token's claims; "" when it is ambiguous.
func (m *RoleMapping) pick(claims map[string]any, custom string) string {
	if m.Type == "Token" {
		roles := values(claims["cognito:roles"])
		pref, _ := claims["cognito:preferred_role"].(string)
		switch {
		case custom != "" && slices.Contains(roles, custom):
			return custom
		case pref != "":
			return pref
		case len(roles) == 1:
			return roles[0]
		}
		return ""
	}
	for _, r := range m.RulesConfiguration.Rules {
		vs := values(claims[r.Claim])
		var ok bool
		switch r.MatchType {
		case "Equals":
			ok = slices.Contains(vs, r.Value)
		case "NotEqual":
			ok = len(vs) > 0 && !slices.Contains(vs, r.Value)
		case "Contains":
			ok = slices.ContainsFunc(vs, func(v string) bool { return strings.Contains(v, r.Value) })
		case "StartsWith":
			ok = slices.ContainsFunc(vs, func(v string) bool { return strings.HasPrefix(v, r.Value) })
		}
		if ok {
			return r.RoleARN
		}
	}
	return ""
}

// values is a claim as strings: a list's items, or the value itself.
func values(v any) []string {
	switch v := v.(type) {
	case nil:
		return nil
	case string:
		return []string{v}
	case []any:
		var out []string
		for _, x := range v {
			out = append(out, values(x)...)
		}
		return out
	case float64:
		return []string{strconv.FormatFloat(v, 'f', -1, 64)}
	}
	return []string{fmt.Sprint(v)}
}

func (s *Service) getCredentials(c *core.Ctx, in *struct {
	IdentityId, CustomRoleArn string
	Logins                    map[string]string
}) (any, error) {
	p, ls, err := s.authorize(c, in.IdentityId, in.Logins)
	if err != nil {
		return nil, err
	}
	arn, err := role(p, ls, in.CustomRoleArn)
	if err != nil {
		return nil, err
	}
	_, _, account, res, ok := core.ParseARN(arn)
	if !ok || !strings.HasPrefix(res, "role/") {
		return nil, errNoRole
	}
	const session = "CognitoIdentityCredentials"
	akid, secret := "ASIA"+core.RandString(16, core.UpperNum), core.RandString(40, core.AlphaNum)
	s.hub.AddIdentity(akid, core.Identity{Account: account, Secret: secret, UserID: "AROA" + core.RandString(17, core.UpperNum) + ":" + session,
		ARN: core.GlobalARN("sts", region(in.IdentityId), account, "assumed-role/"+path.Base(res)+"/"+session)})
	return map[string]any{"IdentityId": in.IdentityId, "Credentials": map[string]any{"AccessKeyId": akid, "SecretKey": secret,
		"SessionToken": core.RandString(180, core.AlphaNum), "Expiration": core.Time(time.Now().Add(time.Hour))}}, nil
}

func (s *Service) getOpenIDToken(c *core.Ctx, in *struct {
	IdentityId string
	Logins     map[string]string
}) (any, error) {
	p, ls, err := s.authorize(c, in.IdentityId, in.Logins)
	if err != nil {
		return nil, err
	}
	return map[string]string{"IdentityId": in.IdentityId, "Token": s.token(p.IdentityPoolId, in.IdentityId, plain(ls), 10*time.Minute)}, nil
}

func (s *Service) developerToken(c *core.Ctx, in *struct {
	IdentityPoolId, IdentityId string
	Logins                     map[string]string
	TokenDuration              int64
}) (any, error) {
	p, err := s.snapshot(in.IdentityPoolId)
	if err != nil {
		return nil, err
	}
	dev := p.DeveloperProviderName
	user, ok := in.Logins[dev]
	if dev == "" || !ok {
		return nil, invalid("The Logins map must hold the pool's developer provider name.")
	}
	if in.TokenDuration < 0 || in.TokenDuration > 86400 {
		return nil, invalid("TokenDuration must be between 1 and 86400 seconds.")
	}
	social := maps.Clone(in.Logins)
	delete(social, dev)
	ls, err := s.logins(c, p, social)
	if err != nil {
		return nil, err
	}
	all := append(plain(ls), Login{dev, user})
	s.mu.Lock()
	if x := s.st.Identities[in.IdentityId]; in.IdentityId != "" && (x == nil || x.Pool != p.IdentityPoolId) {
		s.mu.Unlock()
		return nil, notFound("Identity '%s' not found.", in.IdentityId)
	}
	x := s.link(p.IdentityPoolId, in.IdentityId, all)
	s.mu.Unlock()
	ttl := time.Duration(cmp.Or(in.TokenDuration, 900)) * time.Second
	return map[string]string{"IdentityId": x.Id, "Token": s.token(p.IdentityPoolId, x.Id, all, ttl)}, nil
}

// developer finds a pool's identity by id or by developer user; lock held.
func (s *Service) developer(pool, id, user string) (*Pool, *Identity, error) {
	p, err := s.pool(pool)
	if err != nil {
		return nil, nil, err
	}
	for _, x := range s.st.Identities {
		if x.Pool == pool && (x.Id == id || id == "" && slices.Contains(x.Logins, Login{p.DeveloperProviderName, user})) {
			return p, x, nil
		}
	}
	return nil, nil, notFound("Identity not found for %s.", cmp.Or(id, user))
}

func (s *Service) lookupDeveloper(c *core.Ctx, in *struct {
	IdentityPoolId, IdentityId, DeveloperUserIdentifier string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, x, err := s.developer(in.IdentityPoolId, in.IdentityId, in.DeveloperUserIdentifier)
	if err != nil {
		return nil, err
	}
	users := []string{}
	for _, l := range x.Logins {
		if l.Provider == p.DeveloperProviderName {
			users = append(users, l.Sub)
		}
	}
	return map[string]any{"IdentityId": x.Id, "DeveloperUserIdentifierList": users}, nil
}

func (s *Service) mergeDevelopers(c *core.Ctx, in *struct {
	SourceUserIdentifier, DestinationUserIdentifier, DeveloperProviderName, IdentityPoolId string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.IdentityPoolId)
	if err != nil {
		return nil, err
	}
	if in.DeveloperProviderName != p.DeveloperProviderName {
		return nil, invalid("Developer provider name %q is not the pool's.", in.DeveloperProviderName)
	}
	_, src, err := s.developer(in.IdentityPoolId, "", in.SourceUserIdentifier)
	if err != nil {
		return nil, err
	}
	_, dst, err := s.developer(in.IdentityPoolId, "", in.DestinationUserIdentifier)
	if err != nil {
		return nil, err
	}
	if src != dst {
		s.link(p.IdentityPoolId, dst.Id, src.Logins)
		delete(s.st.Identities, src.Id)
	}
	return map[string]string{"IdentityId": dst.Id}, nil
}

func (s *Service) unlinkDeveloper(c *core.Ctx, in *struct {
	IdentityId, IdentityPoolId, DeveloperProviderName, DeveloperUserIdentifier string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, x, err := s.developer(in.IdentityPoolId, in.IdentityId, "")
	if err != nil {
		return nil, err
	}
	x.Logins = slices.DeleteFunc(slices.Clone(x.Logins), func(l Login) bool { return l == Login{in.DeveloperProviderName, in.DeveloperUserIdentifier} })
	x.Modified = time.Now()
	return nil, nil
}

type identityShape struct {
	IdentityId                     string
	Logins                         []string
	CreationDate, LastModifiedDate core.Time
}

func (x *Identity) shape() identityShape {
	providers := []string{}
	for _, l := range x.Logins {
		if !slices.Contains(providers, l.Provider) {
			providers = append(providers, l.Provider)
		}
	}
	return identityShape{x.Id, providers, core.Time(x.Created), core.Time(x.Modified)}
}

func (s *Service) listIdentities(c *core.Ctx, in *struct {
	IdentityPoolId string
	MaxResults     int
	NextToken      string
}) (any, error) {
	s.mu.Lock()
	_, err := s.pool(in.IdentityPoolId)
	var all []identityShape
	for _, x := range s.st.Identities {
		if x.Pool == in.IdentityPoolId {
			all = append(all, x.shape())
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	items, next := page(all, func(x identityShape) string { return x.IdentityId }, in.NextToken, in.MaxResults)
	return map[string]any{"IdentityPoolId": in.IdentityPoolId, "Identities": items, "NextToken": next}, nil
}

func (s *Service) describeIdentity(c *core.Ctx, in *struct{ IdentityId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if x := s.st.Identities[in.IdentityId]; x != nil {
		return x.shape(), nil
	}
	return nil, notFound("Identity '%s' not found.", in.IdentityId)
}

func (s *Service) deleteIdentities(c *core.Ctx, in *struct{ IdentityIdsToDelete []string }) (any, error) {
	if n := len(in.IdentityIdsToDelete); n == 0 || n > 60 {
		return nil, invalid("IdentityIdsToDelete must hold 1 to 60 identity ids.")
	}
	s.mu.Lock()
	for _, id := range in.IdentityIdsToDelete {
		delete(s.st.Identities, id)
	}
	s.mu.Unlock()
	return map[string]any{"UnprocessedIdentityIds": []any{}}, nil
}

// ---- tags ----

// tagged finds the pool of an identity pool ARN; lock held.
func (s *Service) tagged(arn string) (*Pool, error) {
	_, _, _, res, _ := core.ParseARN(arn)
	if id, ok := strings.CutPrefix(res, "identitypool/"); ok && s.st.Pools[id] != nil {
		return s.st.Pools[id], nil
	}
	return nil, notFound("Resource %s not found.", arn)
}

func (s *Service) tagResource(c *core.Ctx, in *struct {
	ResourceArn string
	Tags        map[string]string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.tagged(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	tags := maps.Clone(p.IdentityPoolTags)
	if tags == nil {
		tags = map[string]string{}
	}
	maps.Copy(tags, in.Tags)
	p.IdentityPoolTags = tags
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, in *struct {
	ResourceArn string
	TagKeys     []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.tagged(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	p.IdentityPoolTags = maps.Clone(p.IdentityPoolTags)
	for _, k := range in.TagKeys {
		delete(p.IdentityPoolTags, k)
	}
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, in *struct{ ResourceArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.tagged(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	if p.IdentityPoolTags == nil {
		return map[string]any{"Tags": map[string]string{}}, nil
	}
	return map[string]any{"Tags": p.IdentityPoolTags}, nil
}

// page sorts items by key and returns those after token, at most max (default 60).
func page[T any](items []T, key func(T) string, token string, max int) ([]T, string) {
	slices.SortFunc(items, func(a, b T) int { return strings.Compare(key(a), key(b)) })
	items = items[sort.Search(len(items), func(i int) bool { return key(items[i]) > token }):]
	if max <= 0 {
		max = 60
	}
	if len(items) > max {
		return items[:max], key(items[max-1])
	}
	if items == nil {
		items = []T{}
	}
	return items, ""
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	pools := core.Table{Name: "Identity pools", Columns: []string{"Id", "Name", "Account", "Unauthenticated", "Identities"}}
	ids := core.Table{Name: "Identities", Columns: []string{"Id", "Pool", "Logins"}}
	count := map[string]int{}
	for _, k := range core.SortedKeys(s.st.Identities) {
		x := s.st.Identities[k]
		count[x.Pool]++
		ids.Rows = append(ids.Rows, []string{x.Id, x.Pool, strings.Join(x.shape().Logins, ", ")})
	}
	for _, k := range core.SortedKeys(s.st.Pools) {
		p := s.st.Pools[k]
		pools.Rows = append(pools.Rows, []string{p.IdentityPoolId, p.IdentityPoolName, p.Account, strconv.FormatBool(p.AllowUnauthenticatedIdentities), strconv.Itoa(count[k])})
	}
	return []core.Table{pools, ids}
}

// Inspect returns a pool with its roles and role mappings.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.st.Pools[id]; p != nil && table == "Identity pools" {
		return *p
	}
	if x := s.st.Identities[id]; x != nil && table == "Identities" {
		return *x
	}
	return nil
}
