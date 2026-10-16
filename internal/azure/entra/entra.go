// Package entra emulates Microsoft Entra ID's sign-in side (login.microsoftonline.com
// on pogi's main port) and Azure Managed Identity: the OAuth2 token endpoints
// (client credentials with a secret or a client assertion, password, refresh
// token, device code), OIDC discovery and the JWKS, and the token endpoints
// that VMs and App Service hand to managed identities (IMDS and
// IDENTITY_ENDPOINT). Tokens are RS256 JWTs signed with a key that is created
// on first use and persisted. The directory of app registrations, service
// principals and users lives in the graph service; this one reads it through
// the two methods of the directory interface, so it works without Graph too.
//
// Any tenant name in a path is accepted (a GUID, a domain, common,
// organizations, consumers); every token belongs to arm.TenantID, unless the
// path names another GUID. azidentity needs an https authority host and, for
// an authority that isn't Microsoft's, DisableInstanceDiscovery.
package entra

import (
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

// directory is what the Graph service offers for authenticating clients and users.
type directory interface {
	// App finds an app registration or service principal by client ID: its
	// object ID and the client secrets it accepts.
	App(appID string) (oid string, secrets []string, ok bool)
	// User finds a user by principal name or object ID; password is "" when none is set.
	User(name string) (oid, upn, password string, ok bool)
}

// Service is Entra ID and Managed Identity.
type Service struct {
	h  *core.Hub
	mu sync.Mutex
	st state
	rk *rsa.PrivateKey
}

type state struct {
	Key        []byte                    `json:"key,omitempty"` // PKCS#8 DER of the signing key
	Identities map[string]map[string]any `json:"identities"`    // lower-cased resource ID -> user-assigned identity
}

// New creates the service and registers the user-assigned identity type with the Resource Manager.
func New(h *core.Hub) *Service {
	s := &Service{h: h}
	s.Reset()
	arm.Register(h, s.identityProvider())
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "entra", Cloud: core.Azure, Title: "Entra ID", Protocol: "azure-json", Match: match}
}

var guid = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// isTenant reports whether a first path segment names a tenant.
func isTenant(s string) bool {
	switch s {
	case "common", "organizations", "consumers":
		return true
	}
	return guid.MatchString(s) || strings.Contains(s, ".")
}

// match claims /<tenant>/oauth2/..., /<tenant>/v2.0/.well-known/..., the
// discovery and user-realm paths, and the managed identity endpoints.
func match(r *http.Request) bool {
	p := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	switch {
	case r.URL.Path == "/metadata/identity/oauth2/token" || r.URL.Path == "/msi/token":
		return true
	case len(p) < 2 || !isTenant(p[0]):
		return false
	}
	switch strings.Join(p[1:], "/") {
	case "oauth2/token", "oauth2/devicecode", "oauth2/v2.0/token", "oauth2/v2.0/devicecode",
		".well-known/openid-configuration", "v2.0/.well-known/openid-configuration",
		"discovery/keys", "discovery/v2.0/keys", "discovery/instance":
		return true
	}
	return len(p) == 3 && strings.EqualFold(p[1], "userrealm")
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	if r.URL.Path == "/metadata/identity/oauth2/token" || r.URL.Path == "/msi/token" {
		s.managed(w, r, c)
		return
	}
	p := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	tenant, what := p[0], strings.Join(p[1:], "/")
	base := "http://" + r.Host
	if r.TLS != nil {
		base = "https://" + r.Host
	}
	tid := arm.TenantID
	if guid.MatchString(tenant) {
		tid = strings.ToLower(tenant)
	}
	c.Op = what
	if err := c.Intercept(); err != nil {
		oauthError(w, c, err.(*core.Error).Status, "temporarily_unavailable", 0, err.Error())
		return
	}
	switch what {
	case "oauth2/token":
		s.token(w, r, c, tid, true)
	case "oauth2/v2.0/token":
		s.token(w, r, c, tid, false)
	case "oauth2/devicecode", "oauth2/v2.0/devicecode":
		s.deviceCode(w, r, c, tid, base, what == "oauth2/devicecode")
	case ".well-known/openid-configuration", "v2.0/.well-known/openid-configuration":
		writeJSON(w, http.StatusOK, openID(base, tid, what != ".well-known/openid-configuration"))
	case "discovery/keys", "discovery/v2.0/keys":
		writeJSON(w, http.StatusOK, s.jwks())
	case "discovery/instance":
		host := r.Host
		writeJSON(w, http.StatusOK, map[string]any{"tenant_discovery_endpoint": base + "/" + tid + "/v2.0/.well-known/openid-configuration",
			"api-version": "1.1", "metadata": []any{map[string]any{"preferred_network": host, "preferred_cache": host, "aliases": []string{host}}}})
	default: // userrealm/<name>
		writeJSON(w, http.StatusOK, map[string]any{"ver": "1.0", "account_type": "Managed", "domain_name": "pogi.onmicrosoft.com",
			"cloud_instance_name": "microsoftonline.com", "cloud_audience_urn": "urn:federation:MicrosoftOnline"})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// oauthError writes Entra's error body: {"error", "error_description", "error_codes", ...}.
func oauthError(w http.ResponseWriter, c *core.Ctx, status int, code string, aadsts int, desc string) {
	c.ErrCode = code
	now := time.Now().UTC()
	if aadsts != 0 {
		desc = "AADSTS" + strconv.Itoa(aadsts) + ": " + desc
	}
	body := map[string]any{"error": code, "error_description": desc + "\r\nTrace ID: " + core.UUID() + "\r\nCorrelation ID: " + c.RequestID + "\r\nTimestamp: " + now.Format("2006-01-02 15:04:05Z"),
		"error_codes": []int{aadsts}, "timestamp": now.Format("2006-01-02 15:04:05Z"), "trace_id": core.UUID(), "correlation_id": c.RequestID}
	if aadsts != 0 {
		body["error_uri"] = "https://login.microsoftonline.com/error?code=" + strconv.Itoa(aadsts)
	}
	writeJSON(w, status, body)
}

// issuer is the iss claim of a token and the issuer of the discovery document.
func issuer(tid string, v2 bool) string {
	if v2 {
		return "https://login.microsoftonline.com/" + tid + "/v2.0"
	}
	return "https://sts.windows.net/" + tid + "/"
}

func openID(base, tid string, v2 bool) map[string]any {
	t := base + "/" + tid
	doc := map[string]any{
		"token_endpoint":                        t + "/oauth2/token",
		"token_endpoint_auth_methods_supported": []string{"client_secret_post", "private_key_jwt", "client_secret_basic"},
		"jwks_uri":                              t + "/discovery/keys",
		"response_modes_supported":              []string{"query", "fragment", "form_post"},
		"subject_types_supported":               []string{"pairwise"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"response_types_supported":              []string{"code", "id_token", "code id_token", "token id_token", "token"},
		"scopes_supported":                      []string{"openid"},
		"issuer":                                issuer(tid, v2),
		"authorization_endpoint":                t + "/oauth2/authorize",
		"device_authorization_endpoint":         t + "/oauth2/devicecode",
		"claims_supported":                      []string{"sub", "iss", "aud", "exp", "iat", "tid", "oid", "upn", "name", "roles", "scp", "appid"},
		"tenant_region_scope":                   "NA",
		"cloud_instance_name":                   "microsoftonline.com",
		"msgraph_host":                          strings.TrimPrefix(strings.TrimPrefix(base, "https://"), "http://"),
	}
	if v2 {
		doc["token_endpoint"] = t + "/oauth2/v2.0/token"
		doc["jwks_uri"] = t + "/discovery/v2.0/keys"
		doc["scopes_supported"] = []string{"openid", "profile", "email", "offline_access"}
		doc["authorization_endpoint"] = t + "/oauth2/v2.0/authorize"
		doc["device_authorization_endpoint"] = t + "/oauth2/v2.0/devicecode"
	}
	return doc
}

// ---- signing key and JWTs ----

// signer returns the signing key, created on first use.
func (s *Service) signer() *rsa.PrivateKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rk == nil {
		if k, err := x509.ParsePKCS8PrivateKey(s.st.Key); err == nil {
			s.rk, _ = k.(*rsa.PrivateKey)
		}
	}
	if s.rk == nil {
		s.rk = newKey()
		s.st.Key, _ = x509.MarshalPKCS8PrivateKey(s.rk)
	}
	return s.rk
}

func (s *Service) kid() string {
	sum := sha256.Sum256(s.signer().N.Bytes())
	return hex.EncodeToString(sum[:8])
}

func (s *Service) jwks() map[string]any {
	k := s.signer()
	return map[string]any{"keys": []any{map[string]any{"kty": "RSA", "use": "sig", "kid": s.kid(), "x5t": s.kid(),
		"n": b64(k.N.Bytes()), "e": b64(big.NewInt(int64(k.E)).Bytes())}}}
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Identities: map[string]map[string]any{}}
	s.mu.Unlock() // the signing key stays: it is not data
}

func (s *Service) Save() ([]byte, error) {
	s.signer() // so that a restart keeps tokens valid
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Identities == nil {
		st.Identities = map[string]map[string]any{}
	}
	s.mu.Lock()
	s.st, s.rk = st, nil
	s.mu.Unlock()
	return nil
}

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Managed identities", Columns: []string{"Name", "Client ID", "Principal ID", "Resource group"}}
	for _, k := range core.SortedKeys(s.st.Identities) {
		d := s.st.Identities[k]
		id, _ := arm.ParseID(str(d["id"]))
		p, _ := d["properties"].(map[string]any)
		t.Rows = append(t.Rows, []string{str(d["name"]), str(p["clientId"]), str(p["principalId"]), id.ResourceGroup})
	}
	return []core.Table{t}
}

func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range s.st.Identities {
		if d["name"] == id {
			return d
		}
	}
	return nil
}

func str(v any) string { s, _ := v.(string); return s }

func (s *Service) dir() directory {
	d, _ := s.h.Service("graph").(directory)
	return d
}
