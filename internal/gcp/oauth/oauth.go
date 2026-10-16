// Package oauth emulates Google's OAuth2 token endpoint (/token and
// /oauth2/v4/token), its tokeninfo and certificate endpoints, and the Security
// Token Service (POST /v1/token). The jwt-bearer grant verifies assertions
// against the service account keys IAM keeps; ID tokens are signed by gapi's
// shared key, whose JWKS is /oauth2/v3/certs.
package oauth

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
)

// Service is the token endpoint.
type Service struct{ hub *core.Hub }

// New creates the OAuth2 token endpoint and STS.
func New(h *core.Hub) *Service { return &Service{h} }

func (s *Service) Info() core.Info {
	return core.Info{Name: "gcpoauth", Cloud: core.GCP, Title: "OAuth2 and STS", Match: match}
}

const (
	jwtBearer     = "urn:ietf:params:oauth:grant-type:jwt-bearer"
	tokenExchange = "urn:ietf:params:oauth:grant-type:token-exchange"
	accessType    = "urn:ietf:params:oauth:token-type:access_token"
	lifetime      = time.Hour
	// user is the identity behind refresh tokens and authorization codes: pogi has no user directory.
	user = "user@example.com"
)

func match(r *http.Request) bool {
	if strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-") || r.Header.Get("Amz-Sdk-Invocation-Id") != "" {
		return false // an AWS SDK call: the IAM Identity Center OIDC endpoint also has a POST /token
	}
	switch r.URL.Path {
	case "/token", "/oauth2/v4/token", "/oauth2/v3/token", "/o/oauth2/token", "/v1/token", "/revoke":
		return r.Method == http.MethodPost
	case "/oauth2/v3/tokeninfo", "/oauth2/v1/tokeninfo":
		return r.Method == http.MethodGet || r.Method == http.MethodPost
	case "/oauth2/v3/certs", "/oauth2/v1/certs":
		return r.Method == http.MethodGet
	}
	return false
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	c.Op = strings.TrimPrefix(r.URL.Path, "/")
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		gapi.JSONErr(w, c, e.Status, e.Code, e.Msg)
		return
	}
	switch r.URL.Path {
	case "/v1/token":
		s.sts(w, r, c)
	case "/revoke":
		gapi.JSON(w, map[string]any{})
	case "/oauth2/v3/tokeninfo", "/oauth2/v1/tokeninfo":
		s.tokeninfo(w, r, c)
	case "/oauth2/v3/certs", "/oauth2/v1/certs":
		s.certs(w, r)
	default:
		s.token(w, r, c)
	}
}

// ---- helpers ----

// params reads a form or JSON body, plus the query string.
func params(r *http.Request) url.Values {
	v := r.URL.Query()
	body := core.Body(r)
	if strings.Contains(r.Header.Get("Content-Type"), "json") {
		var m map[string]any
		json.Unmarshal(body, &m)
		for k, x := range m {
			v.Set(k, fmt.Sprint(x))
		}
		return v
	}
	form, _ := url.ParseQuery(string(body))
	for k, x := range form {
		v[k] = x
	}
	return v
}

// fail writes an OAuth2 error: {"error": code, "error_description": msg}.
func fail(w http.ResponseWriter, c *core.Ctx, status int, code, msg string) {
	c.ErrCode = code
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": code, "error_description": msg})
}

func send(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(v)
}

// ---- /token ----

func (s *Service) token(w http.ResponseWriter, r *http.Request, c *core.Ctx) {
	p := params(r)
	switch g := p.Get("grant_type"); g {
	case jwtBearer:
		s.jwtBearer(w, c, p.Get("assertion"))
	case "refresh_token":
		rt := p.Get("refresh_token")
		if rt == "" {
			fail(w, c, 400, "invalid_request", "Missing required parameter: refresh_token")
			return
		}
		send(w, map[string]any{"access_token": gapi.AccessToken(refreshEmail(rt), p.Get("scope"), lifetime),
			"expires_in": int(lifetime.Seconds()) - 1, "scope": p.Get("scope"), "token_type": "Bearer"})
	case "authorization_code":
		if p.Get("code") == "" {
			fail(w, c, 400, "invalid_request", "Missing required parameter: code")
			return
		}
		send(w, map[string]any{"access_token": gapi.AccessToken(user, "", lifetime), "expires_in": int(lifetime.Seconds()) - 1,
			"refresh_token": "1//pogi-" + base64.RawURLEncoding.EncodeToString([]byte(user)), "token_type": "Bearer",
			"id_token": gapi.OIDCToken(user, p.Get("client_id"))})
	case "":
		fail(w, c, 400, "invalid_request", "Required parameter is missing: grant_type")
	default:
		fail(w, c, 400, "unsupported_grant_type", "Invalid grant_type: "+g)
	}
}

// refreshEmail is the user behind a refresh token: ours name one, any other (a real gcloud login) is the default user.
func refreshEmail(rt string) string {
	if b, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(rt, "1//pogi-")); err == nil && strings.HasPrefix(rt, "1//pogi-") {
		return string(b)
	}
	return user
}

// jwtBearer exchanges a JWT signed with a service account key for an access
// token (claim scope) or an ID token (claim target_audience).
func (s *Service) jwtBearer(w http.ResponseWriter, c *core.Ctx, assertion string) {
	j, err := gapi.ParseJWT(assertion)
	if err != nil {
		fail(w, c, 400, "invalid_grant", "Invalid JWT: Token must be a short-lived token (60 minutes) and in a reasonable timeframe. Check your iat and exp values in the JWT claim.")
		return
	}
	email := j.Str("iss")
	dir, _ := s.hub.Service("gcpiam").(gapi.KeyDirectory)
	var keys map[string]*rsa.PublicKey
	if dir != nil {
		keys = dir.ServiceAccountKeys(email)
	}
	if keys == nil {
		fail(w, c, 400, "invalid_grant", "Invalid grant: account not found")
		return
	}
	ok := false
	if kid, named := j.Header["kid"].(string); named {
		pub := keys[kid]
		ok = pub != nil && j.Verify(pub)
	} else {
		for _, pub := range keys {
			ok = ok || j.Verify(pub)
		}
	}
	if !ok {
		fail(w, c, 400, "invalid_grant", "Invalid JWT Signature.")
		return
	}
	if j.Exp().Before(time.Now().Add(-time.Minute)) {
		fail(w, c, 400, "invalid_grant", "Invalid JWT: Token must be a short-lived token (60 minutes) and in a reasonable timeframe. Check your iat and exp values in the JWT claim.")
		return
	}
	if aud := j.Str("target_audience"); aud != "" {
		send(w, map[string]string{"id_token": gapi.OIDCToken(email, aud)})
		return
	}
	scope := j.Str("scope")
	if scope == "" {
		fail(w, c, 400, "invalid_scope", "Invalid OAuth scope or ID token audience provided.")
		return
	}
	send(w, map[string]any{"access_token": gapi.AccessToken(email, scope, lifetime), "expires_in": int(lifetime.Seconds()) - 1, "token_type": "Bearer"})
}

// ---- tokeninfo and certs ----

func (s *Service) tokeninfo(w http.ResponseWriter, r *http.Request, c *core.Ctx) {
	p := params(r)
	bad := func() { fail(w, c, 400, "invalid_token", "Invalid Value") }
	if idt := p.Get("id_token"); idt != "" {
		j, err := gapi.ParseJWT(idt)
		if err != nil || !j.Verify(gapi.PublicKey()) || j.Exp().Before(time.Now()) {
			bad()
			return
		}
		out := map[string]string{"alg": "RS256", "kid": gapi.KeyID, "typ": "JWT"}
		for k, v := range j.Claims {
			out[k] = fmt.Sprint(v)
			if f, isNum := v.(float64); isNum {
				out[k] = strconv.FormatInt(int64(f), 10)
			}
		}
		send(w, out)
		return
	}
	t, ok := gapi.ParseAccessToken(p.Get("access_token"))
	if !ok {
		bad()
		return
	}
	out := map[string]string{"azp": "pogi", "aud": "pogi", "scope": t.Scope, "exp": strconv.FormatInt(t.Expires.Unix(), 10),
		"expires_in": strconv.Itoa(int(time.Until(t.Expires).Seconds())), "access_type": "offline"}
	if t.Email != "" {
		out["email"], out["email_verified"] = t.Email, "true"
	}
	send(w, out)
}

// certs serves the shared ID-token key: as a JWKS (v3) or as X.509 (v1).
func (s *Service) certs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "public, max-age=3600")
	if strings.HasSuffix(r.URL.Path, "/v1/certs") {
		gapi.JSON(w, map[string]string{gapi.KeyID: string(gapi.PEM("CERTIFICATE", gapi.Cert(gapi.SigningKey(), gapi.KeyID)))})
		return
	}
	gapi.JSON(w, map[string]any{"keys": []any{gapi.JWK(gapi.KeyID, gapi.PublicKey())}})
}

// ---- STS ----

// sts is the token exchange behind workload identity federation: it takes
// any subject token and returns a federated access token.
// ponytail: the subject token is not verified against the pool's provider; verify it (OIDC JWKS) to reject bad ones.
func (s *Service) sts(w http.ResponseWriter, r *http.Request, c *core.Ctx) {
	p := params(r)
	switch {
	case p.Get("grant_type") != tokenExchange:
		fail(w, c, 400, "unsupported_grant_type", "Invalid grant_type: "+p.Get("grant_type"))
	case p.Get("subject_token") == "":
		fail(w, c, 400, "invalid_request", "Required parameter is missing: subject_token")
	case p.Get("subject_token_type") == "":
		fail(w, c, 400, "invalid_request", "Required parameter is missing: subject_token_type")
	case p.Get("audience") == "":
		fail(w, c, 400, "invalid_request", "Required parameter is missing: audience")
	default:
		send(w, map[string]any{"access_token": gapi.AccessToken("", p.Get("scope"), lifetime), "issued_token_type": accessType,
			"token_type": "Bearer", "expires_in": int(lifetime.Seconds()) - 1})
	}
}
