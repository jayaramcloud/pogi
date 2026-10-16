package entra

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

const (
	graphResource = "https://graph.microsoft.com"
	graphAppID    = "00000003-0000-0000-c000-000000000000"
	clientAssert  = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
	deviceGrant   = "urn:ietf:params:oauth:grant-type:device_code"
	defaultUser   = "admin@pogi.onmicrosoft.com" // who a device code signs in
)

func newKey() *rsa.PrivateKey {
	k, _ := rsa.GenerateKey(rand.Reader, 2048)
	return k
}

// uuidOf derives a stable UUID from a name, for clients and users the
// directory doesn't know.
func uuidOf(name string) string {
	h := sha1.Sum([]byte("pogi:" + name))
	h[6], h[8] = h[6]&0x0f|0x50, h[8]&0x3f|0x80
	x := hex.EncodeToString(h[:16])
	return x[:8] + "-" + x[8:12] + "-" + x[12:16] + "-" + x[16:20] + "-" + x[20:]
}

// sign makes an RS256 JWT.
func (s *Service) sign(claims map[string]any) string {
	enc := func(v any) string { b, _ := json.Marshal(v); return b64(b) }
	in := enc(map[string]any{"typ": "JWT", "alg": "RS256", "kid": s.kid()}) + "." + enc(claims)
	sum := sha256.Sum256([]byte(in))
	sig, _ := rsa.SignPKCS1v15(rand.Reader, s.signer(), crypto.SHA256, sum[:])
	return in + "." + b64(sig)
}

// verify checks a JWT this service signed and returns its claims.
func (s *Service) verify(tok string) (map[string]any, bool) {
	p := strings.Split(tok, ".")
	if len(p) != 3 {
		return nil, false
	}
	sig, err := base64.RawURLEncoding.DecodeString(p[2])
	sum := sha256.Sum256([]byte(p[0] + "." + p[1]))
	if err != nil || rsa.VerifyPKCS1v15(&s.signer().PublicKey, crypto.SHA256, sum[:], sig) != nil {
		return nil, false
	}
	claims := unverified(tok)
	exp, _ := claims["exp"].(float64)
	return claims, claims != nil && int64(exp) > time.Now().Unix()
}

// unverified returns the claims of any JWT without checking its signature.
func unverified(tok string) map[string]any {
	p := strings.Split(tok, ".")
	if len(p) != 3 {
		return nil
	}
	b, err := base64.RawURLEncoding.DecodeString(p[1])
	if err != nil {
		return nil
	}
	var claims map[string]any
	json.Unmarshal(b, &claims)
	return claims
}

// subject is who a token is for.
type subject struct {
	tid, clientID, oid string
	user               bool
	upn, name          string
	resource           string
	scopes             []string // delegated permissions of a user token
	v1                 bool
	mi                 string // resource ID of a user-assigned managed identity
}

const lifetime = time.Hour

// access makes the access token for sub.
func (s *Service) access(sub subject) string {
	now := time.Now()
	// The shape follows the resource, not the endpoint, as in Entra: Resource Manager, Graph
	// and the other Microsoft APIs get 1.0 tokens (appid, an sts.windows.net issuer) even from
	// the v2.0 endpoint; only an app's own API (api://... or its client ID) gets a 2.0 token.
	sub.v1 = sub.v1 || !strings.HasPrefix(sub.resource, "api://") && !guid.MatchString(sub.resource)
	c := map[string]any{"aud": sub.resource, "iss": issuer(sub.tid, !sub.v1), "iat": now.Unix(), "nbf": now.Unix(), "exp": now.Add(lifetime).Unix(),
		"oid": sub.oid, "sub": sub.oid, "tid": sub.tid, "uti": core.RandString(22, core.AlphaNum), "idp": issuer(sub.tid, false)}
	if sub.v1 {
		c["ver"], c["appid"] = "1.0", sub.clientID
	} else {
		c["ver"], c["azp"] = "2.0", sub.clientID
	}
	if sub.user {
		c["upn"], c["name"], c["unique_name"], c["scp"] = sub.upn, sub.name, sub.upn, strings.Join(sub.scopes, " ")
		if sub.v1 {
			c["appidacr"] = "0"
		} else {
			c["azpacr"] = "0"
		}
		c["sub"] = uuidOf("sub:" + sub.oid + sub.clientID)
	} else {
		c["roles"], c["idtyp"] = []string{}, "app"
		if sub.v1 {
			c["appidacr"] = "1"
		} else {
			c["azpacr"] = "1"
		}
	}
	if sub.mi != "" {
		c["xms_mirid"], c["appidacr"] = sub.mi, "2"
	}
	return s.sign(c)
}

func (s *Service) idToken(sub subject) string {
	now := time.Now()
	return s.sign(map[string]any{"aud": sub.clientID, "iss": issuer(sub.tid, true), "iat": now.Unix(), "nbf": now.Unix(), "exp": now.Add(lifetime).Unix(),
		"oid": sub.oid, "sub": uuidOf("sub:" + sub.oid + sub.clientID), "tid": sub.tid, "name": sub.name, "preferred_username": sub.upn, "ver": "2.0"})
}

// splitScope reads a v2 scope: the resource it names and the permissions
// asked of it. OpenID scopes name nothing.
func splitScope(scope string) (resource string, perms []string) {
	for _, sc := range strings.Fields(scope) {
		switch sc {
		case "openid", "profile", "email", "offline_access":
			continue
		}
		res, perm := graphResource, sc
		if i := strings.LastIndexByte(sc, '/'); i >= 0 {
			res, perm = sc[:i], sc[i+1:]
		}
		if resource == "" {
			resource = res
		}
		if perm != "" && perm != ".default" {
			perms = append(perms, perm)
		}
	}
	if resource == "" {
		resource = graphResource
	}
	return resource, perms
}

// token is the token endpoint.
func (s *Service) token(w http.ResponseWriter, r *http.Request, c *core.Ctx, tid string, v1 bool) {
	form, _ := url.ParseQuery(string(core.Body(r)))
	for k, v := range r.URL.Query() { // some clients put parameters in the query
		if form.Get(k) == "" {
			form[k] = v
		}
	}
	clientID, secret := form.Get("client_id"), form.Get("client_secret")
	if id, pw, ok := r.BasicAuth(); ok { // client_secret_basic
		clientID, secret = id, pw
	}
	assertion := form.Get("client_assertion")
	if clientID == "" && assertion != "" {
		claims := unverified(assertion)
		clientID, _ = claims["sub"].(string)
	}
	if clientID == "" {
		oauthError(w, c, http.StatusBadRequest, "invalid_request", 900144, "The request body must contain the following parameter: 'client_id'.")
		return
	}
	grant := form.Get("grant_type")
	if grant == "device_code" { // MSAL sends the short name, the RFC has the URN; Entra takes both
		grant = deviceGrant
	}
	c.Op = grant
	sub := subject{tid: tid, clientID: clientID, v1: v1}
	scope := form.Get("scope")
	if v1 {
		scope = form.Get("resource")
	}
	switch grant {
	case "client_credentials":
		if !v1 { // MSAL adds the OpenID scopes to this one
			scope = strings.Join(slices.DeleteFunc(strings.Fields(scope), func(s string) bool { return !strings.HasSuffix(s, "/.default") }), " ")
		}
		if !v1 && !strings.HasSuffix(scope, "/.default") {
			oauthError(w, c, http.StatusBadRequest, "invalid_scope", 70011, "The provided value for the input parameter 'scope' is not valid. The scope "+scope+" is not valid. The scope format is invalid. Scope must be in a valid resource format and end with /.default.")
			return
		}
		oid, ok := s.authenticate(w, c, clientID, secret, assertion != "")
		if !ok {
			return
		}
		sub.oid = oid
		sub.resource = strings.TrimSuffix(scope, "/.default")
		if v1 {
			sub.resource = scope
		}
	case "password", deviceGrant, "refresh_token":
		if !s.userGrant(w, c, form, &sub, grant) {
			return
		}
	default:
		oauthError(w, c, http.StatusBadRequest, "unsupported_grant_type", 70003, "The app is configured with an unsupported grant type '"+grant+"'.")
		return
	}
	resp := map[string]any{"token_type": "Bearer", "expires_in": int(lifetime.Seconds()), "ext_expires_in": int(lifetime.Seconds()),
		"access_token": s.access(sub)}
	if v1 {
		now := time.Now()
		resp["expires_in"], resp["ext_expires_in"] = strconv.Itoa(int(lifetime.Seconds())), strconv.Itoa(int(lifetime.Seconds()))
		resp["expires_on"], resp["not_before"] = strconv.FormatInt(now.Add(lifetime).Unix(), 10), strconv.FormatInt(now.Unix(), 10)
		resp["resource"] = sub.resource
	} else if sub.user {
		resp["scope"] = form.Get("scope")
	}
	if sub.user {
		ci, _ := json.Marshal(map[string]string{"uid": sub.oid, "utid": tid})
		resp["client_info"], resp["id_token"] = b64(ci), s.idToken(sub)
		resp["refresh_token"] = s.sign(map[string]any{"typ": "refresh", "cid": clientID, "oid": sub.oid, "upn": sub.upn, "name": sub.name,
			"scope": scope, "tid": tid, "exp": time.Now().Add(90 * 24 * time.Hour).Unix(), "jti": core.UUID()})
	}
	writeJSON(w, http.StatusOK, resp)
}

// authenticate checks a client credentials request against the directory.
// Any client and any secret pass, unless the app registration holds secrets
// (the secret must be one of them) or Config.ValidateSigs is on (the client
// must be registered, and present a secret or a client assertion).
// ponytail: client assertions are accepted unverified, the issuer of a federated credential is not checked.
func (s *Service) authenticate(w http.ResponseWriter, c *core.Ctx, clientID, secret string, assertion bool) (oid string, ok bool) {
	strict := s.h.Cfg.ValidateSigs
	oid, secrets, found := "", []string(nil), false
	if d := s.dir(); d != nil {
		oid, secrets, found = d.App(clientID)
	}
	switch {
	case !found && strict:
		oauthError(w, c, http.StatusUnauthorized, "invalid_client", 700016, "Application with identifier '"+clientID+"' was not found in the directory 'pogi'. This can happen if the application has not been installed by the administrator of the tenant or consented to by any user in the tenant.")
		return "", false
	case !found:
		return uuidOf(clientID), true
	case assertion:
		return oid, true
	case secret == "" && (len(secrets) > 0 || strict):
		oauthError(w, c, http.StatusUnauthorized, "invalid_client", 7000218, "The request body must contain the following parameter: 'client_assertion' or 'client_secret'.")
		return "", false
	case len(secrets) > 0 && !slices.Contains(secrets, secret):
		oauthError(w, c, http.StatusUnauthorized, "invalid_client", 7000215, "Invalid client secret provided. Ensure the secret being sent in the request is the client secret value, not the client secret ID, for a secret added to app '"+clientID+"'.")
		return "", false
	}
	return oid, true
}

// userGrant resolves the password, device code and refresh token grants to a user.
func (s *Service) userGrant(w http.ResponseWriter, c *core.Ctx, form url.Values, sub *subject, grant string) bool {
	sub.user = true
	scope := form.Get("scope")
	if sub.v1 {
		scope = form.Get("resource")
	}
	switch grant {
	case "password":
		name, pw := form.Get("username"), form.Get("password")
		oid, upn, want, found := "", name, "", false
		if d := s.dir(); d != nil {
			oid, upn, want, found = d.User(name)
		}
		switch {
		case !found && s.h.Cfg.ValidateSigs:
			oauthError(w, c, http.StatusBadRequest, "invalid_grant", 50034, "The user account "+name+" does not exist in the 'pogi' directory. To sign into this application, the account must be added to the directory.")
			return false
		case !found:
			oid, upn = uuidOf(name), name
		case want != "" && want != pw:
			oauthError(w, c, http.StatusBadRequest, "invalid_grant", 50126, "Error validating credentials due to invalid username or password.")
			return false
		}
		sub.oid, sub.upn, sub.name = oid, upn, upn
	case deviceGrant:
		claims, ok := s.verify(form.Get("device_code"))
		if !ok || claims["typ"] != "device" {
			oauthError(w, c, http.StatusBadRequest, "expired_token", 70019, "Verification code expired. Refresh the code and try again.")
			return false
		}
		scope, _ = claims["scope"].(string)
		sub.oid, sub.upn, sub.name = s.defaultUser()
	case "refresh_token":
		claims, ok := s.verify(form.Get("refresh_token"))
		if !ok || claims["typ"] != "refresh" {
			oauthError(w, c, http.StatusBadRequest, "invalid_grant", 70000, "The provided value for the input parameter 'refresh_token' is not valid. The refresh token is invalid or expired.")
			return false
		}
		if sc := form.Get("scope"); sc != "" {
			scope = sc
		} else {
			scope, _ = claims["scope"].(string)
		}
		sub.oid, sub.upn, sub.name = str(claims["oid"]), str(claims["upn"]), str(claims["name"])
	}
	if sub.v1 {
		sub.resource = scope
		return true
	}
	sub.resource, sub.scopes = splitScope(scope)
	return true
}

func (s *Service) defaultUser() (oid, upn, name string) {
	if d := s.dir(); d != nil {
		if o, u, _, ok := d.User(defaultUser); ok {
			return o, u, u
		}
	}
	return uuidOf(defaultUser), defaultUser, "pogi admin"
}

// deviceCode starts a device flow. It is approved at once: the code signs in
// the directory's admin user, and the polling grant succeeds on its first try.
func (s *Service) deviceCode(w http.ResponseWriter, r *http.Request, c *core.Ctx, tid, base string, v1 bool) {
	form, _ := url.ParseQuery(string(core.Body(r)))
	if form.Get("client_id") == "" {
		oauthError(w, c, http.StatusBadRequest, "invalid_request", 900144, "The request body must contain the following parameter: 'client_id'.")
		return
	}
	scope := form.Get("scope")
	if v1 {
		scope = form.Get("resource")
	}
	code := core.RandString(8, core.Upper)
	dev := s.sign(map[string]any{"typ": "device", "cid": form.Get("client_id"), "scope": scope, "tid": tid, "exp": time.Now().Add(15 * time.Minute).Unix(), "jti": core.UUID()})
	uri := base + "/" + tid + "/deviceauth"
	writeJSON(w, http.StatusOK, map[string]any{"user_code": code, "device_code": dev, "verification_uri": uri, "expires_in": 900, "interval": 1,
		"message": "To sign in, use a web browser to open the page " + uri + " and enter the code " + code + " to authenticate."})
}
