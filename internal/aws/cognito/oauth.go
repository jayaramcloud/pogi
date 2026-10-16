package cognito

import (
	"cmp"
	"crypto/rsa"
	"crypto/sha256"
	"html/template"
	"maps"
	"math/big"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Hosted endpoints share the main port with every other service: the OIDC
// documents live under the pool id, and /oauth2/* and the login page find
// their pool through client_id (or the token).

var wellKnown = regexp.MustCompile(`^/([a-z]{2}(?:-[a-z]+)+-\d+_[0-9A-Za-z]+)/\.well-known/(jwks\.json|openid-configuration)$`)

func hostedPath(r *http.Request) bool {
	switch r.URL.Path {
	case "/oauth2/token", "/oauth2/authorize", "/oauth2/userInfo", "/oauth2/userinfo":
		return true
	case "/login", "/logout":
		return r.URL.Query().Get("client_id") != ""
	}
	return wellKnown.MatchString(r.URL.Path)
}

// base is the scheme and host the client used; tokens name it in their issuer.
func base(r *http.Request) string {
	host := cmp.Or(r.Host, "localhost:7644")
	if r.TLS != nil {
		return "https://" + host
	}
	return "http://" + host
}

func issuer(c *core.Ctx, pool string) string {
	host := c.Host
	if !strings.Contains(host, "://") {
		host = "http://" + cmp.Or(host, "localhost:7644")
	}
	return host + "/" + pool
}

func (s *Service) hosted(w http.ResponseWriter, r *http.Request) {
	if m := wellKnown.FindStringSubmatch(r.URL.Path); m != nil {
		s.discovery(w, r, m[1], m[2])
		return
	}
	switch r.URL.Path {
	case "/oauth2/token":
		s.token(w, r)
	case "/oauth2/authorize":
		p, cl, err := s.oauthClient(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if idp := r.URL.Query().Get("identity_provider"); idp != "" && idp != "COGNITO" {
			s.mu.Lock()
			known := p.Providers[idp] != nil && slices.Contains(cl.SupportedIdentityProviders, idp)
			s.mu.Unlock()
			if !known {
				http.Error(w, "invalid identity_provider", http.StatusBadRequest)
				return
			}
			http.Error(w, "pogi stores identity provider "+idp+" but does not sign users in through it; use the pool's own sign-in.", http.StatusNotImplemented)
			return
		}
		if slices.ContainsFunc(strings.Fields(r.URL.Query().Get("scope")), func(x string) bool { return !slices.Contains(cl.AllowedOAuthScopes, x) }) {
			e := url.Values{"error": {"invalid_scope"}} // sent back to the callback, like AWS
			if st := r.URL.Query().Get("state"); st != "" {
				e.Set("state", st)
			}
			sep := "?"
			switch {
			case r.URL.Query().Get("response_type") == "token": // the implicit flow answers in the fragment
				sep = "#"
			case strings.Contains(r.URL.Query().Get("redirect_uri"), "?"):
				sep = "&"
			}
			http.Redirect(w, r, r.URL.Query().Get("redirect_uri")+sep+e.Encode(), http.StatusFound)
			return
		}
		http.Redirect(w, r, "/login?"+r.URL.RawQuery, http.StatusFound)
	case "/oauth2/userInfo", "/oauth2/userinfo":
		s.userInfo(w, r)
	case "/login":
		s.login(w, r)
	case "/logout":
		s.logout(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Service) discovery(w http.ResponseWriter, r *http.Request, id, doc string) {
	s.mu.Lock()
	var pub *rsa.PublicKey
	var kid string
	if p := s.st.Pools[id]; p != nil {
		pub, kid = &p.priv.PublicKey, p.Kid
	}
	s.mu.Unlock()
	if pub == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"message": "User pool " + id + " does not exist."})
		return
	}
	b := base(r)
	if doc == "jwks.json" {
		writeJSON(w, http.StatusOK, map[string]any{"keys": []map[string]string{{"alg": "RS256", "kty": "RSA", "use": "sig", "kid": kid,
			"n": b64.EncodeToString(pub.N.Bytes()), "e": b64.EncodeToString(big.NewInt(int64(pub.E)).Bytes())}}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"issuer": b + "/" + id, "jwks_uri": b + "/" + id + "/.well-known/jwks.json",
		"authorization_endpoint": b + "/oauth2/authorize", "token_endpoint": b + "/oauth2/token",
		"userinfo_endpoint": b + "/oauth2/userInfo", "end_session_endpoint": b + "/logout",
		"response_types_supported": []string{"code", "token"}, "subject_types_supported": []string{"public"},
		"id_token_signing_alg_values_supported": []string{"RS256"}, "scopes_supported": []string{"openid", "email", "phone", "profile"},
		"token_endpoint_auth_methods_supported": []string{"client_secret_basic", "client_secret_post"},
	})
}

// oauthErr is an OAuth2 error code ("invalid_grant").
type oauthErr string

func (e oauthErr) Error() string { return string(e) }

// oauthClient checks client_id, response_type and redirect_uri of an authorize or login request.
func (s *Service) oauthClient(q url.Values) (*Pool, *Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, cl, err := s.client(q.Get("client_id"))
	if err != nil {
		return nil, nil, oauthErr("invalid_client")
	}
	flow := map[string]string{"code": "code", "token": "implicit"}[q.Get("response_type")]
	switch {
	case !cl.AllowedOAuthFlowsUserPoolClient || flow == "" || !slices.Contains(cl.AllowedOAuthFlows, flow):
		return nil, nil, oauthErr("unauthorized_client")
	case !slices.Contains(cl.CallbackURLs, q.Get("redirect_uri")):
		return nil, nil, oauthErr("redirect_mismatch")
	}
	return p, cl, nil
}

// oauthScope is the requested scopes the client may have; all of them when none are requested.
func oauthScope(cl *Client, requested string) string {
	want := strings.Fields(requested)
	if len(want) == 0 {
		want = cl.AllowedOAuthScopes
	}
	var out []string
	for _, sc := range want {
		if slices.Contains(cl.AllowedOAuthScopes, sc) {
			out = append(out, sc)
		}
	}
	return strings.Join(out, " ")
}

type authCode struct {
	ch                          *challenge
	redirect, challenge, method string
	exp                         time.Time
}

var loginPage = template.Must(template.New("login").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Sign in</title>
<style>body{font:16px system-ui,sans-serif;max-width:22rem;margin:4rem auto;padding:0 1rem}label{display:block;margin-bottom:1rem}
input,button{display:block;width:100%;box-sizing:border-box;margin-top:.3rem;padding:.6rem;font:inherit}.err{color:#b00020}</style></head>
<body><h1>Sign in</h1><p>User pool {{.Pool}} (pogi)</p>{{with .Error}}<p class="err" role="alert">{{.}}</p>{{end}}
{{if .Local}}<form method="post"><label>Username <input name="username" autocomplete="username" value="{{.Username}}" required autofocus></label>
<label>Password <input name="password" type="password" autocomplete="current-password" required></label>
<button>Sign in</button></form>{{end}}
{{range .Providers}}<p><a href="{{.Href}}">Continue with {{.Name}}</a></p>{{end}}</body></html>`))

type loginProvider struct{ Name, Href string }

// loginProviders are the identity providers the client offers besides the pool's own sign-in,
// and whether the username and password form is shown.
func loginProviders(p *Pool, cl *Client, q url.Values) (list []loginProvider, local bool) {
	local = len(cl.SupportedIdentityProviders) == 0 || slices.Contains(cl.SupportedIdentityProviders, "COGNITO")
	for _, n := range cl.SupportedIdentityProviders {
		if p.Providers[n] != nil {
			v := maps.Clone(q)
			v.Set("identity_provider", n)
			list = append(list, loginProvider{n, "/oauth2/authorize?" + v.Encode()})
		}
	}
	return list, local
}

// login is a local stand-in for the hosted UI: a username/password form that
// redirects to the callback with a code (or tokens for the implicit flow).
func (s *Service) login(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	p, cl, err := s.oauthClient(q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	data := struct {
		Pool, Username, Error string
		Local                 bool
		Providers             []loginProvider
	}{Pool: p.ID}
	s.mu.Lock()
	data.Providers, data.Local = loginProviders(p, cl, q)
	s.mu.Unlock()
	if r.Method == http.MethodPost {
		data.Username = r.PostFormValue("username")
		to, err := s.signIn(&core.Ctx{Host: base(r)}, cl, data.Username, r.PostFormValue("password"), q)
		if err == nil {
			http.Redirect(w, r, to, http.StatusFound)
			return
		}
		data.Error = core.AsError(err).Msg
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	loginPage.Execute(w, data)
}

// signIn checks the login form and returns the callback URL to redirect to.
func (s *Service) signIn(c *core.Ctx, cl *Client, name, pw string, q url.Values) (string, error) {
	ch, pre, err := s.start(&authInput{ClientId: cl.ClientId, AuthParameters: map[string]string{"USERNAME": name}}, "", false)
	if err == nil {
		err = s.run(pre, nil)
	}
	if err == nil {
		err = s.password(ch, pw)
	}
	if err == nil {
		s.mu.Lock()
		_, u, _, e := s.grantee(ch.grant)
		switch {
		case e != nil:
			err = e
		case u.Status == "FORCE_CHANGE_PASSWORD":
			err = core.Errorf(400, "NotAuthorizedException", "Set a permanent password first: sign in through the API or use AdminSetUserPassword.")
		}
		s.mu.Unlock()
	}
	if err != nil {
		return "", err
	}
	ch.scope, ch.nonce, ch.source = oauthScope(cl, q.Get("scope")), q.Get("nonce"), "TokenGeneration_HostedAuth"
	v := url.Values{}
	if st := q.Get("state"); st != "" {
		v.Set("state", st)
	}
	redirect := q.Get("redirect_uri")
	if q.Get("response_type") == "token" {
		res, err := s.finish(c, ch)
		if err != nil {
			return "", err
		}
		t := res.AuthenticationResult
		v.Set("access_token", t.AccessToken)
		v.Set("token_type", "Bearer")
		v.Set("expires_in", strconv.Itoa(t.ExpiresIn))
		if t.IdToken != "" {
			v.Set("id_token", t.IdToken)
		}
		return redirect + "#" + v.Encode(), nil
	}
	code := core.UUID()
	now := time.Now()
	s.mu.Lock()
	for k, x := range s.grants {
		if now.After(x.exp) {
			delete(s.grants, k)
		}
	}
	s.grants[code] = &authCode{ch: ch, redirect: redirect, challenge: q.Get("code_challenge"), method: q.Get("code_challenge_method"), exp: now.Add(5 * time.Minute)}
	s.mu.Unlock()
	v.Set("code", code)
	sep := "?"
	if strings.Contains(redirect, "?") {
		sep = "&"
	}
	return redirect + sep + v.Encode(), nil
}

func (s *Service) logout(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	s.mu.Lock()
	_, cl, err := s.client(q.Get("client_id"))
	s.mu.Unlock()
	if err != nil || !slices.Contains(cl.LogoutURLs, q.Get("logout_uri")) {
		http.Error(w, "invalid logout_uri", http.StatusBadRequest)
		return
	}
	http.Redirect(w, r, q.Get("logout_uri"), http.StatusFound)
}

// token is POST /oauth2/token: client_credentials, authorization_code (with PKCE) and refresh_token.
func (s *Service) token(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "invalid_request"})
		return
	}
	r.ParseForm()
	f := r.PostForm
	id, secret, basic := r.BasicAuth()
	if !basic {
		id, secret = f.Get("client_id"), f.Get("client_secret")
	}
	s.mu.Lock()
	p, cl, err := s.client(id)
	s.mu.Unlock()
	if err != nil || cl.ClientSecret != secret {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid_client"})
		return
	}
	c := &core.Ctx{Host: base(r)}
	var t *tokens
	switch f.Get("grant_type") {
	case "client_credentials":
		t, err = s.clientCredentials(c, p.ID, cl, f.Get("scope"))
	case "authorization_code":
		t, err = s.exchange(c, cl, f)
	case "refresh_token":
		s.mu.Lock()
		x, jti, e := p.session(cl, f.Get("refresh_token"))
		s.mu.Unlock()
		if err = e; err == nil {
			t, err = s.issue(c, grant{pool: p.ID, user: x.Username, client: cl.ClientId, source: "TokenGeneration_RefreshTokens", scope: x.Scope, jti: jti})
		}
	default:
		err = oauthErr("unsupported_grant_type")
	}
	if err != nil {
		code, ok := err.(oauthErr)
		if !ok {
			code = "invalid_grant"
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": string(code)})
		return
	}
	out := map[string]any{"access_token": t.AccessToken, "expires_in": t.ExpiresIn, "token_type": "Bearer"}
	if t.IdToken != "" {
		out["id_token"] = t.IdToken
	}
	if t.RefreshToken != "" {
		out["refresh_token"] = t.RefreshToken
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, out)
}

// clientCredentials signs a machine access token with resource-server scopes.
func (s *Service) clientCredentials(c *core.Ctx, pool string, cl *Client, scope string) (*tokens, error) {
	if !slices.Contains(cl.AllowedOAuthFlows, "client_credentials") || cl.ClientSecret == "" {
		return nil, oauthErr("unauthorized_client")
	}
	scopes := strings.Fields(scope)
	if len(scopes) == 0 {
		scopes = slices.DeleteFunc(slices.Clone(cl.AllowedOAuthScopes), func(x string) bool { return slices.Contains(stdScopes, x) })
	}
	if len(scopes) == 0 || slices.ContainsFunc(scopes, func(x string) bool { return !slices.Contains(cl.AllowedOAuthScopes, x) }) {
		return nil, oauthErr("invalid_scope")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(pool)
	if err != nil {
		return nil, err
	}
	now, ttl := time.Now(), cl.ttl("access")
	tok := p.sign(map[string]any{"sub": cl.ClientId, "client_id": cl.ClientId, "token_use": "access", "scope": strings.Join(scopes, " "),
		"auth_time": now.Unix(), "iss": issuer(c, p.ID), "iat": now.Unix(), "exp": now.Add(ttl).Unix(), "version": 2, "jti": core.UUID()})
	return &tokens{AccessToken: tok, ExpiresIn: int(ttl / time.Second), TokenType: "Bearer"}, nil
}

// exchange redeems an authorization code, checking redirect_uri and the PKCE verifier.
func (s *Service) exchange(c *core.Ctx, cl *Client, f url.Values) (*tokens, error) {
	s.mu.Lock()
	ac := s.grants[f.Get("code")]
	delete(s.grants, f.Get("code"))
	s.mu.Unlock()
	if ac == nil || time.Now().After(ac.exp) || ac.ch.client != cl.ClientId || ac.redirect != f.Get("redirect_uri") {
		return nil, oauthErr("invalid_grant")
	}
	if ac.challenge != "" {
		v := f.Get("code_verifier")
		if ac.method == "S256" {
			sum := sha256.Sum256([]byte(v))
			v = b64.EncodeToString(sum[:])
		}
		if v != ac.challenge {
			return nil, oauthErr("invalid_grant")
		}
	}
	res, err := s.finish(c, ac.ch)
	if err != nil {
		return nil, err
	}
	return res.AuthenticationResult, nil
}

// userInfo is GET /oauth2/userInfo with a Bearer access token.
func (s *Service) userInfo(w http.ResponseWriter, r *http.Request) {
	token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	out := map[string]string{}
	s.mu.Lock()
	_, u, _, err := s.authed(token, "openid", adminScope)
	if err == nil {
		maps.Copy(out, u.Attrs)
		out["username"] = u.Username
	}
	s.mu.Unlock()
	if err != nil {
		w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid_token", "error_description": core.AsError(err).Msg})
		return
	}
	writeJSON(w, http.StatusOK, out)
}
