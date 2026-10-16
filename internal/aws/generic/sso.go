package generic

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// IAM Identity Center sign-in: the OIDC endpoint behind `aws sso login` (device
// and authorization-code grants, refresh) and the portal that turns the token
// into role credentials. Clients, device codes and tokens are resources of the
// sso-oidc service, so they persist and show in the console.
//
// Assignments, permission sets and users come from the sso-admin and
// identitystore services of the same hub (peer).

const (
	grantDevice   = "urn:ietf:params:oauth:grant-type:device_code"
	ssoTokenTTL   = 8 * time.Hour
	ssoDeviceTTL  = 10 * time.Minute
	ssoDevicePath = "/_pogi/sso/device"
)

func init() {
	// Callers of these two send no signature, so signing names cannot route them.
	matchers["sso-oidc"] = func(r *http.Request) bool {
		switch r.URL.Path {
		case "/client/register", "/device_authorization", "/token":
			return r.Method == http.MethodPost && r.Header.Get("Authorization") == ""
		case "/authorize":
			return r.Method == http.MethodGet
		}
		return false
	}
	Raw("sso-oidc", oidcAuthorize)
	matchers["sso"] = func(r *http.Request) bool { return r.Header.Get("x-amz-sso_bearer_token") != "" }
	starters["sso-oidc"] = append(starters["sso-oidc"], func(s *Service) { s.h.Mount(ssoDevicePath, http.HandlerFunc(s.devicePage)) })

	Override("sso-oidc", "RegisterClient", func(c *Call) (map[string]any, error) {
		id, secret := "pogi-"+core.RandString(22, core.AlphaNum), "pogi-secret-"+core.RandString(48, core.AlphaNum)
		now := time.Now().UTC()
		c.Svc.putRes(&Resource{Type: "Client", Key: id, Name: str(c.In["clientName"]), Account: c.Account, Region: c.key(), Created: now, Updated: now,
			Data: map[string]any{"clientSecret": secret, "clientType": c.In["clientType"], "scopes": c.In["scopes"], "grantTypes": c.In["grantTypes"], "redirectUris": c.In["redirectUris"]}})
		base := "http://" + c.Core.Host
		return map[string]any{"clientId": id, "clientSecret": secret, "clientIdIssuedAt": float64(now.Unix()), "clientSecretExpiresAt": float64(now.Add(90 * 24 * time.Hour).Unix()),
			"authorizationEndpoint": base + "/authorize", "tokenEndpoint": base + "/token"}, nil
	})
	Override("sso-oidc", "StartDeviceAuthorization", func(c *Call) (map[string]any, error) {
		if _, err := c.Svc.client(c, str(c.In["clientId"]), str(c.In["clientSecret"])); err != nil {
			return nil, err
		}
		dev := "pogi-device-" + core.RandString(40, core.AlphaNum)
		user := core.RandString(4, core.Upper) + "-" + core.RandString(4, core.Upper)
		now := time.Now().UTC()
		c.Svc.putRes(&Resource{Type: "Device", Key: dev, Name: user, Account: c.Account, Region: c.key(), Created: now, Updated: now,
			Data: map[string]any{"clientId": c.In["clientId"], "userCode": user, "exp": float64(now.Add(ssoDeviceTTL).Unix())}})
		uri := "http://" + c.Core.Host + ssoDevicePath
		return map[string]any{"deviceCode": dev, "userCode": user, "verificationUri": uri, "verificationUriComplete": uri + "?user_code=" + user,
			"expiresIn": float64(ssoDeviceTTL / time.Second), "interval": 1.0}, nil
	})
	Override("sso-oidc", "CreateToken", func(c *Call) (map[string]any, error) {
		cl, err := c.Svc.client(c, str(c.In["clientId"]), str(c.In["clientSecret"]))
		if err != nil {
			return nil, err
		}
		var user string
		var refresh *Resource
		switch g := str(c.In["grantType"]); g {
		case grantDevice:
			d := c.Svc.find("Device", str(c.In["deviceCode"]))
			if d == nil || d.Data["clientId"] != cl.Key {
				return nil, oidcErr(400, "InvalidGrantException", "The device code is not valid.")
			}
			if float64(time.Now().Unix()) > d.Data["exp"].(float64) {
				c.Svc.dropRes(d)
				return nil, oidcErr(400, "ExpiredTokenException", "The device code has expired.")
			}
			polls, _ := d.Data["polls"].(float64)
			d.Data["polls"] = polls + 1
			// ponytail: a device approves itself on its second poll so `aws sso login` runs unattended; the verification page approves sooner.
			if d.Data["user"] == nil && polls >= 1 {
				d.Data["user"] = c.Svc.defaultUser()
			}
			c.Svc.putRes(d)
			if d.Data["user"] == nil {
				return nil, oidcErr(400, "AuthorizationPendingException", "The authorization request is pending approval.")
			}
			user = str(d.Data["user"])
			c.Svc.dropRes(d)
		case "authorization_code":
			code := c.Svc.find("AuthCode", str(c.In["code"]))
			if code == nil || code.Data["clientId"] != cl.Key || code.Data["redirectUri"] != str(c.In["redirectUri"]) {
				return nil, oidcErr(400, "InvalidGrantException", "The authorization code is not valid.")
			}
			sum := sha256.Sum256([]byte(str(c.In["codeVerifier"])))
			if base64.RawURLEncoding.EncodeToString(sum[:]) != strings.TrimRight(str(code.Data["challenge"]), "=") {
				return nil, oidcErr(400, "InvalidGrantException", "The code verifier does not match the code challenge.")
			}
			user = str(code.Data["user"])
			c.Svc.dropRes(code)
		case "refresh_token":
			for _, t := range c.Svc.all("Token") {
				if t.Data["refreshToken"] == c.In["refreshToken"] && t.Data["clientId"] == cl.Key {
					refresh = t
				}
			}
			if refresh == nil {
				return nil, oidcErr(400, "InvalidGrantException", "The refresh token is not valid.")
			}
			user = str(refresh.Data["user"])
		default:
			return nil, oidcErr(400, "UnsupportedGrantTypeException", "The grant type "+g+" is not supported.")
		}
		now := time.Now().UTC()
		tok := "aoa" + core.RandString(120, core.AlphaNum)
		rt := "aor" + core.RandString(120, core.AlphaNum)
		if refresh != nil {
			rt = str(refresh.Data["refreshToken"])
		}
		c.Svc.putRes(&Resource{Type: "Token", Key: tok, Name: user, Account: c.Account, Region: c.key(), Created: now, Updated: now,
			Data: map[string]any{"user": user, "clientId": cl.Key, "refreshToken": rt, "exp": float64(now.Add(ssoTokenTTL).Unix())}})
		return map[string]any{"accessToken": tok, "tokenType": "Bearer", "expiresIn": float64(ssoTokenTTL / time.Second), "refreshToken": rt}, nil
	})

	Override("sso", "ListAccounts", func(c *Call) (map[string]any, error) {
		user, err := c.Svc.portalUser(c)
		if err != nil {
			return nil, err
		}
		var accts []any
		for _, id := range c.Svc.assignedAccounts(user) {
			accts = append(accts, map[string]any{"accountId": id, "accountName": "pogi-" + id, "emailAddress": "pogi+" + id + "@localhost"})
		}
		return map[string]any{"accountList": accts}, nil
	})
	Override("sso", "ListAccountRoles", func(c *Call) (map[string]any, error) {
		user, err := c.Svc.portalUser(c)
		if err != nil {
			return nil, err
		}
		var roles []any
		for _, n := range c.Svc.assignedRoles(user, str(c.In["accountId"])) {
			roles = append(roles, map[string]any{"roleName": n, "accountId": c.In["accountId"]})
		}
		return map[string]any{"roleList": roles}, nil
	})
	Override("sso", "GetRoleCredentials", func(c *Call) (map[string]any, error) {
		user, err := c.Svc.portalUser(c)
		if err != nil {
			return nil, err
		}
		acct, role := str(c.In["accountId"]), str(c.In["roleName"])
		// ponytail: with no assignments at all any account and role is allowed, so a bare `aws sso login` profile works; once one exists they are enforced.
		if len(c.Svc.peerAll("sso-admin", "AccountAssignment")) > 0 && !slices.Contains(c.Svc.assignedRoles(user, acct), role) {
			return nil, core.Errorf(404, "ResourceNotFoundException", "No access")
		}
		arn := ssoRoleARN(c.Svc, acct, role, user)
		akid, secret, token := "ASIA"+core.RandString(16, core.UpperNum), core.RandString(40, core.AlphaNum), "FwoGZXIvYXdzE"+core.RandString(180, core.AlphaNum)
		c.Hub().AddIdentity(akid, core.Identity{Account: acct, ARN: arn, UserID: "AROA" + core.RandString(17, core.UpperNum) + ":" + user, Secret: secret})
		return map[string]any{"roleCredentials": map[string]any{"accessKeyId": akid, "secretAccessKey": secret, "sessionToken": token,
			"expiration": float64(time.Now().Add(time.Hour).UnixMilli())}}, nil
	})
	Override("sso", "Logout", func(c *Call) (map[string]any, error) {
		if _, err := c.Svc.portalUser(c); err != nil {
			return nil, err
		}
		oidc := c.Svc.peer("sso-oidc")
		if t := oidc.find("Token", str(c.In["accessToken"])); t != nil {
			oidc.dropRes(t)
		}
		return map[string]any{}, nil
	})
}

// ssoRoleName is the IAM role an assignment of a permission set creates in the account.
func ssoRoleName(psName, psArn string) string {
	sum := sha256.Sum256([]byte(psArn))
	return fmt.Sprintf("AWSReservedSSO_%s_%x", psName, sum[:8])
}

// ssoRoleARN is the assumed-role ARN the credentials of a role name carry (the permission set's, when it exists).
func ssoRoleARN(s *Service, acct, role, user string) string {
	arn := role
	for _, ps := range s.peerAll("sso-admin", "PermissionSet") {
		if str(ps.Data["Name"]) == role || ps.IDs["PermissionSetArn"] == role {
			arn = ps.IDs["PermissionSetArn"]
		}
	}
	return core.GlobalARN("sts", core.DefaultRegion, acct, "assumed-role/"+ssoRoleName(role, arn)+"/"+user)
}

func oidcErr(status int, code, msg string) error { return core.Errorf(status, code, "%s", msg) }

// client checks a client id and secret.
func (s *Service) client(c *Call, id, secret string) (*Resource, error) {
	cl := s.find("Client", id)
	if cl == nil || cl.Data["clientSecret"] != secret {
		return nil, oidcErr(401, "InvalidClientException", "The client credentials are not valid.")
	}
	return cl, nil
}

// portalUser is the signed-in user of a portal call, from its bearer token.
func (s *Service) portalUser(c *Call) (string, error) {
	t := s.peer("sso-oidc").find("Token", str(c.In["accessToken"]))
	if t == nil || float64(time.Now().Unix()) > t.Data["exp"].(float64) {
		return "", core.Errorf(401, "UnauthorizedException", "Session token not found or invalid")
	}
	return str(t.Data["user"]), nil
}

// defaultUser is who a sign-in is approved for: the oldest user of the identity store, else "pogi".
func (s *Service) defaultUser() string {
	if us := s.peerAll("identitystore", "User"); len(us) > 0 {
		return str(us[0].Data["UserName"])
	}
	return "pogi"
}

// assignedAccounts and assignedRoles list what the assignments give a user, directly or through groups.
func (s *Service) assignedAccounts(user string) []string {
	var out []string
	for _, a := range s.assignmentsOf(user) {
		if id := str(a.Data["TargetId"]); !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out
}

func (s *Service) assignedRoles(user, acct string) []string {
	var out []string
	for _, a := range s.assignmentsOf(user) {
		if str(a.Data["TargetId"]) != acct {
			continue
		}
		for _, ps := range s.peerAll("sso-admin", "PermissionSet") {
			if ps.IDs["PermissionSetArn"] == str(a.Data["PermissionSetArn"]) && !slices.Contains(out, str(ps.Data["Name"])) {
				out = append(out, str(ps.Data["Name"]))
			}
		}
	}
	return out
}

func (s *Service) assignmentsOf(user string) []*Resource {
	var uid string
	for _, u := range s.peerAll("identitystore", "User") {
		if str(u.Data["UserName"]) == user {
			uid = u.ID
		}
	}
	groups := map[string]bool{}
	for _, m := range s.peerAll("identitystore", "GroupMembership") {
		if mid, _ := m.Data["MemberId"].(map[string]any); uid != "" && str(mid["UserId"]) == uid {
			groups[str(m.Data["GroupId"])] = true
		}
	}
	var out []*Resource
	for _, a := range s.peerAll("sso-admin", "AccountAssignment") {
		switch pid := str(a.Data["PrincipalId"]); str(a.Data["PrincipalType"]) {
		case "USER":
			if uid != "" && pid == uid {
				out = append(out, a)
			}
		case "GROUP":
			if groups[pid] {
				out = append(out, a)
			}
		}
	}
	return out
}

func isLoopback(u *url.URL) bool {
	h := u.Hostname()
	return h == "127.0.0.1" || h == "::1" || h == "localhost"
}

// redirectAllowed says whether the sign-in code may be sent to u. It must be one of
// the URIs the client registered. Loopback URIs match on scheme, host and path with
// any port, as AWS allows for native apps. A client that registered none may use
// loopback only.
func redirectAllowed(cl *Resource, u *url.URL) bool {
	var reg []string
	switch v := cl.Data["redirectUris"].(type) {
	case []string:
		reg = v
	case []any:
		for _, x := range v {
			reg = append(reg, str(x))
		}
	}
	if len(reg) == 0 {
		return isLoopback(u)
	}
	for _, r := range reg {
		ru, err := url.Parse(r)
		if err != nil {
			continue
		}
		if r == u.String() || isLoopback(ru) && isLoopback(u) && ru.Scheme == u.Scheme && ru.Hostname() == u.Hostname() && ru.Path == u.Path {
			return true
		}
	}
	return false
}

// oidcAuthorize is the browser half of the authorization-code grant (/authorize),
// which has no operation in the model: it approves and redirects back at once.
func oidcAuthorize(s *Service, w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet || r.URL.Path != "/authorize" {
		return false
	}
	q := r.URL.Query()
	redirect, err := url.Parse(q.Get("redirect_uri"))
	cl := s.find("Client", q.Get("client_id"))
	if cl == nil || err != nil || redirect.Host == "" || !redirectAllowed(cl, redirect) {
		http.Error(w, "invalid_request: unknown client or redirect_uri", http.StatusBadRequest)
		return true
	}
	code := "pogi-code-" + core.RandString(40, core.AlphaNum)
	now := time.Now().UTC()
	s.putRes(&Resource{Type: "AuthCode", Key: code, Name: code, Account: core.DefaultAccount, Region: core.DefaultRegion, Created: now, Updated: now,
		Data: map[string]any{"clientId": q.Get("client_id"), "challenge": q.Get("code_challenge"), "redirectUri": q.Get("redirect_uri"), "user": s.defaultUser()}})
	rq := redirect.Query()
	rq.Set("code", code)
	rq.Set("state", q.Get("state"))
	redirect.RawQuery = rq.Encode()
	http.Redirect(w, r, redirect.String(), http.StatusFound)
	return true
}

// devicePage is the verification URL: visiting it approves the device code.
func (s *Service) devicePage(w http.ResponseWriter, r *http.Request) {
	code := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("user_code")))
	var dev *Resource
	for _, d := range s.all("Device") {
		if d.Data["userCode"] == code {
			dev = d
		}
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if code == "" {
		fmt.Fprint(w, `<!doctype html><title>pogi SSO</title><h1>Sign in</h1><form method="get"><label>Code <input name="user_code" autofocus></label> <button>Approve</button></form>`)
		return
	}
	if dev == nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "<!doctype html><title>pogi SSO</title><h1>Unknown code</h1><p>No pending sign-in has the code %s.</p>", html.EscapeString(code))
		return
	}
	user := r.URL.Query().Get("user")
	if user == "" {
		user = s.defaultUser()
	}
	dev.Data["user"] = user
	s.putRes(dev)
	fmt.Fprintf(w, "<!doctype html><title>pogi SSO</title><h1>Signed in</h1><p>The device with code %s is approved as %s. You can close this window.</p>", html.EscapeString(code), html.EscapeString(user))
}

// ---- resources outside a Call

func (s *Service) putRes(r *Resource) {
	s.mu.Lock()
	s.st.Res[r.skey()] = r
	s.mu.Unlock()
}

func (s *Service) dropRes(r *Resource) {
	s.mu.Lock()
	delete(s.st.Res, r.skey())
	s.mu.Unlock()
}

// all returns the resources of a type in every account and region, oldest first.
func (s *Service) all(typ string) []*Resource {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Resource
	for _, r := range s.st.Res {
		if r.Type == typ {
			out = append(out, r.clone())
		}
	}
	slices.SortFunc(out, func(a, b *Resource) int {
		if c := a.Created.Compare(b.Created); c != 0 {
			return c
		}
		return strings.Compare(a.Key, b.Key)
	})
	return out
}

// find returns the resource of a type with a key in any account and region, or nil.
func (s *Service) find(typ, key string) *Resource {
	for _, r := range s.all(typ) {
		if r.Key == key {
			return r
		}
	}
	return nil
}

// peer is another generic service of the hub, or a service with no resources.
func (s *Service) peer(name string) *Service {
	if p, ok := s.h.Service(name).(*Service); ok {
		return p
	}
	return &Service{st: state{Res: map[string]*Resource{}}}
}

func (s *Service) peerAll(name, typ string) []*Resource { return s.peer(name).all(typ) }
