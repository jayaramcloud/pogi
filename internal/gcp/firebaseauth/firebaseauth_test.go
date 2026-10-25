package firebaseauth_test

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/auth"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/firebaseauth"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type env struct {
	t    *testing.T
	svc  *firebaseauth.Service
	url  string
	auth *auth.Client
}

// start serves the emulator and gives the official Admin SDK in emulator mode (FIREBASE_AUTH_EMULATOR_HOST) for project "proj".
func start(t *testing.T) *env {
	t.Helper()
	var svc *firebaseauth.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = firebaseauth.New(h); return svc })
	t.Setenv("FIREBASE_AUTH_EMULATOR_HOST", strings.TrimPrefix(url, "http://"))
	app := must(firebase.NewApp(ctx, &firebase.Config{ProjectID: "proj"}, option.WithoutAuthentication()))
	return &env{t, svc, url, must(app.Auth(ctx))}
}

// call posts JSON like a Firebase client SDK would and returns the status and decoded body.
func (e *env) call(method, path string, in any) (int, map[string]any) {
	e.t.Helper()
	var rd io.Reader
	if in != nil {
		rd = bytes.NewReader(must(json.Marshal(in)))
	}
	req := must(http.NewRequest(method, e.url+path, rd))
	req.Header.Set("Content-Type", "application/json")
	resp := must(http.DefaultClient.Do(req))
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		e.t.Fatalf("%s %s: %d %s", method, path, resp.StatusCode, raw)
	}
	return resp.StatusCode, out
}

const accounts = "/identitytoolkit.googleapis.com/v1/projects/proj/accounts"

// ok calls an accounts method and insists on 200.
func (e *env) ok(op string, in any) map[string]any {
	e.t.Helper()
	code, out := e.call("POST", accounts+op+"?key=fake-api-key", in)
	if code != 200 {
		e.t.Fatalf("%s: %d %v", op, code, out)
	}
	return out
}

func (e *env) wantErr(op string, in any, msg string) {
	e.t.Helper()
	code, out := e.call("POST", accounts+op, in)
	got, _ := out["error"].(map[string]any)["message"].(string)
	if code != 400 || !strings.HasPrefix(got, msg) {
		e.t.Fatalf("%s: want 400 %s, got %d %v", op, msg, code, out)
	}
}

func (e *env) signUp(email, pw string) string {
	return e.ok(":signUp", map[string]any{"email": email, "password": pw, "returnSecureToken": true})["idToken"].(string)
}

func TestAdminSDK(t *testing.T) {
	e := start(t)
	u := must(e.auth.CreateUser(ctx, (&auth.UserToCreate{}).Email("Ada@Example.com").Password("secret1").DisplayName("Ada").EmailVerified(true)))
	if u.Email != "ada@example.com" || u.DisplayName != "Ada" || !u.EmailVerified || u.UID == "" || len(u.ProviderUserInfo) != 1 || u.ProviderUserInfo[0].ProviderID != "password" ||
		u.UserMetadata.CreationTimestamp == 0 || u.TokensValidAfterMillis == 0 {
		t.Fatalf("created = %+v / %+v", u.UserInfo, u.ProviderUserInfo)
	}
	if got := must(e.auth.GetUserByEmail(ctx, "ada@example.com")); got.UID != u.UID {
		t.Fatalf("by email = %v", got.UID)
	}
	if _, err := e.auth.GetUser(ctx, "nobody"); !auth.IsUserNotFound(err) {
		t.Fatalf("missing user: %v", err)
	}
	if _, err := e.auth.CreateUser(ctx, (&auth.UserToCreate{}).Email("ada@example.com")); !auth.IsEmailAlreadyExists(err) {
		t.Fatalf("duplicate email: %v", err)
	}
	if _, err := e.auth.CreateUser(ctx, (&auth.UserToCreate{}).UID(u.UID)); !auth.IsUIDAlreadyExists(err) {
		t.Fatalf("duplicate uid: %v", err)
	}

	must0(e.auth.SetCustomUserClaims(ctx, u.UID, map[string]any{"role": "admin", "level": 3}))
	got := must(e.auth.GetUser(ctx, u.UID))
	if got.CustomClaims["role"] != "admin" || got.CustomClaims["level"] != float64(3) {
		t.Fatalf("claims = %v", got.CustomClaims)
	}
	up := must(e.auth.UpdateUser(ctx, u.UID, (&auth.UserToUpdate{}).DisplayName("").PhoneNumber("+15551234567").Disabled(true)))
	if up.DisplayName != "" || up.PhoneNumber != "+15551234567" || !up.Disabled {
		t.Fatalf("updated = %+v", up.UserInfo)
	}
	if _, err := e.auth.CreateUser(ctx, (&auth.UserToCreate{}).PhoneNumber("+15551234567")); !auth.IsPhoneNumberAlreadyExists(err) {
		t.Fatalf("duplicate phone: %v", err)
	}
	must0(e.auth.SetCustomUserClaims(ctx, u.UID, nil))
	if got := must(e.auth.GetUser(ctx, u.UID)); got.CustomClaims != nil {
		t.Fatalf("claims after clearing = %v", got.CustomClaims)
	}

	// Import, list and delete in bulk.
	res := must(e.auth.ImportUsers(ctx, []*auth.UserToImport{(&auth.UserToImport{}).UID("imp1").Email("imp1@example.com"), (&auth.UserToImport{}).UID("imp2").Email("imp2@example.com")}))
	if res.SuccessCount != 2 || res.FailureCount != 0 {
		t.Fatalf("import = %+v", res)
	}
	res = must(e.auth.ImportUsers(ctx, []*auth.UserToImport{(&auth.UserToImport{}).UID("imp1")}))
	if res.FailureCount != 1 {
		t.Fatalf("duplicate import = %+v", res)
	}
	var uids []string
	for it := e.auth.Users(ctx, ""); ; {
		x, err := it.Next()
		if err == iterator.Done {
			break
		}
		uids = append(uids, must(x, err).UID)
	}
	if len(uids) != 3 {
		t.Fatalf("listed %v", uids)
	}
	del := must(e.auth.DeleteUsers(ctx, []string{"imp1", "imp2", "never-existed"}))
	if del.SuccessCount != 3 || del.FailureCount != 0 {
		t.Fatalf("batch delete = %+v", del)
	}
	must0(e.auth.DeleteUser(ctx, u.UID))
	if _, err := e.auth.GetUser(ctx, u.UID); !auth.IsUserNotFound(err) {
		t.Fatalf("deleted user: %v", err)
	}
	if err := e.auth.DeleteUser(ctx, u.UID); !auth.IsUserNotFound(err) {
		t.Fatalf("deleting twice: %v", err)
	}
}

func must0(err error) {
	if err != nil {
		panic(err)
	}
}

func TestSignInAndVerifyIDToken(t *testing.T) {
	e := start(t)
	// A client signs up over REST, signs in, and the Admin SDK verifies the token.
	e.signUp("Grace@Example.com", "hopper")
	in := e.ok(":signInWithPassword", map[string]any{"email": "grace@example.com", "password": "hopper", "returnSecureToken": true})
	if in["registered"] != true || in["expiresIn"] != "3600" || in["refreshToken"] == "" {
		t.Fatalf("sign in = %v", in)
	}
	tok := in["idToken"].(string)
	uid := in["localId"].(string)
	d := must(e.auth.VerifyIDToken(ctx, tok))
	if d.UID != uid || d.Issuer != "https://securetoken.google.com/proj" || d.Audience != "proj" || d.Firebase.SignInProvider != "password" ||
		d.Claims["email"] != "grace@example.com" || d.Claims["email_verified"] != false || d.Claims["user_id"] != uid ||
		d.Firebase.Identities["email"].([]any)[0] != "grace@example.com" {
		t.Fatalf("decoded = %+v", d)
	}

	// Custom claims set by the Admin SDK ride in the next ID token.
	must0(e.auth.SetCustomUserClaims(ctx, uid, map[string]any{"role": "admin"}))
	tok2 := e.ok(":signInWithPassword", map[string]any{"email": "grace@example.com", "password": "hopper"})["idToken"].(string)
	d2 := must(e.auth.VerifyIDToken(ctx, tok2))
	if d2.Claims["role"] != "admin" || d.Claims["role"] != nil {
		t.Fatalf("claims: new %v, old %v", d2.Claims["role"], d.Claims["role"])
	}
	must(e.auth.VerifyIDTokenAndCheckRevoked(ctx, tok2))

	// Wrong credentials.
	e.wantErr(":signInWithPassword", map[string]any{"email": "grace@example.com", "password": "wrong!"}, "INVALID_PASSWORD")
	e.wantErr(":signInWithPassword", map[string]any{"email": "nobody@example.com", "password": "whatever"}, "EMAIL_NOT_FOUND")
	e.wantErr(":signUp", map[string]any{"email": "grace@example.com", "password": "hopper"}, "EMAIL_EXISTS")
	e.wantErr(":signUp", map[string]any{"email": "new@example.com", "password": "123"}, "WEAK_PASSWORD")
	e.wantErr(":signUp", map[string]any{"email": "not-an-email", "password": "123456"}, "INVALID_EMAIL")
	must(e.auth.UpdateUser(ctx, uid, (&auth.UserToUpdate{}).Disabled(true)))
	e.wantErr(":signInWithPassword", map[string]any{"email": "grace@example.com", "password": "hopper"}, "USER_DISABLED")
	if _, err := e.auth.VerifyIDToken(ctx, tok2); !auth.IsUserDisabled(err) {
		t.Fatalf("token of a disabled user: %v", err)
	}
	must(e.auth.UpdateUser(ctx, uid, (&auth.UserToUpdate{}).Disabled(false)))

	// Rejected tokens: tampered claims, garbage, another project, an expired one, a deleted user's.
	parts := strings.Split(tok2, ".")
	claims := decode(t, parts[1])
	claims["aud"] = "other-project"
	tampered := parts[0] + "." + encode(claims) + "." + parts[2]
	if _, err := e.auth.VerifyIDToken(ctx, tampered); !auth.IsIDTokenInvalid(err) {
		t.Fatalf("tampered aud: %v", err)
	}
	claims["aud"], claims["sub"] = "proj", "someone-else"
	if _, err := e.auth.VerifyIDToken(ctx, parts[0]+"."+encode(claims)+"."+parts[2]); !auth.IsUserNotFound(err) {
		t.Fatalf("tampered sub: %v", err) // emulator mode checks the user, since it skips the signature
	}
	if _, err := e.auth.VerifyIDToken(ctx, "not.a.jwt"); !auth.IsIDTokenInvalid(err) {
		t.Fatalf("garbage: %v", err)
	}
	if _, err := e.auth.VerifyIDToken(ctx, ""); err == nil {
		t.Fatal("empty token accepted")
	}
	e.svc.SetClock(func() time.Time { return time.Now().Add(-2 * time.Hour) })
	old := e.ok(":signInWithPassword", map[string]any{"email": "grace@example.com", "password": "hopper"})
	e.svc.SetClock(time.Now)
	if _, err := e.auth.VerifyIDToken(ctx, old["idToken"].(string)); !auth.IsIDTokenExpired(err) {
		t.Fatalf("expired token: %v", err)
	}

	// Revoking refresh tokens: tokens issued before are rejected by the revocation check, and cannot be refreshed.
	e.svc.SetClock(func() time.Time { return time.Now().Add(-10 * time.Second) })
	pre := e.ok(":signUp", map[string]any{"email": "rev@example.com", "password": "secret1"})
	e.svc.SetClock(time.Now)
	must(e.auth.VerifyIDTokenAndCheckRevoked(ctx, pre["idToken"].(string)))
	must0(e.auth.RevokeRefreshTokens(ctx, pre["localId"].(string)))
	if _, err := e.auth.VerifyIDTokenAndCheckRevoked(ctx, pre["idToken"].(string)); !auth.IsIDTokenRevoked(err) {
		t.Fatalf("revoked token: %v", err)
	}
	if code, out := e.call("POST", "/securetoken.googleapis.com/v1/token", map[string]any{"grant_type": "refresh_token", "refresh_token": pre["refreshToken"]}); code != 400 ||
		out["error"].(map[string]any)["message"] != "TOKEN_EXPIRED" {
		t.Fatalf("refreshing a revoked token: %d %v", code, out)
	}

	must0(e.auth.DeleteUser(ctx, uid))
	if _, err := e.auth.VerifyIDToken(ctx, tok2); !auth.IsUserNotFound(err) {
		t.Fatalf("token of a deleted user: %v", err)
	}
}

func decode(t *testing.T, seg string) map[string]any {
	m := map[string]any{}
	must0(json.Unmarshal(must(base64.RawURLEncoding.DecodeString(seg)), &m))
	return m
}

func encode(m map[string]any) string {
	return base64.RawURLEncoding.EncodeToString(must(json.Marshal(m)))
}

// TestSignature: outside emulator mode a verifier checks the RS256 signature against the
// certificates Google publishes; the emulator serves its own at the same path.
func TestSignature(t *testing.T) {
	e := start(t)
	tok := e.signUp("sig@example.com", "secret1")
	resp := must(http.Get(e.url + "/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com"))
	var certs map[string]string
	must0(json.NewDecoder(resp.Body).Decode(&certs))
	resp.Body.Close()
	seg := strings.Split(tok, ".")
	if hdr := decode(t, seg[0]); hdr["alg"] != "RS256" || certs[hdr["kid"].(string)] == "" {
		t.Fatalf("header = %v, certs %v", hdr, certs)
	}
	block, _ := pem.Decode([]byte(certs["pogi-firebase"]))
	pub := must(x509.ParseCertificate(block.Bytes)).PublicKey.(*rsa.PublicKey)
	verify := func(signed, sig string) error {
		sum := sha256.Sum256([]byte(signed))
		return rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], must(base64.RawURLEncoding.DecodeString(sig)))
	}
	if err := verify(seg[0]+"."+seg[1], seg[2]); err != nil {
		t.Fatalf("signature: %v", err)
	}
	claims := decode(t, seg[1])
	claims["user_id"] = "x"
	if verify(seg[0]+"."+encode(claims), seg[2]) == nil {
		t.Fatal("a tampered payload verified")
	}
	if verify(seg[0]+"."+seg[1], strings.Repeat("A", len(seg[2]))) == nil {
		t.Fatal("a wrong signature verified")
	}
}

func TestAnonymousRefreshAndLinking(t *testing.T) {
	e := start(t)
	anon := e.ok(":signUp", map[string]any{"returnSecureToken": true})
	d := must(e.auth.VerifyIDToken(ctx, anon["idToken"].(string)))
	if d.Firebase.SignInProvider != "anonymous" || d.Claims["email"] != nil || anon["email"] != nil {
		t.Fatalf("anonymous = %+v", d)
	}
	uid := anon["localId"].(string)

	// Linking email and password to the anonymous account, as a client does with its ID token.
	link := e.ok(":update", map[string]any{"idToken": anon["idToken"], "email": "linked@example.com", "password": "linked1", "returnSecureToken": true})
	if link["email"] != "linked@example.com" || link["idToken"] == nil {
		t.Fatalf("link = %v", link)
	}
	if got := e.ok(":signInWithPassword", map[string]any{"email": "linked@example.com", "password": "linked1"}); got["localId"] != uid {
		t.Fatalf("signed in as %v, want %v", got["localId"], uid)
	}
	look := e.ok(":lookup", map[string]any{"idToken": link["idToken"]})
	users := look["users"].([]any)
	if len(users) != 1 || users[0].(map[string]any)["email"] != "linked@example.com" {
		t.Fatalf("lookup = %v", look)
	}
	e.ok(":update", map[string]any{"idToken": link["idToken"], "displayName": "Linked", "photoUrl": "https://example.com/p.png"})
	if u := must(e.auth.GetUser(ctx, uid)); u.DisplayName != "Linked" || u.PhotoURL != "https://example.com/p.png" {
		t.Fatalf("profile = %+v", u.UserInfo)
	}

	// securetoken.googleapis.com/v1/token exchanges the refresh token, form-encoded like the SDKs send it.
	form := url.Values{"grant_type": {"refresh_token"}, "refresh_token": {link["refreshToken"].(string)}}
	resp := must(http.PostForm(e.url+"/securetoken.googleapis.com/v1/token?key=fake", form))
	var ref map[string]any
	must0(json.NewDecoder(resp.Body).Decode(&ref))
	resp.Body.Close()
	if resp.StatusCode != 200 || ref["user_id"] != uid || ref["project_id"] != "proj" || ref["token_type"] != "Bearer" || ref["id_token"] != ref["access_token"] {
		t.Fatalf("refresh = %d %v", resp.StatusCode, ref)
	}
	if d := must(e.auth.VerifyIDToken(ctx, ref["id_token"].(string))); d.UID != uid || d.Claims["email"] != "linked@example.com" {
		t.Fatalf("refreshed token = %+v", d)
	}
	if code, out := e.call("POST", "/securetoken.googleapis.com/v1/token", map[string]any{"grant_type": "refresh_token", "refresh_token": "bogus"}); code != 400 ||
		out["error"].(map[string]any)["message"] != "INVALID_REFRESH_TOKEN" {
		t.Fatalf("bad refresh token: %d %v", code, out)
	}

	// A client deletes its own account.
	e.ok(":delete", map[string]any{"idToken": ref["id_token"]})
	if _, err := e.auth.GetUser(ctx, uid); !auth.IsUserNotFound(err) {
		t.Fatalf("deleted: %v", err)
	}
}

func TestCustomTokenAndIdp(t *testing.T) {
	e := start(t)
	// The Admin SDK in emulator mode makes unsigned custom tokens.
	ct := must(e.auth.CustomTokenWithClaims(ctx, "svc-user", map[string]any{"tier": "gold"}))
	in := e.ok(":signInWithCustomToken", map[string]any{"token": ct, "returnSecureToken": true})
	if in["isNewUser"] != true || in["localId"] != "svc-user" {
		t.Fatalf("custom sign in = %v", in)
	}
	d := must(e.auth.VerifyIDToken(ctx, in["idToken"].(string)))
	if d.UID != "svc-user" || d.Firebase.SignInProvider != "custom" || d.Claims["tier"] != "gold" {
		t.Fatalf("custom token = %+v", d)
	}
	if again := e.ok(":signInWithCustomToken", map[string]any{"token": ct}); again["isNewUser"] != false {
		t.Fatalf("second sign in = %v", again)
	}
	if got := must(e.auth.GetUser(ctx, "svc-user")); got.UID != "svc-user" {
		t.Fatal("the custom token's user was not created")
	}
	e.wantErr(":signInWithCustomToken", map[string]any{"token": "garbage"}, "INVALID_CUSTOM_TOKEN")

	// The emulator's fake IdP: id_token is JSON.
	idp := e.ok(":signInWithIdp", map[string]any{"requestUri": "http://localhost", "returnSecureToken": true,
		"postBody": "providerId=google.com&id_token=" + url.QueryEscape(`{"sub":"g-123","email":"Idp@Example.com","email_verified":true,"name":"Idp User"}`)})
	if idp["isNewUser"] != true || idp["providerId"] != "google.com" || idp["email"] != "idp@example.com" || idp["displayName"] != "Idp User" {
		t.Fatalf("idp = %v", idp)
	}
	d = must(e.auth.VerifyIDToken(ctx, idp["idToken"].(string)))
	if d.Firebase.SignInProvider != "google.com" || d.Firebase.Identities["google.com"].([]any)[0] != "g-123" {
		t.Fatalf("idp token = %+v", d)
	}
	if again := e.ok(":signInWithIdp", map[string]any{"postBody": "providerId=google.com&id_token=" + url.QueryEscape(`{"sub":"g-123"}`)}); again["localId"] != idp["localId"] || again["isNewUser"] != false {
		t.Fatalf("second idp sign in = %v", again)
	}
	u := must(e.auth.GetUserByProviderUID(ctx, "google.com", "g-123"))
	if u.UID != idp["localId"] || !u.EmailVerified {
		t.Fatalf("by provider = %+v", u.UserInfo)
	}
	e.wantErr(":signInWithIdp", map[string]any{"postBody": "providerId=google.com"}, "INVALID_IDP_RESPONSE")
}

func TestOOBCodes(t *testing.T) {
	e := start(t)
	uid := must(e.auth.CreateUser(ctx, (&auth.UserToCreate{}).Email("reset@example.com").Password("oldpass"))).UID
	e.wantErr(":sendOobCode", map[string]any{"requestType": "PASSWORD_RESET", "email": "nobody@example.com"}, "EMAIL_NOT_FOUND")
	e.wantErr(":sendOobCode", map[string]any{"requestType": "NOPE", "email": "reset@example.com"}, "INVALID_REQ_TYPE")

	// A client asks for a password reset email; the emulator keeps the code instead of sending it.
	out := e.ok(":sendOobCode", map[string]any{"requestType": "PASSWORD_RESET", "email": "reset@example.com"})
	if out["email"] != "reset@example.com" || out["oobLink"] != nil {
		t.Fatalf("send = %v", out)
	}
	code, list := e.call("GET", "/emulator/v1/projects/proj/oobCodes", nil)
	codes := list["oobCodes"].([]any)
	if code != 200 || len(codes) != 1 {
		t.Fatalf("oobCodes = %d %v", code, list)
	}
	oob := codes[0].(map[string]any)
	if oob["email"] != "reset@example.com" || oob["requestType"] != "PASSWORD_RESET" || oob["oobCode"] == "" || !strings.Contains(oob["oobLink"].(string), "oobCode="+oob["oobCode"].(string)) {
		t.Fatalf("oob = %v", oob)
	}
	if _, other := e.call("GET", "/emulator/v1/projects/elsewhere/oobCodes", nil); len(other["oobCodes"].([]any)) != 0 {
		t.Fatalf("another project sees %v", other)
	}

	// Completing the reset with the code sets the password.
	if got := e.ok(":resetPassword", map[string]any{"oobCode": oob["oobCode"]}); got["email"] != "reset@example.com" || got["requestType"] != "PASSWORD_RESET" {
		t.Fatalf("check code = %v", got)
	}
	e.wantErr(":resetPassword", map[string]any{"oobCode": oob["oobCode"], "newPassword": "123"}, "WEAK_PASSWORD")
	e.ok(":resetPassword", map[string]any{"oobCode": oob["oobCode"], "newPassword": "newpass1"})
	e.ok(":signInWithPassword", map[string]any{"email": "reset@example.com", "password": "newpass1"})
	e.wantErr(":signInWithPassword", map[string]any{"email": "reset@example.com", "password": "oldpass"}, "INVALID_PASSWORD")
	e.wantErr(":resetPassword", map[string]any{"oobCode": oob["oobCode"], "newPassword": "again123"}, "INVALID_OOB_CODE") // spent

	// The Admin SDK's links come from the same codes; applying a verification code verifies the email.
	link := must(e.auth.EmailVerificationLink(ctx, "reset@example.com"))
	pl := must(e.auth.PasswordResetLink(ctx, "reset@example.com"))
	if !strings.Contains(link, "mode=verifyEmail") || !strings.Contains(pl, "mode=resetPassword") {
		t.Fatalf("links: %s %s", link, pl)
	}
	vcode := must(url.Parse(link)).Query().Get("oobCode")
	e.ok(":update", map[string]any{"oobCode": vcode})
	if u := must(e.auth.GetUser(ctx, uid)); !u.EmailVerified {
		t.Fatal("email not verified by the code")
	}
	if _, list = e.call("GET", "/emulator/v1/projects/proj/oobCodes", nil); len(list["oobCodes"].([]any)) != 1 { // spent codes are gone; the Admin SDK's reset link remains
		t.Fatalf("oobCodes = %v", list)
	}

	// A client asks for an email verification with its ID token.
	tok := e.ok(":signInWithPassword", map[string]any{"email": "reset@example.com", "password": "newpass1"})["idToken"]
	e.ok(":sendOobCode", map[string]any{"requestType": "VERIFY_EMAIL", "idToken": tok})

	// Codes still pending are in the console's inbox.
	var kinds []string
	for _, m := range e.svc.Outbox() {
		if m.To != "reset@example.com" || m.Subject == "" || !strings.HasPrefix(m.Link, "http://") {
			t.Fatalf("mail = %+v", m)
		}
		kinds = append(kinds, m.Kind)
	}
	if strings.Join(kinds, ",") != "PASSWORD_RESET,VERIFY_EMAIL" {
		t.Fatalf("outbox kinds = %v", kinds)
	}
}

func TestEmulatorEndpoints(t *testing.T) {
	e := start(t)
	e.signUp("one@example.com", "secret1")
	other := func(email string) {
		code, out := e.call("POST", "/identitytoolkit.googleapis.com/v1/projects/second/accounts:signUp", map[string]any{"email": email, "password": "secret1"})
		if code != 200 {
			t.Fatalf("sign up in the second project: %d %v", code, out)
		}
	}
	other("two@example.com")
	// Requests that name no project belong to the default one.
	unqualified := e.call
	if code, out := unqualified("POST", "/identitytoolkit.googleapis.com/v1/accounts:signUp", map[string]any{"returnSecureToken": true}); code != 200 ||
		strings.Split(out["idToken"].(string), ".")[1] == "" || decode(t, strings.Split(out["idToken"].(string), ".")[1])["aud"] != core.DefaultProject {
		t.Fatalf("default project sign up: %d %v", code, out)
	}

	if code, out := e.call("GET", "/emulator/v1/projects/proj/config", nil); code != 200 || out["signIn"] == nil {
		t.Fatalf("config = %d %v", code, out)
	}
	if code, _ := e.call("DELETE", "/emulator/v1/projects/proj/accounts", nil); code != 200 {
		t.Fatalf("clear = %d", code)
	}
	if got := must(e.auth.GetUsers(ctx, []auth.UserIdentifier{auth.EmailIdentifier{Email: "one@example.com"}})); len(got.Users) != 0 {
		t.Fatalf("accounts survived the clear: %v", got.Users)
	}
	if code, out := e.call("POST", "/identitytoolkit.googleapis.com/v1/projects/second/accounts:lookup", map[string]any{"email": []string{"two@example.com"}}); code != 200 || out["users"] == nil {
		t.Fatalf("the other project lost its accounts: %d %v", code, out)
	}
	if code, _ := e.call("GET", "/emulator/v1/projects/proj/nothing", nil); code != 404 {
		t.Fatalf("unknown emulator path = %d", code)
	}
	if code, _ := e.call("POST", "/identitytoolkit.googleapis.com/v1/accounts:unknownOp", map[string]any{}); code != 501 {
		t.Fatalf("unknown op = %d", code)
	}
}

func TestPersistenceAndConsole(t *testing.T) {
	e := start(t)
	tok := e.signUp("keep@example.com", "secret1")
	saved := must(e.svc.Save())
	if tables := e.svc.Resources(); len(tables) != 1 || len(tables[0].Rows) != 1 || tables[0].Rows[0][2] != "keep@example.com" {
		t.Fatalf("tables = %v", tables)
	}
	uid := must(e.auth.GetUserByEmail(ctx, "keep@example.com")).UID
	b, _ := json.Marshal(e.svc.Inspect("Users", "proj/"+uid))
	if !strings.Contains(string(b), "keep@example.com") || strings.Contains(string(b), "hash") {
		t.Fatalf("inspect = %s", b)
	}

	// A restarted emulator still knows the user and the signing key: old tokens keep verifying.
	e2 := start(t)
	if err := e2.svc.Load(saved); err != nil {
		t.Fatal(err)
	}
	e2.ok(":signInWithPassword", map[string]any{"email": "keep@example.com", "password": "secret1"})
	if d := must(e2.auth.VerifyIDToken(ctx, tok)); d.UID != uid {
		t.Fatalf("old token = %+v", d)
	}
	if code, _ := e2.call("POST", accounts+":lookup?key=x", map[string]any{"idToken": tok}); code != 200 {
		t.Fatalf("old token no longer parses: %d", code)
	}
	e2.svc.Reset()
	if _, err := e2.auth.GetUser(ctx, uid); !auth.IsUserNotFound(err) {
		t.Fatalf("after reset: %v", err)
	}
}

// maxResults that does not fit an int32 is refused: 4294967297 used to wrap to 1 and return one user.
func TestBatchGetMaxResultsRange(t *testing.T) {
	e := start(t)
	for _, v := range []string{"4294967297", "-1"} {
		code, out := e.call("GET", accounts+":batchGet?maxResults="+v, nil)
		msg, _ := out["error"].(map[string]any)["message"].(string)
		if code != 400 || !strings.HasPrefix(msg, "Invalid value for maxResults") {
			t.Errorf("maxResults=%s: %d %v, want 400 Invalid value for maxResults", v, code, out)
		}
	}
	if code, out := e.call("GET", accounts+":batchGet?maxResults=5", nil); code != 200 {
		t.Errorf("maxResults=5: %d %v", code, out)
	}
}
