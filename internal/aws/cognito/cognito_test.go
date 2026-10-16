package cognito_test

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base32"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/nrndev/pogi/internal/aws/cognito"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

// The Cognito SDK is not a dependency of this module, so the tests speak the
// JSON 1.1 protocol directly: the same requests the SDKs send.

type M = map[string]any

type apiError struct{ Code, Message string }

func (e *apiError) Error() string { return e.Code + ": " + e.Message }

type api struct {
	t   *testing.T
	url string
	hc  *http.Client
}

// public operations are unsigned in AWS; everything else is SigV4-signed.
var public = []string{"SignUp", "ConfirmSignUp", "ResendConfirmationCode", "ForgotPassword", "ConfirmForgotPassword",
	"InitiateAuth", "RespondToAuthChallenge", "GetTokensFromRefreshToken", "AssociateSoftwareToken", "VerifySoftwareToken", "GetUser", "UpdateUserAttributes", "VerifyUserAttribute", "GlobalSignOut", "ChangePassword"}

func (a *api) call(op string, in any) (M, error) {
	b, _ := json.Marshal(in)
	req, _ := http.NewRequest(http.MethodPost, a.url+"/", bytes.NewReader(b))
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	if !slices.Contains(public, op) {
		awstest.Sign(a.t, req, "cognito-idp")
	}
	res, err := a.hc.Do(req)
	if err != nil {
		a.t.Fatal(err)
	}
	defer res.Body.Close()
	var out M
	json.NewDecoder(res.Body).Decode(&out)
	if res.StatusCode != http.StatusOK {
		return nil, &apiError{str(out, "__type"), str(out, "message")}
	}
	return out, nil
}

func (a *api) must(op string, in any) M {
	a.t.Helper()
	out, err := a.call(op, in)
	if err != nil {
		a.t.Fatalf("%s: %v", op, err)
	}
	return out
}

func (a *api) fails(op string, in any, code string) *apiError {
	a.t.Helper()
	_, err := a.call(op, in)
	var e *apiError
	if !errors.As(err, &e) || e.Code != code {
		a.t.Fatalf("%s: want %s, got %v", op, code, err)
	}
	return e
}

func (a *api) pool(in M) string {
	a.t.Helper()
	if in["PoolName"] == nil {
		in["PoolName"] = "test"
	}
	return str(a.must("CreateUserPool", in), "UserPool", "Id")
}

func (a *api) client(in M) M {
	a.t.Helper()
	return a.must("CreateUserPoolClient", in)["UserPoolClient"].(M)
}

// str follows path through nested JSON objects to a string.
func str(v any, path ...string) string {
	for _, p := range path {
		m, _ := v.(M)
		v = m[p]
	}
	s, _ := v.(string)
	return s
}

func getJSON(t *testing.T, u string, out any) {
	t.Helper()
	res, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %s", u, res.Status)
	}
	json.NewDecoder(res.Body).Decode(out)
}

func setup(t *testing.T) (*api, *cognito.Service) {
	var svc *cognito.Service
	_, u := core.ServeTest(t, func(h *core.Hub) core.Service { svc = cognito.New(h); return svc })
	return &api{t, u, http.DefaultClient}, svc
}

func lastCode(svc *cognito.Service, typ string) cognito.Code {
	codes := svc.Codes()
	for i := len(codes) - 1; i >= 0; i-- {
		if codes[i].Type == typ {
			return codes[i]
		}
	}
	return cognito.Code{}
}

var b64 = base64.RawURLEncoding

// verifyJWT checks an RS256 token against the pool's JWKS, issuer and expiry, and returns its claims.
func verifyJWT(t *testing.T, base, pool, token string) M {
	t.Helper()
	var jwks struct {
		Keys []struct{ Kid, Alg, N, E string }
	}
	getJSON(t, base+"/"+pool+"/.well-known/jwks.json", &jwks)
	part := strings.Split(token, ".")
	if len(part) != 3 {
		t.Fatalf("not a JWT: %q", token)
	}
	var hdr struct{ Kid, Alg string }
	head, _ := b64.DecodeString(part[0])
	json.Unmarshal(head, &hdr)
	for _, k := range jwks.Keys {
		if k.Kid != hdr.Kid {
			continue
		}
		n, _ := b64.DecodeString(k.N)
		e, _ := b64.DecodeString(k.E)
		sig, _ := b64.DecodeString(part[2])
		sum := sha256.Sum256([]byte(part[0] + "." + part[1]))
		pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
		if hdr.Alg != "RS256" || rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], sig) != nil {
			t.Fatalf("bad signature on %s", token)
		}
		var claims M
		body, _ := b64.DecodeString(part[1])
		json.Unmarshal(body, &claims)
		if exp, _ := claims["exp"].(float64); claims["iss"] != base+"/"+pool || exp <= float64(time.Now().Unix()) {
			t.Fatalf("claims = %v", claims)
		}
		return claims
	}
	t.Fatalf("kid %s is not in the JWKS", hdr.Kid)
	return nil
}

func secretHash(secret, username, clientID string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(username + clientID))
	return base64.StdEncoding.EncodeToString(m.Sum(nil))
}

func hasAttr(user M, key, name, value string) bool {
	list, _ := user[key].([]any)
	return slices.ContainsFunc(list, func(a any) bool { return str(a, "Name") == name && str(a, "Value") == value })
}

func TestPoolsAndClients(t *testing.T) {
	a, _ := setup(t)
	pool := a.pool(M{"PoolName": "app", "Policies": M{"PasswordPolicy": M{"MinimumLength": 10, "RequireNumbers": true}},
		"Schema":       []M{{"Name": "tenant", "AttributeDataType": "String", "Mutable": true}, {"Name": "email", "Required": true, "Mutable": true}},
		"UserPoolTags": M{"team": "core"}, "UsernameConfiguration": M{"CaseSensitive": false}})
	if !regexp.MustCompile(`^us-east-1_[0-9A-Za-z]{9}$`).MatchString(pool) {
		t.Fatalf("pool id %q", pool)
	}
	d := a.must("DescribeUserPool", M{"UserPoolId": pool})["UserPool"].(M)
	arn := str(d, "Arn")
	schema, _ := json.Marshal(d["SchemaAttributes"])
	if d["Name"] != "app" || arn != "arn:aws:cognito-idp:us-east-1:000000000000:userpool/"+pool || d["MfaConfiguration"] != "OFF" ||
		!strings.Contains(string(schema), `"Name":"custom:tenant"`) || d["Policies"].(M)["PasswordPolicy"].(M)["MinimumLength"] != 10.0 {
		t.Fatalf("describe = %v", d)
	}
	if pools := a.must("ListUserPools", M{"MaxResults": 10})["UserPools"].([]any); len(pools) != 1 || str(pools[0], "Id") != pool {
		t.Fatalf("pools = %v", pools)
	}
	a.must("UpdateUserPool", M{"UserPoolId": pool, "MfaConfiguration": "OPTIONAL"})
	d = a.must("DescribeUserPool", M{"UserPoolId": pool})["UserPool"].(M)
	if d["MfaConfiguration"] != "OPTIONAL" || d["Policies"].(M)["PasswordPolicy"].(M)["MinimumLength"] != 8.0 {
		t.Fatalf("update should replace settings: %v", d)
	}
	a.must("TagResource", M{"ResourceArn": arn, "Tags": M{"env": "dev"}})
	a.must("UntagResource", M{"ResourceArn": arn, "TagKeys": []string{"team"}})
	if tags := a.must("ListTagsForResource", M{"ResourceArn": arn})["Tags"].(M); len(tags) != 1 || tags["env"] != "dev" {
		t.Fatalf("tags = %v", tags)
	}

	cl := a.client(M{"UserPoolId": pool, "ClientName": "web", "GenerateSecret": true, "ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH"}})
	id := str(cl, "ClientId")
	if len(id) != 26 || len(str(cl, "ClientSecret")) == 0 || cl["RefreshTokenValidity"] != 30.0 {
		t.Fatalf("client = %v", cl)
	}
	up := a.must("UpdateUserPoolClient", M{"UserPoolId": pool, "ClientId": id, "ClientName": "web2", "AccessTokenValidity": 5,
		"TokenValidityUnits": M{"AccessToken": "minutes"}})["UserPoolClient"].(M)
	if up["ClientName"] != "web2" || up["ClientSecret"] != cl["ClientSecret"] || up["ExplicitAuthFlows"] != nil {
		t.Fatalf("updated client = %v", up)
	}
	if list := a.must("ListUserPoolClients", M{"UserPoolId": pool, "MaxResults": 10})["UserPoolClients"].([]any); len(list) != 1 {
		t.Fatalf("clients = %v", list)
	}
	a.fails("CreateUserPoolClient", M{"UserPoolId": pool, "ClientName": "m2m", "AllowedOAuthFlows": []string{"client_credentials"}}, "InvalidOAuthFlowException")
	a.fails("CreateUserPoolClient", M{"UserPoolId": pool, "ClientName": "x", "AllowedOAuthScopes": []string{"api/nope"}}, "ScopeDoesNotExistException")
	a.must("DeleteUserPoolClient", M{"UserPoolId": pool, "ClientId": id})
	a.fails("DescribeUserPoolClient", M{"UserPoolId": pool, "ClientId": id}, "ResourceNotFoundException")

	a.must("CreateUserPoolDomain", M{"UserPoolId": pool, "Domain": "my-app"})
	if dd := a.must("DescribeUserPoolDomain", M{"Domain": "my-app"})["DomainDescription"].(M); dd["UserPoolId"] != pool || dd["Status"] != "ACTIVE" {
		t.Fatalf("domain = %v", dd)
	}
	a.fails("DeleteUserPool", M{"UserPoolId": pool}, "InvalidParameterException")
	a.must("DeleteUserPoolDomain", M{"UserPoolId": pool, "Domain": "my-app"})
	if dd := a.must("DescribeUserPoolDomain", M{"Domain": "my-app"})["DomainDescription"].(M); len(dd) != 0 {
		t.Fatalf("deleted domain = %v", dd)
	}
	a.must("DeleteUserPool", M{"UserPoolId": pool})
	a.fails("DescribeUserPool", M{"UserPoolId": pool}, "ResourceNotFoundException")
}

func TestSignUpAndTokens(t *testing.T) {
	a, svc := setup(t)
	pool := a.pool(M{"AutoVerifiedAttributes": []string{"email"}, "Schema": []M{{"Name": "tenant", "AttributeDataType": "String", "Mutable": true}}})
	id := str(a.client(M{"UserPoolId": pool, "ClientName": "app", "ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"}}), "ClientId")
	attrs := []M{{"Name": "email", "Value": "jane@example.com"}, {"Name": "custom:tenant", "Value": "acme"}}

	a.fails("SignUp", M{"ClientId": id, "Username": "jane", "Password": "short", "UserAttributes": attrs}, "InvalidPasswordException")
	a.fails("SignUp", M{"ClientId": id, "Username": "jane", "Password": "Passw0rd!", "UserAttributes": []M{{"Name": "custom:nope", "Value": "x"}}}, "InvalidParameterException")
	up := a.must("SignUp", M{"ClientId": id, "Username": "jane", "Password": "Passw0rd!", "UserAttributes": attrs})
	if up["UserConfirmed"] != false || str(up, "UserSub") == "" || str(up, "CodeDeliveryDetails", "DeliveryMedium") != "EMAIL" {
		t.Fatalf("sign up = %v", up)
	}
	a.fails("SignUp", M{"ClientId": id, "Username": "jane", "Password": "Passw0rd!"}, "UsernameExistsException")
	login := M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": id, "AuthParameters": M{"USERNAME": "jane", "PASSWORD": "Passw0rd!"}}
	a.fails("InitiateAuth", login, "UserNotConfirmedException")

	code := lastCode(svc, "SignUp")
	var outbox []cognito.Code
	getJSON(t, a.url+"/_aws/cognito/codes?pool="+pool, &outbox)
	if code.Destination != "jane@example.com" || len(code.Code) != 6 || len(outbox) != 1 || outbox[0].Code != code.Code {
		t.Fatalf("code = %+v, outbox = %+v", code, outbox)
	}
	a.fails("ConfirmSignUp", M{"ClientId": id, "Username": "jane", "ConfirmationCode": "x" + code.Code}, "CodeMismatchException")
	a.must("ConfirmSignUp", M{"ClientId": id, "Username": "jane", "ConfirmationCode": code.Code})

	e := a.fails("InitiateAuth", M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": id, "AuthParameters": M{"USERNAME": "jane", "PASSWORD": "wrong"}}, "NotAuthorizedException")
	if e.Message != "Incorrect username or password." {
		t.Fatalf("message = %q", e.Message)
	}
	a.fails("InitiateAuth", M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": id, "AuthParameters": M{"USERNAME": "nobody", "PASSWORD": "x"}}, "UserNotFoundException")
	res := a.must("InitiateAuth", login)["AuthenticationResult"].(M)
	idc := verifyJWT(t, a.url, pool, str(res, "IdToken"))
	ac := verifyJWT(t, a.url, pool, str(res, "AccessToken"))
	if idc["token_use"] != "id" || idc["aud"] != id || idc["email_verified"] != true || idc["cognito:username"] != "jane" ||
		idc["custom:tenant"] != "acme" || idc["sub"] != str(up, "UserSub") || idc["origin_jti"] != ac["origin_jti"] {
		t.Fatalf("id claims = %v", idc)
	}
	if ac["token_use"] != "access" || ac["client_id"] != id || ac["scope"] != "aws.cognito.signin.user.admin" || ac["username"] != "jane" {
		t.Fatalf("access claims = %v", ac)
	}
	var oidc M
	getJSON(t, a.url+"/"+pool+"/.well-known/openid-configuration", &oidc)
	if oidc["issuer"] != a.url+"/"+pool || oidc["jwks_uri"] != a.url+"/"+pool+"/.well-known/jwks.json" {
		t.Fatalf("openid-configuration = %v", oidc)
	}

	access := str(res, "AccessToken")
	if u := a.must("GetUser", M{"AccessToken": access}); u["Username"] != "jane" || !hasAttr(u, "UserAttributes", "email_verified", "true") {
		t.Fatalf("get user = %v", u)
	}
	a.fails("GetUser", M{"AccessToken": access + "x"}, "NotAuthorizedException")
	upd := a.must("UpdateUserAttributes", M{"AccessToken": access, "UserAttributes": []M{{"Name": "email", "Value": "jane@new.example"}}})
	if list := upd["CodeDeliveryDetailsList"].([]any); len(list) != 1 || !hasAttr(a.must("GetUser", M{"AccessToken": access}), "UserAttributes", "email_verified", "false") {
		t.Fatalf("update attributes = %v", upd)
	}
	a.must("VerifyUserAttribute", M{"AccessToken": access, "AttributeName": "email", "Code": lastCode(svc, "UpdateUserAttribute").Code})
	if !hasAttr(a.must("GetUser", M{"AccessToken": access}), "UserAttributes", "email_verified", "true") {
		t.Fatal("email should be verified again")
	}

	refresh := M{"AuthFlow": "REFRESH_TOKEN_AUTH", "ClientId": id, "AuthParameters": M{"REFRESH_TOKEN": str(res, "RefreshToken")}}
	again := a.must("InitiateAuth", refresh)["AuthenticationResult"].(M)
	if str(again, "RefreshToken") != "" || verifyJWT(t, a.url, pool, str(again, "AccessToken"))["origin_jti"] != ac["origin_jti"] {
		t.Fatalf("refresh = %v", again)
	}

	// The state, signing key included, survives a snapshot.
	snap, err := svc.Save()
	if err != nil {
		t.Fatal(err)
	}
	svc.Reset()
	a.fails("GetUser", M{"AccessToken": access}, "NotAuthorizedException")
	if err := svc.Load(snap); err != nil {
		t.Fatal(err)
	}
	a.must("GetUser", M{"AccessToken": str(again, "AccessToken")})

	// Signing out revokes the refresh token and every access token of the user.
	a.must("GlobalSignOut", M{"AccessToken": access})
	a.fails("GetUser", M{"AccessToken": str(again, "AccessToken")}, "NotAuthorizedException")
	a.fails("InitiateAuth", refresh, "NotAuthorizedException")
	verifyJWT(t, a.url, pool, str(a.must("InitiateAuth", login), "AuthenticationResult", "IdToken"))
}

func TestHTTPSIssuer(t *testing.T) {
	h, _ := core.ServeTest(t, func(h *core.Hub) core.Service { return cognito.New(h) })
	base, roots := core.ListenTLS(t, h, h)
	a := &api{t, base, &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}}}
	pool := a.pool(M{})
	id := str(a.client(M{"UserPoolId": pool, "ClientName": "app", "ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH"}}), "ClientId")
	a.must("AdminCreateUser", M{"UserPoolId": pool, "Username": "hal", "MessageAction": "SUPPRESS"})
	a.must("AdminSetUserPassword", M{"UserPoolId": pool, "Username": "hal", "Password": "Passw0rd!", "Permanent": true})
	res := a.must("InitiateAuth", M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": id, "AuthParameters": M{"USERNAME": "hal", "PASSWORD": "Passw0rd!"}})
	body, _ := b64.DecodeString(strings.Split(str(res, "AuthenticationResult", "IdToken"), ".")[1])
	var claims M
	json.Unmarshal(body, &claims)
	if !strings.HasPrefix(base, "https://") || claims["iss"] != base+"/"+pool {
		t.Fatalf("iss = %v over %s", claims["iss"], base)
	}
}

func TestAdminCreateUserNewPassword(t *testing.T) {
	a, svc := setup(t)
	pool := a.pool(M{})
	id := str(a.client(M{"UserPoolId": pool, "ClientName": "admin", "ExplicitAuthFlows": []string{"ALLOW_ADMIN_USER_PASSWORD_AUTH", "ALLOW_USER_PASSWORD_AUTH"}}), "ClientId")
	user := M{"UserPoolId": pool, "Username": "bob", "DesiredDeliveryMediums": []string{"EMAIL"},
		"UserAttributes": []M{{"Name": "email", "Value": "bob@example.com"}, {"Name": "email_verified", "Value": "true"}}}
	created := a.must("AdminCreateUser", user)["User"].(M)
	temp := lastCode(svc, "AdminCreateUser")
	if created["UserStatus"] != "FORCE_CHANGE_PASSWORD" || temp.Destination != "bob@example.com" || len(temp.Code) < 8 {
		t.Fatalf("created = %v, invite = %+v", created, temp)
	}
	a.fails("AdminCreateUser", user, "UsernameExistsException")

	ch := a.must("AdminInitiateAuth", M{"UserPoolId": pool, "ClientId": id, "AuthFlow": "ADMIN_USER_PASSWORD_AUTH",
		"AuthParameters": M{"USERNAME": "bob", "PASSWORD": temp.Code}})
	if ch["ChallengeName"] != "NEW_PASSWORD_REQUIRED" || str(ch, "Session") == "" || !strings.Contains(str(ch, "ChallengeParameters", "userAttributes"), "bob@example.com") {
		t.Fatalf("challenge = %v", ch)
	}
	answer := M{"ClientId": id, "ChallengeName": "NEW_PASSWORD_REQUIRED", "Session": ch["Session"],
		"ChallengeResponses": M{"USERNAME": "bob", "NEW_PASSWORD": "weak"}}
	a.fails("RespondToAuthChallenge", answer, "InvalidPasswordException")
	answer["ChallengeResponses"].(M)["NEW_PASSWORD"] = "N3w-Passw0rd"
	done := a.must("RespondToAuthChallenge", answer)
	verifyJWT(t, a.url, pool, str(done, "AuthenticationResult", "IdToken"))
	if u := a.must("AdminGetUser", M{"UserPoolId": pool, "Username": "bob"}); u["UserStatus"] != "CONFIRMED" {
		t.Fatalf("user = %v", u)
	}

	login := M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": id, "AuthParameters": M{"USERNAME": "bob", "PASSWORD": "N3w-Passw0rd"}}
	a.must("AdminDisableUser", M{"UserPoolId": pool, "Username": "bob"})
	if e := a.fails("InitiateAuth", login, "NotAuthorizedException"); e.Message != "User is disabled." {
		t.Fatalf("disabled = %v", e)
	}
	a.must("AdminEnableUser", M{"UserPoolId": pool, "Username": "bob"})
	a.must("InitiateAuth", login)
	a.must("AdminResetUserPassword", M{"UserPoolId": pool, "Username": "bob"})
	a.fails("InitiateAuth", login, "PasswordResetRequiredException")
	a.must("AdminDeleteUser", M{"UserPoolId": pool, "Username": "bob"})
	a.fails("AdminGetUser", M{"UserPoolId": pool, "Username": "bob"}, "UserNotFoundException")
}

const srpHex = "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DD" +
	"EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7ED" +
	"EE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3DC2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F" +
	"83655D23DCA3AD961C62F356208552BB9ED529077096966D670C354E4ABC9804F1746C08CA18217C32905E462E36CE3B" +
	"E39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9DE2BCBF6955817183995497CEA956AE515D2261898FA0510" +
	"15728E5A8AAAC42DAD33170D04507A33A85521ABDF1CBA64ECFB850458DBEF0A8AEA71575D060C7DB3970F85A6E1E4C7" +
	"ABF5AE8CDB0933D71E8C94E04A25619DCEE3D2261AD2EE6BF12FFA06D98A0864D87602733EC86A64521F2B18177B200C" +
	"BBE117577A615D6C770988C0BAD946E208E24FA074E5AB3143DB5BFCE0FD108E4B82D120A93AD2CAFFFFFFFFFFFFFFFF"

// pad and sum mirror padHex and hash of amazon-cognito-identity-js's AuthenticationHelper.
func pad(n *big.Int) []byte {
	b := n.Bytes()
	if len(b) == 0 || b[0]&0x80 != 0 {
		return append([]byte{0}, b...)
	}
	return b
}

func sum(parts ...[]byte) []byte {
	h := sha256.New()
	for _, p := range parts {
		h.Write(p)
	}
	return h.Sum(nil)
}

func mac(key []byte, parts ...[]byte) []byte {
	m := hmac.New(sha256.New, key)
	for _, p := range parts {
		m.Write(p)
	}
	return m.Sum(nil)
}

// srpLogin is the client half of USER_SRP_AUTH, computed the way amazon-cognito-identity-js does.
func srpLogin(a *api, pool, client, username, password string) (M, error) {
	N, _ := new(big.Int).SetString(srpHex, 16)
	g := big.NewInt(2)
	k := new(big.Int).SetBytes(sum(pad(N), pad(g)))
	small, _ := rand.Int(rand.Reader, N)
	A := new(big.Int).Exp(g, small, N)
	first, err := a.call("InitiateAuth", M{"AuthFlow": "USER_SRP_AUTH", "ClientId": client, "AuthParameters": M{"USERNAME": username, "SRP_A": A.Text(16)}})
	if err != nil {
		return nil, err
	}
	p := first["ChallengeParameters"].(M)
	B, _ := new(big.Int).SetString(str(p, "SRP_B"), 16)
	salt, _ := new(big.Int).SetString(str(p, "SALT"), 16)
	userID, short := str(p, "USER_ID_FOR_SRP"), pool[strings.IndexByte(pool, '_')+1:]
	u := new(big.Int).SetBytes(sum(pad(A), pad(B)))
	x := new(big.Int).SetBytes(sum(pad(salt), sum([]byte(short+userID+":"+password))))
	S := new(big.Int).Sub(B, new(big.Int).Mul(k, new(big.Int).Exp(g, x, N))) // (B - k·g^x)^(a + u·x)
	S.Mod(S, N).Exp(S, new(big.Int).Add(small, new(big.Int).Mul(u, x)), N)
	key := mac(mac(pad(u), pad(S)), []byte("Caldera Derived Key\x01"))[:16] // HKDF, one block
	ts := time.Now().UTC().Format("Mon Jan 2 15:04:05 UTC 2006")
	block, _ := base64.StdEncoding.DecodeString(str(p, "SECRET_BLOCK"))
	sig := mac(key, []byte(short+userID), block, []byte(ts))
	return a.call("RespondToAuthChallenge", M{"ClientId": client, "ChallengeName": "PASSWORD_VERIFIER", "ChallengeResponses": M{
		"USERNAME": userID, "TIMESTAMP": ts, "PASSWORD_CLAIM_SECRET_BLOCK": str(p, "SECRET_BLOCK"),
		"PASSWORD_CLAIM_SIGNATURE": base64.StdEncoding.EncodeToString(sig)}})
}

func TestSRP(t *testing.T) {
	a, _ := setup(t)
	pool := a.pool(M{"UsernameAttributes": []string{"email"}})
	id := str(a.client(M{"UserPoolId": pool, "ClientName": "spa"}), "ClientId") // default flows: SRP, refresh, custom
	sub := str(a.must("SignUp", M{"ClientId": id, "Username": "ann@example.com", "Password": "Passw0rd!"}), "UserSub")
	a.must("AdminConfirmSignUp", M{"UserPoolId": pool, "Username": "ann@example.com"})

	for range 3 { // a fresh a, b and salt-dependent padding each time
		res, err := srpLogin(a, pool, id, "ann@example.com", "Passw0rd!")
		if err != nil {
			t.Fatal(err)
		}
		claims := verifyJWT(t, a.url, pool, str(res, "AuthenticationResult", "IdToken"))
		if claims["cognito:username"] != sub || claims["email"] != "ann@example.com" {
			t.Fatalf("claims = %v", claims)
		}
	}
	var e *apiError
	if _, err := srpLogin(a, pool, id, "ann@example.com", "Wrong-Passw0rd"); !errors.As(err, &e) || e.Code != "NotAuthorizedException" {
		t.Fatalf("wrong password: %v", err)
	}
	a.fails("InitiateAuth", M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": id, "AuthParameters": M{"USERNAME": "ann@example.com", "PASSWORD": "Passw0rd!"}}, "InvalidParameterException")
	a.fails("SignUp", M{"ClientId": id, "Username": "not-an-email", "Password": "Passw0rd!"}, "InvalidParameterException")
	a.fails("SignUp", M{"ClientId": id, "Username": "ann@example.com", "Password": "Passw0rd!"}, "UsernameExistsException")
}

func TestSecretHashAndGroups(t *testing.T) {
	a, _ := setup(t)
	pool := a.pool(M{})
	cl := a.client(M{"UserPoolId": pool, "ClientName": "server", "GenerateSecret": true, "ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"}})
	id, secret := str(cl, "ClientId"), str(cl, "ClientSecret")
	a.must("AdminCreateUser", M{"UserPoolId": pool, "Username": "carl", "TemporaryPassword": "Temp-Passw0rd", "MessageAction": "SUPPRESS"})
	a.must("AdminSetUserPassword", M{"UserPoolId": pool, "Username": "carl", "Password": "Passw0rd!", "Permanent": true})
	role := "arn:aws:iam::000000000000:role/admin"
	a.must("CreateGroup", M{"UserPoolId": pool, "GroupName": "readers", "Precedence": 5})
	a.must("CreateGroup", M{"UserPoolId": pool, "GroupName": "admins", "Precedence": 1, "RoleArn": role})
	a.fails("CreateGroup", M{"UserPoolId": pool, "GroupName": "admins"}, "GroupExistsException")
	for _, g := range []string{"readers", "admins"} {
		a.must("AdminAddUserToGroup", M{"UserPoolId": pool, "Username": "carl", "GroupName": g})
	}

	params := M{"USERNAME": "carl", "PASSWORD": "Passw0rd!"}
	login := M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": id, "AuthParameters": params}
	a.fails("InitiateAuth", login, "NotAuthorizedException")
	params["SECRET_HASH"] = secretHash("wrong", "carl", id)
	a.fails("InitiateAuth", login, "NotAuthorizedException")
	params["SECRET_HASH"] = secretHash(secret, "carl", id)
	res := a.must("InitiateAuth", login)["AuthenticationResult"].(M)
	idc := verifyJWT(t, a.url, pool, str(res, "IdToken"))
	ac := verifyJWT(t, a.url, pool, str(res, "AccessToken"))
	groups, _ := json.Marshal(idc["cognito:groups"])
	agroups, _ := json.Marshal(ac["cognito:groups"])
	if string(groups) != `["admins","readers"]` || string(agroups) != string(groups) || idc["cognito:preferred_role"] != role {
		t.Fatalf("id claims = %v, access claims = %v", idc, ac)
	}
	a.fails("InitiateAuth", M{"AuthFlow": "REFRESH_TOKEN_AUTH", "ClientId": id, "AuthParameters": M{"REFRESH_TOKEN": res["RefreshToken"]}}, "NotAuthorizedException")
	a.must("InitiateAuth", M{"AuthFlow": "REFRESH_TOKEN_AUTH", "ClientId": id, "AuthParameters": M{"REFRESH_TOKEN": res["RefreshToken"], "SECRET_HASH": secretHash(secret, "carl", id)}})

	if users := a.must("ListUsersInGroup", M{"UserPoolId": pool, "GroupName": "readers"})["Users"].([]any); len(users) != 1 || str(users[0], "Username") != "carl" {
		t.Fatalf("users in group = %v", users)
	}
	a.must("AdminRemoveUserFromGroup", M{"UserPoolId": pool, "Username": "carl", "GroupName": "admins"})
	a.must("DeleteGroup", M{"UserPoolId": pool, "GroupName": "readers"})
	if gs := a.must("AdminListGroupsForUser", M{"UserPoolId": pool, "Username": "carl"})["Groups"].([]any); len(gs) != 0 {
		t.Fatalf("groups = %v", gs)
	}
	if gs := a.must("ListGroups", M{"UserPoolId": pool})["Groups"].([]any); len(gs) != 1 || str(gs[0], "GroupName") != "admins" {
		t.Fatalf("pool groups = %v", gs)
	}
}

func TestListUsersFilter(t *testing.T) {
	a, _ := setup(t)
	pool := a.pool(M{})
	for _, name := range []string{"amy", "andy", "bea"} {
		a.must("AdminCreateUser", M{"UserPoolId": pool, "Username": name, "MessageAction": "SUPPRESS",
			"UserAttributes": []M{{"Name": "email", "Value": name + "@x.test"}, {"Name": "given_name", "Value": strings.ToUpper(name)}}})
	}
	a.must("AdminSetUserPassword", M{"UserPoolId": pool, "Username": "bea", "Password": "Passw0rd!", "Permanent": true})
	names := func(in M) []string {
		var out []string
		for _, u := range a.must("ListUsers", in)["Users"].([]any) {
			out = append(out, str(u, "Username"))
		}
		return out
	}
	for filter, want := range map[string]string{
		`email = "amy@x.test"`:              "amy",
		`username ^= "a"`:                   "amy,andy",
		`cognito:user_status = "CONFIRMED"`: "bea",
		`status = "Enabled"`:                "amy,andy,bea",
		`given_name ^= "AN"`:                "andy",
		`email = "nobody@x.test"`:           "",
	} {
		if got := strings.Join(names(M{"UserPoolId": pool, "Filter": filter}), ","); got != want {
			t.Errorf("%s: got %q, want %q", filter, got, want)
		}
	}
	first := a.must("ListUsers", M{"UserPoolId": pool, "Limit": 2, "AttributesToGet": []string{"email"}})
	users := first["Users"].([]any)
	if len(users) != 2 || str(first, "PaginationToken") == "" || len(users[0].(M)["Attributes"].([]any)) != 1 {
		t.Fatalf("first page = %v", first)
	}
	if rest := names(M{"UserPoolId": pool, "Limit": 2, "PaginationToken": first["PaginationToken"]}); len(rest) != 1 || rest[0] != "bea" {
		t.Fatalf("second page = %v", rest)
	}
	a.fails("ListUsers", M{"UserPoolId": pool, "Filter": `email = amy`}, "InvalidParameterException")
	a.fails("ListUsers", M{"UserPoolId": pool, "Filter": `custom:x = "1"`}, "InvalidParameterException")
}

func TestForgotPassword(t *testing.T) {
	a, svc := setup(t)
	pool := a.pool(M{"AutoVerifiedAttributes": []string{"email"}})
	id := str(a.client(M{"UserPoolId": pool, "ClientName": "app", "ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH"}}), "ClientId")
	a.must("SignUp", M{"ClientId": id, "Username": "dee", "Password": "Passw0rd!", "UserAttributes": []M{{"Name": "email", "Value": "dee@example.com"}}})
	a.must("ConfirmSignUp", M{"ClientId": id, "Username": "dee", "ConfirmationCode": lastCode(svc, "SignUp").Code})

	fp := a.must("ForgotPassword", M{"ClientId": id, "Username": "dee"})
	if str(fp, "CodeDeliveryDetails", "Destination") != "d***@e***" {
		t.Fatalf("forgot password = %v", fp)
	}
	code := lastCode(svc, "ForgotPassword").Code
	confirm := M{"ClientId": id, "Username": "dee", "ConfirmationCode": "x" + code, "Password": "An0ther-Passw0rd"}
	a.fails("ConfirmForgotPassword", confirm, "CodeMismatchException")
	confirm["ConfirmationCode"] = code
	a.must("ConfirmForgotPassword", confirm)
	a.fails("ConfirmForgotPassword", confirm, "ExpiredCodeException")
	a.fails("InitiateAuth", M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": id, "AuthParameters": M{"USERNAME": "dee", "PASSWORD": "Passw0rd!"}}, "NotAuthorizedException")
	res := a.must("InitiateAuth", M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": id, "AuthParameters": M{"USERNAME": "dee", "PASSWORD": "An0ther-Passw0rd"}})
	a.must("ChangePassword", M{"AccessToken": str(res, "AuthenticationResult", "AccessToken"), "PreviousPassword": "An0ther-Passw0rd", "ProposedPassword": "Th1rd-Passw0rd"})
	a.must("InitiateAuth", M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": id, "AuthParameters": M{"USERNAME": "dee", "PASSWORD": "Th1rd-Passw0rd"}})
	a.fails("ForgotPassword", M{"ClientId": id, "Username": "nobody"}, "UserNotFoundException")
}

func TestOAuth(t *testing.T) {
	a, _ := setup(t)
	pool := a.pool(M{})
	a.must("CreateResourceServer", M{"UserPoolId": pool, "Identifier": "https://api.example.com", "Name": "api",
		"Scopes": []M{{"ScopeName": "read", "ScopeDescription": "Read things"}}})
	if rs := a.must("ListResourceServers", M{"UserPoolId": pool, "MaxResults": 10})["ResourceServers"].([]any); len(rs) != 1 {
		t.Fatalf("resource servers = %v", rs)
	}
	m2m := a.client(M{"UserPoolId": pool, "ClientName": "m2m", "GenerateSecret": true, "AllowedOAuthFlowsUserPoolClient": true,
		"AllowedOAuthFlows": []string{"client_credentials"}, "AllowedOAuthScopes": []string{"https://api.example.com/read"}})
	token := func(form url.Values, user, pass string) (int, M) {
		req, _ := http.NewRequest(http.MethodPost, a.url+"/oauth2/token", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if user != "" {
			req.SetBasicAuth(user, pass)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		var out M
		json.NewDecoder(res.Body).Decode(&out)
		return res.StatusCode, out
	}
	id, secret := str(m2m, "ClientId"), str(m2m, "ClientSecret")
	status, out := token(url.Values{"grant_type": {"client_credentials"}, "scope": {"https://api.example.com/read"}}, id, secret)
	if status != http.StatusOK || out["token_type"] != "Bearer" || out["expires_in"] != 3600.0 {
		t.Fatalf("client_credentials = %d %v", status, out)
	}
	if c := verifyJWT(t, a.url, pool, str(out, "access_token")); c["scope"] != "https://api.example.com/read" || c["client_id"] != id || c["sub"] != id || c["token_use"] != "access" {
		t.Fatalf("m2m claims = %v", c)
	}
	if status, out = token(url.Values{"grant_type": {"client_credentials"}}, id, "wrong"); status != http.StatusUnauthorized || out["error"] != "invalid_client" {
		t.Fatalf("bad secret = %d %v", status, out)
	}
	if status, out = token(url.Values{"grant_type": {"client_credentials"}, "scope": {"https://api.example.com/write"}}, id, secret); out["error"] != "invalid_scope" {
		t.Fatalf("bad scope = %d %v", status, out)
	}

	// Authorization code with PKCE through the local login page.
	web := str(a.client(M{"UserPoolId": pool, "ClientName": "web", "AllowedOAuthFlowsUserPoolClient": true, "AllowedOAuthFlows": []string{"code"},
		"AllowedOAuthScopes": []string{"openid", "email"}, "CallbackURLs": []string{"http://localhost:3000/cb"}}), "ClientId")
	a.must("AdminCreateUser", M{"UserPoolId": pool, "Username": "eve", "MessageAction": "SUPPRESS", "UserAttributes": []M{{"Name": "email", "Value": "eve@example.com"}}})
	a.must("AdminSetUserPassword", M{"UserPoolId": pool, "Username": "eve", "Password": "Passw0rd!", "Permanent": true})
	verifier := "a-long-and-random-pkce-code-verifier-0123456789"
	challenge := sha256.Sum256([]byte(verifier))
	q := url.Values{"response_type": {"code"}, "client_id": {web}, "redirect_uri": {"http://localhost:3000/cb"}, "scope": {"openid email"},
		"state": {"xyz"}, "code_challenge": {b64.EncodeToString(challenge[:])}, "code_challenge_method": {"S256"}}
	browser := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := browser.Get(a.url + "/oauth2/authorize?" + q.Encode())
	if err != nil || res.StatusCode != http.StatusFound || !strings.HasPrefix(res.Header.Get("Location"), "/login?") {
		t.Fatalf("authorize = %v %v", res, err)
	}
	res, err = browser.PostForm(a.url+"/login?"+q.Encode(), url.Values{"username": {"eve"}, "password": {"wrong"}})
	if page, _ := io.ReadAll(res.Body); err != nil || res.StatusCode != http.StatusOK || !strings.Contains(string(page), "Incorrect username or password.") {
		t.Fatalf("bad login = %v %s", err, page)
	}
	res, err = browser.PostForm(a.url+"/login?"+q.Encode(), url.Values{"username": {"eve"}, "password": {"Passw0rd!"}})
	if err != nil || res.StatusCode != http.StatusFound {
		t.Fatalf("login = %v %v", res, err)
	}
	cb, _ := url.Parse(res.Header.Get("Location"))
	if cb.Host != "localhost:3000" || cb.Query().Get("state") != "xyz" || cb.Query().Get("code") == "" {
		t.Fatalf("callback = %s", cb)
	}
	exchange := url.Values{"grant_type": {"authorization_code"}, "client_id": {web}, "code": {cb.Query().Get("code")},
		"redirect_uri": {"http://localhost:3000/cb"}, "code_verifier": {"wrong"}}
	if status, out = token(exchange, "", ""); out["error"] != "invalid_grant" {
		t.Fatalf("bad verifier = %d %v", status, out)
	}
	res, _ = browser.PostForm(a.url+"/login?"+q.Encode(), url.Values{"username": {"eve"}, "password": {"Passw0rd!"}})
	cb, _ = url.Parse(res.Header.Get("Location"))
	exchange.Set("code", cb.Query().Get("code"))
	exchange.Set("code_verifier", verifier)
	status, out = token(exchange, "", "")
	if status != http.StatusOK || str(out, "refresh_token") == "" {
		t.Fatalf("exchange = %d %v", status, out)
	}
	if c := verifyJWT(t, a.url, pool, str(out, "id_token")); c["aud"] != web || c["email"] != "eve@example.com" {
		t.Fatalf("id claims = %v", c)
	}
	if c := verifyJWT(t, a.url, pool, str(out, "access_token")); c["scope"] != "openid email" {
		t.Fatalf("access claims = %v", c)
	}
	if status, again := token(exchange, "", ""); status != http.StatusBadRequest || again["error"] != "invalid_grant" {
		t.Fatalf("code reuse = %d %v", status, again)
	}
	pre, _ := http.NewRequest(http.MethodOptions, a.url+"/oauth2/userInfo", nil)
	pre.Header.Set("Origin", "http://localhost:3000")
	pre.Header.Set("Access-Control-Request-Headers", "authorization")
	if res, err = http.DefaultClient.Do(pre); err != nil || res.StatusCode != http.StatusNoContent || res.Header.Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Fatalf("preflight = %v %v", res, err)
	}
	req, _ := http.NewRequest(http.MethodGet, a.url+"/oauth2/userInfo", nil)
	req.Header.Set("Authorization", "Bearer "+str(out, "access_token"))
	req.Header.Set("Origin", "http://localhost:3000")
	res, err = http.DefaultClient.Do(req)
	var info M
	if err == nil {
		json.NewDecoder(res.Body).Decode(&info)
	}
	if info["username"] != "eve" || info["email"] != "eve@example.com" || res.Header.Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Fatalf("userInfo = %v %v", info, err)
	}
	if status, again := token(url.Values{"grant_type": {"refresh_token"}, "client_id": {web}, "refresh_token": {str(out, "refresh_token")}}, "", ""); status != http.StatusOK || again["id_token"] == nil {
		t.Fatalf("refresh = %d %v", status, again)
	}
}

func TestMFA(t *testing.T) {
	a, svc := setup(t)
	pool := a.pool(M{"MfaConfiguration": "ON"})
	id := str(a.client(M{"UserPoolId": pool, "ClientName": "app", "ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH"}}), "ClientId")
	a.must("AdminCreateUser", M{"UserPoolId": pool, "Username": "gus", "MessageAction": "SUPPRESS", "UserAttributes": []M{{"Name": "phone_number", "Value": "+15555550100"}}})
	a.must("AdminSetUserPassword", M{"UserPoolId": pool, "Username": "gus", "Password": "Passw0rd!", "Permanent": true})
	login := M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": id, "AuthParameters": M{"USERNAME": "gus", "PASSWORD": "Passw0rd!"}}

	ch := a.must("InitiateAuth", login)
	code := lastCode(svc, "Authentication")
	if ch["ChallengeName"] != "SMS_MFA" || str(ch, "ChallengeParameters", "CODE_DELIVERY_DESTINATION") != "+*******0100" || code.Destination != "+15555550100" {
		t.Fatalf("challenge = %v, code = %+v", ch, code)
	}
	answer := M{"ClientId": id, "ChallengeName": "SMS_MFA", "Session": ch["Session"], "ChallengeResponses": M{"USERNAME": "gus", "SMS_MFA_CODE": "x"}}
	a.fails("RespondToAuthChallenge", answer, "CodeMismatchException")
	answer["ChallengeResponses"].(M)["SMS_MFA_CODE"] = code.Code // the same session may retry
	verifyJWT(t, a.url, pool, str(a.must("RespondToAuthChallenge", answer), "AuthenticationResult", "AccessToken"))

	a.must("AdminSetUserMFAPreference", M{"UserPoolId": pool, "Username": "gus", "SoftwareTokenMfaSettings": M{"Enabled": true, "PreferredMfa": true}})
	if u := a.must("AdminGetUser", M{"UserPoolId": pool, "Username": "gus"}); u["PreferredMfaSetting"] != "SOFTWARE_TOKEN_MFA" {
		t.Fatalf("user = %v", u)
	}
	ch = a.must("InitiateAuth", login)
	if ch["ChallengeName"] != "SOFTWARE_TOKEN_MFA" {
		t.Fatalf("challenge = %v", ch)
	}
	a.must("RespondToAuthChallenge", M{"ClientId": id, "ChallengeName": "SOFTWARE_TOKEN_MFA", "Session": ch["Session"],
		"ChallengeResponses": M{"USERNAME": "gus", "SOFTWARE_TOKEN_MFA_CODE": "123456"}})
}

const triggers = `
def handler(event, context):
    src, req, res = event["triggerSource"], event["request"], event["response"]
    if src == "PreSignUp_SignUp":
        if req["userAttributes"].get("email", "").endswith("@blocked.test"):
            raise Exception("blocked domain")
        res.update(autoConfirmUser=True, autoVerifyEmail=True)
    elif src.startswith("TokenGeneration_") and event["version"] == "2":
        res["claimsAndScopeOverrideDetails"] = {"accessTokenGeneration": {"claimsToAddOrOverride": {"tenant": "acme"}, "scopesToAdd": ["extra"]}}
    elif src.startswith("TokenGeneration_"):
        res["claimsOverrideDetails"] = {"claimsToAddOrOverride": {"tenant": "acme"}, "claimsToSuppress": ["email"]}
    elif src == "DefineAuthChallenge_Authentication":
        s = req["session"]
        if s and s[-1]["challengeResult"]:
            res.update(issueTokens=True, failAuthentication=False)
        elif len(s) >= 3:
            res.update(issueTokens=False, failAuthentication=True)
        else:
            res.update(issueTokens=False, failAuthentication=False, challengeName="CUSTOM_CHALLENGE")
    elif src == "CreateAuthChallenge_Authentication":
        res.update(publicChallengeParameters={"question": "2+2"}, privateChallengeParameters={"answer": "4"}, challengeMetadata="MATH")
    elif src == "VerifyAuthChallengeResponse_Authentication":
        res["answerCorrect"] = req["challengeAnswer"] == req["privateChallengeParameters"]["answer"]
    return event
`

func TestLambdaTriggers(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	var svc *cognito.Service
	_, u := core.ServeTest(t,
		func(h *core.Hub) core.Service { return lambdasvc.New(h) },
		func(h *core.Hub) core.Service { svc = cognito.New(h); return svc })
	a := &api{t, u, http.DefaultClient}
	var zipped bytes.Buffer
	zw := zip.NewWriter(&zipped)
	w, _ := zw.Create("app.py")
	io.WriteString(w, triggers)
	zw.Close()
	fn, err := lambda.NewFromConfig(awstest.Config(t, u)).CreateFunction(context.Background(), &lambda.CreateFunctionInput{
		FunctionName: aws.String("triggers"), Runtime: types.RuntimePython312, Handler: aws.String("app.handler"),
		Role: aws.String("arn:aws:iam::000000000000:role/r"), Code: &types.FunctionCode{ZipFile: zipped.Bytes()}})
	if err != nil {
		t.Fatal(err)
	}
	arn := aws.ToString(fn.FunctionArn)
	pool := a.pool(M{"AutoVerifiedAttributes": []string{"email"}, "LambdaConfig": M{"PreSignUp": arn, "PreTokenGeneration": arn,
		"DefineAuthChallenge": arn, "CreateAuthChallenge": arn, "VerifyAuthChallengeResponse": arn}})
	id := str(a.client(M{"UserPoolId": pool, "ClientName": "app", "ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH", "ALLOW_CUSTOM_AUTH"}}), "ClientId")

	up := a.must("SignUp", M{"ClientId": id, "Username": "fay", "Password": "Passw0rd!", "UserAttributes": []M{{"Name": "email", "Value": "fay@example.com"}}})
	if up["UserConfirmed"] != true || up["CodeDeliveryDetails"] != nil || len(svc.Codes()) != 0 {
		t.Fatalf("PreSignUp should auto-confirm: %v", up)
	}
	if user := a.must("AdminGetUser", M{"UserPoolId": pool, "Username": "fay"}); user["UserStatus"] != "CONFIRMED" || !hasAttr(user, "UserAttributes", "email_verified", "true") {
		t.Fatalf("user = %v", user)
	}
	e := a.fails("SignUp", M{"ClientId": id, "Username": "spam", "Password": "Passw0rd!", "UserAttributes": []M{{"Name": "email", "Value": "x@blocked.test"}}}, "UserLambdaValidationException")
	if e.Message != "PreSignUp failed with error blocked domain." {
		t.Fatalf("message = %q", e.Message)
	}

	res := a.must("InitiateAuth", M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": id, "AuthParameters": M{"USERNAME": "fay", "PASSWORD": "Passw0rd!"}})
	if c := verifyJWT(t, a.url, pool, str(res, "AuthenticationResult", "IdToken")); c["tenant"] != "acme" || c["email"] != nil || c["cognito:username"] != "fay" {
		t.Fatalf("PreTokenGeneration claims = %v", c)
	}

	ch := a.must("InitiateAuth", M{"AuthFlow": "CUSTOM_AUTH", "ClientId": id, "AuthParameters": M{"USERNAME": "fay"}})
	if ch["ChallengeName"] != "CUSTOM_CHALLENGE" || str(ch, "ChallengeParameters", "question") != "2+2" {
		t.Fatalf("custom challenge = %v", ch)
	}
	answer := func(session, ans string) M {
		return a.must("RespondToAuthChallenge", M{"ClientId": id, "ChallengeName": "CUSTOM_CHALLENGE", "Session": session,
			"ChallengeResponses": M{"USERNAME": "fay", "ANSWER": ans}})
	}
	ch = answer(str(ch, "Session"), "5")
	if ch["ChallengeName"] != "CUSTOM_CHALLENGE" {
		t.Fatalf("a wrong answer should ask again: %v", ch)
	}
	done := answer(str(ch, "Session"), "4")
	verifyJWT(t, a.url, pool, str(done, "AuthenticationResult", "AccessToken"))

	// A V2 PreTokenGeneration trigger can change the access token too.
	v2 := a.pool(M{"LambdaConfig": M{"PreSignUp": arn, "PreTokenGenerationConfig": M{"LambdaVersion": "V2_0", "LambdaArn": arn}}})
	id2 := str(a.client(M{"UserPoolId": v2, "ClientName": "app", "ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH"}}), "ClientId")
	a.must("SignUp", M{"ClientId": id2, "Username": "gil", "Password": "Passw0rd!"})
	res = a.must("InitiateAuth", M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": id2, "AuthParameters": M{"USERNAME": "gil", "PASSWORD": "Passw0rd!"}})
	if c := verifyJWT(t, a.url, v2, str(res, "AuthenticationResult", "AccessToken")); c["tenant"] != "acme" || c["scope"] != "aws.cognito.signin.user.admin extra" {
		t.Fatalf("V2 access claims = %v", c)
	}
}

// floci #5393 #5147 #4761 #2530 #2066: an email must look like one, token
// validity has AWS's limits, admin_only blocks ForgotPassword, a user cannot
// set email_verified on themselves (and a refused update changes nothing), and
// PasswordHistorySize stops password reuse.
func TestCognitoInputRules(t *testing.T) {
	a, _ := setup(t)
	flows := []string{"ALLOW_USER_PASSWORD_AUTH"}
	pool := a.pool(M{"AutoVerifiedAttributes": []string{"email"}, "Policies": M{"PasswordPolicy": M{"MinimumLength": 8, "PasswordHistorySize": 2}}})
	id := str(a.client(M{"UserPoolId": pool, "ClientName": "app", "ExplicitAuthFlows": flows}), "ClientId")
	login := func(user, pw string) string {
		return str(a.must("InitiateAuth", M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": id, "AuthParameters": M{"USERNAME": user, "PASSWORD": pw}}), "AuthenticationResult", "AccessToken")
	}

	// #5393
	a.fails("SignUp", M{"ClientId": id, "Username": "u1", "Password": "Passw0rd!", "UserAttributes": []M{{"Name": "email", "Value": "not-an-email"}}}, "InvalidParameterException")
	a.fails("AdminCreateUser", M{"UserPoolId": pool, "Username": "u2", "MessageAction": "SUPPRESS", "UserAttributes": []M{{"Name": "email", "Value": "nope"}}}, "InvalidParameterException")

	// #5147
	for name, in := range map[string]M{
		"access 1 minute":   {"AccessTokenValidity": 1, "TokenValidityUnits": M{"AccessToken": "minutes"}},
		"id 1 minute":       {"IdTokenValidity": 1, "TokenValidityUnits": M{"IdToken": "minutes"}},
		"refresh 20 min":    {"RefreshTokenValidity": 20, "TokenValidityUnits": M{"RefreshToken": "minutes"}},
		"access 2 days":     {"AccessTokenValidity": 2, "TokenValidityUnits": M{"AccessToken": "days"}},
		"refresh 4000 days": {"RefreshTokenValidity": 4000, "TokenValidityUnits": M{"RefreshToken": "days"}},
	} {
		in["UserPoolId"], in["ClientName"] = pool, "x"
		if _, err := a.call("CreateUserPoolClient", in); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	a.must("CreateUserPoolClient", M{"UserPoolId": pool, "ClientName": "ok", "AccessTokenValidity": 5, "TokenValidityUnits": M{"AccessToken": "minutes"}, "RefreshTokenValidity": 60, "IdTokenValidity": 1})

	// #2530: the user's own token cannot set email_verified, and nothing else in that request is kept
	a.must("AdminCreateUser", M{"UserPoolId": pool, "Username": "u3", "MessageAction": "SUPPRESS", "UserAttributes": []M{{"Name": "email", "Value": "u3@example.com"}}})
	a.must("AdminSetUserPassword", M{"UserPoolId": pool, "Username": "u3", "Password": "Passw0rd!", "Permanent": true})
	tok := login("u3", "Passw0rd!")
	a.fails("UpdateUserAttributes", M{"AccessToken": tok, "UserAttributes": []M{{"Name": "email_verified", "Value": "true"}, {"Name": "name", "Value": "Sneaky"}}}, "InvalidParameterException")
	if u := a.must("AdminGetUser", M{"UserPoolId": pool, "Username": "u3"}); hasAttr(u, "UserAttributes", "name", "Sneaky") || hasAttr(u, "UserAttributes", "email_verified", "true") {
		t.Errorf("a refused update changed the user: %v", u)
	}

	// #2066: history of 2 = the current password and the one before it
	a.fails("ChangePassword", M{"AccessToken": tok, "PreviousPassword": "Passw0rd!", "ProposedPassword": "Passw0rd!"}, "InvalidPasswordException")
	a.must("ChangePassword", M{"AccessToken": tok, "PreviousPassword": "Passw0rd!", "ProposedPassword": "Second-Passw0rd"})
	a.fails("ChangePassword", M{"AccessToken": tok, "PreviousPassword": "Second-Passw0rd", "ProposedPassword": "Passw0rd!"}, "InvalidPasswordException")
	a.must("ChangePassword", M{"AccessToken": tok, "PreviousPassword": "Second-Passw0rd", "ProposedPassword": "Third-Passw0rd"})
	a.must("ChangePassword", M{"AccessToken": tok, "PreviousPassword": "Third-Passw0rd", "ProposedPassword": "Passw0rd!"}) // two changes ago: allowed again

	// #4761
	recover := a.pool(M{"AccountRecoverySetting": M{"RecoveryMechanisms": []M{{"Name": "admin_only", "Priority": 1}}}})
	rc := str(a.client(M{"UserPoolId": recover, "ClientName": "app", "ExplicitAuthFlows": flows}), "ClientId")
	a.must("AdminCreateUser", M{"UserPoolId": recover, "Username": "u4", "MessageAction": "SUPPRESS",
		"UserAttributes": []M{{"Name": "email", "Value": "u4@example.com"}, {"Name": "email_verified", "Value": "true"}}})
	if _, err := a.call("ForgotPassword", M{"ClientId": rc, "Username": "u4"}); err == nil {
		t.Error("ForgotPassword worked under admin_only")
	}
}

// permanent makes a confirmed user with a password.
func permanent(a *api, pool, name string, attrs ...M) {
	a.t.Helper()
	a.must("AdminCreateUser", M{"UserPoolId": pool, "Username": name, "MessageAction": "SUPPRESS", "UserAttributes": attrs})
	a.must("AdminSetUserPassword", M{"UserPoolId": pool, "Username": name, "Password": "Passw0rd!", "Permanent": true})
}

func passwordLogin(a *api, client, name string) M {
	a.t.Helper()
	return a.must("InitiateAuth", M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": client, "AuthParameters": M{"USERNAME": name, "PASSWORD": "Passw0rd!"}})
}

// floci #234: GetTokensFromRefreshToken, with refresh token rotation.
func TestRefreshTokenRotation(t *testing.T) {
	a, _ := setup(t)
	pool := a.pool(M{})
	flows := []string{"ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"}
	plain := str(a.client(M{"UserPoolId": pool, "ClientName": "plain", "ExplicitAuthFlows": flows}), "ClientId")
	rot := str(a.client(M{"UserPoolId": pool, "ClientName": "rot", "ExplicitAuthFlows": flows,
		"RefreshTokenRotation": M{"Feature": "ENABLED", "RetryGracePeriodSeconds": 0}}), "ClientId")
	graced := str(a.client(M{"UserPoolId": pool, "ClientName": "graced", "ExplicitAuthFlows": flows,
		"RefreshTokenRotation": M{"Feature": "ENABLED", "RetryGracePeriodSeconds": 60}}), "ClientId")
	permanent(a, pool, "u1")

	// no rotation: the same refresh token keeps working and no new one comes back
	rt := str(passwordLogin(a, plain, "u1"), "AuthenticationResult", "RefreshToken")
	for range 2 {
		out := a.must("GetTokensFromRefreshToken", M{"RefreshToken": rt, "ClientId": plain})
		if str(out, "AuthenticationResult", "AccessToken") == "" || str(out, "AuthenticationResult", "IdToken") == "" || str(out, "AuthenticationResult", "RefreshToken") != "" {
			t.Fatalf("without rotation: %v", out)
		}
	}

	// rotation: a new refresh token each time, the old one is dead at once
	first := passwordLogin(a, rot, "u1")
	rt = str(first, "AuthenticationResult", "RefreshToken")
	out := a.must("GetTokensFromRefreshToken", M{"RefreshToken": rt, "ClientId": rot})
	next := str(out, "AuthenticationResult", "RefreshToken")
	if next == "" || next == rt || str(out, "AuthenticationResult", "AccessToken") == "" {
		t.Fatalf("rotation: %v", out)
	}
	a.fails("GetTokensFromRefreshToken", M{"RefreshToken": rt, "ClientId": rot}, "NotAuthorizedException")
	a.must("GetUser", M{"AccessToken": str(first, "AuthenticationResult", "AccessToken")}) // earlier tokens live on
	a.must("GetUser", M{"AccessToken": str(out, "AuthenticationResult", "AccessToken")})
	if again := a.must("GetTokensFromRefreshToken", M{"RefreshToken": next, "ClientId": rot}); str(again, "AuthenticationResult", "RefreshToken") == "" {
		t.Fatalf("second rotation: %v", again)
	}

	// grace period: the old token still works for a retry
	rt = str(passwordLogin(a, graced, "u1"), "AuthenticationResult", "RefreshToken")
	a.must("GetTokensFromRefreshToken", M{"RefreshToken": rt, "ClientId": graced})
	a.must("GetTokensFromRefreshToken", M{"RefreshToken": rt, "ClientId": graced})

	a.fails("GetTokensFromRefreshToken", M{"RefreshToken": "nope", "ClientId": plain}, "NotAuthorizedException")
	a.fails("CreateUserPoolClient", M{"UserPoolId": pool, "ClientName": "x", "RefreshTokenRotation": M{"Feature": "MAYBE"}}, "InvalidParameterException")
	a.fails("CreateUserPoolClient", M{"UserPoolId": pool, "ClientName": "x", "RefreshTokenRotation": M{"Feature": "ENABLED", "RetryGracePeriodSeconds": 61}}, "InvalidParameterException")
	// the setting is returned as it was sent
	if got := a.must("DescribeUserPoolClient", M{"UserPoolId": pool, "ClientId": rot})["UserPoolClient"].(M)["RefreshTokenRotation"]; str(got, "Feature") != "ENABLED" {
		t.Errorf("RefreshTokenRotation = %v", got)
	}
}

// floci #4449, #4003: choice-based sign-in.
func TestUserAuth(t *testing.T) {
	a, svc := setup(t)
	pool := a.pool(M{"Policies": M{"SignInPolicy": M{"AllowedFirstAuthFactors": []string{"PASSWORD", "EMAIL_OTP"}}}})
	flows := []string{"ALLOW_USER_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"}
	id := str(a.client(M{"UserPoolId": pool, "ClientName": "app", "ExplicitAuthFlows": flows, "PreventUserExistenceErrors": "ENABLED"}), "ClientId")
	permanent(a, pool, "u1", M{"Name": "email", "Value": "u1@example.com"}, M{"Name": "email_verified", "Value": "true"})
	start := func(name string, extra M) M {
		p := M{"USERNAME": name}
		for k, v := range extra {
			p[k] = v
		}
		return a.must("InitiateAuth", M{"AuthFlow": "USER_AUTH", "ClientId": id, "AuthParameters": p})
	}
	answer := func(ch M, name string, r M) (M, error) {
		return a.call("RespondToAuthChallenge", M{"ClientId": id, "ChallengeName": name, "Session": ch["Session"], "ChallengeResponses": r})
	}

	ch := start("u1", nil)
	if ch["ChallengeName"] != "SELECT_CHALLENGE" || fmt.Sprint(ch["AvailableChallenges"]) != "[PASSWORD PASSWORD_SRP EMAIL_OTP]" {
		t.Fatalf("first step: %v", ch)
	}
	if _, err := answer(ch, "SELECT_CHALLENGE", M{"USERNAME": "u1", "ANSWER": "WEB_AUTHN"}); err == nil || !strings.Contains(err.Error(), "InvalidParameterException") {
		t.Fatalf("unavailable answer: %v", err)
	}
	ch = start("u1", nil)
	pw, err := answer(ch, "SELECT_CHALLENGE", M{"USERNAME": "u1", "ANSWER": "PASSWORD"})
	if err != nil || pw["ChallengeName"] != "PASSWORD" || pw["AvailableChallenges"] != nil {
		t.Fatalf("PASSWORD chosen: %v %v", pw, err)
	}
	if _, err := answer(pw, "PASSWORD", M{"USERNAME": "u1", "PASSWORD": "wrong"}); err == nil {
		t.Fatal("wrong password was accepted")
	}
	pw, _ = answer(start("u1", nil), "SELECT_CHALLENGE", M{"USERNAME": "u1", "ANSWER": "PASSWORD"})
	done, err := answer(pw, "PASSWORD", M{"USERNAME": "u1", "PASSWORD": "Passw0rd!"})
	if err != nil || str(done, "AuthenticationResult", "AccessToken") == "" {
		t.Fatalf("PASSWORD answered: %v %v", done, err)
	}

	// the preferred challenge skips the menu; the code goes to the outbox
	otp := start("u1", M{"PREFERRED_CHALLENGE": "EMAIL_OTP"})
	if otp["ChallengeName"] != "EMAIL_OTP" || str(otp, "ChallengeParameters", "CODE_DELIVERY_DELIVERY_MEDIUM") != "EMAIL" {
		t.Fatalf("EMAIL_OTP: %v", otp)
	}
	if _, err := answer(otp, "EMAIL_OTP", M{"USERNAME": "u1", "EMAIL_OTP_CODE": "000000"}); err == nil || !strings.Contains(err.Error(), "CodeMismatchException") {
		t.Fatalf("wrong code: %v", err)
	}
	done, err = answer(otp, "EMAIL_OTP", M{"USERNAME": "u1", "EMAIL_OTP_CODE": lastCode(svc, "Authentication").Code})
	if err != nil || str(done, "AuthenticationResult", "AccessToken") == "" {
		t.Fatalf("EMAIL_OTP answered: %v %v", done, err)
	}

	// an unknown user gets a menu too, and fails at the password
	ghost := start("nobody", nil)
	if ghost["ChallengeName"] != "SELECT_CHALLENGE" {
		t.Fatalf("unknown user: %v", ghost)
	}
	g, err := answer(ghost, "SELECT_CHALLENGE", M{"USERNAME": "nobody", "ANSWER": "PASSWORD"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := answer(g, "PASSWORD", M{"USERNAME": "nobody", "PASSWORD": "Passw0rd!"}); err == nil || !strings.Contains(err.Error(), "NotAuthorizedException") {
		t.Fatalf("unknown user's password: %v", err)
	}

	// a client without ALLOW_USER_AUTH refuses the flow
	other := str(a.client(M{"UserPoolId": pool, "ClientName": "no", "ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH"}}), "ClientId")
	a.fails("InitiateAuth", M{"AuthFlow": "USER_AUTH", "ClientId": other, "AuthParameters": M{"USERNAME": "u1"}}, "InvalidParameterException")
}

// totpNow is a TOTP code as an authenticator app computes it (RFC 6238).
func totpNow(secret string) string {
	key, _ := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(secret)
	m := hmac.New(sha1.New, key)
	binary.Write(m, binary.BigEndian, time.Now().Unix()/30)
	sum := m.Sum(nil)
	o := sum[len(sum)-1] & 0xf
	return fmt.Sprintf("%06d", (binary.BigEndian.Uint32(sum[o:o+4])&0x7fffffff)%1000000)
}

// floci #4573: software token MFA with MFA_SETUP and a real TOTP check.
func TestSoftwareTokenMFA(t *testing.T) {
	a, _ := setup(t)
	pool := a.pool(M{})
	a.must("SetUserPoolMfaConfig", M{"UserPoolId": pool, "MfaConfiguration": "ON", "SoftwareTokenMfaConfiguration": M{"Enabled": true}})
	id := str(a.client(M{"UserPoolId": pool, "ClientName": "app", "ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH"}}), "ClientId")
	permanent(a, pool, "u1")

	ch := passwordLogin(a, id, "u1")
	if ch["ChallengeName"] != "MFA_SETUP" || !strings.Contains(str(ch, "ChallengeParameters", "MFAS_CAN_SETUP"), "SOFTWARE_TOKEN_MFA") {
		t.Fatalf("first sign-in: %v", ch)
	}
	respond := func(session any) (M, error) {
		return a.call("RespondToAuthChallenge", M{"ClientId": id, "ChallengeName": "MFA_SETUP", "Session": session, "ChallengeResponses": M{"USERNAME": "u1"}})
	}
	if _, err := respond(ch["Session"]); err == nil || !strings.Contains(err.Error(), "InvalidParameterException") {
		t.Fatalf("MFA_SETUP before a token was verified: %v", err)
	}
	ch = passwordLogin(a, id, "u1")
	assoc := a.must("AssociateSoftwareToken", M{"Session": ch["Session"]})
	secret := str(assoc, "SecretCode")
	if len(secret) != 32 || assoc["Session"] == nil || assoc["Session"] == ch["Session"] {
		t.Fatalf("AssociateSoftwareToken: %v", assoc)
	}
	a.fails("VerifySoftwareToken", M{"Session": assoc["Session"], "UserCode": "000000"}, "EnableSoftwareTokenMFAException")
	ver := a.must("VerifySoftwareToken", M{"Session": assoc["Session"], "UserCode": totpNow(secret)})
	if ver["Status"] != "SUCCESS" || ver["Session"] == nil {
		t.Fatalf("VerifySoftwareToken: %v", ver)
	}
	done, err := respond(ver["Session"])
	if err != nil || str(done, "AuthenticationResult", "AccessToken") == "" {
		t.Fatalf("MFA_SETUP answered: %v %v", done, err)
	}

	// the next sign-in asks for a code from the authenticator, and only a real one passes
	ch = passwordLogin(a, id, "u1")
	if ch["ChallengeName"] != "SOFTWARE_TOKEN_MFA" {
		t.Fatalf("second sign-in: %v", ch)
	}
	code := func(c string) (M, error) {
		return a.call("RespondToAuthChallenge", M{"ClientId": id, "ChallengeName": "SOFTWARE_TOKEN_MFA", "Session": ch["Session"],
			"ChallengeResponses": M{"USERNAME": "u1", "SOFTWARE_TOKEN_MFA_CODE": c}})
	}
	if _, err := code("123456"); err == nil || !strings.Contains(err.Error(), "CodeMismatchException") {
		t.Fatalf("123456 for a user with a token: %v", err)
	}
	done, err = code(totpNow(secret))
	if err != nil || str(done, "AuthenticationResult", "AccessToken") == "" {
		t.Fatalf("real code: %v %v", done, err)
	}

	// a signed-in user can replace the token with an access token
	token := str(done, "AuthenticationResult", "AccessToken")
	n := a.must("AssociateSoftwareToken", M{"AccessToken": token})
	a.fails("VerifySoftwareToken", M{"AccessToken": token, "UserCode": "000000"}, "EnableSoftwareTokenMFAException")
	if v := a.must("VerifySoftwareToken", M{"AccessToken": token, "UserCode": totpNow(str(n, "SecretCode"))}); v["Status"] != "SUCCESS" {
		t.Fatalf("VerifySoftwareToken with an access token: %v", v)
	}
	a.fails("AssociateSoftwareToken", M{"Session": "bogus"}, "NotAuthorizedException")
}

// floci #5413: identity providers, and the managed login page that offers them.
func TestIdentityProviders(t *testing.T) {
	a, _ := setup(t)
	pool := a.pool(M{})
	google := M{"UserPoolId": pool, "ProviderName": "Google", "ProviderType": "Google", "IdpIdentifiers": []string{"g"},
		"ProviderDetails": M{"client_id": "x", "client_secret": "y", "authorize_scopes": "openid email"}, "AttributeMapping": M{"email": "email"}}
	out := a.must("CreateIdentityProvider", google)["IdentityProvider"].(M)
	if out["ProviderName"] != "Google" || str(out, "ProviderDetails", "client_id") != "x" || out["CreationDate"] == nil || str(out, "AttributeMapping", "email") != "email" {
		t.Fatalf("created: %v", out)
	}
	a.fails("CreateIdentityProvider", google, "DuplicateProviderException")
	a.fails("CreateIdentityProvider", M{"UserPoolId": pool, "ProviderName": "Other", "ProviderType": "Google", "IdpIdentifiers": []string{"g"},
		"ProviderDetails": M{"client_id": "x", "client_secret": "y", "authorize_scopes": "openid"}}, "DuplicateProviderException")
	a.fails("CreateIdentityProvider", M{"UserPoolId": pool, "ProviderName": "Bad", "ProviderType": "Google", "ProviderDetails": M{"client_id": "x"}}, "InvalidParameterException")
	a.fails("CreateIdentityProvider", M{"UserPoolId": pool, "ProviderName": "Bad", "ProviderType": "Nope", "ProviderDetails": M{}}, "InvalidParameterException")
	a.fails("CreateIdentityProvider", M{"UserPoolId": pool, "ProviderName": "COGNITO", "ProviderType": "Google", "ProviderDetails": google["ProviderDetails"]}, "InvalidParameterException")
	a.must("CreateIdentityProvider", M{"UserPoolId": pool, "ProviderName": "Corp", "ProviderType": "SAML", "ProviderDetails": M{"MetadataURL": "https://idp.example.com/metadata"}})

	list := a.must("ListIdentityProviders", M{"UserPoolId": pool})["Providers"].([]any)
	if len(list) != 2 || str(list[0], "ProviderName") != "Corp" || str(list[1], "ProviderType") != "Google" {
		t.Fatalf("list: %v", list)
	}
	if got := a.must("GetIdentityProviderByIdentifier", M{"UserPoolId": pool, "IdpIdentifier": "g"})["IdentityProvider"]; str(got, "ProviderName") != "Google" {
		t.Fatalf("by identifier: %v", got)
	}
	up := a.must("UpdateIdentityProvider", M{"UserPoolId": pool, "ProviderName": "Google",
		"ProviderDetails": M{"client_id": "x2", "client_secret": "y", "authorize_scopes": "openid"}})["IdentityProvider"]
	if str(up, "ProviderDetails", "client_id") != "x2" || str(a.must("DescribeIdentityProvider", M{"UserPoolId": pool, "ProviderName": "Google"})["IdentityProvider"], "ProviderDetails", "client_id") != "x2" {
		t.Fatalf("update: %v", up)
	}

	// an app client can name providers of its pool, not others
	a.fails("CreateUserPoolClient", M{"UserPoolId": pool, "ClientName": "x", "SupportedIdentityProviders": []string{"Facebook"}}, "InvalidParameterException")
	cb := "http://localhost:3000/cb"
	id := str(a.client(M{"UserPoolId": pool, "ClientName": "web", "SupportedIdentityProviders": []string{"COGNITO", "Google"}, "AllowedOAuthFlowsUserPoolClient": true,
		"AllowedOAuthFlows": []string{"code"}, "AllowedOAuthScopes": []string{"openid"}, "CallbackURLs": []string{cb}}), "ClientId")
	q := url.Values{"client_id": {id}, "response_type": {"code"}, "redirect_uri": {cb}}
	res, err := http.Get(a.url + "/login?" + q.Encode())
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if !strings.Contains(string(page), "Continue with Google") || !strings.Contains(string(page), `name="password"`) || strings.Contains(string(page), "Continue with Corp") {
		t.Fatalf("login page:\n%s", page)
	}
	q.Set("identity_provider", "Google")
	if res, _ = http.Get(a.url + "/oauth2/authorize?" + q.Encode()); res.StatusCode != http.StatusNotImplemented {
		t.Errorf("authorize through Google = %d, want 501", res.StatusCode)
	}
	q.Set("identity_provider", "Corp")
	if res, _ = http.Get(a.url + "/oauth2/authorize?" + q.Encode()); res.StatusCode != http.StatusBadRequest {
		t.Errorf("authorize through a provider the client does not offer = %d, want 400", res.StatusCode)
	}

	a.must("DeleteIdentityProvider", M{"UserPoolId": pool, "ProviderName": "Google"})
	a.fails("DescribeIdentityProvider", M{"UserPoolId": pool, "ProviderName": "Google"}, "ResourceNotFoundException")
	if sp := a.must("DescribeUserPoolClient", M{"UserPoolId": pool, "ClientId": id})["UserPoolClient"].(M)["SupportedIdentityProviders"].([]any); len(sp) != 1 || sp[0] != "COGNITO" {
		t.Errorf("client providers after delete = %v", sp)
	}
}

// floci #3997, #1570, #4770, #5143: client default flows, sub as an admin
// Username, a refused scope at authorize, and the sender of a captured email.
func TestFlociLowFixes(t *testing.T) {
	a, svc := setup(t)
	pool := a.pool(M{"AutoVerifiedAttributes": []string{"email"},
		"EmailConfiguration": M{"EmailSendingAccount": "DEVELOPER", "From": "Acme <noreply@acme.test>"}})
	cl := a.client(M{"UserPoolId": pool, "ClientName": "x", "AllowedOAuthFlowsUserPoolClient": true, "AllowedOAuthFlows": []string{"code"},
		"AllowedOAuthScopes": []string{"openid"}, "CallbackURLs": []string{"http://localhost:3000/cb"}})
	if f, _ := cl["ExplicitAuthFlows"].([]any); len(f) != 3 {
		t.Errorf("ExplicitAuthFlows = %v, want the three defaults", f)
	}
	id := str(cl, "ClientId")
	a.must("SignUp", M{"ClientId": id, "Username": "u1", "Password": "Passw0rd!", "UserAttributes": []M{{"Name": "email", "Value": "u1@example.com"}}})
	if from := lastCode(svc, "SignUp").From; from != "Acme <noreply@acme.test>" {
		t.Errorf("sender = %q", from)
	}
	var sub string
	for _, at := range a.must("AdminGetUser", M{"UserPoolId": pool, "Username": "u1"})["UserAttributes"].([]any) {
		if at.(M)["Name"] == "sub" {
			sub = at.(M)["Value"].(string)
		}
	}
	if _, err := a.call("AdminGetUser", M{"UserPoolId": pool, "Username": sub}); err != nil {
		t.Errorf("AdminGetUser by sub: %v", err)
	}
	browser := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	q := url.Values{"response_type": {"code"}, "client_id": {id}, "redirect_uri": {"http://localhost:3000/cb"}, "scope": {"openid phone"}, "state": {"s1"}}
	res, err := browser.Get(a.url + "/oauth2/authorize?" + q.Encode())
	if err != nil || !strings.Contains(res.Header.Get("Location"), "error=invalid_scope") || !strings.Contains(res.Header.Get("Location"), "state=s1") {
		t.Errorf("authorize with a refused scope: %v %v", res, err)
	}
}
