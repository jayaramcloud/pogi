package cognito

import (
	"cmp"
	"crypto"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/nrndev/pogi/internal/core"
)

var b64 = base64.RawURLEncoding

// ---- passwords ----

// setPassword keeps a PBKDF2 hash and the SRP verifier; never the password.
func (p *Pool) setPassword(u *User, pw string) {
	if n := p.Settings.Policies.PasswordPolicy.PasswordHistorySize; n > 0 && u.Hash != "" { // remember the old password
		u.History = append(u.History, pwHash{u.Salt, u.Hash})
		u.History = u.History[max(len(u.History)-(n-1), 0):]
	}
	salt := make([]byte, 16)
	rand.Read(salt)
	if salt[0] == 0 {
		salt[0] = 1 // SRP clients disagree on a SALT with a leading zero byte
	}
	u.Salt = hex.EncodeToString(salt)
	u.Hash = hex.EncodeToString(hashPassword(pw, salt))
	u.Verifier = srpVerifier(p.ID, u.Username, pw, salt).Text(16)
	u.Modified = time.Now()
}

// ponytail: 4096 PBKDF2 rounds keep sign-in fast; an emulator is not a vault.
func hashPassword(pw string, salt []byte) []byte {
	k, _ := pbkdf2.Key(sha256.New, pw, salt, 4096, 32)
	return k
}

func (u *User) passwordIs(pw string) bool { return pwHash{u.Salt, u.Hash}.is(pw) }

func (h pwHash) is(pw string) bool {
	salt, _ := hex.DecodeString(h.Salt)
	want, _ := hex.DecodeString(h.Hash)
	return len(want) > 0 && hmac.Equal(hashPassword(pw, salt), want)
}

// checkNewPassword is checkPolicy plus the password history: the new password
// may not match any of the PasswordHistorySize most recent passwords, the
// current one included.
func (p *Pool) checkNewPassword(u *User, pw string) error {
	if err := p.checkPolicy(pw); err != nil {
		return err
	}
	if p.Settings.Policies.PasswordPolicy.PasswordHistorySize > 0 && (u.passwordIs(pw) || slices.ContainsFunc(u.History, func(h pwHash) bool { return h.is(pw) })) {
		return core.Errorf(400, "InvalidPasswordException", "Password does not conform to policy: Password cannot be reused")
	}
	return nil
}

func (p *Pool) checkPolicy(pw string) error {
	pp := p.Settings.Policies.PasswordPolicy
	symbol := func(r rune) bool { return unicode.IsPunct(r) || unicode.IsSymbol(r) || r == ' ' }
	msg := ""
	switch {
	case len([]rune(pw)) < pp.MinimumLength:
		msg = "Password not long enough"
	case pp.RequireUppercase && !strings.ContainsFunc(pw, unicode.IsUpper):
		msg = "Password must have uppercase characters"
	case pp.RequireLowercase && !strings.ContainsFunc(pw, unicode.IsLower):
		msg = "Password must have lowercase characters"
	case pp.RequireNumbers && !strings.ContainsFunc(pw, unicode.IsDigit):
		msg = "Password must have numeric characters"
	case pp.RequireSymbols && !strings.ContainsFunc(pw, symbol):
		msg = "Password must have symbol characters"
	default:
		return nil
	}
	return core.Errorf(400, "InvalidPasswordException", "Password did not conform with policy: %s", msg)
}

// ---- codes and the outbox ----

// send is an outbox message prepared under the lock and delivered after it.
type send struct {
	code Code
	attr string
	msg  *trig // CustomMessage
}

// code issues a 6-digit code for purpose and prepares its delivery to attr.
func (p *Pool) code(u *User, purpose, attr, source, client string, meta map[string]string) send {
	code := core.RandString(6, digits)
	ttl := 24 * time.Hour
	if purpose == "forgot" {
		ttl = time.Hour
	}
	if u.Codes == nil {
		u.Codes = map[string]*Pending{}
	}
	u.Codes[purpose] = &Pending{Code: code, Attr: attr, Exp: time.Now().Add(ttl)}
	return p.sendTo(u, attr, code, source, client, meta)
}

func (p *Pool) sendTo(u *User, attr, code, source, client string, meta map[string]string) send {
	req := map[string]any{"userAttributes": u.eventAttrs(), "codeParameter": "{####}", "linkParameter": "{##Click Here##}", "clientMetadata": meta}
	if source == "AdminCreateUser" {
		req["usernameParameter"] = "{username}"
	}
	return send{attr: attr, msg: p.trig("CustomMessage_"+source, u.Username, client, req),
		code: Code{Pool: p.ID, Username: u.Username, Destination: u.Attrs[attr], From: p.sender(attr), Code: code, Type: source, Timestamp: time.Now()}}
}

// sender is who an email to attr comes from: the pool's EmailConfiguration From,
// else Cognito's default address. SMS has none.
func (p *Pool) sender(attr string) string {
	if attr == "phone_number" {
		return ""
	}
	ec, _ := p.Settings.EmailConfiguration.(map[string]any)
	from, _ := ec["From"].(string)
	return cmp.Or(from, "no-reply@verificationemail.com")
}

// deliver runs the CustomMessage triggers and fills the outbox; call it without the lock.
func (s *Service) deliver(sends ...send) error {
	for _, x := range sends {
		var m struct{ SmsMessage, EmailMessage string }
		if err := s.run(x.msg, &m); err != nil {
			return err
		}
		c := x.code
		if msg := map[bool]string{true: m.SmsMessage, false: m.EmailMessage}[x.attr == "phone_number"]; msg != "" {
			c.Message = strings.NewReplacer("{####}", c.Code, "{username}", c.Username).Replace(msg)
		}
		s.mu.Lock()
		s.st.Codes = append(s.st.Codes, c)
		if n := len(s.st.Codes) - outboxMax; n > 0 {
			s.st.Codes = s.st.Codes[n:]
		}
		s.mu.Unlock()
	}
	return nil
}

func (x send) details() map[string]string {
	return map[string]string{"Destination": mask(x.code.Destination), "AttributeName": x.attr,
		"DeliveryMedium": map[bool]string{true: "SMS", false: "EMAIL"}[x.attr == "phone_number"]}
}

func mask(v string) string {
	if i := strings.IndexByte(v, '@'); i > 0 && i+1 < len(v) {
		return v[:1] + "***@" + v[i+1:i+2] + "***"
	}
	if len(v) > 4 {
		return "+*******" + v[len(v)-4:]
	}
	return v
}

// check validates a pending code without using it up.
func (u *User) check(purpose, code string) error {
	pc := u.Codes[purpose]
	switch {
	case pc == nil:
		return core.Errorf(400, "ExpiredCodeException", "Invalid code provided, please request a code again.")
	case pc.Tries >= 5:
		delete(u.Codes, purpose)
		return core.Errorf(400, "LimitExceededException", "Attempt limit exceeded, please try after some time.")
	case pc.Code != code:
		pc.Tries++
		return core.Errorf(400, "CodeMismatchException", "Invalid verification code provided, please try again.")
	case time.Now().After(pc.Exp):
		delete(u.Codes, purpose)
		return core.Errorf(400, "ExpiredCodeException", "Invalid code provided, please request a code again.")
	}
	return nil
}

// verifyTarget is where a sign-up code goes: an auto-verified phone number, else email.
func (p *Pool) verifyTarget(u *User) string {
	for _, a := range []string{"phone_number", "email"} {
		if u.Attrs[a] != "" && slices.Contains(p.Settings.AutoVerifiedAttributes, a) {
			return a
		}
	}
	return ""
}

// verify marks attr verified; an alias held by another user moves only when forced.
func (p *Pool) verify(u *User, attr string, force bool) error {
	if slices.Contains(p.Settings.AliasAttributes, attr) {
		if o := p.owner(attr, u.Attrs[attr], u, true); o != nil {
			if !force {
				return core.Errorf(400, "AliasExistsException", "An account with the given %s already exists.", attr)
			}
			o.Attrs[attr+"_verified"] = "false"
		}
	}
	u.Attrs[attr+"_verified"] = "true"
	u.Modified = time.Now()
	return nil
}

// recoveryAttrs lists where ForgotPassword may send a code, in the order of
// AccountRecoverySetting. admin_only gives an empty list. Without a setting:
// email, then phone number.
func (p *Pool) recoveryAttrs() []string {
	set, _ := p.Settings.AccountRecoverySetting.(map[string]any)
	list, _ := set["RecoveryMechanisms"].([]any)
	if len(list) == 0 {
		return []string{"email", "phone_number"}
	}
	prio := func(m any) float64 { n, _ := m.(map[string]any)["Priority"].(float64); return n }
	slices.SortStableFunc(list, func(a, b any) int { return cmp.Compare(prio(a), prio(b)) })
	var out []string
	for _, m := range list {
		name, _ := m.(map[string]any)["Name"].(string)
		switch name {
		case "verified_email":
			out = append(out, "email")
		case "verified_phone_number":
			out = append(out, "phone_number")
		}
	}
	return out
}

// recovery sends a password reset code to a verified email or phone number
// that the pool's account recovery setting allows.
func (p *Pool) recovery(u *User, client string, meta map[string]string) (send, error) {
	for _, a := range p.recoveryAttrs() {
		if u.Attrs[a] != "" && u.Attrs[a+"_verified"] == "true" {
			return p.code(u, "forgot", a, "ForgotPassword", client, meta), nil
		}
	}
	return send{}, invalid("Cannot reset password for the user as there is no registered/verified email or phone_number")
}

func (u *User) eventAttrs() map[string]string {
	m := maps.Clone(u.Attrs)
	m["cognito:user_status"] = u.Status
	return m
}

// ---- Lambda triggers ----

// trig is a Lambda trigger invocation prepared under the lock and run without it.
type trig struct {
	arn, name string
	ev        map[string]any
}

// trig prepares the pool's trigger for source ("PreSignUp_SignUp"); nil when the pool has none.
func (p *Pool) trig(source, user, client string, req map[string]any) *trig {
	name, _, _ := strings.Cut(source, "_")
	if name == "TokenGeneration" {
		name = "PreTokenGeneration"
	}
	arn, _ := p.Settings.LambdaConfig[name].(string)
	version := "1"
	if cfg, ok := p.Settings.LambdaConfig[name+"Config"].(map[string]any); ok { // PreTokenGenerationConfig
		if a, _ := cfg["LambdaArn"].(string); a != "" {
			arn = a
		}
		if v, _ := cfg["LambdaVersion"].(string); v != "" {
			version = strings.TrimSuffix(strings.TrimPrefix(v, "V"), "_0")
		}
	}
	if arn == "" {
		return nil
	}
	return &trig{arn: arn, name: name, ev: map[string]any{"version": version, "triggerSource": source, "region": p.Region,
		"userPoolId": p.ID, "userName": user, "request": req, "response": map[string]any{},
		"callerContext": map[string]any{"awsSdkVersion": "aws-sdk-unknown-unknown", "clientId": cmp.Or(client, "CLIENT_ID_NOT_APPLICABLE")}}}
}

// run invokes a trigger and decodes its response into out. A function error
// fails the operation with UserLambdaValidationException, as in Cognito.
func (s *Service) run(t *trig, out any) error {
	if t == nil {
		return nil
	}
	payload, err := json.Marshal(t.ev)
	if err != nil {
		return err
	}
	res, err := s.hub.Invoke(t.arn, payload)
	var fe *core.FunctionError
	switch {
	case errors.As(err, &fe):
		var e struct{ ErrorMessage string }
		json.Unmarshal(fe.Payload, &e)
		return core.Errorf(400, "UserLambdaValidationException", "%s failed with error %s.", t.name, e.ErrorMessage)
	case err != nil:
		return core.Errorf(400, "UnexpectedLambdaException", "%s invocation failed: %v", t.name, err)
	}
	var ev struct{ Response json.RawMessage }
	if json.Unmarshal(res, &ev) != nil || out != nil && len(ev.Response) > 0 && json.Unmarshal(ev.Response, out) != nil {
		return core.Errorf(400, "InvalidLambdaResponseException", "Invalid %s response.", t.name)
	}
	return nil
}

// ---- self-service ----

// withClientUser runs fn under the lock on the user a client-side request names.
func (s *Service) withClientUser(in *userInput, fn func(p *Pool, cl *Client, u *User) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, cl, err := s.client(in.ClientId)
	if err != nil {
		return err
	}
	if err := cl.checkHash(in.SecretHash, in.Username); err != nil {
		return err
	}
	u := p.user(in.Username)
	if u == nil {
		return core.Errorf(400, "UserNotFoundException", "Username/client id combination not found.")
	}
	return fn(p, cl, u)
}

// withToken runs fn under the lock on the user an access token belongs to.
func (s *Service) withToken(token string, fn func(p *Pool, u *User, client string) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, u, client, err := s.authed(token, adminScope)
	if err != nil {
		return err
	}
	return fn(p, u, client)
}

func (s *Service) signUp(c *core.Ctx, in *userInput) (any, error) {
	s.mu.Lock()
	p, cl, err := s.client(in.ClientId)
	if err == nil {
		err = cl.checkHash(in.SecretHash, in.Username)
	}
	if err == nil && p.Settings.AdminCreateUserConfig != nil && p.Settings.AdminCreateUserConfig.AllowAdminCreateUserOnly {
		err = core.Errorf(400, "NotAuthorizedException", "SignUp is not permitted for this user pool")
	}
	if err == nil {
		err = p.checkPolicy(in.Password)
	}
	var u *User
	if err == nil {
		u, err = p.newUser(in.Username, in.UserAttributes)
	}
	var pre *trig
	if err == nil {
		pre = p.trig("PreSignUp_SignUp", u.Username, cl.ClientId, map[string]any{"userAttributes": maps.Clone(u.Attrs),
			"validationData": attrMap(in.ValidationData), "clientMetadata": in.ClientMetadata})
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	var auto struct{ AutoConfirmUser, AutoVerifyEmail, AutoVerifyPhone bool }
	if err := s.run(pre, &auto); err != nil {
		return nil, err
	}

	s.mu.Lock()
	if err := p.unique(u); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	p.setPassword(u, in.Password)
	u.Status = "UNCONFIRMED"
	for attr, on := range map[string]bool{"email": auto.AutoVerifyEmail, "phone_number": auto.AutoVerifyPhone} {
		if on && u.Attrs[attr] != "" {
			u.Attrs[attr+"_verified"] = "true"
		}
	}
	p.Users[p.key(u.Username)] = u
	out := map[string]any{"UserConfirmed": auto.AutoConfirmUser, "UserSub": u.Attrs["sub"]}
	var sends []send
	var post *trig
	if auto.AutoConfirmUser {
		u.Status = "CONFIRMED"
		post = p.trig("PostConfirmation_ConfirmSignUp", u.Username, cl.ClientId, map[string]any{"userAttributes": u.eventAttrs(), "clientMetadata": in.ClientMetadata})
	} else if attr := p.verifyTarget(u); attr != "" {
		x := p.code(u, "signup", attr, "SignUp", cl.ClientId, in.ClientMetadata)
		sends, out["CodeDeliveryDetails"] = []send{x}, x.details()
	}
	s.mu.Unlock()
	if err := s.deliver(sends...); err != nil {
		return nil, err
	}
	return out, s.run(post, nil)
}

func (s *Service) confirmSignUp(c *core.Ctx, in *userInput) (any, error) {
	var post *trig
	err := s.withClientUser(in, func(p *Pool, cl *Client, u *User) error {
		if u.Status != "UNCONFIRMED" {
			return core.Errorf(400, "NotAuthorizedException", "User cannot be confirmed. Current status is %s", u.Status)
		}
		if err := u.check("signup", in.ConfirmationCode); err != nil {
			return err
		}
		if attr := u.Codes["signup"].Attr; attr != "" {
			if err := p.verify(u, attr, in.ForceAliasCreation); err != nil {
				return err
			}
		}
		delete(u.Codes, "signup")
		u.Status, u.Modified = "CONFIRMED", time.Now()
		post = p.trig("PostConfirmation_ConfirmSignUp", u.Username, cl.ClientId, map[string]any{"userAttributes": u.eventAttrs(), "clientMetadata": in.ClientMetadata})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return nil, s.run(post, nil)
}

// sendCode delivers a code issued under the lock and returns its CodeDeliveryDetails.
func (s *Service) sendCode(x send, err error) (any, error) {
	if err == nil {
		err = s.deliver(x)
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{"CodeDeliveryDetails": x.details()}, nil
}

func (s *Service) resendConfirmationCode(c *core.Ctx, in *userInput) (any, error) {
	var x send
	err := s.withClientUser(in, func(p *Pool, cl *Client, u *User) error {
		attr := p.verifyTarget(u)
		switch {
		case u.Status != "UNCONFIRMED":
			return invalid("User is already confirmed.")
		case attr == "":
			return invalid("Auto verification not turned on.")
		}
		x = p.code(u, "signup", attr, "ResendCode", cl.ClientId, in.ClientMetadata)
		return nil
	})
	return s.sendCode(x, err)
}

func (s *Service) forgotPassword(c *core.Ctx, in *userInput) (any, error) {
	var x send
	err := s.withClientUser(in, func(p *Pool, cl *Client, u *User) (err error) {
		x, err = p.recovery(u, cl.ClientId, in.ClientMetadata)
		return err
	})
	return s.sendCode(x, err)
}

func (s *Service) confirmForgotPassword(c *core.Ctx, in *userInput) (any, error) {
	var post *trig
	err := s.withClientUser(in, func(p *Pool, cl *Client, u *User) error {
		if err := p.checkNewPassword(u, in.Password); err != nil {
			return err
		}
		if err := u.check("forgot", in.ConfirmationCode); err != nil {
			return err
		}
		delete(u.Codes, "forgot")
		p.setPassword(u, in.Password)
		if u.Status == "RESET_REQUIRED" || u.Status == "FORCE_CHANGE_PASSWORD" {
			u.Status = "CONFIRMED"
		}
		post = p.trig("PostConfirmation_ConfirmForgotPassword", u.Username, cl.ClientId, map[string]any{"userAttributes": u.eventAttrs(), "clientMetadata": in.ClientMetadata})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return nil, s.run(post, nil)
}

func (s *Service) changePassword(c *core.Ctx, in *userInput) (any, error) {
	return nil, s.withToken(in.AccessToken, func(p *Pool, u *User, _ string) error {
		if !u.passwordIs(in.PreviousPassword) {
			return errAuth()
		}
		if err := p.checkNewPassword(u, in.ProposedPassword); err != nil {
			return err
		}
		p.setPassword(u, in.ProposedPassword)
		return nil
	})
}

func (s *Service) getUser(c *core.Ctx, in *userInput) (any, error) {
	var out map[string]any
	err := s.withToken(in.AccessToken, func(p *Pool, u *User, _ string) error {
		out = u.detail(false)
		return nil
	})
	return out, err
}

func (s *Service) deleteUser(c *core.Ctx, in *userInput) (any, error) {
	return nil, s.withToken(in.AccessToken, func(p *Pool, u *User, _ string) error {
		p.deleteUser(u)
		return nil
	})
}

// checkWritable applies the app client's write permissions to a user's own
// update: WriteAttributes when set, else everything but the _verified flags.
func (p *Pool) checkWritable(client string, attrs []Attr) error {
	var allowed []string
	if cl := p.Clients[client]; cl != nil {
		allowed = cl.WriteAttributes
	}
	for _, a := range attrs {
		if len(allowed) == 0 && strings.HasSuffix(a.Name, "_verified") || len(allowed) > 0 && !slices.Contains(allowed, a.Name) {
			return invalid("Invalid attributes given, %s is not writeable.", a.Name)
		}
	}
	return nil
}

func (s *Service) updateUserAttributes(c *core.Ctx, in *userInput) (any, error) {
	var sends []send
	err := s.withToken(in.AccessToken, func(p *Pool, u *User, client string) (err error) {
		if err = p.checkWritable(client, in.UserAttributes); err != nil {
			return err
		}
		sends, err = p.setAttrs(u, in.UserAttributes, client, in.ClientMetadata)
		return err
	})
	if err == nil {
		err = s.deliver(sends...)
	}
	if err != nil {
		return nil, err
	}
	list := []map[string]string{}
	for _, x := range sends {
		list = append(list, x.details())
	}
	return map[string]any{"CodeDeliveryDetailsList": list}, nil
}

func (s *Service) deleteUserAttributes(c *core.Ctx, in *userInput) (any, error) {
	return nil, s.withToken(in.AccessToken, func(p *Pool, u *User, _ string) error { return p.deleteAttrs(u, in.UserAttributeNames) })
}

func (s *Service) getUserAttributeVerificationCode(c *core.Ctx, in *userInput) (any, error) {
	var x send
	err := s.withToken(in.AccessToken, func(p *Pool, u *User, client string) error {
		if a := in.AttributeName; a != "email" && a != "phone_number" || u.Attrs[a] == "" {
			return invalid("Invalid attribute name %s.", a)
		}
		x = p.code(u, "attr:"+in.AttributeName, in.AttributeName, "VerifyUserAttribute", client, in.ClientMetadata)
		return nil
	})
	return s.sendCode(x, err)
}

func (s *Service) verifyUserAttribute(c *core.Ctx, in *userInput) (any, error) {
	return nil, s.withToken(in.AccessToken, func(p *Pool, u *User, _ string) error {
		purpose := "attr:" + in.AttributeName
		if err := u.check(purpose, in.Code); err != nil {
			return err
		}
		if err := p.verify(u, in.AttributeName, false); err != nil {
			return err
		}
		delete(u.Codes, purpose)
		return nil
	})
}

func (s *Service) globalSignOut(c *core.Ctx, in *userInput) (any, error) {
	return nil, s.withToken(in.AccessToken, func(p *Pool, u *User, _ string) error {
		p.signOut(u.Username)
		return nil
	})
}

func (s *Service) setUserMFAPreference(c *core.Ctx, in *userInput) (any, error) {
	return nil, s.withToken(in.AccessToken, func(p *Pool, u *User, _ string) error {
		u.setMFA(in)
		return nil
	})
}

// revokeToken ends the session of a refresh token, which also revokes its access tokens.
func (s *Service) revokeToken(c *core.Ctx, in *userInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, cl, err := s.client(in.ClientId)
	if err != nil {
		return nil, err
	}
	if cl.ClientSecret != in.ClientSecret {
		return nil, core.Errorf(400, "NotAuthorizedException", "Client authentication failed.")
	}
	if x, jti, err := p.session(cl, in.Token); err == nil && x != nil {
		delete(p.Sessions, jti)
	}
	return nil, nil
}

// ---- sign-in ----

type authInput struct {
	UserPoolId, ClientId, AuthFlow, ChallengeName, Session string
	AuthParameters, ChallengeResponses, ClientMetadata     map[string]string
}

type authResult struct {
	ChallengeName        string `json:",omitempty"`
	Session              string `json:",omitempty"`
	ChallengeParameters  map[string]string
	AuthenticationResult *tokens  `json:",omitempty"`
	AvailableChallenges  []string `json:",omitempty"` // USER_AUTH: SELECT_CHALLENGE only
}

type tokens struct {
	AccessToken  string
	ExpiresIn    int
	TokenType    string
	RefreshToken string `json:",omitempty"`
	IdToken      string `json:",omitempty"`
}

// grant says who gets tokens, through which client and with which scope; jti
// names the session being refreshed.
type grant struct {
	pool, user, client, source, scope, nonce, jti string
	meta                                          map[string]string
}

// challenge is a sign-in in progress, stored under its Session (or, for
// PASSWORD_VERIFIER, its SECRET_BLOCK).
type challenge struct {
	grant
	key, name, flow, srpA, cmeta, code string
	secret                             string // software token secret being set up (MFA_SETUP)
	verified                           bool   // the secret passed VerifySoftwareToken
	ghost                              bool   // USER_AUTH for a user that does not exist (PreventUserExistenceErrors)
	exp                                time.Time
	b, B                               *big.Int
	history                            []map[string]any // DefineAuthChallenge session
	private                            any              // privateChallengeParameters
}

var (
	defaultFlows = []string{"ALLOW_USER_SRP_AUTH", "ALLOW_REFRESH_TOKEN_AUTH", "ALLOW_CUSTOM_AUTH"}
	legacyFlows  = map[string]string{"USER_PASSWORD_AUTH": "USER_PASSWORD_AUTH", "ADMIN_USER_PASSWORD_AUTH": "ADMIN_NO_SRP_AUTH", "CUSTOM_AUTH": "CUSTOM_AUTH_FLOW_ONLY"}
)

// allows reports whether ExplicitAuthFlows enable flow; legacy settings (no ALLOW_ prefix) always allowed SRP and refresh.
func (cl *Client) allows(flow string) bool {
	f := cl.ExplicitAuthFlows
	if len(f) == 0 {
		f = defaultFlows
	}
	if slices.Contains(f, "ALLOW_"+flow) {
		return true
	}
	if slices.ContainsFunc(f, func(x string) bool { return strings.HasPrefix(x, "ALLOW_") }) {
		return false
	}
	return flow == "USER_SRP_AUTH" || flow == "REFRESH_TOKEN_AUTH" || slices.Contains(f, legacyFlows[flow])
}

// checkHash verifies SECRET_HASH = Base64(HMAC-SHA256(secret, username + clientId)) for any of names.
func (cl *Client) checkHash(hash string, names ...string) error {
	if cl.ClientSecret == "" {
		return nil
	}
	if hash == "" {
		return core.Errorf(400, "NotAuthorizedException", "Client %s is configured for secret but secret was not received", cl.ClientId)
	}
	for _, n := range names {
		m := hmac.New(sha256.New, []byte(cl.ClientSecret))
		m.Write([]byte(n + cl.ClientId))
		if n != "" && hmac.Equal([]byte(hash), []byte(base64.StdEncoding.EncodeToString(m.Sum(nil)))) {
			return nil
		}
	}
	return core.Errorf(400, "NotAuthorizedException", "Unable to verify secret hash for client %s", cl.ClientId)
}

// grantee resolves a grant's pool, enabled user and client; lock held.
func (s *Service) grantee(g grant) (*Pool, *User, *Client, error) {
	p, err := s.pool(g.pool)
	if err != nil {
		return nil, nil, nil, err
	}
	cl, u := p.Clients[g.client], p.Users[p.key(g.user)]
	switch {
	case cl == nil:
		return nil, nil, nil, errNoClient(g.client)
	case u == nil:
		return nil, nil, nil, errNoUser()
	case !u.Enabled:
		return nil, nil, nil, core.Errorf(400, "NotAuthorizedException", "User is disabled.")
	}
	return p, u, cl, nil
}

func (s *Service) initiateAuth(c *core.Ctx, in *authInput) (any, error) {
	return s.initiate(c, in, false)
}
func (s *Service) adminInitiateAuth(c *core.Ctx, in *authInput) (any, error) {
	return s.initiate(c, in, true)
}

func (s *Service) initiate(c *core.Ctx, in *authInput, admin bool) (any, error) {
	flow := in.AuthFlow
	switch flow {
	case "REFRESH_TOKEN_AUTH", "REFRESH_TOKEN":
		return s.refresh(c, in, admin)
	case "ADMIN_NO_SRP_AUTH":
		flow = "ADMIN_USER_PASSWORD_AUTH"
	case "USER_PASSWORD_AUTH", "ADMIN_USER_PASSWORD_AUTH", "USER_SRP_AUTH", "CUSTOM_AUTH", "USER_AUTH":
	default:
		return nil, invalid("Initiate Auth method not supported.")
	}
	ch, pre, err := s.start(in, flow, admin)
	if err == nil {
		err = s.run(pre, nil)
	}
	if err != nil {
		return nil, err
	}
	switch flow {
	case "USER_AUTH":
		return s.userAuth(c, ch, in.AuthParameters)
	case "USER_SRP_AUTH":
		return s.srpChallenge(ch)
	case "CUSTOM_AUTH":
		if in.AuthParameters["CHALLENGE_NAME"] == "SRP_A" {
			ch.history = append(ch.history, map[string]any{"challengeName": "SRP_A", "challengeResult": true, "challengeMetadata": nil})
		}
		return s.define(c, ch)
	}
	if err := s.password(ch, in.AuthParameters["PASSWORD"]); err != nil {
		return nil, err
	}
	return s.passed(c, ch)
}

// start resolves the client and user of a sign-in and prepares PreAuthentication.
// flow is "" for the hosted login page, which needs no flow or secret checks.
func (s *Service) start(in *authInput, flow string, admin bool) (*challenge, *trig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, cl, err := s.client(in.ClientId)
	if err != nil {
		return nil, nil, err
	}
	name := in.AuthParameters["USERNAME"]
	if flow != "" {
		switch {
		case admin && in.UserPoolId != p.ID:
			return nil, nil, errNoClient(in.ClientId)
		case !cl.allows(flow) || flow == "ADMIN_USER_PASSWORD_AUTH" && !admin:
			return nil, nil, invalid("%s flow not enabled for this client", flow)
		case name == "":
			return nil, nil, invalid("Missing required parameter USERNAME")
		}
		if err := cl.checkHash(in.AuthParameters["SECRET_HASH"], name); err != nil {
			return nil, nil, err
		}
	}
	u := p.user(name)
	switch {
	case u == nil && flow == "USER_AUTH" && cl.PreventUserExistenceErrors == "ENABLED":
		// choice-based sign-in answers like it would for a real user, then fails at the password
		return &challenge{grant: grant{pool: p.ID, user: name, client: cl.ClientId, source: "TokenGeneration_Authentication", scope: adminScope},
			flow: flow, ghost: true}, nil, nil
	case u == nil && cl.PreventUserExistenceErrors == "ENABLED":
		return nil, nil, errAuth()
	case u == nil:
		return nil, nil, errNoUser()
	case !u.Enabled:
		return nil, nil, core.Errorf(400, "NotAuthorizedException", "User is disabled.")
	case u.Status == "RESET_REQUIRED":
		return nil, nil, core.Errorf(400, "PasswordResetRequiredException", "Password reset required for the user")
	}
	ch := &challenge{grant: grant{pool: p.ID, user: u.Username, client: cl.ClientId, source: "TokenGeneration_Authentication",
		scope: adminScope, meta: in.ClientMetadata}, flow: flow, srpA: in.AuthParameters["SRP_A"], history: []map[string]any{}}
	pre := p.trig("PreAuthentication_Authentication", u.Username, cl.ClientId, map[string]any{"userAttributes": u.eventAttrs(),
		"validationData": map[string]string{}, "userNotFound": false})
	return ch, pre, nil
}

// password checks the first factor of the password flows and the hosted login.
func (s *Service) password(ch *challenge, pw string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, u, _, err := s.grantee(ch.grant)
	switch {
	case err != nil:
		return err
	case !u.passwordIs(pw):
		return errAuth()
	case u.Status == "UNCONFIRMED":
		return core.Errorf(400, "UserNotConfirmedException", "User is not confirmed.")
	}
	return nil
}

// passed continues a sign-in after the password: a temporary password must be replaced, then MFA, then tokens.
func (s *Service) passed(c *core.Ctx, ch *challenge) (any, error) {
	s.mu.Lock()
	p, u, _, err := s.grantee(ch.grant)
	if err != nil || u.Status != "FORCE_CHANGE_PASSWORD" {
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return s.mfa(c, ch)
	}
	defer s.mu.Unlock()
	attrs := maps.Clone(u.Attrs)
	delete(attrs, "sub")
	required := []string{}
	for _, a := range p.Schema {
		if a.Required && u.Attrs[a.Name] == "" {
			required = append(required, "userAttributes."+a.Name)
		}
	}
	ua, _ := json.Marshal(attrs)
	ra, _ := json.Marshal(required)
	return s.ask(ch, "NEW_PASSWORD_REQUIRED", map[string]string{"USER_ID_FOR_SRP": u.Username, "userAttributes": string(ua), "requiredAttributes": string(ra)}), nil
}

// mfa asks for an MFA code when the pool wants one, else finishes the sign-in.
func (s *Service) mfa(c *core.Ctx, ch *challenge) (any, error) {
	s.mu.Lock()
	p, u, _, err := s.grantee(ch.grant)
	kind := ""
	if err == nil {
		kind = p.mfaFor(u)
	}
	if kind == "" {
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return s.finish(c, ch)
	}
	params := map[string]string{"USER_ID_FOR_SRP": u.Username}
	var sends []send
	if kind == "MFA_SETUP" {
		params = map[string]string{"MFAS_CAN_SETUP": `["SOFTWARE_TOKEN_MFA"]`}
	}
	if kind == "SMS_MFA" {
		ch.code = core.RandString(6, digits)
		sends = append(sends, p.sendTo(u, "phone_number", ch.code, "Authentication", ch.client, ch.meta))
		params["CODE_DELIVERY_DELIVERY_MEDIUM"], params["CODE_DELIVERY_DESTINATION"] = "SMS", mask(u.Attrs["phone_number"])
	}
	res := s.ask(ch, kind, params)
	s.mu.Unlock()
	return res, s.deliver(sends...)
}

// mfaFor is the MFA challenge a sign-in needs, if any.
// A user with no method yet starts MFA_SETUP when the pool offers software tokens.
// ponytail: with no phone number and no software token set up in the pool, the user gets
// SOFTWARE_TOKEN_MFA and 123456 passes, where AWS would refuse the sign-in.
func (p *Pool) mfaFor(u *User) string {
	switch cfg := p.Settings.MfaConfiguration; {
	case cfg != "ON" && (cfg != "OPTIONAL" || len(u.MFA) == 0):
		return ""
	case u.Preferred != "":
		return u.Preferred
	case len(u.MFA) > 0:
		return u.MFA[0]
	case u.Attrs["phone_number"] != "":
		return "SMS_MFA"
	case p.totpEnabled():
		return "MFA_SETUP"
	}
	return "SOFTWARE_TOKEN_MFA"
}

// finish runs PostAuthentication and issues the tokens.
func (s *Service) finish(c *core.Ctx, ch *challenge) (*authResult, error) {
	s.mu.Lock()
	p, u, _, err := s.grantee(ch.grant)
	var post *trig
	if err == nil {
		post = p.trig("PostAuthentication_Authentication", u.Username, ch.client, map[string]any{"userAttributes": u.eventAttrs(),
			"newDeviceUsed": false, "clientMetadata": ch.meta})
	}
	s.mu.Unlock()
	if err == nil {
		err = s.run(post, nil)
	}
	if err != nil {
		return nil, err
	}
	t, err := s.issue(c, ch.grant)
	if err != nil {
		return nil, err
	}
	return &authResult{ChallengeParameters: map[string]string{}, AuthenticationResult: t}, nil
}

// ask parks the sign-in until the client answers challenge name; lock held.
func (s *Service) ask(ch *challenge, name string, params map[string]string) *authResult {
	now := time.Now()
	for k, x := range s.pending {
		if now.After(x.exp) {
			delete(s.pending, k)
		}
	}
	b := make([]byte, 48)
	rand.Read(b)
	ch.key, ch.name, ch.exp = base64.StdEncoding.EncodeToString(b), name, now.Add(3*time.Minute)
	s.pending[ch.key] = ch
	res := &authResult{ChallengeName: name, ChallengeParameters: params, Session: ch.key}
	if name == "PASSWORD_VERIFIER" { // answered with SECRET_BLOCK, not a Session
		params["SECRET_BLOCK"], res.Session = ch.key, ""
	}
	return res
}

func (s *Service) respondToAuthChallenge(c *core.Ctx, in *authInput) (any, error) {
	return s.respond(c, in, false)
}

func (s *Service) adminRespondToAuthChallenge(c *core.Ctx, in *authInput) (any, error) {
	return s.respond(c, in, true)
}

func (s *Service) respond(c *core.Ctx, in *authInput, admin bool) (any, error) {
	r := in.ChallengeResponses
	key := in.Session
	s.mu.Lock()
	ch := s.pending[key]
	if ch == nil { // PASSWORD_VERIFIER is answered with the SECRET_BLOCK
		key = r["PASSWORD_CLAIM_SECRET_BLOCK"]
		ch = s.pending[key]
	}
	delete(s.pending, key)
	_, cl, err := s.client(in.ClientId)
	switch {
	case err != nil:
	case ch == nil || time.Now().After(ch.exp) || ch.client != cl.ClientId || admin && in.UserPoolId != ch.pool:
		err = core.Errorf(400, "NotAuthorizedException", "Invalid session for the user, session is expired.")
	case in.ChallengeName != ch.name:
		err = invalid("Invalid challenge name %s, expected %s.", in.ChallengeName, ch.name)
	default:
		err = cl.checkHash(r["SECRET_HASH"], r["USERNAME"], ch.user)
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	res, err := s.answer(c, ch, r)
	var e *core.Error
	if errors.As(err, &e) && (e.Code == "CodeMismatchException" || e.Code == "InvalidPasswordException") {
		s.mu.Lock()
		s.pending[key] = ch // the client may try again with the same session
		s.mu.Unlock()
	}
	return res, err
}

func (s *Service) answer(c *core.Ctx, ch *challenge, r map[string]string) (any, error) {
	if ch.ghost && ch.name != "SELECT_CHALLENGE" {
		return nil, errAuth()
	}
	switch ch.name {
	case "PASSWORD_VERIFIER":
		if err := s.srpVerify(ch, r); err != nil {
			return nil, err
		}
		if ch.flow == "CUSTOM_AUTH" {
			ch.history = append(ch.history, map[string]any{"challengeName": "PASSWORD_VERIFIER", "challengeResult": true, "challengeMetadata": nil})
			return s.define(c, ch)
		}
		return s.passed(c, ch)
	case "NEW_PASSWORD_REQUIRED":
		if err := s.newPassword(ch, r); err != nil {
			return nil, err
		}
		ch.source = "TokenGeneration_NewPasswordChallenge"
		return s.mfa(c, ch)
	case "SMS_MFA", "SOFTWARE_TOKEN_MFA":
		if !s.mfaCodeOK(ch, r[ch.name+"_CODE"]) {
			return nil, core.Errorf(400, "CodeMismatchException", "Invalid code or auth state for the user.")
		}
		return s.finish(c, ch)
	case "MFA_SETUP":
		return s.finishSetup(c, ch)
	case "SELECT_CHALLENGE", "PASSWORD", "EMAIL_OTP", "SMS_OTP":
		return s.firstFactor(c, ch, r)
	case "CUSTOM_CHALLENGE":
		return s.verifyCustom(c, ch, r["ANSWER"])
	}
	return nil, invalid("Unsupported challenge %s.", ch.name)
}

// newPassword answers NEW_PASSWORD_REQUIRED: a permanent password and any required attributes.
func (s *Service) newPassword(ch *challenge, r map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, u, _, err := s.grantee(ch.grant)
	if err != nil {
		return err
	}
	pw := r["NEW_PASSWORD"]
	if err := p.checkPolicy(pw); err != nil {
		return err
	}
	var attrs []Attr
	for k, v := range r {
		if name, ok := strings.CutPrefix(k, "userAttributes."); ok {
			attrs = append(attrs, Attr{name, v})
		}
	}
	merged := maps.Clone(u.Attrs)
	maps.Copy(merged, attrMap(attrs))
	for _, a := range p.Schema {
		if a.Required && merged[a.Name] == "" {
			return invalid("Invalid attributes given, %s is missing", a.Name)
		}
	}
	if _, err := p.setAttrs(u, attrs, ch.client, ch.meta); err != nil {
		return err
	}
	p.setPassword(u, pw)
	u.Status = "CONFIRMED"
	return nil
}

// define asks DefineAuthChallenge what the CUSTOM_AUTH flow does next.
func (s *Service) define(c *core.Ctx, ch *challenge) (any, error) {
	s.mu.Lock()
	p, u, _, err := s.grantee(ch.grant)
	var t *trig
	if err == nil {
		t = p.trig("DefineAuthChallenge_Authentication", u.Username, ch.client, map[string]any{"userAttributes": u.eventAttrs(),
			"session": ch.history, "clientMetadata": ch.meta, "userNotFound": false})
		if t == nil {
			err = invalid("Custom auth lambda trigger is not configured for the user pool.")
		}
	}
	s.mu.Unlock()
	var d struct {
		ChallengeName                   string
		IssueTokens, FailAuthentication bool
	}
	if err == nil {
		err = s.run(t, &d)
	}
	switch {
	case err != nil:
		return nil, err
	case d.IssueTokens:
		return s.finish(c, ch)
	case d.FailAuthentication || d.ChallengeName == "":
		return nil, errAuth()
	case d.ChallengeName == "PASSWORD_VERIFIER":
		return s.srpChallenge(ch)
	}

	s.mu.Lock()
	if p, u, _, err = s.grantee(ch.grant); err == nil {
		t = p.trig("CreateAuthChallenge_Authentication", u.Username, ch.client, map[string]any{"userAttributes": u.eventAttrs(),
			"challengeName": d.ChallengeName, "session": ch.history, "clientMetadata": ch.meta, "userNotFound": false})
	}
	s.mu.Unlock()
	var cr struct {
		PublicChallengeParameters  map[string]string
		PrivateChallengeParameters any
		ChallengeMetadata          string
	}
	if err == nil {
		err = s.run(t, &cr)
	}
	if err != nil {
		return nil, err
	}
	ch.private, ch.cmeta = cr.PrivateChallengeParameters, cr.ChallengeMetadata
	params := map[string]string{"USERNAME": ch.user}
	maps.Copy(params, cr.PublicChallengeParameters)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ask(ch, d.ChallengeName, params), nil
}

// verifyCustom checks a CUSTOM_CHALLENGE answer with VerifyAuthChallengeResponse, then asks define again.
func (s *Service) verifyCustom(c *core.Ctx, ch *challenge, answer string) (any, error) {
	s.mu.Lock()
	p, u, _, err := s.grantee(ch.grant)
	var t *trig
	if err == nil {
		t = p.trig("VerifyAuthChallengeResponse_Authentication", u.Username, ch.client, map[string]any{"userAttributes": u.eventAttrs(),
			"privateChallengeParameters": ch.private, "challengeAnswer": answer, "clientMetadata": ch.meta, "userNotFound": false})
	}
	s.mu.Unlock()
	var v struct{ AnswerCorrect bool }
	if err == nil {
		err = s.run(t, &v)
	}
	if err != nil {
		return nil, err
	}
	ch.history = append(ch.history, map[string]any{"challengeName": ch.name, "challengeResult": v.AnswerCorrect, "challengeMetadata": ch.cmeta})
	return s.define(c, ch)
}

// refresh is REFRESH_TOKEN_AUTH: new id and access tokens for a live session.
func (s *Service) refresh(c *core.Ctx, in *authInput, admin bool) (any, error) {
	ap := in.AuthParameters
	s.mu.Lock()
	p, cl, err := s.client(in.ClientId)
	var x *Session
	var jti string
	switch {
	case err != nil:
	case admin && in.UserPoolId != p.ID:
		err = errNoClient(in.ClientId)
	case !cl.allows("REFRESH_TOKEN_AUTH"):
		err = invalid("REFRESH_TOKEN_AUTH flow not enabled for this client")
	default:
		if x, jti, err = p.session(cl, ap["REFRESH_TOKEN"]); err == nil {
			err = cl.checkHash(ap["SECRET_HASH"], ap["USERNAME"], x.Username)
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	t, err := s.issue(c, grant{pool: p.ID, user: x.Username, client: cl.ClientId, source: "TokenGeneration_RefreshTokens",
		scope: x.Scope, meta: in.ClientMetadata, jti: jti})
	if err != nil {
		return nil, err
	}
	return &authResult{ChallengeParameters: map[string]string{}, AuthenticationResult: t}, nil
}

// session finds the live session of a refresh token; lock held. Sessions are
// immutable once created, so callers may read them after unlocking.
func (p *Pool) session(cl *Client, refresh string) (*Session, string, error) {
	jti := originJTI(refresh)
	x := p.Sessions[jti]
	switch {
	case x == nil || x.Client != cl.ClientId:
		return nil, "", core.Errorf(400, "NotAuthorizedException", "Invalid Refresh Token")
	case time.Now().After(x.Exp):
		return nil, "", core.Errorf(400, "NotAuthorizedException", "Refresh Token has expired")
	case !x.Rotated.IsZero() && time.Now().After(x.Rotated.Add(time.Duration(x.Grace)*time.Second)):
		return nil, "", core.Errorf(400, "NotAuthorizedException", "Invalid Refresh Token")
	}
	return x, jti, nil
}

// originJTI derives a session id from its refresh token, so refresh tokens are never stored.
func originJTI(refresh string) string {
	h := sha256.Sum256([]byte(refresh))
	x := hex.EncodeToString(h[:16])
	return x[:8] + "-" + x[8:12] + "-" + x[12:16] + "-" + x[16:20] + "-" + x[20:]
}

// ---- SRP-6a, as Cognito and amazon-cognito-identity-js do it ----

var (
	srpN, _ = new(big.Int).SetString("FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1"+
		"29024E088A67CC74020BBEA63B139B22514A08798E3404DD"+
		"EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245"+
		"E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7ED"+
		"EE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3D"+
		"C2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F"+
		"83655D23DCA3AD961C62F356208552BB9ED529077096966D"+
		"670C354E4ABC9804F1746C08CA18217C32905E462E36CE3B"+
		"E39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9"+
		"DE2BCBF6955817183995497CEA956AE515D2261898FA0510"+
		"15728E5A8AAAC42DAD33170D04507A33A85521ABDF1CBA64"+
		"ECFB850458DBEF0A8AEA71575D060C7DB3970F85A6E1E4C7"+
		"ABF5AE8CDB0933D71E8C94E04A25619DCEE3D2261AD2EE6B"+
		"F12FFA06D98A0864D87602733EC86A64521F2B18177B200C"+
		"BBE117577A615D6C770988C0BAD946E208E24FA074E5AB31"+
		"43DB5BFCE0FD108E4B82D120A93AD2CAFFFFFFFFFFFFFFFF", 16) // RFC 5054 3072-bit group
	srpG = big.NewInt(2)
	srpK = new(big.Int).SetBytes(hashOf(padHex(srpN), padHex(srpG)))
)

// padHex is the clients' encoding of a number: minimal big-endian bytes, plus a
// leading zero byte when the high bit is set.
func padHex(n *big.Int) []byte {
	b := n.Bytes()
	if len(b) == 0 || b[0]&0x80 != 0 {
		return append([]byte{0}, b...)
	}
	return b
}

func hashOf(parts ...[]byte) []byte {
	h := sha256.New()
	for _, p := range parts {
		h.Write(p)
	}
	return h.Sum(nil)
}

func poolShort(id string) string { return id[strings.IndexByte(id, '_')+1:] }

// srpVerifier is v = g^x, x = H(salt | H(poolName | username | ":" | password)).
func srpVerifier(pool, username, pw string, salt []byte) *big.Int {
	x := new(big.Int).SetBytes(hashOf(padHex(new(big.Int).SetBytes(salt)), hashOf([]byte(poolShort(pool)+username+":"+pw))))
	return new(big.Int).Exp(srpG, x, srpN)
}

// srpChallenge answers SRP_A with B = kv + g^b and the user's salt.
func (s *Service) srpChallenge(ch *challenge) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, u, _, err := s.grantee(ch.grant)
	if err != nil {
		return nil, err
	}
	A, ok := new(big.Int).SetString(ch.srpA, 16)
	if !ok || new(big.Int).Mod(A, srpN).Sign() == 0 {
		return nil, invalid("SRP_A is missing or invalid.")
	}
	v, ok := new(big.Int).SetString(u.Verifier, 16)
	if !ok {
		return nil, errAuth()
	}
	ch.b, _ = rand.Int(rand.Reader, srpN)
	ch.B = new(big.Int).Mul(srpK, v)
	ch.B.Add(ch.B, new(big.Int).Exp(srpG, ch.b, srpN)).Mod(ch.B, srpN)
	return s.ask(ch, "PASSWORD_VERIFIER", map[string]string{"SALT": u.Salt, "SRP_B": ch.B.Text(16),
		"USERNAME": u.Username, "USER_ID_FOR_SRP": u.Username}), nil
}

// srpVerify checks PASSWORD_CLAIM_SIGNATURE = HMAC(K, poolName | userId | secretBlock | timestamp),
// K = HKDF(S = (A·v^u)^b, salt u, "Caldera Derived Key").
func (s *Service) srpVerify(ch *challenge, r map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, u, _, err := s.grantee(ch.grant)
	if err != nil {
		return err
	}
	A, _ := new(big.Int).SetString(ch.srpA, 16)
	v, _ := new(big.Int).SetString(u.Verifier, 16)
	scramble := new(big.Int).SetBytes(hashOf(padHex(A), padHex(ch.B)))
	S := new(big.Int).Exp(v, scramble, srpN)
	S.Mul(S, A).Mod(S, srpN).Exp(S, ch.b, srpN)
	key, _ := hkdf.Key(sha256.New, padHex(S), padHex(scramble), "Caldera Derived Key", 16)
	block, _ := base64.StdEncoding.DecodeString(r["PASSWORD_CLAIM_SECRET_BLOCK"])
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(poolShort(p.ID) + u.Username))
	mac.Write(block)
	mac.Write([]byte(r["TIMESTAMP"]))
	sig, _ := base64.StdEncoding.DecodeString(r["PASSWORD_CLAIM_SIGNATURE"])
	switch {
	case !hmac.Equal(mac.Sum(nil), sig):
		return errAuth()
	case u.Status == "UNCONFIRMED":
		return core.Errorf(400, "UserNotConfirmedException", "User is not confirmed.")
	}
	return nil
}

// ---- tokens ----

// issue runs PreTokenGeneration and signs the id and access tokens, starting a
// session (with a refresh token) unless g refreshes one.
func (s *Service) issue(c *core.Ctx, g grant) (*tokens, error) {
	s.mu.Lock()
	p, u, cl, err := s.grantee(g)
	var t *trig
	if err == nil {
		names, roles, preferred := groupClaims(p.groupsOf(u))
		req := map[string]any{"userAttributes": u.eventAttrs(), "clientMetadata": g.meta,
			"groupConfiguration": map[string]any{"groupsToOverride": names, "iamRolesToOverride": roles, "preferredRole": preferred}}
		if t = p.trig(g.source, u.Username, cl.ClientId, req); t != nil && t.ev["version"] != "1" {
			req["scopes"] = strings.Fields(g.scope)
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	var o override
	if err := s.run(t, &o); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if p, u, cl, err = s.grantee(g); err != nil {
		return nil, err
	}
	now := time.Now()
	jti, refresh := g.jti, ""
	x := p.Sessions[jti]
	switch {
	case jti != "" && x == nil:
		return nil, core.Errorf(400, "NotAuthorizedException", "Refresh Token has been revoked")
	case x == nil:
		refresh = core.RandHex(48)
		jti = originJTI(refresh)
		x = &Session{Username: u.Username, Client: cl.ClientId, Scope: g.scope, AuthTime: now, Exp: now.Add(cl.ttl("refresh"))}
		for k, old := range p.Sessions { // ponytail: O(n) prune per sign-in
			if now.After(old.Exp) {
				delete(p.Sessions, k)
			}
		}
		p.Sessions[jti] = x
	}
	event := core.UUID()
	claims := func(use string, ttl time.Duration) map[string]any {
		return map[string]any{"sub": u.Attrs["sub"], "iss": issuer(c, p.ID), "token_use": use, "auth_time": x.AuthTime.Unix(),
			"iat": now.Unix(), "exp": now.Add(ttl).Unix(), "jti": core.UUID(), "origin_jti": jti, "event_id": event}
	}
	access := claims("access", cl.ttl("access"))
	access["client_id"], access["username"], access["scope"], access["version"] = cl.ClientId, u.Username, g.scope, 2
	id := claims("id", cl.ttl("id"))
	id["aud"], id["cognito:username"] = cl.ClientId, u.Username
	if g.nonce != "" {
		id["nonce"] = g.nonce
	}
	for k, v := range u.Attrs {
		if len(cl.ReadAttributes) == 0 || slices.Contains(cl.ReadAttributes, k) {
			id[k] = claimValue(k, v)
		}
	}
	names, roles, preferred := groupClaims(p.groupsOf(u))
	if len(names) > 0 {
		id["cognito:groups"], access["cognito:groups"] = names, names
	}
	if len(roles) > 0 {
		id["cognito:roles"], id["cognito:preferred_role"] = roles, preferred
	}
	o.apply(id, access)
	out := &tokens{AccessToken: p.sign(access), ExpiresIn: int(cl.ttl("access") / time.Second), TokenType: "Bearer", RefreshToken: refresh}
	if g.scope == adminScope || slices.Contains(strings.Fields(g.scope), "openid") {
		out.IdToken = p.sign(id)
	}
	return out, nil
}

func groupClaims(gs []*Group) (names, roles []string, preferred any) {
	names, roles = []string{}, []string{}
	for _, g := range gs {
		names = append(names, g.GroupName)
		if g.RoleArn != "" {
			roles = append(roles, g.RoleArn)
			if preferred == nil {
				preferred = g.RoleArn
			}
		}
	}
	return names, roles, preferred
}

func claimValue(k, v string) any {
	switch k {
	case "email_verified", "phone_number_verified":
		return v == "true"
	case "updated_at":
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return v
}

var unitDur = map[string]time.Duration{"seconds": time.Second, "minutes": time.Minute, "hours": time.Hour, "days": 24 * time.Hour}

// ttl is a token lifetime: access and id default to 1 hour, refresh to 30 days.
func (cl *Client) ttl(kind string) time.Duration {
	var u TokenValidityUnits
	if cl.TokenValidityUnits != nil {
		u = *cl.TokenValidityUnits
	}
	v, unit, def := cl.AccessTokenValidity, u.AccessToken, time.Hour
	switch kind {
	case "id":
		v, unit = cl.IdTokenValidity, u.IdToken
	case "refresh":
		v, unit, def = cl.RefreshTokenValidity, cmp.Or(u.RefreshToken, "days"), 30*24*time.Hour
	}
	if v == 0 {
		return def
	}
	return time.Duration(v) * unitDur[cmp.Or(unit, "hours")]
}

// override is a PreTokenGeneration response: V1 claimsOverrideDetails or V2 claimsAndScopeOverrideDetails.
type override struct {
	V1 *struct {
		claimEdit
		GroupOverrideDetails *groupEdit
	} `json:"claimsOverrideDetails"`
	V2 *struct {
		IdTokenGeneration, AccessTokenGeneration *claimEdit
		GroupOverrideDetails                     *groupEdit
	} `json:"claimsAndScopeOverrideDetails"`
}

type claimEdit struct {
	ClaimsToAddOrOverride         map[string]any
	ClaimsToSuppress              []string
	ScopesToAdd, ScopesToSuppress []string
}

type groupEdit struct {
	GroupsToOverride, IamRolesToOverride []string
	PreferredRole                        *string
}

var reserved = map[string]bool{"acr": true, "amr": true, "aud": true, "at_hash": true, "auth_time": true, "azp": true,
	"client_id": true, "cognito:username": true, "event_id": true, "exp": true, "iat": true, "identities": true, "iss": true,
	"jti": true, "nbf": true, "nonce": true, "origin_jti": true, "scope": true, "sub": true, "token_use": true, "username": true, "version": true}

func (o *override) apply(id, access map[string]any) {
	if v := o.V1; v != nil {
		v.apply(id)
		v.GroupOverrideDetails.apply(id, access)
	}
	if v := o.V2; v != nil {
		v.IdTokenGeneration.apply(id)
		v.AccessTokenGeneration.apply(access)
		v.GroupOverrideDetails.apply(id, access)
	}
}

func (e *claimEdit) apply(claims map[string]any) {
	if e == nil {
		return
	}
	for k, v := range e.ClaimsToAddOrOverride {
		if !reserved[k] {
			claims[k] = v
		}
	}
	for _, k := range e.ClaimsToSuppress {
		if !reserved[k] {
			delete(claims, k)
		}
	}
	if sc, ok := claims["scope"].(string); ok && len(e.ScopesToAdd)+len(e.ScopesToSuppress) > 0 {
		f := slices.DeleteFunc(strings.Fields(sc), func(x string) bool { return slices.Contains(e.ScopesToSuppress, x) })
		for _, x := range e.ScopesToAdd {
			if !slices.Contains(f, x) {
				f = append(f, x)
			}
		}
		claims["scope"] = strings.Join(f, " ")
	}
}

func (g *groupEdit) apply(id, access map[string]any) {
	if g == nil {
		return
	}
	set := func(m map[string]any, k string, v []string) {
		if len(v) == 0 {
			delete(m, k)
		} else {
			m[k] = v
		}
	}
	if g.GroupsToOverride != nil {
		set(id, "cognito:groups", g.GroupsToOverride)
		set(access, "cognito:groups", g.GroupsToOverride)
	}
	if g.IamRolesToOverride != nil {
		set(id, "cognito:roles", g.IamRolesToOverride)
	}
	if g.PreferredRole != nil {
		id["cognito:preferred_role"] = *g.PreferredRole
	}
}

// sign makes an RS256 JWT with the pool's key.
func (p *Pool) sign(claims map[string]any) string {
	h, _ := json.Marshal(map[string]string{"kid": p.Kid, "alg": "RS256"})
	body, _ := json.Marshal(claims)
	msg := b64.EncodeToString(h) + "." + b64.EncodeToString(body)
	sum := sha256.Sum256([]byte(msg))
	sig, _ := rsa.SignPKCS1v15(nil, p.priv, crypto.SHA256, sum[:])
	return msg + "." + b64.EncodeToString(sig)
}

// verify checks a token's signature against the pool its issuer names, and its expiry; lock held.
func (s *Service) verify(token string) (*Pool, map[string]any, error) {
	bad := core.Errorf(400, "NotAuthorizedException", "Invalid Access Token")
	part := strings.Split(token, ".")
	if len(part) != 3 {
		return nil, nil, bad
	}
	payload, err1 := b64.DecodeString(part[1])
	sig, err2 := b64.DecodeString(part[2])
	var claims map[string]any
	if err1 != nil || err2 != nil || json.Unmarshal(payload, &claims) != nil {
		return nil, nil, bad
	}
	iss, _ := claims["iss"].(string)
	p := s.st.Pools[iss[strings.LastIndexByte(iss, '/')+1:]]
	sum := sha256.Sum256([]byte(part[0] + "." + part[1]))
	if p == nil || rsa.VerifyPKCS1v15(&p.priv.PublicKey, crypto.SHA256, sum[:], sig) != nil {
		return nil, nil, bad
	}
	if exp, _ := claims["exp"].(float64); float64(time.Now().Unix()) >= exp {
		return nil, nil, core.Errorf(400, "NotAuthorizedException", "Access Token has expired")
	}
	return p, claims, nil
}

// authed checks an access token (signature, expiry, scope, revocation) and returns its user; lock held.
func (s *Service) authed(token string, scopes ...string) (*Pool, *User, string, error) {
	p, claims, err := s.verify(token)
	if err != nil {
		return nil, nil, "", err
	}
	scope, _ := claims["scope"].(string)
	name, _ := claims["username"].(string)
	client, _ := claims["client_id"].(string)
	jti, _ := claims["origin_jti"].(string)
	x := p.Sessions[jti]
	switch {
	case claims["token_use"] != "access":
		return nil, nil, "", core.Errorf(400, "NotAuthorizedException", "Invalid Access Token")
	case !slices.ContainsFunc(strings.Fields(scope), func(sc string) bool { return slices.Contains(scopes, sc) }):
		return nil, nil, "", core.Errorf(400, "NotAuthorizedException", "Access Token does not have required scopes")
	case x == nil || x.Username != name:
		return nil, nil, "", core.Errorf(400, "NotAuthorizedException", "Access Token has been revoked")
	}
	u := p.Users[p.key(name)]
	switch {
	case u == nil:
		return nil, nil, "", errNoUser()
	case !u.Enabled:
		return nil, nil, "", core.Errorf(400, "NotAuthorizedException", "User is disabled.")
	}
	return p, u, client, nil
}
