// Package firebaseauth emulates Firebase Authentication like the official
// Auth emulator: point clients at it with FIREBASE_AUTH_EMULATOR_HOST=localhost:7644.
//
// It serves the Identity Toolkit v1 accounts API (/identitytoolkit.googleapis.com/v1/accounts:*
// and /v1/projects/{p}/accounts:*): sign up (email, anonymous), sign in (password,
// custom token, fake IdP token), lookup, update, delete, batch calls, and OOB codes
// for password reset and email verification. Tokens refresh at securetoken.googleapis.com/v1/token.
// The emulator's own REST extras live under /emulator/v1/projects/{p}/: DELETE accounts,
// GET oobCodes and GET config.
//
// ID tokens are RS256 JWTs with Firebase's claims; their certificates are served at
// /robot/v1/metadata/x509/securetoken@system.gserviceaccount.com. In emulator mode the Admin
// SDKs skip the signature and check the claims and the user, which holds for these tokens too.
// Requests that name no project belong to core.DefaultProject.
package firebaseauth

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
)

const (
	kid          = "pogi-firebase"
	certPath     = "/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com"
	toolkit      = "/identitytoolkit.googleapis.com/"
	secureToken  = "/securetoken.googleapis.com/"
	emulatorBase = "/emulator/v1/projects/"
	tokenTTL     = time.Hour
	customAud    = "https://identitytoolkit.googleapis.com/google.identity.identitytoolkit.v1.IdentityToolkit"
)

// Service is the Firebase Authentication emulator.
type Service struct {
	hub *core.Hub
	mu  sync.Mutex
	st  state
	now func() time.Time
	key *rsa.PrivateKey // made on first use, saved with the state
}

type state struct {
	Projects map[string]*project `json:"projects"`
	Key      []byte              `json:"key,omitempty"` // PKCS#8 signing key
}

type project struct {
	Users   map[string]*User    `json:"users"`
	OOB     []OOB               `json:"oob,omitempty"`
	Refresh map[string]*refresh `json:"refresh,omitempty"`
}

// User is an account. Timestamps are milliseconds, validSince is seconds.
type User struct {
	ID            string     `json:"localId"`
	Email         string     `json:"email,omitempty"`
	EmailVerified bool       `json:"emailVerified,omitempty"`
	DisplayName   string     `json:"displayName,omitempty"`
	PhotoURL      string     `json:"photoUrl,omitempty"`
	Phone         string     `json:"phoneNumber,omitempty"`
	Salt          string     `json:"salt,omitempty"`
	Hash          string     `json:"hash,omitempty"` // sha256(salt+password); ponytail: a salted SHA-256 is no password hash for real use, but this is a local emulator
	Disabled      bool       `json:"disabled,omitempty"`
	Claims        string     `json:"customAttributes,omitempty"` // JSON object
	Providers     []Provider `json:"providers,omitempty"`
	Created       int64      `json:"createdAt"`
	LastLogin     int64      `json:"lastLoginAt,omitempty"`
	LastRefresh   int64      `json:"lastRefreshAt,omitempty"`
	ValidSince    int64      `json:"validSince"`
}

// Provider is a sign-in method of a user: password, phone or a federated IdP.
type Provider struct {
	ID          string `json:"providerId"`
	RawID       string `json:"rawId"`
	Email       string `json:"email,omitempty"`
	DisplayName string `json:"displayName,omitempty"`
	PhotoURL    string `json:"photoUrl,omitempty"`
}

type refresh struct {
	UID      string         `json:"uid"`
	Provider string         `json:"provider"`
	Claims   map[string]any `json:"claims,omitempty"` // developer claims of a custom token
	Issued   int64          `json:"issued"`           // seconds
}

// OOB is an out-of-band code the emulator "emailed": a password reset, an email verification or a sign-in link.
type OOB struct {
	Email string    `json:"email"`
	Type  string    `json:"requestType"`
	Code  string    `json:"oobCode"`
	Link  string    `json:"oobLink"`
	Time  time.Time `json:"time"`
}

// New creates the Firebase Auth service.
func New(h *core.Hub) *Service {
	s := &Service{hub: h, now: time.Now}
	s.Reset()
	if h != nil {
		h.Emulator(func(r *http.Request) bool { _, _, ok := adminPath(r); return ok }, http.HandlerFunc(s.admin))
	}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "firebaseauth", Cloud: core.GCP, Title: "Firebase Authentication", Match: match}
}

func match(r *http.Request) bool {
	p := r.URL.Path
	return strings.HasPrefix(p, toolkit) || strings.HasPrefix(p, secureToken) || p == certPath
}

// SetClock replaces the clock that stamps and checks tokens (tests mint expired ones with it).
func (s *Service) SetClock(f func() time.Time) {
	s.mu.Lock()
	s.now = f
	s.mu.Unlock()
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Projects: map[string]*project{}, Key: s.st.Key} // the signing key outlives a reset: tokens in clients' hands stay checkable
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
	if st.Projects == nil {
		st.Projects = map[string]*project{}
	}
	for _, p := range st.Projects {
		if p.Users == nil {
			p.Users = map[string]*User{}
		}
		if p.Refresh == nil {
			p.Refresh = map[string]*refresh{}
		}
	}
	s.mu.Lock()
	s.st, s.key = st, nil
	s.mu.Unlock()
	return nil
}

// project returns a project's state, making it when asked to. Lock held.
func (s *Service) project(id string, create bool) *project {
	p := s.st.Projects[id]
	if p == nil && create {
		p = &project{Users: map[string]*User{}, Refresh: map[string]*refresh{}}
		s.st.Projects[id] = p
	}
	return p
}

// ---- errors and bodies ----

// apiError is an Identity Toolkit error: {"error": {"code", "message": "EMAIL_EXISTS"}}.
type apiError struct {
	Status int
	Msg    string
}

func (e *apiError) Error() string { return e.Msg }

func bad(msg string) error { return &apiError{http.StatusBadRequest, msg} }

func writeErr(w http.ResponseWriter, c *core.Ctx, err error) {
	e, ok := err.(*apiError)
	if !ok {
		ce := core.AsError(err)
		e = &apiError{ce.Status, ce.Code}
	}
	if c != nil {
		c.ErrCode = e.Msg
	}
	writeJSON(w, e.Status, map[string]any{"error": map[string]any{"code": e.Status, "message": e.Msg, "status": core.GoogleStatus(e.Status),
		"errors": []map[string]string{{"message": e.Msg, "domain": "global", "reason": "invalid"}}}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// body is a decoded JSON request. The same field is a string in one call and
// a list in another (email, localId, phoneNumber), so it is read loosely.
type body map[string]any

func (b body) str(k string) string { s, _ := b[k].(string); return s }

func (b body) strs(k string) []string {
	switch v := b[k].(type) {
	case string:
		return []string{v}
	case []any:
		var out []string
		for _, x := range v {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func (b body) flag(k string) bool { v, _ := b[k].(bool); return v }

func (b body) obj(k string) body { v, _ := b[k].(map[string]any); return v }

func (b body) num(k string) int64 { // numbers arrive as numbers or strings
	switch v := b[k].(type) {
	case float64:
		return int64(v)
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	}
	return 0
}

func readBody(r *http.Request) (body, error) {
	b := body{}
	raw := core.Body(r)
	if len(strings.TrimSpace(string(raw))) == 0 {
		return b, nil
	}
	if strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
		q, err := url.ParseQuery(string(raw))
		if err != nil {
			return nil, bad("INVALID_REQUEST")
		}
		for k := range q {
			b[k] = q.Get(k)
		}
		return b, nil
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, bad("INVALID_REQUEST")
	}
	return b, nil
}

// ---- tokens ----

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// signer returns the signing key. Lock held.
func (s *Service) signer() *rsa.PrivateKey {
	if s.key == nil {
		if k, err := x509.ParsePKCS8PrivateKey(s.st.Key); err == nil {
			s.key = k.(*rsa.PrivateKey)
		} else {
			s.key, _ = rsa.GenerateKey(rand.Reader, 2048)
			s.st.Key, _ = x509.MarshalPKCS8PrivateKey(s.key)
		}
	}
	return s.key
}

func (s *Service) sign(claims map[string]any) string {
	h, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": kid})
	pl, _ := json.Marshal(claims)
	in := b64(h) + "." + b64(pl)
	sum := sha256.Sum256([]byte(in))
	sig, _ := rsa.SignPKCS1v15(rand.Reader, s.signer(), crypto.SHA256, sum[:])
	return in + "." + b64(sig)
}

// parse checks the signature and expiry of an ID token of this emulator and returns its claims.
func (s *Service) parse(token string) (map[string]any, error) {
	seg := strings.Split(token, ".")
	if len(seg) != 3 {
		return nil, bad("INVALID_ID_TOKEN")
	}
	sig, err := base64.RawURLEncoding.DecodeString(seg[2])
	sum := sha256.Sum256([]byte(seg[0] + "." + seg[1]))
	if err != nil || rsa.VerifyPKCS1v15(&s.signer().PublicKey, crypto.SHA256, sum[:], sig) != nil {
		return nil, bad("INVALID_ID_TOKEN")
	}
	claims, ok := decodeSegment(seg[1])
	if !ok {
		return nil, bad("INVALID_ID_TOKEN")
	}
	if exp, _ := claims["exp"].(float64); int64(exp) < s.now().Unix() {
		return nil, bad("TOKEN_EXPIRED")
	}
	return claims, nil
}

func decodeSegment(seg string) (map[string]any, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(seg)
	m := map[string]any{}
	return m, err == nil && json.Unmarshal(raw, &m) == nil
}

var reserved = []string{"acr", "amr", "at_hash", "aud", "auth_time", "azp", "cnf", "c_hash", "exp", "firebase", "iat", "iss", "jti", "nbf", "nonce", "sub"}

// issue makes an ID token and a refresh token for a signing-in user. Lock held.
// provider is firebase.sign_in_provider; extra are a custom token's developer claims.
func (s *Service) issue(pid string, p *project, u *User, provider string, extra map[string]any) map[string]any {
	now := s.now()
	claims := map[string]any{}
	for _, m := range []map[string]any{extra, customClaims(u)} {
		for k, v := range m {
			if !slices.Contains(reserved, k) {
				claims[k] = v
			}
		}
	}
	ids := map[string]any{}
	if u.Email != "" {
		ids["email"] = []string{u.Email}
	}
	if u.Phone != "" {
		ids["phone"] = []string{u.Phone}
	}
	for _, pr := range u.Providers {
		if pr.ID != "password" && pr.ID != "phone" {
			ids[pr.ID] = []string{pr.RawID}
		}
	}
	claims["iss"], claims["aud"] = "https://securetoken.google.com/"+pid, pid
	claims["auth_time"], claims["iat"], claims["exp"] = now.Unix(), now.Unix(), now.Add(tokenTTL).Unix()
	claims["sub"], claims["user_id"] = u.ID, u.ID
	for k, v := range map[string]string{"email": u.Email, "phone_number": u.Phone, "name": u.DisplayName, "picture": u.PhotoURL} {
		if v != "" {
			claims[k] = v
		}
	}
	if u.Email != "" {
		claims["email_verified"] = u.EmailVerified
	}
	claims["firebase"] = map[string]any{"identities": ids, "sign_in_provider": provider}
	rt := "AMf-vB" + core.RandHex(32)
	p.Refresh[rt] = &refresh{UID: u.ID, Provider: provider, Claims: extra, Issued: now.Unix()}
	return map[string]any{"idToken": s.sign(claims), "refreshToken": rt, "expiresIn": strconv.Itoa(int(tokenTTL.Seconds()))}
}

func customClaims(u *User) map[string]any {
	m := map[string]any{}
	json.Unmarshal([]byte(u.Claims), &m)
	return m
}

// ---- users ----

func (u *User) info() map[string]any {
	m := map[string]any{"localId": u.ID, "createdAt": strconv.FormatInt(u.Created, 10), "validSince": strconv.FormatInt(u.ValidSince, 10),
		"emailVerified": u.EmailVerified, "disabled": u.Disabled, "lastLoginAt": strconv.FormatInt(u.LastLogin, 10)}
	for k, v := range map[string]string{"email": u.Email, "displayName": u.DisplayName, "photoUrl": u.PhotoURL, "phoneNumber": u.Phone, "customAttributes": u.Claims} {
		if v != "" {
			m[k] = v
		}
	}
	if u.LastRefresh != 0 {
		m["lastRefreshAt"] = time.UnixMilli(u.LastRefresh).UTC().Format(time.RFC3339Nano)
	}
	var pi []map[string]string
	for _, p := range u.Providers {
		e := map[string]string{"providerId": p.ID, "rawId": p.RawID, "federatedId": p.RawID}
		for k, v := range map[string]string{"email": p.Email, "displayName": p.DisplayName, "photoUrl": p.PhotoURL} {
			if v != "" {
				e[k] = v
			}
		}
		if p.ID == "phone" {
			e["phoneNumber"] = p.RawID
		}
		pi = append(pi, e)
	}
	if pi != nil {
		m["providerUserInfo"] = pi
	}
	if u.Hash != "" {
		m["passwordUpdatedAt"] = float64(u.ValidSince * 1000)
	}
	return m
}

func hash(salt, password string) string {
	h := sha256.Sum256([]byte(salt + password))
	return hex.EncodeToString(h[:])
}

func (u *User) setPassword(pw string, now time.Time) {
	u.Salt = core.RandHex(8)
	u.Hash = hash(u.Salt, pw)
	u.ValidSince = now.Unix() // earlier sessions are revoked
}

func (u *User) checkPassword(pw string) bool {
	return u.Hash != "" && subtle.ConstantTimeCompare([]byte(u.Hash), []byte(hash(u.Salt, pw))) == 1
}

func (u *User) addProvider(p Provider) {
	if i := slices.IndexFunc(u.Providers, func(x Provider) bool { return x.ID == p.ID }); i >= 0 {
		u.Providers[i] = p
		return
	}
	u.Providers = append(u.Providers, p)
}

func (u *User) dropProvider(id string) {
	u.Providers = slices.DeleteFunc(u.Providers, func(p Provider) bool { return p.ID == id })
}

const idAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func newUID() string {
	b := make([]byte, 28)
	rand.Read(b)
	for i := range b {
		b[i] = idAlphabet[int(b[i])%len(idAlphabet)]
	}
	return string(b)
}

func validEmail(e string) bool {
	at := strings.LastIndexByte(e, '@')
	return at > 0 && at < len(e)-1 && !strings.ContainsAny(e, " \t")
}

func checkPassword(pw string) error {
	if len(pw) < 6 {
		return bad("WEAK_PASSWORD : Password should be at least 6 characters")
	}
	return nil
}

func checkClaims(s string) error {
	m := map[string]any{}
	if s != "" && (json.Unmarshal([]byte(s), &m) != nil || len(s) > 1000) {
		return bad("INVALID_CLAIMS")
	}
	return nil
}

// byEmail finds a user; email is lower-cased as Firebase does.
func (p *project) byEmail(email string) *User {
	for _, u := range p.Users {
		if u.Email != "" && u.Email == strings.ToLower(email) {
			return u
		}
	}
	return nil
}

func (p *project) byProvider(id, raw string) *User {
	for _, u := range p.Users {
		if slices.ContainsFunc(u.Providers, func(x Provider) bool { return x.ID == id && x.RawID == raw }) {
			return u
		}
	}
	return nil
}

func (p *project) byPhone(phone string) *User {
	for _, u := range p.Users {
		if u.Phone != "" && u.Phone == phone {
			return u
		}
	}
	return nil
}

func (p *project) sorted() []*User {
	out := make([]*User, 0, len(p.Users))
	for _, id := range core.SortedKeys(p.Users) {
		out = append(out, p.Users[id])
	}
	return out
}

// create makes an account from sign-up, Admin create or batch create fields.
func (s *Service) create(p *project, in body) (*User, error) {
	now := s.now()
	u := &User{ID: in.str("localId"), Created: now.UnixMilli(), ValidSince: now.Unix(), DisplayName: in.str("displayName"), PhotoURL: in.str("photoUrl"),
		EmailVerified: in.flag("emailVerified"), Disabled: in.flag("disabled"), Claims: in.str("customAttributes")}
	if u.ID == "" {
		u.ID = newUID()
	}
	switch {
	case len(u.ID) > 128:
		return nil, bad("INVALID_LOCAL_ID : The local ID must be at most 128 characters long.")
	case p.Users[u.ID] != nil:
		return nil, bad("DUPLICATE_LOCAL_ID")
	}
	if err := checkClaims(u.Claims); err != nil {
		return nil, err
	}
	if e := in.str("email"); e != "" {
		if !validEmail(e) {
			return nil, bad("INVALID_EMAIL")
		}
		if u.Email = strings.ToLower(e); p.byEmail(u.Email) != nil {
			return nil, bad("EMAIL_EXISTS")
		}
	}
	if pw := cmpOr(in.str("password"), in.str("rawPassword")); pw != "" {
		if u.Email == "" {
			return nil, bad("MISSING_EMAIL")
		}
		if err := checkPassword(pw); err != nil {
			return nil, err
		}
		u.setPassword(pw, now)
	}
	if u.Email != "" {
		u.addProvider(Provider{ID: "password", RawID: u.Email, Email: u.Email, DisplayName: u.DisplayName, PhotoURL: u.PhotoURL})
	}
	if ph := in.str("phoneNumber"); ph != "" {
		if !strings.HasPrefix(ph, "+") {
			return nil, bad("INVALID_PHONE_NUMBER : Invalid format.")
		}
		if p.byPhone(ph) != nil {
			return nil, bad("PHONE_NUMBER_EXISTS")
		}
		u.Phone = ph
		u.addProvider(Provider{ID: "phone", RawID: ph})
	}
	p.Users[u.ID] = u
	return u, nil
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// ---- routing ----

// adminPath splits /emulator/v1/projects/{p}/{what}, for what in accounts, oobCodes and config.
func adminPath(r *http.Request) (project, what string, ok bool) {
	rest, ok := strings.CutPrefix(r.URL.Path, emulatorBase)
	project, what, _ = strings.Cut(rest, "/")
	return project, what, ok && project != "" && (what == "accounts" || what == "oobCodes" || what == "config")
}

// admin serves the emulator's REST extras.
func (s *Service) admin(w http.ResponseWriter, r *http.Request) {
	pid, what, _ := adminPath(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case what == "accounts" && r.Method == http.MethodDelete:
		delete(s.st.Projects, pid)
		writeJSON(w, http.StatusOK, map[string]any{})
	case what == "oobCodes" && r.Method == http.MethodGet:
		codes := []OOB{}
		if p := s.project(pid, false); p != nil {
			codes = slices.Clone(p.OOB)
		}
		writeJSON(w, http.StatusOK, map[string]any{"oobCodes": codes})
	case what == "config" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"signIn": map[string]any{"allowDuplicateEmails": false}, "usageMode": "DEFAULT"})
	default:
		writeErr(w, nil, &apiError{http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED"})
	}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.hub != nil && s.hub.CORS(w, r) { // routed by Match, so the gateway leaves CORS to us
		return
	}
	c := core.C(r)
	switch p := r.URL.Path; {
	case p == certPath:
		c.Op = "Certificates"
		s.mu.Lock()
		defer s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]string{kid: s.cert()})
	case p == secureToken+"v1/token":
		c.Op = "Token"
		s.serve(w, r, c, s.token)
	default:
		rest, _ := strings.CutPrefix(strings.TrimPrefix(p, toolkit), "v1")
		pid := ""
		if after, ok := strings.CutPrefix(rest, "/projects/"); ok {
			pid, rest, _ = strings.Cut(after, "/")
			rest = "/" + rest
		}
		op, ok := strings.CutPrefix(rest, "/accounts")
		h := s.ops()[strings.TrimPrefix(op, ":")]
		if !ok || h == nil || (op != "" && op[0] != ':') || (op == "" && pid == "") {
			c.Op = "Unknown"
			writeErr(w, c, &apiError{http.StatusNotImplemented, "pogi's Firebase Auth does not implement " + r.Method + " " + p})
			return
		}
		c.Op = cmpOr(strings.TrimPrefix(op, ":"), "createUser")
		s.serve(w, r, c, func(r *http.Request, in body) (any, error) { return h(s, r, pid, in) })
	}
}

// serve decodes the request, runs h under the lock and writes its result.
func (s *Service) serve(w http.ResponseWriter, r *http.Request, c *core.Ctx, h func(*http.Request, body) (any, error)) {
	in, err := readBody(r)
	if err == nil {
		err = c.Intercept()
	}
	if err != nil {
		writeErr(w, c, err)
		return
	}
	for k, v := range r.URL.Query() { // GET parameters (batchGet) and the key
		if _, ok := in[k]; !ok {
			in[k] = v[0]
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out, err := h(r, in)
	if err != nil {
		writeErr(w, c, err)
		return
	}
	s.hub.MarkDirty()
	writeJSON(w, http.StatusOK, out)
}

// cert is a self-signed certificate of the signing key, as Google publishes its keys. Lock held.
func (s *Service) cert() string {
	k := s.signer()
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "securetoken@system.gserviceaccount.com"},
		NotBefore: time.Now().Add(-24 * time.Hour), NotAfter: time.Now().AddDate(10, 0, 0)}
	der, _ := x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

type op func(s *Service, r *http.Request, project string, in body) (any, error)

func (s *Service) ops() map[string]op {
	return map[string]op{
		"":                      (*Service).adminCreate,
		"signUp":                (*Service).signUp,
		"signInWithPassword":    (*Service).signInPassword,
		"signInWithCustomToken": (*Service).signInCustom,
		"signInWithIdp":         (*Service).signInIdp,
		"lookup":                (*Service).lookup,
		"update":                (*Service).update,
		"delete":                (*Service).deleteUser,
		"batchDelete":           (*Service).batchDelete,
		"batchCreate":           (*Service).batchCreate,
		"batchGet":              (*Service).batchGet,
		"sendOobCode":           (*Service).sendOob,
		"resetPassword":         (*Service).resetPassword,
	}
}

// target resolves the project (path, else the ID token's, else the default) and
// the user a call is about: the ID token's (a client) or localId (the Admin SDK). Lock held.
func (s *Service) target(pid string, in body, create bool) (string, *project, *User, error) {
	var uid string
	if tok := in.str("idToken"); tok != "" {
		claims, err := s.parse(tok)
		if err != nil {
			return "", nil, nil, err
		}
		aud, _ := claims["aud"].(string)
		sub, _ := claims["sub"].(string)
		if pid != "" && pid != aud {
			return "", nil, nil, bad("INVALID_ID_TOKEN")
		}
		pid, uid = aud, sub
		p := s.project(pid, create)
		if p == nil || p.Users[uid] == nil {
			return "", nil, nil, bad("USER_NOT_FOUND")
		}
		if iat, _ := claims["iat"].(float64); int64(iat) < p.Users[uid].ValidSince {
			return "", nil, nil, bad("TOKEN_EXPIRED")
		}
		return pid, p, p.Users[uid], nil
	}
	pid = cmpOr(pid, core.DefaultProject)
	p := s.project(pid, create)
	if ids := in.strs("localId"); len(ids) == 1 && p != nil {
		return pid, p, p.Users[ids[0]], nil
	}
	return pid, p, nil, nil
}

// signedIn finishes a sign-in: stamps the login and issues the tokens.
func (s *Service) signedIn(pid string, p *project, u *User, provider string, extra map[string]any, kind string, fields map[string]any) (any, error) {
	if u.Disabled {
		return nil, bad("USER_DISABLED")
	}
	u.LastLogin = s.now().UnixMilli()
	out := s.issue(pid, p, u, provider, extra)
	out["kind"], out["localId"] = kind, u.ID
	if u.Email != "" {
		out["email"] = u.Email
	}
	if u.DisplayName != "" {
		out["displayName"] = u.DisplayName
	}
	for k, v := range fields {
		out[k] = v
	}
	return out, nil
}

// ---- accounts ----

func (s *Service) adminCreate(_ *http.Request, pid string, in body) (any, error) {
	p := s.project(pid, true)
	u, err := s.create(p, in)
	if err != nil {
		return nil, err
	}
	return map[string]any{"kind": "identitytoolkit#SignupNewUserResponse", "localId": u.ID}, nil
}

func (s *Service) signUp(_ *http.Request, pid string, in body) (any, error) {
	pid = cmpOr(pid, core.DefaultProject)
	p := s.project(pid, true)
	provider := "password"
	if in.str("email") == "" && in.str("password") == "" { // anonymous
		provider = "anonymous"
	} else if in.str("email") == "" {
		return nil, bad("MISSING_EMAIL")
	} else if in.str("password") == "" {
		return nil, bad("MISSING_PASSWORD")
	}
	u, err := s.create(p, in)
	if err != nil {
		return nil, err
	}
	return s.signedIn(pid, p, u, provider, nil, "identitytoolkit#SignupNewUserResponse", nil)
}

func (s *Service) signInPassword(_ *http.Request, pid string, in body) (any, error) {
	pid = cmpOr(pid, core.DefaultProject)
	p := s.project(pid, true)
	switch {
	case in.str("email") == "":
		return nil, bad("MISSING_EMAIL")
	case in.str("password") == "":
		return nil, bad("MISSING_PASSWORD")
	}
	u := p.byEmail(in.str("email"))
	if u == nil {
		return nil, bad("EMAIL_NOT_FOUND")
	}
	if !u.checkPassword(in.str("password")) {
		return nil, bad("INVALID_PASSWORD")
	}
	return s.signedIn(pid, p, u, "password", nil, "identitytoolkit#VerifyPasswordResponse", map[string]any{"registered": true})
}

// signInCustom accepts the custom tokens the Admin SDKs make. In emulator mode they are
// unsigned (alg none); with a service account they are RS256.
// ponytail: a signed custom token is accepted without checking its signature against the
// service account's certificate; wire the IAM service's keys in when one exists.
func (s *Service) signInCustom(_ *http.Request, pid string, in body) (any, error) {
	pid = cmpOr(pid, core.DefaultProject)
	seg := strings.Split(in.str("token"), ".")
	if len(seg) != 3 {
		return nil, bad("INVALID_CUSTOM_TOKEN : First argument to signInWithCustomToken must be a valid Firebase custom token.")
	}
	hdr, ok1 := decodeSegment(seg[0])
	claims, ok2 := decodeSegment(seg[1])
	uid, _ := claims["uid"].(string)
	if alg, _ := hdr["alg"].(string); !ok1 || !ok2 || (alg != "none" && alg != "RS256") || uid == "" || len(uid) > 128 || claims["aud"] != customAud {
		return nil, bad("INVALID_CUSTOM_TOKEN : The custom token format is incorrect or the token is invalid for some reason.")
	}
	if exp, ok := claims["exp"].(float64); ok && int64(exp) < s.now().Unix() {
		return nil, bad("INVALID_CUSTOM_TOKEN : The custom token has expired.")
	}
	p := s.project(pid, true)
	u, isNew := p.Users[uid], false
	if u == nil {
		now := s.now()
		u, isNew = &User{ID: uid, Created: now.UnixMilli(), ValidSince: now.Unix()}, true
		p.Users[uid] = u
	}
	dev, _ := claims["claims"].(map[string]any)
	return s.signedIn(pid, p, u, "custom", dev, "identitytoolkit#VerifyCustomTokenResponse", map[string]any{"isNewUser": isNew})
}

// signInIdp signs in with the emulator's fake IdP tokens: postBody carries
// id_token=<JSON or JWT with sub, email, ...>&providerId=google.com.
// ponytail: no real IdP is contacted; an existing account with the same email is signed in.
func (s *Service) signInIdp(_ *http.Request, pid string, in body) (any, error) {
	pid = cmpOr(pid, core.DefaultProject)
	post, err := url.ParseQuery(in.str("postBody"))
	provider, tok := post.Get("providerId"), post.Get("id_token")
	if err != nil || provider == "" || tok == "" {
		return nil, bad("INVALID_IDP_RESPONSE : Unable to parse the IdP response.")
	}
	info := map[string]any{}
	if seg := strings.Split(tok, "."); len(seg) == 3 {
		info, _ = decodeSegment(seg[1])
	} else if json.Unmarshal([]byte(tok), &info) != nil {
		return nil, bad("INVALID_IDP_RESPONSE : The id_token is neither JSON nor a JWT.")
	}
	str := func(k ...string) string {
		for _, n := range k {
			if v, ok := info[n].(string); ok && v != "" {
				return v
			}
		}
		return ""
	}
	raw, email := str("sub", "user_id", "id", "localId"), strings.ToLower(str("email"))
	if raw == "" {
		return nil, bad("INVALID_IDP_RESPONSE : The id_token has no sub.")
	}
	p := s.project(pid, true)
	u, isNew := p.byProvider(provider, raw), false
	if u == nil && email != "" {
		u = p.byEmail(email)
	}
	if u == nil {
		now := s.now()
		u, isNew = &User{ID: newUID(), Created: now.UnixMilli(), ValidSince: now.Unix(), Email: email, DisplayName: str("name", "displayName"),
			PhotoURL: str("picture", "photoUrl")}, true
		u.EmailVerified, _ = info["email_verified"].(bool)
		p.Users[u.ID] = u
	}
	u.addProvider(Provider{ID: provider, RawID: raw, Email: email, DisplayName: str("name", "displayName"), PhotoURL: str("picture", "photoUrl")})
	raws, _ := json.Marshal(info)
	return s.signedIn(pid, p, u, provider, nil, "identitytoolkit#VerifyAssertionResponse", map[string]any{"federatedId": raw, "providerId": provider,
		"emailVerified": u.EmailVerified, "isNewUser": isNew, "rawUserInfo": string(raws), "photoUrl": u.PhotoURL})
}

// lookup finds the signed-in user (idToken) or the users the Admin SDK asks for by localId, email, phoneNumber or federatedUserId.
func (s *Service) lookup(_ *http.Request, pid string, in body) (any, error) {
	var users []map[string]any
	if in.str("idToken") != "" {
		_, _, u, err := s.target(pid, in, false)
		if err != nil {
			return nil, err
		}
		users = append(users, u.info())
	} else if p := s.project(cmpOr(pid, core.DefaultProject), false); p != nil {
		emails, phones, ids := lower(in.strs("email")), in.strs("phoneNumber"), in.strs("localId")
		fed, _ := in["federatedUserId"].([]any)
		for _, x := range p.sorted() {
			hit := slices.Contains(ids, x.ID) || x.Email != "" && slices.Contains(emails, x.Email) || x.Phone != "" && slices.Contains(phones, x.Phone)
			for _, f := range fed {
				fm, _ := f.(map[string]any)
				hit = hit || p.byProvider(fmt.Sprint(fm["providerId"]), fmt.Sprint(fm["rawId"])) == x
			}
			if hit {
				users = append(users, x.info())
			}
		}
	}
	out := map[string]any{"kind": "identitytoolkit#GetAccountInfoResponse"}
	if users != nil {
		out["users"] = users
	}
	return out, nil
}

func lower(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = strings.ToLower(s)
	}
	return out
}

func (s *Service) update(_ *http.Request, pid string, in body) (any, error) {
	if code := in.str("oobCode"); code != "" {
		return s.applyCode(pid, code)
	}
	pid, p, u, err := s.target(pid, in, false)
	if err != nil {
		return nil, err
	}
	if u == nil {
		return nil, bad("USER_NOT_FOUND")
	}
	now := s.now()
	revoke := false
	if _, ok := in["email"]; ok {
		e := strings.ToLower(in.str("email"))
		switch {
		case !validEmail(e):
			return nil, bad("INVALID_EMAIL")
		case p.byEmail(e) != nil && p.byEmail(e) != u:
			return nil, bad("EMAIL_EXISTS")
		}
		if e != u.Email && !in.flag("emailVerified") {
			u.EmailVerified = false
		}
		u.Email = e
		u.addProvider(Provider{ID: "password", RawID: e, Email: e})
	}
	if pw := in.str("password"); pw != "" {
		if err := checkPassword(pw); err != nil {
			return nil, err
		}
		if u.Email == "" {
			return nil, bad("MISSING_EMAIL")
		}
		u.setPassword(pw, now)
		u.addProvider(Provider{ID: "password", RawID: u.Email, Email: u.Email})
		revoke = true
	}
	if v, ok := in["emailVerified"]; ok {
		u.EmailVerified, _ = v.(bool)
	}
	if v, ok := in["disableUser"]; ok {
		u.Disabled, _ = v.(bool)
	}
	for _, k := range []string{"displayName", "photoUrl"} {
		if v, ok := in[k]; ok {
			str, _ := v.(string)
			if k == "displayName" {
				u.DisplayName = str
			} else {
				u.PhotoURL = str
			}
		}
	}
	if v, ok := in["customAttributes"]; ok {
		cs, _ := v.(string)
		if err := checkClaims(cs); err != nil {
			return nil, err
		}
		u.Claims = cs
	}
	if ph := in.str("phoneNumber"); ph != "" {
		if !strings.HasPrefix(ph, "+") {
			return nil, bad("INVALID_PHONE_NUMBER : Invalid format.")
		}
		if o := p.byPhone(ph); o != nil && o != u {
			return nil, bad("PHONE_NUMBER_EXISTS")
		}
		u.Phone = ph
		u.addProvider(Provider{ID: "phone", RawID: ph})
	}
	if vs := in.num("validSince"); vs != 0 {
		u.ValidSince = vs
	}
	for _, a := range in.strs("deleteAttribute") {
		switch a {
		case "DISPLAY_NAME":
			u.DisplayName = ""
		case "PHOTO_URL":
			u.PhotoURL = ""
		case "PASSWORD":
			u.Hash, u.Salt = "", ""
			u.dropProvider("password")
		case "EMAIL":
			u.Email, u.EmailVerified, u.Hash, u.Salt = "", false, "", ""
			u.dropProvider("password")
		}
	}
	for _, id := range in.strs("deleteProvider") {
		if id == "phone" {
			u.Phone = ""
		}
		u.dropProvider(id)
	}
	if l := in.obj("linkProviderUserInfo"); l != nil {
		u.addProvider(Provider{ID: l.str("providerId"), RawID: l.str("rawId"), Email: l.str("email"), DisplayName: l.str("displayName"), PhotoURL: l.str("photoUrl")})
	}
	out := map[string]any{"kind": "identitytoolkit#SetAccountInfoResponse"}
	for k, v := range u.info() {
		switch k {
		case "localId", "email", "displayName", "photoUrl", "emailVerified", "providerUserInfo":
			out[k] = v
		}
	}
	if (revoke || in.flag("returnSecureToken")) && in.str("idToken") != "" { // a client that changed its password gets fresh tokens
		for k, v := range s.issue(pid, p, u, "password", nil) {
			out[k] = v
		}
	}
	return out, nil
}

func (s *Service) deleteUser(_ *http.Request, pid string, in body) (any, error) {
	_, p, u, err := s.target(pid, in, false)
	if err != nil {
		return nil, err
	}
	if u == nil {
		return nil, bad("USER_NOT_FOUND")
	}
	p.remove(u.ID)
	return map[string]any{"kind": "identitytoolkit#DeleteAccountResponse"}, nil
}

func (p *project) remove(uid string) {
	delete(p.Users, uid)
	for t, r := range p.Refresh {
		if r.UID == uid {
			delete(p.Refresh, t)
		}
	}
}

func (s *Service) batchDelete(_ *http.Request, pid string, in body) (any, error) {
	if p := s.project(cmpOr(pid, core.DefaultProject), false); p != nil {
		for _, id := range in.strs("localIds") {
			p.remove(id) // users that are gone count as deleted
		}
	}
	return map[string]any{}, nil
}

func (s *Service) batchGet(_ *http.Request, pid string, in body) (any, error) {
	p := s.project(cmpOr(pid, core.DefaultProject), false)
	var all []map[string]any
	if p != nil {
		for _, u := range p.sorted() {
			all = append(all, u.info())
		}
	}
	size := int32(in.num("maxResults"))
	if size < 0 {
		return nil, bad("Invalid value for maxResults: must be 0 or more")
	}
	if size == 0 {
		size = 20
	}
	page, next := gapi.Page(all, size, in.str("nextPageToken"))
	out := map[string]any{"kind": "identitytoolkit#DownloadAccountResponse"}
	if len(page) > 0 {
		out["users"] = page
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return out, nil
}

// batchCreate imports users. ponytail: password hashes (passwordHash, salt and a hash algorithm) are not imported, rawPassword is.
func (s *Service) batchCreate(_ *http.Request, pid string, in body) (any, error) {
	p := s.project(cmpOr(pid, core.DefaultProject), true)
	var errs []map[string]any
	users, _ := in["users"].([]any)
	for i, raw := range users {
		m, _ := raw.(map[string]any)
		u, err := s.create(p, m)
		if err != nil {
			errs = append(errs, map[string]any{"index": i, "message": err.Error()})
			continue
		}
		if c := body(m).num("createdAt"); c != 0 {
			u.Created = c
		}
	}
	out := map[string]any{"kind": "identitytoolkit#UploadAccountResponse"}
	if errs != nil {
		out["error"] = errs
	}
	return out, nil
}

// ---- OOB codes ----

var oobModes = map[string]string{"PASSWORD_RESET": "resetPassword", "VERIFY_EMAIL": "verifyEmail", "EMAIL_SIGNIN": "signIn", "VERIFY_AND_CHANGE_EMAIL": "verifyAndChangeEmail"}

func (s *Service) sendOob(r *http.Request, pid string, in body) (any, error) {
	typ := in.str("requestType")
	mode := oobModes[typ]
	if mode == "" {
		return nil, bad("INVALID_REQ_TYPE : Unsupported request parameter.")
	}
	_, p, u, err := s.target(pid, in, true)
	if err != nil {
		return nil, err
	}
	email := strings.ToLower(in.str("email"))
	if typ == "VERIFY_EMAIL" && u != nil {
		email = u.Email
	}
	switch {
	case email == "":
		return nil, bad("MISSING_EMAIL")
	case typ == "PASSWORD_RESET" && p.byEmail(email) == nil:
		return nil, bad("EMAIL_NOT_FOUND")
	}
	code := core.RandHex(16)
	link := fmt.Sprintf("http://%s/emulator/action?mode=%s&lang=en&oobCode=%s&apiKey=fake-api-key", r.Host, mode, code)
	if c := in.str("continueUrl"); c != "" {
		link += "&continueUrl=" + url.QueryEscape(c)
	}
	p.OOB = append(p.OOB, OOB{Email: email, Type: typ, Code: code, Link: link, Time: s.now()})
	out := map[string]any{"kind": "identitytoolkit#GetOobConfirmationCodeResponse", "email": email}
	if in.flag("returnOobLink") {
		out["oobLink"] = link
	}
	return out, nil
}

// findCode looks an OOB code up in the project (or in all of them without one). Lock held.
func (s *Service) findCode(pid, code, typ string) (*project, int) {
	for id, p := range s.st.Projects {
		if pid != "" && id != pid {
			continue
		}
		if i := slices.IndexFunc(p.OOB, func(o OOB) bool { return o.Code == code && o.Type == typ }); i >= 0 {
			return p, i
		}
	}
	return nil, -1
}

// resetPassword checks a PASSWORD_RESET code and, given newPassword, sets the password.
func (s *Service) resetPassword(_ *http.Request, pid string, in body) (any, error) {
	p, i := s.findCode(pid, in.str("oobCode"), "PASSWORD_RESET")
	if p == nil {
		return nil, bad("INVALID_OOB_CODE")
	}
	o := p.OOB[i]
	if pw := in.str("newPassword"); pw != "" {
		if err := checkPassword(pw); err != nil {
			return nil, err
		}
		u := p.byEmail(o.Email)
		if u == nil {
			return nil, bad("EMAIL_NOT_FOUND")
		}
		u.setPassword(pw, s.now())
		u.addProvider(Provider{ID: "password", RawID: u.Email, Email: u.Email})
		p.OOB = slices.Delete(p.OOB, i, i+1)
	}
	return map[string]any{"kind": "identitytoolkit#ResetPasswordResponse", "email": o.Email, "requestType": "PASSWORD_RESET"}, nil
}

// applyCode confirms a VERIFY_EMAIL code (accounts:update with an oobCode).
func (s *Service) applyCode(pid, code string) (any, error) {
	p, i := s.findCode(pid, code, "VERIFY_EMAIL")
	if p == nil {
		return nil, bad("INVALID_OOB_CODE")
	}
	o := p.OOB[i]
	u := p.byEmail(o.Email)
	if u == nil {
		return nil, bad("EMAIL_NOT_FOUND")
	}
	u.EmailVerified = true
	p.OOB = slices.Delete(p.OOB, i, i+1)
	return map[string]any{"kind": "identitytoolkit#SetAccountInfoResponse", "email": u.Email, "emailVerified": true, "localId": u.ID}, nil
}

// ---- securetoken ----

// token exchanges a refresh token for a new ID token (POST /securetoken.googleapis.com/v1/token).
func (s *Service) token(_ *http.Request, in body) (any, error) {
	if in.str("grant_type") != "refresh_token" {
		return nil, bad("INVALID_GRANT_TYPE")
	}
	rt := in.str("refresh_token")
	if rt == "" {
		return nil, bad("MISSING_REFRESH_TOKEN")
	}
	for pid, p := range s.st.Projects {
		ref := p.Refresh[rt]
		if ref == nil {
			continue
		}
		u := p.Users[ref.UID]
		switch {
		case u == nil:
			return nil, bad("USER_NOT_FOUND")
		case u.Disabled:
			return nil, bad("USER_DISABLED")
		case ref.Issued < u.ValidSince:
			return nil, bad("TOKEN_EXPIRED")
		}
		u.LastRefresh = s.now().UnixMilli()
		out := s.issue(pid, p, u, ref.Provider, ref.Claims)
		delete(p.Refresh, rt) // issue made a new one; the old one is spent
		return map[string]any{"access_token": out["idToken"], "id_token": out["idToken"], "refresh_token": out["refreshToken"],
			"expires_in": out["expiresIn"], "token_type": "Bearer", "user_id": u.ID, "project_id": pid}, nil
	}
	return nil, bad("INVALID_REFRESH_TOKEN")
}

// ---- inbox and console ----

// Mail is a message the emulator would have sent: the console's inbox lists them.
type Mail struct {
	Time                    time.Time
	To, Kind, Subject, Link string
}

// Outbox returns the OOB messages of all projects, oldest first.
func (s *Service) Outbox() []Mail {
	s.mu.Lock()
	defer s.mu.Unlock()
	subjects := map[string]string{"PASSWORD_RESET": "Reset your password", "VERIFY_EMAIL": "Verify your email address",
		"EMAIL_SIGNIN": "Sign in", "VERIFY_AND_CHANGE_EMAIL": "Verify your new email address"}
	var out []Mail
	for _, id := range core.SortedKeys(s.st.Projects) {
		for _, o := range s.st.Projects[id].OOB {
			out = append(out, Mail{o.Time, o.Email, o.Type, subjects[o.Type], o.Link})
		}
	}
	slices.SortStableFunc(out, func(a, b Mail) int { return a.Time.Compare(b.Time) })
	return out
}

// ClearOutbox drops every pending OOB code, so their links stop working (the console's "Clear inbox").
func (s *Service) ClearOutbox() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.st.Projects {
		p.OOB = nil
	}
}

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Users", Columns: []string{"User", "Project", "Email", "Providers", "Disabled", "Created"}}
	for _, pid := range core.SortedKeys(s.st.Projects) {
		for _, u := range s.st.Projects[pid].sorted() {
			var ids []string
			for _, p := range u.Providers {
				ids = append(ids, p.ID)
			}
			if ids == nil {
				ids = []string{"anonymous"}
			}
			t.Rows = append(t.Rows, []string{pid + "/" + u.ID, pid, u.Email, strings.Join(ids, ", "), strconv.FormatBool(u.Disabled),
				time.UnixMilli(u.Created).UTC().Format(time.DateTime)})
		}
	}
	return []core.Table{t}
}

// Inspect returns a user, without the password hash.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	pid, uid, _ := strings.Cut(id, "/")
	if p := s.project(pid, false); table == "Users" && p != nil && p.Users[uid] != nil {
		return p.Users[uid].info()
	}
	return nil
}
