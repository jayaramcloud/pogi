package gapi

import (
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// ---- the shared signing key ----

// KeyID is the kid of the shared signing key.
const KeyID = "pogi"

var (
	keyMu sync.Mutex
	key   *rsa.PrivateKey
)

// SigningKey is pogi's stand-in for Google's own ID-token key. It signs every
// ID token (OIDCToken: IAM Credentials, the token endpoint, Cloud Tasks and
// Scheduler) and its public half is served at /oauth2/v3/certs. It is made on
// first use; the IAM service keeps it across restarts with SetSigningKey.
func SigningKey() *rsa.PrivateKey {
	keyMu.Lock()
	defer keyMu.Unlock()
	if key == nil {
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			panic(err)
		}
		key = k
	}
	return key
}

// SetSigningKey installs a saved signing key.
func SetSigningKey(k *rsa.PrivateKey) {
	keyMu.Lock()
	key = k
	keyMu.Unlock()
}

// PublicKey verifies the tokens OIDCToken makes.
func PublicKey() *rsa.PublicKey { return &SigningKey().PublicKey }

// OIDCToken returns an RS256 ID token for the service account, as Google
// would send it: iss, sub, azp, email, email_verified, aud, iat and exp.
func OIDCToken(email, audience string) string {
	now := time.Now()
	return SignJWT(KeyID, SigningKey(), map[string]any{
		"iss": "https://accounts.google.com", "sub": core.RandHex(10), "azp": core.RandHex(10), "email": email,
		"email_verified": true, "aud": audience, "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()})
}

// ---- JWTs and keys ----

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// SignJWT signs claims with RS256.
func SignJWT(kid string, k *rsa.PrivateKey, claims any) string {
	enc := func(v any) string { b, _ := json.Marshal(v); return b64(b) }
	in := enc(map[string]any{"alg": "RS256", "typ": "JWT", "kid": kid}) + "." + enc(claims)
	sum := sha256.Sum256([]byte(in))
	sig, _ := rsa.SignPKCS1v15(rand.Reader, k, crypto.SHA256, sum[:])
	return in + "." + b64(sig)
}

// JWT is a parsed compact JWT whose signature has not been checked.
type JWT struct {
	Header, Claims map[string]any
	signed         string
	sig            []byte
}

// ParseJWT splits and decodes a compact JWT.
func ParseJWT(s string) (*JWT, error) {
	p := strings.Split(s, ".")
	if len(p) != 3 {
		return nil, errors.New("not a JWT")
	}
	j := &JWT{signed: p[0] + "." + p[1]}
	var err error
	if j.sig, err = base64.RawURLEncoding.DecodeString(p[2]); err != nil {
		return nil, err
	}
	for i, dst := range []*map[string]any{&j.Header, &j.Claims} {
		b, err := base64.RawURLEncoding.DecodeString(p[i])
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(b, dst); err != nil {
			return nil, err
		}
	}
	return j, nil
}

// Verify checks the RS256 signature.
func (j *JWT) Verify(pub *rsa.PublicKey) bool {
	sum := sha256.Sum256([]byte(j.signed))
	return rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], j.sig) == nil
}

// Str is a string claim ("" when absent).
func (j *JWT) Str(claim string) string { s, _ := j.Claims[claim].(string); return s }

// Exp is the exp claim as a time (zero when absent).
func (j *JWT) Exp() time.Time {
	f, _ := j.Claims["exp"].(float64)
	return time.Unix(int64(f), 0)
}

// JWK is the JSON Web Key of an RSA public key.
func JWK(kid string, pub *rsa.PublicKey) map[string]any {
	return map[string]any{"kty": "RSA", "alg": "RS256", "use": "sig", "kid": kid,
		"n": b64(pub.N.Bytes()), "e": b64(big.NewInt(int64(pub.E)).Bytes())}
}

// Cert makes a self-signed X.509 certificate (DER) for a key, as Google
// publishes for service account keys.
func Cert(k *rsa.PrivateKey, cn string) []byte {
	now := time.Now()
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 100))
	t := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: cn},
		NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(10, 0, 0), KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, t, t, &k.PublicKey, k)
	if err != nil {
		panic(err)
	}
	return der
}

// PEM encodes DER bytes with the given block type.
func PEM(typ string, der []byte) []byte { return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}) }

// KeyDirectory is what the IAM service offers the token endpoint.
type KeyDirectory interface {
	// ServiceAccountKeys returns the enabled public keys of an enabled
	// service account by key id; nil when it doesn't exist or is disabled.
	ServiceAccountKeys(email string) map[string]*rsa.PublicKey
}

// ---- access tokens ----

const tokenPrefix = "ya29.pogi-"

// AccessToken returns an opaque ya29 access token that carries its email,
// scope and expiry, authenticated so Token reads it back. Nothing is stored.
func AccessToken(email, scope string, ttl time.Duration) string {
	b, _ := json.Marshal(map[string]any{"e": email, "s": scope, "x": time.Now().Add(ttl).Unix(), "n": core.RandHex(4)})
	return tokenPrefix + b64(b) + "." + b64(tokenMAC(b))
}

// OAuthToken returns an access token for no one in particular.
func OAuthToken() string { return AccessToken("", "", time.Hour) }

// Token is what an access token carries.
type Token struct {
	Email, Scope string
	Expires      time.Time
}

// ParseAccessToken reads a token made by AccessToken; ok is false for any
// other string, and for one that has expired.
func ParseAccessToken(s string) (t Token, ok bool) {
	body, sig, found := strings.Cut(strings.TrimPrefix(s, tokenPrefix), ".")
	b, err := base64.RawURLEncoding.DecodeString(body)
	mac, err2 := base64.RawURLEncoding.DecodeString(sig)
	if !strings.HasPrefix(s, tokenPrefix) || !found || err != nil || err2 != nil || !hmac.Equal(mac, tokenMAC(b)) {
		return t, false
	}
	var c struct {
		E, S string
		X    int64
	}
	if json.Unmarshal(b, &c) != nil || c.X < time.Now().Unix() {
		return t, false
	}
	return Token{c.E, c.S, time.Unix(c.X, 0)}, true
}

func tokenMAC(b []byte) []byte {
	k := sha256.Sum256(SigningKey().D.Bytes())
	m := hmac.New(sha256.New, k[:])
	m.Write(b)
	return m.Sum(nil)
}
