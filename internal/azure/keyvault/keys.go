package keyvault

import (
	"bytes"
	"cmp"
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"hash"
	"math/big"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

var curves = map[string]elliptic.Curve{"P-256": elliptic.P256(), "P-384": elliptic.P384(), "P-521": elliptic.P521()}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// generate makes key material: PKCS#8 DER for RSA and EC, raw bytes for oct.
// P-256K (secp256k1) is not in the standard library and is refused.
func generate(kty string, size int, crv string) ([]byte, error) {
	switch kty {
	case "RSA", "RSA-HSM":
		size = cmp.Or(size, 2048)
		if size != 2048 && size != 3072 && size != 4096 {
			return nil, badParameter("RSA key_size must be 2048, 3072 or 4096, not %d.", size)
		}
		k, err := rsa.GenerateKey(rand.Reader, size)
		if err != nil {
			return nil, err
		}
		return x509.MarshalPKCS8PrivateKey(k)
	case "EC", "EC-HSM":
		curve := curves[cmp.Or(crv, "P-256")]
		if curve == nil {
			return nil, badParameter("Curve %s is not supported: use P-256, P-384 or P-521.", crv)
		}
		k, err := ecdsa.GenerateKey(curve, rand.Reader)
		if err != nil {
			return nil, err
		}
		return x509.MarshalPKCS8PrivateKey(k)
	case "oct", "oct-HSM":
		size = cmp.Or(size, 256)
		if size != 128 && size != 192 && size != 256 {
			return nil, badParameter("oct key_size must be 128, 192 or 256, not %d.", size)
		}
		return randBytes(size / 8), nil
	}
	return nil, badParameter("Key type %q is not supported.", kty)
}

func defaultOps(kty string) []string {
	switch {
	case strings.HasPrefix(kty, "EC"):
		return []string{"sign", "verify"}
	case strings.HasPrefix(kty, "oct"):
		return []string{"encrypt", "decrypt", "wrapKey", "unwrapKey"}
	}
	return []string{"encrypt", "decrypt", "sign", "verify", "wrapKey", "unwrapKey"}
}

// private returns a key version's *rsa.PrivateKey, *ecdsa.PrivateKey or oct bytes.
func (v *Version) private() (any, error) {
	if strings.HasPrefix(v.Kty, "oct") {
		return v.Private, nil
	}
	return x509.ParsePKCS8PrivateKey(v.Private)
}

// jwk renders a key version's public JSON Web Key (oct keys show no material).
func jwk(kid string, v *Version) map[string]any {
	m := map[string]any{"kid": kid, "kty": v.Kty, "key_ops": v.Ops}
	k, _ := v.private()
	switch k := k.(type) {
	case *rsa.PrivateKey:
		m["n"], m["e"] = b64(k.N.Bytes()), b64(big.NewInt(int64(k.E)).Bytes())
	case *ecdsa.PrivateKey:
		p, _ := k.PublicKey.Bytes() // 0x04 | x | y
		n := len(p) / 2
		m["crv"], m["x"], m["y"] = k.Curve.Params().Name, b64(p[1:1+n]), b64(p[1+n:])
	}
	return m
}

// keyIn is the body of create, import and their shared fields.
type keyIn struct {
	Kty        string            `json:"kty"`
	KeySize    int               `json:"key_size"`
	Crv        string            `json:"crv"`
	KeyOps     []string          `json:"key_ops"`
	Attributes attrsIn           `json:"attributes"`
	Tags       map[string]string `json:"tags"`
	Key        *webKey           `json:"key"` // import
	HSM        bool              `json:"Hsm"`
}

// webKey is an imported JSON Web Key; binary members are base64url.
type webKey struct {
	Kty                         string
	KeyOps                      []string `json:"key_ops"`
	Crv, N, E, D, P, Q, X, Y, K string
}

// material turns an imported JWK into stored key material.
func (j *webKey) material() ([]byte, error) {
	num := func(s string) *big.Int {
		b, _ := unb64(s)
		return new(big.Int).SetBytes(b)
	}
	switch {
	case strings.HasPrefix(j.Kty, "RSA"):
		k := &rsa.PrivateKey{PublicKey: rsa.PublicKey{N: num(j.N), E: int(num(j.E).Int64())}, D: num(j.D), Primes: []*big.Int{num(j.P), num(j.Q)}}
		k.Precompute()
		if err := k.Validate(); err != nil {
			return nil, badParameter("The RSA key is not valid (n, e, d, p and q are required): %v", err)
		}
		return x509.MarshalPKCS8PrivateKey(k)
	case strings.HasPrefix(j.Kty, "EC"):
		d, _ := unb64(j.D)
		curve := curves[j.Crv]
		if curve == nil {
			return nil, badParameter("Curve %s is not supported: use P-256, P-384 or P-521.", j.Crv)
		}
		k, err := ecdsa.ParseRawPrivateKey(curve, d)
		if err != nil {
			return nil, badParameter("The EC key is not valid (crv and d are required): %v", err)
		}
		return x509.MarshalPKCS8PrivateKey(k)
	case strings.HasPrefix(j.Kty, "oct"):
		k, _ := unb64(j.K)
		if len(k) != 16 && len(k) != 24 && len(k) != 32 {
			return nil, badParameter("An oct key must be 128, 192 or 256 bits.")
		}
		return k, nil
	}
	return nil, badParameter("Key type %q is not supported.", j.Kty)
}

func (s *Service) createKey(c *call) (any, error) {
	var in keyIn
	if err := c.decode(&in); err != nil {
		return nil, err
	}
	priv, err := generate(in.Kty, in.KeySize, in.Crv)
	if err != nil {
		return nil, err
	}
	return s.addKey(c, in.Kty, priv, in.KeyOps, &in)
}

func (s *Service) importKey(c *call) (any, error) {
	var in keyIn
	if err := c.decode(&in); err != nil {
		return nil, err
	}
	if in.Key == nil {
		return nil, badParameter("The key is required.")
	}
	priv, err := in.Key.material()
	if err != nil {
		return nil, err
	}
	kty := in.Key.Kty
	if in.HSM && !strings.HasSuffix(kty, "-HSM") {
		kty += "-HSM"
	}
	return s.addKey(c, kty, priv, in.Key.KeyOps, &in)
}

func (s *Service) addKey(c *call, kty string, priv []byte, ops []string, in *keyIn) (any, error) {
	if ops == nil {
		ops = defaultOps(kty)
	}
	o, err := s.add(c.vault, "keys", c.name, &Version{Kty: kty, Ops: ops, Private: priv, Tags: in.Tags}, in.Attributes)
	if err != nil {
		return nil, err
	}
	return c.bundle(o, o.latest(), false), nil
}

// rotateKey adds a version with fresh material of the same type and size.
// ponytail: the rotation policy's expiryTime is not applied to the new version.
func (s *Service) rotateKey(c *call) (any, error) {
	o, v, err := s.find(c, false)
	if err != nil {
		return nil, err
	}
	k, err := v.private()
	if err != nil {
		return nil, err
	}
	size, crv := 0, ""
	switch k := k.(type) {
	case *rsa.PrivateKey:
		size = k.N.BitLen()
	case *ecdsa.PrivateKey:
		crv = k.Curve.Params().Name
	case []byte:
		size = len(k) * 8
	}
	priv, err := generate(v.Kty, size, crv)
	if err != nil {
		return nil, err
	}
	if o, err = s.add(c.vault, "keys", o.Name, &Version{Kty: v.Kty, Ops: v.Ops, Private: priv, Tags: v.Tags}, attrsIn{}); err != nil {
		return nil, err
	}
	return c.bundle(o, o.latest(), false), nil
}

// rotationPolicy is a key's rotation policy. pogi stores it; only RotateKey rotates.
type rotationPolicy struct {
	ID              string `json:"id,omitempty"`
	LifetimeActions []any  `json:"lifetimeActions"`
	Attributes      struct {
		ExpiryTime string `json:"expiryTime,omitempty"`
		Created    int64  `json:"created,omitempty"`
		Updated    int64  `json:"updated,omitempty"`
	} `json:"attributes"`
}

func (s *Service) getRotation(c *call) (any, error) {
	o, _, err := s.find(c, false)
	if err != nil {
		return nil, err
	}
	// Azure's default: notify 30 days before expiry.
	p := rotationPolicy{LifetimeActions: []any{map[string]any{"trigger": map[string]string{"timeBeforeExpiry": "P30D"}, "action": map[string]string{"type": "Notify"}}}}
	if o.Rotation != nil {
		p = *o.Rotation
	}
	p.ID = c.id("keys", o.Name, "rotationpolicy")
	return p, nil
}

func (s *Service) setRotation(c *call) (any, error) {
	o, _, err := s.find(c, false)
	if err != nil {
		return nil, err
	}
	var p rotationPolicy
	if err := c.decode(&p); err != nil {
		return nil, err
	}
	p.ID, p.Attributes.Created, p.Attributes.Updated = "", time.Now().Unix(), time.Now().Unix()
	if o.Rotation != nil {
		p.Attributes.Created = o.Rotation.Attributes.Created
	}
	if p.LifetimeActions == nil {
		p.LifetimeActions = []any{}
	}
	o.Rotation = &p
	return s.getRotation(c)
}

// opIn is the body of the key operations; binary members are base64url.
type opIn struct{ Alg, Value, Digest, IV, AAD, Tag string }

// keyOp runs encrypt, decrypt, sign, verify, wrapKey or unwrapKey. Disabled
// keys and operations missing from key_ops are refused; encrypt, wrapKey and
// sign also need the key to be within nbf and exp.
func (s *Service) keyOp(c *call) (any, error) {
	op := strings.ToLower(c.op[:1]) + c.op[1:] // WrapKey → wrapKey, as in key_ops
	o, v, err := s.find(c, false)
	if err != nil {
		return nil, err
	}
	now, a := time.Now().Unix(), v.Attributes
	switch {
	case !a.Enabled:
		return nil, forbidden("Operation %s is not allowed on a disabled key.", op)
	case !slices.Contains(v.Ops, op):
		return nil, forbidden("Operation %s is not permitted on this key.", op)
	case (op == "encrypt" || op == "wrapKey" || op == "sign") && (a.NotBefore > now || a.Expires != 0 && a.Expires <= now):
		return nil, forbidden("Operation %s is not allowed outside the key's validity period.", op)
	}
	var in opIn
	if err := c.decode(&in); err != nil {
		return nil, err
	}
	key, err := v.private()
	if err != nil {
		return nil, err
	}
	out, err := operate(op, key, &in)
	if err != nil {
		return nil, err
	}
	out["kid"] = c.id("keys", o.Name, v.ID)
	return out, nil
}

func unsupported(alg string) error {
	return badParameter("Algorithm %q is not supported for this key and operation.", alg)
}

// operate runs a key operation: RSA keys do them all, EC keys sign and
// verify, oct keys encrypt with AES-GCM and wrap with AES-KW.
func operate(op string, key any, in *opIn) (map[string]any, error) {
	val, err := unb64(in.Value)
	if err != nil {
		return nil, badParameter("value must be base64url.")
	}
	if op == "sign" || op == "verify" {
		digest := val
		if op == "verify" {
			digest, _ = unb64(in.Digest)
		}
		h, err := hashFor(in.Alg, key, digest)
		if err != nil {
			return nil, err
		}
		if op == "verify" {
			return map[string]any{"value": verify(in.Alg, h, key, digest, val)}, nil
		}
		sig, err := sign(in.Alg, h, key, digest)
		if err != nil {
			return nil, err
		}
		return map[string]any{"value": b64(sig)}, nil
	}
	seal := op == "encrypt" || op == "wrapKey"
	switch k := key.(type) {
	case *rsa.PrivateKey:
		res, err := rsaCrypt(in.Alg, k, val, seal)
		if err != nil {
			return nil, err
		}
		return map[string]any{"value": b64(res)}, nil
	case []byte:
		return aesCrypt(op, in, k, val, seal)
	}
	return nil, unsupported(in.Alg)
}

func rsaCrypt(alg string, k *rsa.PrivateKey, val []byte, seal bool) ([]byte, error) {
	var res []byte
	var err error
	oaep := map[string]func() hash.Hash{"RSA-OAEP": sha1.New, "RSA-OAEP-256": sha256.New}[alg]
	switch {
	case oaep != nil && seal:
		res, err = rsa.EncryptOAEP(oaep(), rand.Reader, &k.PublicKey, val, nil)
	case oaep != nil:
		res, err = rsa.DecryptOAEP(oaep(), nil, k, val, nil)
	case alg == "RSA1_5" && seal:
		res, err = rsa.EncryptPKCS1v15(rand.Reader, &k.PublicKey, val)
	case alg == "RSA1_5":
		res, err = rsa.DecryptPKCS1v15(nil, k, val)
	default:
		return nil, unsupported(alg)
	}
	if err != nil {
		return nil, badParameter("%s failed: %v", alg, err)
	}
	return res, nil
}

// aesCrypt is A128GCM/A192GCM/A256GCM for encrypt and decrypt (the IV is
// generated, as Managed HSM does) and A128KW/A192KW/A256KW for wrapKey and unwrapKey.
func aesCrypt(op string, in *opIn, k, val []byte, seal bool) (map[string]any, error) {
	wrap := op == "wrapKey" || op == "unwrapKey"
	mode := "GCM"
	if wrap {
		mode = "KW"
	}
	block, err := aes.NewCipher(k)
	if err != nil || in.Alg != "A"+strconv.Itoa(len(k)*8)+mode {
		return nil, unsupported(in.Alg)
	}
	if wrap {
		var res []byte
		ok := len(val)%8 == 0 && len(val) >= 16
		if ok && seal {
			res = keyWrap(block, val)
		} else if ok {
			res, ok = keyUnwrap(block, val)
		}
		if !ok {
			return nil, badParameter("%s failed: the value is not a valid wrapped key.", in.Alg)
		}
		return map[string]any{"value": b64(res)}, nil
	}
	gcm, _ := cipher.NewGCM(block)
	aad, _ := unb64(in.AAD)
	if seal {
		iv := randBytes(gcm.NonceSize())
		ct := gcm.Seal(nil, iv, val, aad)
		n := len(ct) - gcm.Overhead()
		out := map[string]any{"value": b64(ct[:n]), "iv": b64(iv), "tag": b64(ct[n:])}
		if len(aad) > 0 {
			out["aad"] = b64(aad)
		}
		return out, nil
	}
	iv, _ := unb64(in.IV)
	tag, _ := unb64(in.Tag)
	if len(iv) != gcm.NonceSize() {
		return nil, badParameter("iv must be %d bytes.", gcm.NonceSize())
	}
	pt, err := gcm.Open(nil, iv, append(val, tag...), aad)
	if err != nil {
		return nil, badParameter("%s failed: %v", in.Alg, err)
	}
	return map[string]any{"value": b64(pt)}, nil
}

var kwIV = []byte{0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6}

// keyWrap is the RFC 3394 AES key wrap of p (a multiple of 8 bytes, at least 16).
func keyWrap(b cipher.Block, p []byte) []byte {
	n := len(p) / 8
	a, r, buf := slices.Clone(kwIV), slices.Clone(p), make([]byte, 16)
	for j := range 6 {
		for i := range n {
			copy(buf, a)
			copy(buf[8:], r[8*i:])
			b.Encrypt(buf, buf)
			binary.BigEndian.PutUint64(a, binary.BigEndian.Uint64(buf)^uint64(n*j+i+1))
			copy(r[8*i:], buf[8:])
		}
	}
	return append(a, r...)
}

// keyUnwrap reverses keyWrap and checks the integrity value.
func keyUnwrap(b cipher.Block, w []byte) ([]byte, bool) {
	n := len(w)/8 - 1
	a, r, buf := slices.Clone(w[:8]), slices.Clone(w[8:]), make([]byte, 16)
	for j := 5; j >= 0; j-- {
		for i := n - 1; i >= 0; i-- {
			binary.BigEndian.PutUint64(buf, binary.BigEndian.Uint64(a)^uint64(n*j+i+1))
			copy(buf[8:], r[8*i:])
			b.Decrypt(buf, buf)
			copy(a, buf)
			copy(r[8*i:], buf[8:])
		}
	}
	return r, subtle.ConstantTimeCompare(a, kwIV) == 1
}

// hashFor checks a JWS algorithm (RS*, PS*, ES*) against the key and digest
// and returns its hash. ES256K (secp256k1) is not supported.
func hashFor(alg string, key any, digest []byte) (crypto.Hash, error) {
	h := map[string]crypto.Hash{"256": crypto.SHA256, "384": crypto.SHA384, "512": crypto.SHA512}[alg[min(2, len(alg)):]]
	ok := h != 0
	switch k := key.(type) {
	case *rsa.PrivateKey:
		ok = ok && (strings.HasPrefix(alg, "RS") || strings.HasPrefix(alg, "PS"))
	case *ecdsa.PrivateKey:
		ok = ok && map[string]string{"ES256": "P-256", "ES384": "P-384", "ES512": "P-521"}[alg] == k.Curve.Params().Name
	default:
		ok = false
	}
	if !ok {
		return 0, unsupported(alg)
	}
	if len(digest) != h.Size() {
		return 0, badParameter("A %s digest is %d bytes, not %d.", alg, h.Size(), len(digest))
	}
	return h, nil
}

var pss = &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash}

// sign returns an RSA signature, or an ECDSA one as JWS r||s.
func sign(alg string, h crypto.Hash, key any, digest []byte) ([]byte, error) {
	if k, ok := key.(*ecdsa.PrivateKey); ok {
		r, s, err := ecdsa.Sign(rand.Reader, k, digest)
		if err != nil {
			return nil, err
		}
		n := (k.Curve.Params().BitSize + 7) / 8
		return append(r.FillBytes(make([]byte, n)), s.FillBytes(make([]byte, n))...), nil
	}
	k := key.(*rsa.PrivateKey) // hashFor admits only RSA and EC keys
	if alg[0] == 'P' {
		return rsa.SignPSS(rand.Reader, k, h, digest, pss)
	}
	return rsa.SignPKCS1v15(nil, k, h, digest)
}

func verify(alg string, h crypto.Hash, key any, digest, sig []byte) bool {
	if k, ok := key.(*ecdsa.PrivateKey); ok {
		n := (k.Curve.Params().BitSize + 7) / 8
		return len(sig) == 2*n && ecdsa.Verify(&k.PublicKey, digest, new(big.Int).SetBytes(sig[:n]), new(big.Int).SetBytes(sig[n:]))
	}
	k := key.(*rsa.PrivateKey)
	if alg[0] == 'P' {
		return rsa.VerifyPSS(&k.PublicKey, h, digest, sig, pss) == nil
	}
	return rsa.VerifyPKCS1v15(&k.PublicKey, h, digest, sig) == nil
}

func (s *Service) rng(c *call) (any, error) {
	var in struct{ Count int }
	if err := c.decode(&in); err != nil {
		return nil, err
	}
	if in.Count < 1 || in.Count > 128 {
		return nil, badParameter("count must be between 1 and 128.")
	}
	return map[string]string{"value": b64(randBytes(in.Count))}, nil
}

// ---- certificates ----

// createCert issues a self-signed certificate at once, whatever the policy's
// issuer, and stores its key and secret under the same name and version, as
// Key Vault does. The secret is PEM or PKCS#12 (the default) per the policy.
func (s *Service) createCert(c *call) (any, error) {
	var in struct {
		Policy     json.RawMessage   `json:"policy"`
		Attributes attrsIn           `json:"attributes"`
		Tags       map[string]string `json:"tags"`
	}
	if err := c.decode(&in); err != nil {
		return nil, err
	}
	name := strings.ToLower(c.name)
	if old := s.st.Vaults[c.vault]["certificates"][name]; len(in.Policy) == 0 && old != nil {
		in.Policy = old.Policy // a new version keeps the policy
	}
	var p struct {
		KeyProps struct {
			Kty     string `json:"kty"`
			KeySize int    `json:"key_size"`
			Crv     string `json:"crv"`
		} `json:"key_props"`
		SecretProps struct {
			ContentType string `json:"contentType"`
		} `json:"secret_props"`
		X509Props struct {
			Subject        string `json:"subject"`
			ValidityMonths int    `json:"validity_months"`
			SANs           struct {
				DNSNames []string `json:"dns_names"`
			} `json:"sans"`
		} `json:"x509_props"`
	}
	if len(in.Policy) > 0 && json.Unmarshal(in.Policy, &p) != nil {
		return nil, badParameter("The certificate policy is not valid.")
	}
	if m := p.X509Props.ValidityMonths; m < 0 || m > 1200 {
		return nil, badParameter("The certificate validity_months must be between 1 and 1200.")
	}
	for _, coll := range []string{"keys", "secrets"} {
		if o := s.st.Vaults[c.vault][coll][name]; o != nil && o.Deleted != 0 {
			return nil, conflict(coll, o.Name)
		}
	}
	kty := cmp.Or(p.KeyProps.Kty, "RSA")
	if !strings.HasPrefix(kty, "RSA") && !strings.HasPrefix(kty, "EC") {
		return nil, badParameter("A certificate key must be RSA or EC.")
	}
	priv, err := generate(kty, p.KeyProps.KeySize, p.KeyProps.Crv)
	if err != nil {
		return nil, err
	}
	key, _ := x509.ParsePKCS8PrivateKey(priv)
	signer := key.(crypto.Signer)
	now := time.Now()
	tmpl := &x509.Certificate{
		Subject:     subject(cmp.Or(p.X509Props.Subject, "CN="+c.name)),
		DNSNames:    p.X509Props.SANs.DNSNames,
		NotBefore:   now,
		NotAfter:    now.AddDate(0, cmp.Or(p.X509Props.ValidityMonths, 12), 0),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	if strings.HasPrefix(kty, "RSA") {
		tmpl.KeyUsage |= x509.KeyUsageKeyEncipherment
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, signer.Public(), signer)
	if err != nil {
		return nil, err
	}
	ct := cmp.Or(p.SecretProps.ContentType, "application/x-pkcs12")
	var value string
	if ct == "application/x-pem-file" {
		value = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: priv})) + string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	} else {
		value = base64.StdEncoding.EncodeToString(pfx(der, priv))
	}
	nbf, exp := now.Unix(), tmpl.NotAfter.Unix()
	a := in.Attributes
	a.NotBefore, a.Expires = &nbf, &exp
	o, err := s.add(c.vault, "certificates", c.name, &Version{Cer: der, ContentType: ct, Tags: in.Tags}, a)
	if err != nil {
		return nil, err
	}
	o.Policy = in.Policy
	for coll, v := range map[string]*Version{"keys": {Kty: kty, Ops: defaultOps(kty), Private: priv}, "secrets": {Value: value, ContentType: ct}} {
		v.ID, v.Tags = o.latest().ID, in.Tags
		m, _ := s.add(c.vault, coll, c.name, v, a) // cannot fail: the name is valid and not deleted
		m.Managed = true
	}
	c.status = http.StatusAccepted
	return c.operation(o), nil
}

// operation is a certificate's pending operation, always completed.
func (c *call) operation(o *Object) map[string]any {
	id := c.id("certificates", o.Name, "")
	return map[string]any{"id": id + "/pending", "issuer": map[string]string{"name": "Self"}, "cancellation_requested": false,
		"status": "completed", "target": id, "request_id": o.latest().ID}
}

func (s *Service) certOperation(c *call) (any, error) {
	o, _, err := s.find(c, false)
	if err != nil {
		return nil, err
	}
	return c.operation(o), nil
}

func (s *Service) certPolicy(c *call) (any, error) {
	o, _, err := s.find(c, false)
	if err != nil {
		return nil, err
	}
	return c.policy(o), nil
}

// policy renders a certificate's policy as created, with its id.
func (c *call) policy(o *Object) map[string]any {
	var p map[string]any
	if json.Unmarshal(o.Policy, &p) != nil || p == nil {
		p = map[string]any{}
	}
	p["id"] = c.id("certificates", o.Name, "policy")
	return p
}

func thumbprint(der []byte) string {
	h := sha1.Sum(der)
	return b64(h[:])
}

// subject parses a distinguished name such as "CN=app, O=Contoso".
func subject(dn string) pkix.Name {
	var n pkix.Name
	for _, part := range strings.Split(dn, ",") {
		k, v, _ := strings.Cut(part, "=")
		switch v = strings.TrimSpace(v); strings.ToUpper(strings.TrimSpace(k)) {
		case "CN":
			n.CommonName = v
		case "O":
			n.Organization = append(n.Organization, v)
		case "OU":
			n.OrganizationalUnit = append(n.OrganizationalUnit, v)
		case "L":
			n.Locality = append(n.Locality, v)
		case "ST", "S":
			n.Province = append(n.Province, v)
		case "C":
			n.Country = append(n.Country, v)
		}
	}
	return n
}

// ---- PKCS#12 (RFC 7292) ----

var (
	oidData       = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 7, 1}
	oidShrouded   = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 12, 10, 1, 2}
	oidCertBag    = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 12, 10, 1, 3}
	oidX509Cert   = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 9, 22, 1}
	oidPBE3DESSHA = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 12, 1, 3}
	oidSHA1       = asn1.ObjectIdentifier{1, 3, 14, 3, 2, 26}
)

// bag is an OID with an [0] EXPLICIT value: the shape of ContentInfo, SafeBag and CertBag.
type bag struct {
	ID    asn1.ObjectIdentifier
	Value asn1.RawValue
}

// algData is an algorithm and bytes: DigestInfo and EncryptedPrivateKeyInfo.
type algData struct {
	Alg  pkix.AlgorithmIdentifier
	Data []byte
}

func explicit(v any) asn1.RawValue {
	b, _ := asn1.Marshal(v)
	return asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 0, IsCompound: true, Bytes: b}
}

// pfx packs a certificate and its PKCS#8 key as PKCS#12 with an empty
// password: the key shrouded with pbeWithSHAAnd3-KeyTripleDES-CBC, a SHA-1
// MAC. Every PKCS#12 reader takes that, golang.org/x/crypto/pkcs12 included.
func pfx(cert, key []byte) []byte {
	const iter = 2048
	salt := randBytes(8)
	pad := des.BlockSize - len(key)%des.BlockSize
	enc := append(slices.Clone(key), bytes.Repeat([]byte{byte(pad)}, pad)...)
	block, _ := des.NewTripleDESCipher(p12KDF(salt, 1, 24, iter))
	cipher.NewCBCEncrypter(block, p12KDF(salt, 2, 8, iter)).CryptBlocks(enc, enc)
	params, _ := asn1.Marshal(struct {
		Salt []byte
		Iter int
	}{salt, iter})
	shrouded := algData{pkix.AlgorithmIdentifier{Algorithm: oidPBE3DESSHA, Parameters: asn1.RawValue{FullBytes: params}}, enc}
	// Two safes, certificate then key, the layout OpenSSL writes and x/crypto/pkcs12 insists on.
	certs, _ := asn1.Marshal([]bag{{oidCertBag, explicit(bag{oidX509Cert, explicit(cert)})}})
	keys, _ := asn1.Marshal([]bag{{oidShrouded, explicit(shrouded)}})
	auth, _ := asn1.Marshal([]bag{{oidData, explicit(certs)}, {oidData, explicit(keys)}})
	mac := hmac.New(sha1.New, p12KDF(salt, 3, 20, iter))
	mac.Write(auth)
	out, _ := asn1.Marshal(struct {
		Version int
		Auth    bag
		Mac     macData
	}{3, bag{oidData, explicit(auth)}, macData{algData{pkix.AlgorithmIdentifier{Algorithm: oidSHA1, Parameters: asn1.NullRawValue}, mac.Sum(nil)}, salt, iter}})
	return out
}

type macData struct {
	Digest algData
	Salt   []byte
	Iter   int
}

// p12KDF derives n bytes for purpose id (1 key, 2 IV, 3 MAC) from the empty
// password with SHA-1 (RFC 7292 appendix B.2).
func p12KDF(salt []byte, id byte, n, iter int) []byte {
	const v = 64                    // SHA-1 block size
	fill := func(b []byte) []byte { // repeat b to v bytes; every input here is shorter
		return bytes.Repeat(b, (v+len(b)-1)/len(b))[:v]
	}
	I := slices.Concat(fill(salt), fill([]byte{0, 0})) // the password: BMPString "" with its terminator
	D := bytes.Repeat([]byte{id}, v)
	var out []byte
	for len(out) < n {
		a := sha1.Sum(slices.Concat(D, I))
		for range iter - 1 {
			a = sha1.Sum(a[:])
		}
		out = append(out, a[:]...)
		B := fill(a[:])
		for j := 0; j < len(I); j += v { // I_j = (I_j + B + 1) mod 2^(8v)
			carry := 1
			for k := v - 1; k >= 0; k-- {
				carry += int(I[j+k]) + int(B[k])
				I[j+k] = byte(carry)
				carry >>= 8
			}
		}
	}
	return out[:n]
}
