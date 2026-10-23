// Package kms emulates Cloud KMS over gRPC and its REST mirror, with real
// cryptography: key rings, crypto keys (symmetric, RSA and EC signing, RSA
// decryption, HMAC), versions (rotation, primary, disable, scheduled
// destruction), AES-256-GCM encrypt/decrypt with AAD and CRC32C checks,
// asymmetric sign/decrypt, public keys, MACs, random bytes and IAM policies.
package kms

import (
	"context"
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	_ "crypto/sha1" // the hashes the algorithms name
	_ "crypto/sha256"
	_ "crypto/sha512"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"hash/crc32"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Service is the Cloud KMS emulator.
type Service struct {
	kmspb.UnimplementedKeyManagementServiceServer
	iampb.UnimplementedIAMPolicyServer
	rest  *gapi.REST
	stop  func()
	mu    sync.Mutex
	rings map[string]*ring
	keys  map[string]*key
}

type ring struct {
	p      *kmspb.KeyRing
	policy *iampb.Policy
}

type key struct {
	p       *kmspb.CryptoKey // Primary is filled in on the way out
	policy  *iampb.Policy
	primary int        // version number; 0 = none
	vers    []*version // vers[n-1] is version n
}

type version struct {
	p   *kmspb.CryptoKeyVersion
	mat []byte // AES key, HMAC key or PKCS#8 private key; nil once destroyed
}

type (
	alg     = kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm
	purpose = kmspb.CryptoKey_CryptoKeyPurpose
)

// algo says how to make and use the key of an algorithm.
type algo struct {
	purpose purpose
	kind    byte // 's'ymmetric, 'r'sa, 'e'c, 'h'mac
	bits    int  // RSA modulus
	curve   elliptic.Curve
	hash    crypto.Hash
	pss     bool
}

const (
	sign = kmspb.CryptoKey_ASYMMETRIC_SIGN
	dec  = kmspb.CryptoKey_ASYMMETRIC_DECRYPT
	mac  = kmspb.CryptoKey_MAC
)

var algos = map[alg]algo{
	kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION: {purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT, kind: 's'},

	kmspb.CryptoKeyVersion_RSA_SIGN_PSS_2048_SHA256:   {sign, 'r', 2048, nil, crypto.SHA256, true},
	kmspb.CryptoKeyVersion_RSA_SIGN_PSS_3072_SHA256:   {sign, 'r', 3072, nil, crypto.SHA256, true},
	kmspb.CryptoKeyVersion_RSA_SIGN_PSS_4096_SHA256:   {sign, 'r', 4096, nil, crypto.SHA256, true},
	kmspb.CryptoKeyVersion_RSA_SIGN_PSS_4096_SHA512:   {sign, 'r', 4096, nil, crypto.SHA512, true},
	kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_2048_SHA256: {sign, 'r', 2048, nil, crypto.SHA256, false},
	kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_3072_SHA256: {sign, 'r', 3072, nil, crypto.SHA256, false},
	kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_4096_SHA256: {sign, 'r', 4096, nil, crypto.SHA256, false},
	kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_4096_SHA512: {sign, 'r', 4096, nil, crypto.SHA512, false},
	kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256:        {sign, 'e', 0, elliptic.P256(), crypto.SHA256, false},
	kmspb.CryptoKeyVersion_EC_SIGN_P384_SHA384:        {sign, 'e', 0, elliptic.P384(), crypto.SHA384, false},

	kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_2048_SHA256: {dec, 'r', 2048, nil, crypto.SHA256, false},
	kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_3072_SHA256: {dec, 'r', 3072, nil, crypto.SHA256, false},
	kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_4096_SHA256: {dec, 'r', 4096, nil, crypto.SHA256, false},
	kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_4096_SHA512: {dec, 'r', 4096, nil, crypto.SHA512, false},
	kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_2048_SHA1:   {dec, 'r', 2048, nil, crypto.SHA1, false},
	kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_3072_SHA1:   {dec, 'r', 3072, nil, crypto.SHA1, false},
	kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_4096_SHA1:   {dec, 'r', 4096, nil, crypto.SHA1, false},

	kmspb.CryptoKeyVersion_HMAC_SHA1:   {mac, 'h', 0, nil, crypto.SHA1, false},
	kmspb.CryptoKeyVersion_HMAC_SHA224: {mac, 'h', 0, nil, crypto.SHA224, false},
	kmspb.CryptoKeyVersion_HMAC_SHA256: {mac, 'h', 0, nil, crypto.SHA256, false},
	kmspb.CryptoKeyVersion_HMAC_SHA384: {mac, 'h', 0, nil, crypto.SHA384, false},
	kmspb.CryptoKeyVersion_HMAC_SHA512: {mac, 'h', 0, nil, crypto.SHA512, false},
}

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	locRE      = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+$`)
	ringRE     = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/keyRings/[A-Za-z0-9_-]{1,63}$`)
	idRE       = regexp.MustCompile(`^[A-Za-z0-9_-]{1,63}$`)
	randomRE   = regexp.MustCompile(`^/v1/projects/[^/]+/locations/[^/:]+:generateRandomBytes$`)
)

// New creates the Cloud KMS service and starts its clock (scheduled
// destruction and automatic rotation).
func New(*core.Hub) *Service {
	s := &Service{rings: map[string]*ring{}, keys: map[string]*key{}}
	kr, ck, cv := "locations/*/keyRings", "locations/*/keyRings/*/cryptoKeys", "locations/*/keyRings/*/cryptoKeys/*/cryptoKeyVersions"
	r := map[string]gapi.Route{
		"GET " + kr:                  {Method: "ListKeyRings", Field: "parent"},
		"POST " + kr:                 {Method: "CreateKeyRing", Field: "parent", Body: "keyRing"},
		"GET " + kr + "/*":           {Method: "GetKeyRing", Field: "name"},
		"GET " + ck:                  {Method: "ListCryptoKeys", Field: "parent"},
		"POST " + ck:                 {Method: "CreateCryptoKey", Field: "parent", Body: "cryptoKey"},
		"GET " + ck + "/*":           {Method: "GetCryptoKey", Field: "name"},
		"PATCH " + ck + "/*":         {Method: "UpdateCryptoKey", Field: "cryptoKey.name", Body: "cryptoKey"},
		"GET " + cv:                  {Method: "ListCryptoKeyVersions", Field: "parent"},
		"POST " + cv:                 {Method: "CreateCryptoKeyVersion", Field: "parent", Body: "cryptoKeyVersion"},
		"GET " + cv + "/*":           {Method: "GetCryptoKeyVersion", Field: "name"},
		"PATCH " + cv + "/*":         {Method: "UpdateCryptoKeyVersion", Field: "cryptoKeyVersion.name", Body: "cryptoKeyVersion"},
		"GET " + cv + "/*/publicKey": {Method: "GetPublicKey", Field: "name"},
	}
	for v, m := range map[string]string{"encrypt": "Encrypt", "decrypt": "Decrypt", "updatePrimaryVersion": "UpdateCryptoKeyPrimaryVersion"} {
		r["POST "+ck+"/*:"+v] = gapi.Route{Method: m, Field: "name"}
	}
	for v, m := range map[string]string{"encrypt": "Encrypt", "destroy": "DestroyCryptoKeyVersion", "restore": "RestoreCryptoKeyVersion",
		"asymmetricSign": "AsymmetricSign", "asymmetricDecrypt": "AsymmetricDecrypt", "macSign": "MacSign", "macVerify": "MacVerify"} {
		r["POST "+cv+"/*:"+v] = gapi.Route{Method: m, Field: "name"}
	}
	for _, res := range []string{kr, ck} {
		r["POST "+res+"/*:setIamPolicy"] = gapi.Route{Method: "SetIamPolicy", Field: "resource", Desc: &iampb.IAMPolicy_ServiceDesc}
		r["GET "+res+"/*:getIamPolicy"] = gapi.Route{Method: "GetIamPolicy", Field: "resource", Desc: &iampb.IAMPolicy_ServiceDesc}
		r["POST "+res+"/*:testIamPermissions"] = gapi.Route{Method: "TestIamPermissions", Field: "resource", Desc: &iampb.IAMPolicy_ServiceDesc}
	}
	s.rest = &gapi.REST{Versions: []string{"v1"}, Collections: []string{"keyRings"}, Desc: &kmspb.KeyManagementService_ServiceDesc, Impl: s, Routes: r}
	s.stop = core.Every(time.Second, s.Tick)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "cloudkms", Cloud: core.GCP, Title: "Cloud KMS",
		Match:   func(r *http.Request) bool { return s.rest.Match(r) || randomRE.MatchString(r.URL.Path) },
		Targets: []string{"google.cloud.kms.v1.KeyManagementService"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) {
	kmspb.RegisterKeyManagementServiceServer(g, s)
	if _, ok := g.GetServiceInfo()[iampb.IAMPolicy_ServiceDesc.ServiceName]; !ok { // another service may own IAMPolicy already
		iampb.RegisterIAMPolicyServer(g, s)
	}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !randomRE.MatchString(r.URL.Path) { // POST /v1/{location}:generateRandomBytes has no collection to route on
		s.rest.ServeHTTP(w, r)
		return
	}
	c := core.C(r)
	c.Op = "GenerateRandomBytes"
	in := &kmspb.GenerateRandomBytesRequest{}
	body := core.Body(r)
	if len(strings.TrimSpace(string(body))) == 0 {
		body = []byte("{}")
	}
	err := c.Intercept()
	if err == nil {
		err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(body, in)
	}
	if err != nil {
		gapi.Err(w, c, gapi.Invalid("Invalid JSON payload received. %s", gapi.PlainProto(body, err)))
		return
	}
	in.Location = strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/"), ":generateRandomBytes")
	out, err := s.GenerateRandomBytes(r.Context(), in)
	if err != nil {
		gapi.Err(w, c, err)
		return
	}
	b, _ := protojson.Marshal(out)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Write(b)
}

// Close stops the clock.
func (s *Service) Close() error { s.stop(); return nil }

func (s *Service) Reset() {
	s.mu.Lock()
	s.rings, s.keys = map[string]*ring{}, map[string]*key{}
	s.mu.Unlock()
}

// ---- persistence ----

type savedRing struct {
	Ring   json.RawMessage `json:"ring"`
	Policy json.RawMessage `json:"policy,omitempty"`
}

type savedKey struct {
	Key      json.RawMessage   `json:"key"`
	Policy   json.RawMessage   `json:"policy,omitempty"`
	Primary  int               `json:"primary,omitempty"`
	Versions []json.RawMessage `json:"versions,omitempty"`
	Material [][]byte          `json:"material,omitempty"` // of each version, so ciphertext and signatures survive a restart
}

type saved struct {
	Rings []savedRing `json:"rings"`
	Keys  []savedKey  `json:"keys"`
}

func pj(m proto.Message) json.RawMessage {
	if m == nil {
		return nil
	}
	b, _ := protojson.Marshal(m)
	return b
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := saved{Rings: []savedRing{}, Keys: []savedKey{}}
	for _, n := range core.SortedKeys(s.rings) {
		rg := s.rings[n]
		sr := savedRing{Ring: pj(rg.p)}
		if rg.policy != nil {
			sr.Policy = pj(rg.policy)
		}
		out.Rings = append(out.Rings, sr)
	}
	for _, n := range core.SortedKeys(s.keys) {
		k := s.keys[n]
		sk := savedKey{Key: pj(k.p), Primary: k.primary}
		if k.policy != nil {
			sk.Policy = pj(k.policy)
		}
		for _, v := range k.vers {
			sk.Versions, sk.Material = append(sk.Versions, pj(v.p)), append(sk.Material, v.mat)
		}
		out.Keys = append(out.Keys, sk)
	}
	return json.Marshal(out)
}

func (s *Service) Load(b []byte) error {
	var in saved
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	var err error
	dec := func(raw json.RawMessage, m proto.Message) {
		if err == nil && len(raw) > 0 {
			err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, m)
		}
	}
	rings, keys := map[string]*ring{}, map[string]*key{}
	for _, sr := range in.Rings {
		rg := &ring{p: &kmspb.KeyRing{}, policy: &iampb.Policy{}}
		dec(sr.Ring, rg.p)
		dec(sr.Policy, rg.policy)
		if len(sr.Policy) == 0 {
			rg.policy = nil
		}
		rings[rg.p.Name] = rg
	}
	for _, sk := range in.Keys {
		k := &key{p: &kmspb.CryptoKey{}, policy: &iampb.Policy{}, primary: sk.Primary}
		dec(sk.Key, k.p)
		dec(sk.Policy, k.policy)
		if len(sk.Policy) == 0 {
			k.policy = nil
		}
		for i, raw := range sk.Versions {
			v := &version{p: &kmspb.CryptoKeyVersion{}}
			dec(raw, v.p)
			if i < len(sk.Material) {
				v.mat = sk.Material[i]
			}
			k.vers = append(k.vers, v)
		}
		keys[k.p.Name] = k
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.rings, s.keys = rings, keys
	s.mu.Unlock()
	return nil
}

// ---- helpers ----

func clone[M proto.Message](m M) M { return proto.Clone(m).(M) }

func notFound(kind, name string) error {
	return status.Errorf(codes.NotFound, "%s %s not found.", kind, name)
}

func failed(format string, a ...any) error {
	return status.Errorf(codes.FailedPrecondition, format, a...)
}

func crc(b []byte) *wrapperspb.Int64Value {
	return wrapperspb.Int64(int64(crc32.Checksum(b, castagnoli)))
}

// verify checks a request checksum against the data it covers.
func verify(field, of string, want *wrapperspb.Int64Value, data []byte) (bool, error) {
	if want == nil {
		return false, nil
	}
	if want.Value != crc(data).Value {
		return false, gapi.Invalid("The checksum in field %s did not match the data in field %s.", field, of)
	}
	return true, nil
}

func (v *version) name() string { return v.p.Name }

// render is the key as clients see it: with its primary version.
func (k *key) render() *kmspb.CryptoKey {
	p := clone(k.p)
	if k.primary > 0 {
		p.Primary = clone(k.vers[k.primary-1].p)
	}
	return p
}

// versionOf splits a version name into its key name and number.
func versionOf(name string) (keyName string, n int, ok bool) {
	keyName, num, ok := strings.Cut(name, "/cryptoKeyVersions/")
	n, err := strconv.Atoi(num)
	return keyName, n, ok && err == nil && n > 0
}

// version finds version number n of a key. Lock held.
func (s *Service) version(name string) (*key, *version, error) {
	kn, n, ok := versionOf(name)
	if !ok {
		return nil, nil, gapi.Invalid("Invalid CryptoKeyVersion name: %s", name)
	}
	k := s.keys[kn]
	if k == nil || n > len(k.vers) {
		return nil, nil, notFound("CryptoKeyVersion", name)
	}
	return k, k.vers[n-1], nil
}

// usable finds the version a crypto operation names (a key name means its
// primary, if allowed) and checks the key's purpose and the version's state. Lock held.
func (s *Service) usable(name string, want purpose, primaryOK bool) (*key, *version, error) {
	var k *key
	var v *version
	var err error
	switch {
	case strings.Contains(name, "/cryptoKeyVersions/"):
		k, v, err = s.version(name)
	case primaryOK:
		if k = s.keys[name]; k == nil {
			return nil, nil, notFound("CryptoKey", name)
		}
		if k.primary == 0 {
			return nil, nil, failed("CryptoKey %s has no primary version.", name)
		}
		v = k.vers[k.primary-1]
	default:
		return nil, nil, gapi.Invalid("Invalid CryptoKeyVersion name: %s", name)
	}
	if err != nil {
		return nil, nil, err
	}
	if k.p.Purpose != want {
		return nil, nil, failed("CryptoKey %s has purpose %s, but this operation requires %s.", k.p.Name, k.p.Purpose, want)
	}
	if v.p.State != kmspb.CryptoKeyVersion_ENABLED {
		return nil, nil, failed("CryptoKeyVersion %s is not enabled, current state is: %s.", v.name(), v.p.State)
	}
	return k, v, nil
}

// newVersion makes the next version of k from its template. Lock held.
// ponytail: RSA generation runs under the lock (seconds for 4096 bits); generate
// outside it if concurrent clients ever notice.
func (s *Service) newVersion(k *key) (*version, error) {
	t := k.p.VersionTemplate
	mat, err := generate(algos[t.Algorithm])
	if err != nil {
		return nil, err
	}
	now := timestamppb.Now()
	v := &version{mat: mat, p: &kmspb.CryptoKeyVersion{Name: k.p.Name + "/cryptoKeyVersions/" + strconv.Itoa(len(k.vers)+1),
		State: kmspb.CryptoKeyVersion_ENABLED, ProtectionLevel: t.ProtectionLevel, Algorithm: t.Algorithm, CreateTime: now, GenerateTime: now}}
	k.vers = append(k.vers, v)
	return v, nil
}

func generate(a algo) ([]byte, error) {
	var priv any
	var err error
	switch a.kind {
	case 's', 'h':
		b := make([]byte, 32)
		rand.Read(b)
		return b, nil
	case 'r':
		priv, err = rsa.GenerateKey(rand.Reader, a.bits)
	default:
		priv, err = ecdsa.GenerateKey(a.curve, rand.Reader)
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return x509.MarshalPKCS8PrivateKey(priv)
}

func signer(v *version) (crypto.Signer, error) {
	k, err := x509.ParsePKCS8PrivateKey(v.mat)
	if err != nil {
		return nil, status.Error(codes.Internal, "key material is unreadable")
	}
	return k.(crypto.Signer), nil
}

// ---- key rings ----

func (s *Service) CreateKeyRing(_ context.Context, req *kmspb.CreateKeyRingRequest) (*kmspb.KeyRing, error) {
	if !locRE.MatchString(req.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/<project>/locations/<location>.", req.Parent)
	}
	if !idRE.MatchString(req.KeyRingId) {
		return nil, gapi.Invalid("Invalid key_ring_id %q: use 1-63 letters, digits, - and _.", req.KeyRingId)
	}
	name := req.Parent + "/keyRings/" + req.KeyRingId
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rings[name] != nil {
		return nil, gapi.Exists("KeyRing %s already exists.", name)
	}
	rg := &ring{p: &kmspb.KeyRing{Name: name, CreateTime: timestamppb.Now()}}
	s.rings[name] = rg
	return clone(rg.p), nil
}

func (s *Service) GetKeyRing(_ context.Context, req *kmspb.GetKeyRingRequest) (*kmspb.KeyRing, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rg := s.rings[req.Name]
	if rg == nil {
		return nil, notFound("KeyRing", req.Name)
	}
	return clone(rg.p), nil
}

// ponytail: list filters and order_by are ignored; results are sorted by name.
func (s *Service) ListKeyRings(_ context.Context, req *kmspb.ListKeyRingsRequest) (*kmspb.ListKeyRingsResponse, error) {
	if !locRE.MatchString(req.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/<project>/locations/<location>.", req.Parent)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*kmspb.KeyRing
	for _, n := range core.SortedKeys(s.rings) {
		if strings.HasPrefix(n, req.Parent+"/keyRings/") {
			all = append(all, clone(s.rings[n].p))
		}
	}
	page, next := gapi.Page(all, req.PageSize, req.PageToken)
	return &kmspb.ListKeyRingsResponse{KeyRings: page, NextPageToken: next, TotalSize: int32(len(all))}, nil
}

// ---- crypto keys ----

// template fills in and checks a key's version template against its purpose.
func template(p purpose, t *kmspb.CryptoKeyVersionTemplate) (*kmspb.CryptoKeyVersionTemplate, error) {
	t = clone(cmpOr(t, &kmspb.CryptoKeyVersionTemplate{}))
	if t.ProtectionLevel == kmspb.ProtectionLevel_PROTECTION_LEVEL_UNSPECIFIED {
		t.ProtectionLevel = kmspb.ProtectionLevel_SOFTWARE
	}
	if t.Algorithm == kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED && p == kmspb.CryptoKey_ENCRYPT_DECRYPT {
		t.Algorithm = kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION
	}
	a, ok := algos[t.Algorithm]
	switch {
	case t.Algorithm == kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED:
		return nil, gapi.Invalid("CryptoKey.version_template.algorithm is required for purpose %s.", p)
	case !ok:
		return nil, gapi.Invalid("Algorithm %s is not supported.", t.Algorithm)
	case a.purpose != p:
		return nil, gapi.Invalid("Algorithm %s cannot be used with purpose %s.", t.Algorithm, p)
	}
	return t, nil
}

func cmpOr[T comparable](a, b T) T {
	var zero T
	if a != zero {
		return a
	}
	return b
}

// schedule gives a key with a rotation period its first rotation time.
func schedule(k *kmspb.CryptoKey, now time.Time) error {
	p := k.GetRotationPeriod()
	if p == nil {
		return nil
	}
	if k.Purpose != kmspb.CryptoKey_ENCRYPT_DECRYPT {
		return gapi.Invalid("Rotation is only supported for ENCRYPT_DECRYPT keys.")
	}
	if p.AsDuration() <= 0 {
		return gapi.Invalid("rotation_period must be positive.")
	}
	if k.NextRotationTime == nil {
		k.NextRotationTime = timestamppb.New(now.Add(p.AsDuration()))
	}
	return nil
}

func (s *Service) CreateCryptoKey(_ context.Context, req *kmspb.CreateCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	in := req.CryptoKey
	switch {
	case !ringRE.MatchString(req.Parent):
		return nil, gapi.Invalid("Invalid parent %q: want projects/<project>/locations/<location>/keyRings/<key ring>.", req.Parent)
	case !idRE.MatchString(req.CryptoKeyId):
		return nil, gapi.Invalid("Invalid crypto_key_id %q: use 1-63 letters, digits, - and _.", req.CryptoKeyId)
	case in == nil || in.Purpose == kmspb.CryptoKey_CRYPTO_KEY_PURPOSE_UNSPECIFIED:
		return nil, gapi.Invalid("CryptoKey.purpose is required.")
	}
	t, err := template(in.Purpose, in.VersionTemplate)
	if err != nil {
		return nil, err
	}
	name := req.Parent + "/cryptoKeys/" + req.CryptoKeyId
	k := &key{p: &kmspb.CryptoKey{Name: name, Purpose: in.Purpose, CreateTime: timestamppb.Now(), Labels: in.Labels, VersionTemplate: t,
		NextRotationTime: in.NextRotationTime, RotationSchedule: in.RotationSchedule, ImportOnly: in.ImportOnly,
		DestroyScheduledDuration: cmpOr(in.DestroyScheduledDuration, durationpb.New(30*24*time.Hour))}}
	if err := schedule(k.p, time.Now()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rings[req.Parent] == nil {
		return nil, notFound("KeyRing", req.Parent)
	}
	if s.keys[name] != nil {
		return nil, gapi.Exists("CryptoKey %s already exists.", name)
	}
	if !req.SkipInitialVersionCreation {
		if _, err := s.newVersion(k); err != nil {
			return nil, err
		}
		if in.Purpose == kmspb.CryptoKey_ENCRYPT_DECRYPT {
			k.primary = 1
		}
	}
	s.keys[name] = k
	return k.render(), nil
}

func (s *Service) GetCryptoKey(_ context.Context, req *kmspb.GetCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.keys[req.Name]
	if k == nil {
		return nil, notFound("CryptoKey", req.Name)
	}
	return k.render(), nil
}

func (s *Service) ListCryptoKeys(_ context.Context, req *kmspb.ListCryptoKeysRequest) (*kmspb.ListCryptoKeysResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rings[req.Parent] == nil {
		return nil, notFound("KeyRing", req.Parent)
	}
	var all []*kmspb.CryptoKey
	for _, n := range core.SortedKeys(s.keys) {
		if strings.HasPrefix(n, req.Parent+"/cryptoKeys/") {
			all = append(all, s.keys[n].render())
		}
	}
	page, next := gapi.Page(all, req.PageSize, req.PageToken)
	return &kmspb.ListCryptoKeysResponse{CryptoKeys: page, NextPageToken: next, TotalSize: int32(len(all))}, nil
}

func (s *Service) UpdateCryptoKey(_ context.Context, req *kmspb.UpdateCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	if req.CryptoKey == nil {
		return nil, gapi.Invalid("crypto_key is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.keys[req.CryptoKey.Name]
	if k == nil {
		return nil, notFound("CryptoKey", req.CryptoKey.Name)
	}
	p := clone(k.p)
	if err := gapi.Update(p, req.CryptoKey, req.GetUpdateMask().GetPaths(), "labels", "next_rotation_time", "rotation_period", "version_template"); err != nil {
		return nil, err
	}
	t, err := template(p.Purpose, p.VersionTemplate)
	if err != nil {
		return nil, err
	}
	p.VersionTemplate = t
	if err := schedule(p, time.Now()); err != nil {
		return nil, err
	}
	k.p = p
	return k.render(), nil
}

func (s *Service) UpdateCryptoKeyPrimaryVersion(_ context.Context, req *kmspb.UpdateCryptoKeyPrimaryVersionRequest) (*kmspb.CryptoKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.keys[req.Name]
	if k == nil {
		return nil, notFound("CryptoKey", req.Name)
	}
	if k.p.Purpose != kmspb.CryptoKey_ENCRYPT_DECRYPT {
		return nil, failed("Only ENCRYPT_DECRYPT keys have a primary version.")
	}
	n, err := strconv.Atoi(req.CryptoKeyVersionId)
	if err != nil || n < 1 || n > len(k.vers) {
		return nil, notFound("CryptoKeyVersion", req.Name+"/cryptoKeyVersions/"+req.CryptoKeyVersionId)
	}
	if st := k.vers[n-1].p.State; st != kmspb.CryptoKeyVersion_ENABLED {
		return nil, failed("CryptoKeyVersion %s is not enabled, current state is: %s.", k.vers[n-1].name(), st)
	}
	k.primary = n
	return k.render(), nil
}

// ---- versions ----

func (s *Service) CreateCryptoKeyVersion(_ context.Context, req *kmspb.CreateCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.keys[req.Parent]
	if k == nil {
		return nil, notFound("CryptoKey", req.Parent)
	}
	v, err := s.newVersion(k)
	if err != nil {
		return nil, err
	}
	return clone(v.p), nil
}

func (s *Service) GetCryptoKeyVersion(_ context.Context, req *kmspb.GetCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, v, err := s.version(req.Name)
	if err != nil {
		return nil, err
	}
	return clone(v.p), nil
}

func (s *Service) ListCryptoKeyVersions(_ context.Context, req *kmspb.ListCryptoKeyVersionsRequest) (*kmspb.ListCryptoKeyVersionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.keys[req.Parent]
	if k == nil {
		return nil, notFound("CryptoKey", req.Parent)
	}
	var all []*kmspb.CryptoKeyVersion
	for _, v := range k.vers {
		all = append(all, clone(v.p))
	}
	page, next := gapi.Page(all, req.PageSize, req.PageToken)
	return &kmspb.ListCryptoKeyVersionsResponse{CryptoKeyVersions: page, NextPageToken: next, TotalSize: int32(len(all))}, nil
}

// UpdateCryptoKeyVersion only changes state, between ENABLED and DISABLED.
func (s *Service) UpdateCryptoKeyVersion(_ context.Context, req *kmspb.UpdateCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	if req.CryptoKeyVersion == nil {
		return nil, gapi.Invalid("crypto_key_version is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, v, err := s.version(req.CryptoKeyVersion.Name)
	if err != nil {
		return nil, err
	}
	p := clone(v.p)
	if err := gapi.Update(p, req.CryptoKeyVersion, req.GetUpdateMask().GetPaths(), "state"); err != nil {
		return nil, err
	}
	enabledOrDisabled := func(st kmspb.CryptoKeyVersion_CryptoKeyVersionState) bool {
		return st == kmspb.CryptoKeyVersion_ENABLED || st == kmspb.CryptoKeyVersion_DISABLED
	}
	if !enabledOrDisabled(p.State) || !enabledOrDisabled(v.p.State) {
		return nil, failed("CryptoKeyVersion %s cannot change from state %s to %s.", v.name(), v.p.State, p.State)
	}
	v.p = p
	return clone(p), nil
}

// DestroyCryptoKeyVersion schedules destruction; Tick destroys the material
// once the key's destroy_scheduled_duration has passed.
func (s *Service) DestroyCryptoKeyVersion(_ context.Context, req *kmspb.DestroyCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, v, err := s.version(req.Name)
	if err != nil {
		return nil, err
	}
	if st := v.p.State; st != kmspb.CryptoKeyVersion_ENABLED && st != kmspb.CryptoKeyVersion_DISABLED {
		return nil, failed("CryptoKeyVersion %s cannot be destroyed from state %s.", v.name(), st)
	}
	v.p.State = kmspb.CryptoKeyVersion_DESTROY_SCHEDULED
	v.p.DestroyTime = timestamppb.New(time.Now().Add(k.p.DestroyScheduledDuration.AsDuration()))
	return clone(v.p), nil
}

func (s *Service) RestoreCryptoKeyVersion(_ context.Context, req *kmspb.RestoreCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, v, err := s.version(req.Name)
	if err != nil {
		return nil, err
	}
	if v.p.State != kmspb.CryptoKeyVersion_DESTROY_SCHEDULED {
		return nil, failed("CryptoKeyVersion %s cannot be restored from state %s.", v.name(), v.p.State)
	}
	v.p.State, v.p.DestroyTime = kmspb.CryptoKeyVersion_DISABLED, nil
	return clone(v.p), nil
}

// Tick destroys versions whose time has come and rotates keys that are due.
func (s *Service) Tick(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range s.keys {
		for _, v := range k.vers {
			if v.p.State == kmspb.CryptoKeyVersion_DESTROY_SCHEDULED && !v.p.DestroyTime.AsTime().After(now) {
				v.p.State, v.p.DestroyTime, v.p.DestroyEventTime, v.mat = kmspb.CryptoKeyVersion_DESTROYED, nil, timestamppb.New(now), nil
			}
		}
		if p := k.p.GetRotationPeriod(); p != nil && k.p.NextRotationTime != nil && !k.p.NextRotationTime.AsTime().After(now) {
			if _, err := s.newVersion(k); err == nil {
				k.primary = len(k.vers)
			}
			next := k.p.NextRotationTime.AsTime()
			for !next.After(now) {
				next = next.Add(p.AsDuration())
			}
			k.p.NextRotationTime = timestamppb.New(next)
		}
	}
}

// ---- cryptography ----

const maxData = 64 << 10

func gcm(mat []byte) cipher.AEAD {
	b, _ := aes.NewCipher(mat)
	g, _ := cipher.NewGCM(b)
	return g
}

// Encrypt seals with AES-256-GCM under the primary version (or the version
// named). The ciphertext is 0x01, the version number, the nonce and the
// sealed data, so Decrypt finds the right version after a rotation.
func (s *Service) Encrypt(_ context.Context, req *kmspb.EncryptRequest) (*kmspb.EncryptResponse, error) {
	if len(req.Plaintext) > maxData || len(req.AdditionalAuthenticatedData) > maxData {
		return nil, gapi.Invalid("Plaintext and additional authenticated data must each be at most %d bytes.", maxData)
	}
	vp, err := verify("plaintext_crc32c", "plaintext", req.PlaintextCrc32C, req.Plaintext)
	if err != nil {
		return nil, err
	}
	va, err := verify("additional_authenticated_data_crc32c", "additional_authenticated_data", req.AdditionalAuthenticatedDataCrc32C, req.AdditionalAuthenticatedData)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	k, v, err := s.usable(req.Name, kmspb.CryptoKey_ENCRYPT_DECRYPT, true)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	g := gcm(v.mat)
	_, n, _ := versionOf(v.name())
	s.mu.Unlock()
	blob := binary.BigEndian.AppendUint32([]byte{1}, uint32(n))
	nonce := make([]byte, g.NonceSize())
	rand.Read(nonce)
	blob = g.Seal(append(blob, nonce...), nonce, req.Plaintext, req.AdditionalAuthenticatedData)
	return &kmspb.EncryptResponse{Name: v.name(), Ciphertext: blob, CiphertextCrc32C: crc(blob), VerifiedPlaintextCrc32C: vp,
		VerifiedAdditionalAuthenticatedDataCrc32C: va, ProtectionLevel: k.p.VersionTemplate.ProtectionLevel}, nil
}

func (s *Service) Decrypt(_ context.Context, req *kmspb.DecryptRequest) (*kmspb.DecryptResponse, error) {
	if _, err := verify("ciphertext_crc32c", "ciphertext", req.CiphertextCrc32C, req.Ciphertext); err != nil {
		return nil, err
	}
	if _, err := verify("additional_authenticated_data_crc32c", "additional_authenticated_data", req.AdditionalAuthenticatedDataCrc32C, req.AdditionalAuthenticatedData); err != nil {
		return nil, err
	}
	bad := gapi.Invalid("Decryption failed: the ciphertext is invalid.")
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.keys[req.Name]
	if k == nil {
		return nil, notFound("CryptoKey", req.Name)
	}
	if k.p.Purpose != kmspb.CryptoKey_ENCRYPT_DECRYPT {
		return nil, failed("CryptoKey %s has purpose %s, but this operation requires ENCRYPT_DECRYPT.", k.p.Name, k.p.Purpose)
	}
	ct := req.Ciphertext
	if len(ct) < 5+12+16 || ct[0] != 1 {
		return nil, bad
	}
	n := int(binary.BigEndian.Uint32(ct[1:5]))
	if n < 1 || n > len(k.vers) {
		return nil, bad
	}
	v := k.vers[n-1]
	if v.p.State != kmspb.CryptoKeyVersion_ENABLED {
		return nil, failed("CryptoKeyVersion %s is not enabled, current state is: %s.", v.name(), v.p.State)
	}
	g := gcm(v.mat)
	pt, err := g.Open(nil, ct[5:5+g.NonceSize()], ct[5+g.NonceSize():], req.AdditionalAuthenticatedData)
	if err != nil {
		return nil, bad
	}
	return &kmspb.DecryptResponse{Plaintext: pt, PlaintextCrc32C: crc(pt), UsedPrimary: n == k.primary,
		ProtectionLevel: k.p.VersionTemplate.ProtectionLevel}, nil
}

func (s *Service) AsymmetricSign(_ context.Context, req *kmspb.AsymmetricSignRequest) (*kmspb.AsymmetricSignResponse, error) {
	vd, err := verify("digest_crc32c", "digest", req.DigestCrc32C, digestBytes(req.Digest))
	if err != nil {
		return nil, err
	}
	vx, err := verify("data_crc32c", "data", req.DataCrc32C, req.Data)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	k, v, err := s.usable(req.Name, sign, false)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	pk, err := signer(v)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	a := algos[v.p.Algorithm]
	var digest []byte
	switch d := digestBytes(req.Digest); {
	case d != nil && req.Data != nil, d == nil && req.Data == nil:
		return nil, gapi.Invalid("Exactly one of digest and data must be set.")
	case d != nil:
		if len(d) != a.hash.Size() {
			return nil, gapi.Invalid("The digest must be %d bytes for algorithm %s.", a.hash.Size(), v.p.Algorithm)
		}
		digest = d
	default:
		h := a.hash.New()
		h.Write(req.Data)
		digest = h.Sum(nil)
	}
	var opts crypto.SignerOpts = a.hash
	if a.pss {
		opts = &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: a.hash}
	}
	sig, err := pk.Sign(rand.Reader, digest, opts)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &kmspb.AsymmetricSignResponse{Signature: sig, SignatureCrc32C: crc(sig), Name: v.name(), VerifiedDigestCrc32C: vd,
		VerifiedDataCrc32C: vx, ProtectionLevel: k.p.VersionTemplate.ProtectionLevel}, nil
}

func digestBytes(d *kmspb.Digest) []byte {
	switch x := d.GetDigest().(type) {
	case *kmspb.Digest_Sha256:
		return x.Sha256
	case *kmspb.Digest_Sha384:
		return x.Sha384
	case *kmspb.Digest_Sha512:
		return x.Sha512
	}
	return nil
}

func (s *Service) AsymmetricDecrypt(_ context.Context, req *kmspb.AsymmetricDecryptRequest) (*kmspb.AsymmetricDecryptResponse, error) {
	vc, err := verify("ciphertext_crc32c", "ciphertext", req.CiphertextCrc32C, req.Ciphertext)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	k, v, err := s.usable(req.Name, dec, false)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	pk, err := signer(v)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	pt, err := pk.(*rsa.PrivateKey).Decrypt(nil, req.Ciphertext, &rsa.OAEPOptions{Hash: algos[v.p.Algorithm].hash})
	if err != nil {
		return nil, gapi.Invalid("Decryption failed: the ciphertext is invalid.")
	}
	return &kmspb.AsymmetricDecryptResponse{Plaintext: pt, PlaintextCrc32C: crc(pt), VerifiedCiphertextCrc32C: vc,
		ProtectionLevel: k.p.VersionTemplate.ProtectionLevel}, nil
}

// GetPublicKey returns the PEM (PKIX) public key of an asymmetric version.
func (s *Service) GetPublicKey(_ context.Context, req *kmspb.GetPublicKeyRequest) (*kmspb.PublicKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, _, err := s.version(req.Name)
	if err != nil {
		return nil, err
	}
	if k.p.Purpose != sign && k.p.Purpose != dec {
		return nil, failed("CryptoKey %s has purpose %s, which has no public key.", k.p.Name, k.p.Purpose)
	}
	k, v, err := s.usable(req.Name, k.p.Purpose, false)
	if err != nil {
		return nil, err
	}
	pk, err := signer(v)
	if err != nil {
		return nil, err
	}
	der, err := x509.MarshalPKIXPublicKey(pk.Public())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	return &kmspb.PublicKey{Pem: string(pemBytes), PemCrc32C: crc(pemBytes), Algorithm: v.p.Algorithm, Name: v.name(),
		ProtectionLevel: k.p.VersionTemplate.ProtectionLevel, PublicKeyFormat: kmspb.PublicKey_PEM}, nil
}

func (s *Service) mac(name string, data []byte) ([]byte, *key, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, v, err := s.usable(name, mac, false)
	if err != nil {
		return nil, nil, err
	}
	h := hmac.New(algos[v.p.Algorithm].hash.New, v.mat)
	h.Write(data)
	return h.Sum(nil), k, nil
}

func (s *Service) MacSign(_ context.Context, req *kmspb.MacSignRequest) (*kmspb.MacSignResponse, error) {
	vx, err := verify("data_crc32c", "data", req.DataCrc32C, req.Data)
	if err != nil {
		return nil, err
	}
	sum, k, err := s.mac(req.Name, req.Data)
	if err != nil {
		return nil, err
	}
	return &kmspb.MacSignResponse{Name: req.Name, Mac: sum, MacCrc32C: crc(sum), VerifiedDataCrc32C: vx, ProtectionLevel: k.p.VersionTemplate.ProtectionLevel}, nil
}

func (s *Service) MacVerify(_ context.Context, req *kmspb.MacVerifyRequest) (*kmspb.MacVerifyResponse, error) {
	vx, err := verify("data_crc32c", "data", req.DataCrc32C, req.Data)
	if err != nil {
		return nil, err
	}
	vm, err := verify("mac_crc32c", "mac", req.MacCrc32C, req.Mac)
	if err != nil {
		return nil, err
	}
	sum, k, err := s.mac(req.Name, req.Data)
	if err != nil {
		return nil, err
	}
	return &kmspb.MacVerifyResponse{Name: req.Name, Success: hmac.Equal(sum, req.Mac), VerifiedDataCrc32C: vx, VerifiedMacCrc32C: vm,
		VerifiedSuccessIntegrity: vx && vm, ProtectionLevel: k.p.VersionTemplate.ProtectionLevel}, nil
}

func (s *Service) GenerateRandomBytes(_ context.Context, req *kmspb.GenerateRandomBytesRequest) (*kmspb.GenerateRandomBytesResponse, error) {
	if !locRE.MatchString(req.Location) {
		return nil, gapi.Invalid("Invalid location %q: want projects/<project>/locations/<location>.", req.Location)
	}
	if req.LengthBytes < 8 || req.LengthBytes > 1024 {
		return nil, gapi.Invalid("length_bytes must be between 8 and 1024.")
	}
	b := make([]byte, req.LengthBytes)
	rand.Read(b)
	return &kmspb.GenerateRandomBytesResponse{Data: b, DataCrc32C: crc(b)}, nil
}

// ---- IAM (stored as is; not enforced) ----

// policy finds the policy slot of a key ring or key. Lock held.
func (s *Service) policy(name string) (**iampb.Policy, error) {
	if k := s.keys[name]; k != nil {
		return &k.policy, nil
	}
	if rg := s.rings[name]; rg != nil {
		return &rg.policy, nil
	}
	return nil, notFound("Resource", name)
}

var emptyEtag = []byte("ACAB")

func (s *Service) GetIamPolicy(_ context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.policy(req.Resource)
	if err != nil {
		return nil, err
	}
	if *p == nil {
		return &iampb.Policy{Etag: emptyEtag}, nil
	}
	return clone(*p), nil
}

func (s *Service) SetIamPolicy(_ context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if req.Policy == nil {
		return nil, gapi.Invalid("policy is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.policy(req.Resource)
	if err != nil {
		return nil, err
	}
	cur := emptyEtag
	if *p != nil {
		cur = (*p).Etag
	}
	if len(req.Policy.Etag) > 0 && string(req.Policy.Etag) != string(cur) {
		return nil, status.Error(codes.Aborted, "The etag does not match the current policy; read it again and retry.")
	}
	np := clone(req.Policy)
	np.Etag = []byte(core.RandHex(6))
	*p = np
	return clone(np), nil
}

func (s *Service) TestIamPermissions(_ context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.policy(req.Resource); err != nil {
		return nil, err
	}
	return &iampb.TestIamPermissionsResponse{Permissions: req.Permissions}, nil // ponytail: everything asked is granted
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	rings := core.Table{Name: "Key rings", Columns: []string{"Name", "Project", "Location", "Keys", "Created"}}
	for _, n := range core.SortedKeys(s.rings) {
		p := strings.Split(n, "/")
		count := 0
		for kn := range s.keys {
			if strings.HasPrefix(kn, n+"/cryptoKeys/") {
				count++
			}
		}
		rings.Rows = append(rings.Rows, []string{n, p[1], p[3], strconv.Itoa(count), s.rings[n].p.CreateTime.AsTime().Format(time.DateTime)})
	}
	keys := core.Table{Name: "Keys", Columns: []string{"Name", "Purpose", "Algorithm", "Primary", "Versions", "Created"}}
	for _, n := range core.SortedKeys(s.keys) {
		k := s.keys[n]
		primary := "-"
		if k.primary > 0 {
			primary = strconv.Itoa(k.primary)
		}
		keys.Rows = append(keys.Rows, []string{n, k.p.Purpose.String(), k.p.VersionTemplate.Algorithm.String(), primary,
			strconv.Itoa(len(k.vers)), k.p.CreateTime.AsTime().Format(time.DateTime)})
	}
	return []core.Table{rings, keys}
}

// Inspect returns a key ring or a key with its versions and policy; never key material.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]any{}
	var policy *iampb.Policy
	switch {
	case table == "Key rings" && s.rings[id] != nil:
		out["keyRing"], policy = pj(s.rings[id].p), s.rings[id].policy
	case table == "Keys" && s.keys[id] != nil:
		k := s.keys[id]
		versions := []json.RawMessage{}
		for _, v := range k.vers {
			versions = append(versions, pj(v.p))
		}
		out["cryptoKey"], out["versions"], policy = pj(k.render()), versions, k.policy
	default:
		return nil
	}
	if policy != nil {
		out["policy"] = pj(policy)
	}
	return out
}
