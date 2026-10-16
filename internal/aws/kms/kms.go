// Package kms emulates AWS KMS over JSON 1.1 with real stdlib cryptography:
// symmetric (AES-256-GCM), RSA, ECC and HMAC keys, aliases, key policies,
// tags, rotation and scheduled deletion. Key material lives in state, so
// ciphertexts stay decryptable across restarts and snapshots.
package kms

import (
	"cmp"
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/mldsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"encoding/json"
	"hash"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the KMS emulator.
type Service struct {
	rpc *core.RPC
	mu  sync.Mutex
	st  state
}

type state struct {
	Keys    map[string]*Key   `json:"keys"`    // account/region/key id
	Aliases map[string]*Alias `json:"aliases"` // account/region/alias name
}

// Key is a KMS key. Material keeps every generation of key material (AES or
// HMAC bytes, or a PKCS#8 private key): the last one is current, older ones
// still decrypt after a rotation.
type Key struct {
	Meta           KeyMetadata `json:"metadata"`
	Region         string      `json:"region"`
	Policy         string      `json:"policy"`
	Tags           []Tag       `json:"tags,omitempty"`
	Material       [][]byte    `json:"material"`
	Rotation       bool        `json:"rotation,omitempty"`
	RotationPeriod int         `json:"rotationPeriod,omitempty"`
	Rotations      []Rotation  `json:"rotations,omitempty"`
}

// KeyMetadata is the DescribeKey shape.
type KeyMetadata struct {
	AWSAccountId                string
	KeyId                       string
	Arn                         string
	CreationDate                core.Time
	Enabled                     bool
	Description                 string
	KeyUsage                    string
	KeyState                    string
	DeletionDate                *core.Time `json:",omitempty"`
	Origin                      string
	KeyManager                  string
	CustomerMasterKeySpec       string
	KeySpec                     string
	EncryptionAlgorithms        []string `json:",omitempty"`
	SigningAlgorithms           []string `json:",omitempty"`
	MacAlgorithms               []string `json:",omitempty"`
	MultiRegion                 bool
	PendingDeletionWindowInDays int `json:",omitempty"`
}

// Tag is a KMS tag.
type Tag struct{ TagKey, TagValue string }

// Rotation is one ListKeyRotations entry.
type Rotation struct {
	KeyId        string
	RotationDate core.Time
	RotationType string
}

// Alias points a friendly name at a key id of the same account and region.
type Alias struct {
	AliasName, AliasArn, TargetKeyId string
	CreationDate, LastUpdatedDate    core.Time
}

// spec is what a key spec allows.
type spec struct{ usages, enc, sign, mac []string }

var (
	rsaUse  = []string{"ENCRYPT_DECRYPT", "SIGN_VERIFY"}
	rsaEnc  = []string{"RSAES_OAEP_SHA_1", "RSAES_OAEP_SHA_256"}
	rsaSign = []string{"RSASSA_PSS_SHA_256", "RSASSA_PSS_SHA_384", "RSASSA_PSS_SHA_512", "RSASSA_PKCS1_V1_5_SHA_256", "RSASSA_PKCS1_V1_5_SHA_384", "RSASSA_PKCS1_V1_5_SHA_512"}
	signUse = []string{"SIGN_VERIFY"}
	macUse  = []string{"GENERATE_VERIFY_MAC"}
)

var specs = map[string]spec{
	"SYMMETRIC_DEFAULT":     {usages: []string{"ENCRYPT_DECRYPT"}, enc: []string{"SYMMETRIC_DEFAULT"}},
	"RSA_2048":              {usages: rsaUse, enc: rsaEnc, sign: rsaSign},
	"RSA_3072":              {usages: rsaUse, enc: rsaEnc, sign: rsaSign},
	"RSA_4096":              {usages: rsaUse, enc: rsaEnc, sign: rsaSign},
	"ECC_NIST_P256":         {usages: signUse, sign: []string{"ECDSA_SHA_256"}},
	"ECC_NIST_P384":         {usages: signUse, sign: []string{"ECDSA_SHA_384"}},
	"ECC_NIST_P521":         {usages: signUse, sign: []string{"ECDSA_SHA_512"}},
	"ECC_SECG_P256K1":       {usages: signUse, sign: []string{"ECDSA_SHA_256"}},
	"ECC_NIST_EDWARDS25519": {usages: signUse, sign: []string{"ED25519_SHA_512", "ED25519_PH_SHA_512"}},
	"ML_DSA_44":             {usages: signUse, sign: []string{"ML_DSA_SHAKE_256"}},
	"ML_DSA_65":             {usages: signUse, sign: []string{"ML_DSA_SHAKE_256"}},
	"ML_DSA_87":             {usages: signUse, sign: []string{"ML_DSA_SHAKE_256"}},
	"HMAC_224":              {usages: macUse, mac: []string{"HMAC_SHA_224"}},
	"HMAC_256":              {usages: macUse, mac: []string{"HMAC_SHA_256"}},
	"HMAC_384":              {usages: macUse, mac: []string{"HMAC_SHA_384"}},
	"HMAC_512":              {usages: macUse, mac: []string{"HMAC_SHA_512"}},
}

// New creates the KMS service.
func New(*core.Hub) *Service {
	s := &Service{}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateKey":                           core.H(s.createKey),
		"DescribeKey":                         core.H(s.describeKey),
		"ListKeys":                            core.H(s.listKeys),
		"EnableKey":                           core.H(s.setEnabled),
		"DisableKey":                          core.H(s.setEnabled),
		"ScheduleKeyDeletion":                 core.H(s.scheduleKeyDeletion),
		"CancelKeyDeletion":                   core.H(s.cancelKeyDeletion),
		"UpdateKeyDescription":                core.H(s.updateKeyDescription),
		"GetKeyPolicy":                        core.H(s.keyPolicy),
		"PutKeyPolicy":                        core.H(s.keyPolicy),
		"ListKeyPolicies":                     core.H(s.keyPolicy),
		"EnableKeyRotation":                   core.H(s.rotation),
		"DisableKeyRotation":                  core.H(s.rotation),
		"GetKeyRotationStatus":                core.H(s.rotation),
		"RotateKeyOnDemand":                   core.H(s.rotation),
		"ListKeyRotations":                    core.H(s.listKeyRotations),
		"CreateAlias":                         core.H(s.alias),
		"UpdateAlias":                         core.H(s.alias),
		"DeleteAlias":                         core.H(s.alias),
		"ListAliases":                         core.H(s.listAliases),
		"TagResource":                         core.H(s.tags),
		"UntagResource":                       core.H(s.tags),
		"ListResourceTags":                    core.H(s.tags),
		"Encrypt":                             core.H(s.encrypt),
		"Decrypt":                             core.H(s.decrypt),
		"ReEncrypt":                           core.H(s.reEncrypt),
		"GenerateDataKey":                     core.H(s.generateDataKey),
		"GenerateDataKeyWithoutPlaintext":     core.H(s.generateDataKey),
		"GenerateDataKeyPair":                 core.H(s.generateDataKeyPair),
		"GenerateDataKeyPairWithoutPlaintext": core.H(s.generateDataKeyPair),
		"GenerateRandom":                      core.H(s.generateRandom),
		"Sign":                                core.H(s.sign),
		"Verify":                              core.H(s.sign),
		"GetPublicKey":                        core.H(s.getPublicKey),
		"GenerateMac":                         core.H(s.mac),
		"VerifyMac":                           core.H(s.mac),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "kms", Cloud: core.AWS, Title: "KMS", Targets: []string{"TrentService"}, Signing: []string{"kms"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.sweep()
	s.mu.Unlock()
	s.rpc.ServeHTTP(w, r)
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Keys: map[string]*Key{}, Aliases: map[string]*Alias{}}
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
	if st.Keys == nil {
		st.Keys = map[string]*Key{}
	}
	if st.Aliases == nil {
		st.Aliases = map[string]*Alias{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// sweep deletes keys (and their aliases) whose deletion date has passed;
// lock held. Runs per request, so idle costs nothing.
func (s *Service) sweep() {
	for id, k := range s.st.Keys {
		if d := k.Meta.DeletionDate; d != nil && time.Now().After(time.Time(*d)) {
			delete(s.st.Keys, id)
			for name, a := range s.st.Aliases {
				if a.TargetKeyId == k.Meta.KeyId && strings.HasPrefix(name, k.Meta.AWSAccountId+"/"+k.Region+"/") {
					delete(s.st.Aliases, name)
				}
			}
		}
	}
}

// errorf builds a KMS client error; they are all HTTP 400.
func errorf(code, format string, a ...any) error { return core.Errorf(400, code, format, a...) }

func now() core.Time { return core.Time(time.Now().UTC()) }

func random(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// generate makes fresh key material for a key spec.
func generate(spec string) ([]byte, error) {
	switch spec {
	case "SYMMETRIC_DEFAULT":
		return random(32), nil
	case "HMAC_224", "HMAC_256", "HMAC_384", "HMAC_512":
		bits, _ := strconv.Atoi(spec[5:])
		return random(bits / 8), nil
	}
	priv, err := generatePair(spec)
	if err != nil {
		return nil, err
	}
	return privDER(priv)
}

// generatePair makes an RSA, ECC, Ed25519 or ML-DSA key pair.
func generatePair(spec string) (crypto.Signer, error) {
	switch spec {
	case "RSA_2048", "RSA_3072", "RSA_4096":
		bits, _ := strconv.Atoi(spec[4:])
		return rsa.GenerateKey(rand.Reader, bits)
	case "ECC_NIST_P256":
		return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case "ECC_NIST_P384":
		return ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	case "ECC_NIST_P521":
		return ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	case "ECC_SECG_P256K1":
		k, err := secp256k1.GeneratePrivateKey()
		return k256{k}, err
	case "ECC_NIST_EDWARDS25519":
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		return priv, err
	case "ML_DSA_44":
		return mldsa.GenerateKey(mldsa.MLDSA44())
	case "ML_DSA_65":
		return mldsa.GenerateKey(mldsa.MLDSA65())
	case "ML_DSA_87":
		return mldsa.GenerateKey(mldsa.MLDSA87())
	}
	return nil, errorf("ValidationException", "1 validation error detected: Value '%s' at 'keySpec' failed to satisfy constraint: Member must satisfy enum value set", spec)
}

func defaultPolicy(account, region string) string {
	return `{"Version":"2012-10-17","Id":"key-default-1","Statement":[{"Sid":"Enable IAM User Permissions","Effect":"Allow","Principal":{"AWS":"` +
		core.GlobalARN("iam", region, account, "root") + `"},"Action":"kms:*","Resource":"*"}]}`
}

func newKey(account, region, keySpec, usage string) (*Key, error) {
	m, err := generate(keySpec)
	if err != nil {
		return nil, err
	}
	id, sp := core.UUID(), specs[keySpec]
	k := &Key{Region: region, Policy: defaultPolicy(account, region), Material: [][]byte{m}, Meta: KeyMetadata{
		AWSAccountId: account, KeyId: id, Arn: core.ARN("kms", region, account, "key/"+id), CreationDate: now(), Enabled: true,
		KeyUsage: usage, KeyState: "Enabled", Origin: "AWS_KMS", KeyManager: "CUSTOMER", CustomerMasterKeySpec: keySpec, KeySpec: keySpec,
	}}
	switch usage {
	case "ENCRYPT_DECRYPT":
		k.Meta.EncryptionAlgorithms = sp.enc
	case "SIGN_VERIFY":
		k.Meta.SigningAlgorithms = sp.sign
	case "GENERATE_VERIFY_MAC":
		k.Meta.MacAlgorithms = sp.mac
	}
	return k, nil
}

// key resolves a key id, key ARN, alias name or alias ARN; lock held. The
// AWS managed keys behind alias/aws/* appear on first use, like in AWS.
func (s *Service) key(c *core.Ctx, id string) (*Key, error) {
	account, region, ref := c.Account, c.Region, id
	if strings.HasPrefix(id, "arn:") {
		_, r, a, res, ok := core.ParseARN(id)
		if !ok || !strings.HasPrefix(res, "key/") && !strings.HasPrefix(res, "alias/") {
			return nil, errorf("NotFoundException", "Invalid arn %s", id)
		}
		account, region, ref = a, r, strings.TrimPrefix(res, "key/")
	}
	prefix := account + "/" + region + "/"
	if strings.HasPrefix(ref, "alias/") {
		al := s.st.Aliases[prefix+ref]
		if al == nil && strings.HasPrefix(ref, "alias/aws/") {
			al = s.managedKey(account, region, ref)
		}
		if al == nil {
			return nil, errorf("NotFoundException", "Alias %s is not found.", core.ARN("kms", region, account, ref))
		}
		ref = al.TargetKeyId
	}
	if k := s.st.Keys[prefix+ref]; k != nil {
		return k, nil
	}
	return nil, errorf("NotFoundException", "Key '%s' does not exist", core.ARN("kms", region, account, "key/"+ref))
}

// managedKey creates the AWS managed key behind alias/aws/<service>; lock held.
func (s *Service) managedKey(account, region, alias string) *Alias {
	k, _ := newKey(account, region, "SYMMETRIC_DEFAULT", "ENCRYPT_DECRYPT")
	k.Meta.KeyManager = "AWS"
	k.Meta.Description = "Default key that protects my " + strings.TrimPrefix(alias, "alias/aws/") + " data when no other key is defined"
	k.Rotation, k.RotationPeriod = true, 365
	s.st.Keys[account+"/"+region+"/"+k.Meta.KeyId] = k
	t := now()
	al := &Alias{AliasName: alias, AliasArn: core.ARN("kms", region, account, alias), TargetKeyId: k.Meta.KeyId, CreationDate: t, LastUpdatedDate: t}
	s.st.Aliases[account+"/"+region+"/"+alias] = al
	return al
}

// usable checks that a key's state allows cryptography.
func usable(k *Key) error {
	switch k.Meta.KeyState {
	case "Enabled":
		return nil
	case "Disabled":
		return errorf("DisabledException", "%s is disabled.", k.Meta.Arn)
	}
	return errorf("KMSInvalidStateException", "%s is pending deletion.", k.Meta.Arn)
}

func notPending(k *Key) error {
	if k.Meta.KeyState == "PendingDeletion" {
		return errorf("KMSInvalidStateException", "%s is pending deletion.", k.Meta.Arn)
	}
	return nil
}

// cryptoKey resolves an enabled key with the given usage; lock held.
func (s *Service) cryptoKey(c *core.Ctx, id, usage string) (*Key, error) {
	k, err := s.key(c, id)
	if err != nil {
		return nil, err
	}
	if err := usable(k); err != nil {
		return nil, err
	}
	if k.Meta.KeyUsage != usage {
		return nil, errorf("InvalidKeyUsageException", "%s key usage is %s which is not valid for %s.", k.Meta.Arn, k.Meta.KeyUsage, c.Op)
	}
	return k, nil
}

func checkAlg(k *Key, alg string, supported []string) error {
	if !slices.Contains(supported, alg) {
		return errorf("InvalidKeyUsageException", "Algorithm %s is not supported by %s (%s).", alg, k.Meta.Arn, k.Meta.KeySpec)
	}
	return nil
}

// signer parses the key's current private key.
func (k *Key) signer() crypto.Signer {
	priv, _ := parsePriv(k.Material[len(k.Material)-1])
	return priv
}

func gcm(key []byte) cipher.AEAD {
	b, _ := aes.NewCipher(key)
	g, _ := cipher.NewGCM(b)
	return g
}

func oaepHash(alg string) hash.Hash {
	if alg == "RSAES_OAEP_SHA_1" {
		return sha1.New()
	}
	return sha256.New()
}

// Symmetric ciphertext blobs are self-describing:
// 0x01 | key id (36) | material generation (uint32) | nonce (12) | AES-256-GCM sealed data.
// The header and the encryption context are authenticated as AAD.
const hdr = 1 + 36 + 4

func aad(header []byte, ctx map[string]string) []byte {
	b := slices.Clone(header)
	if len(ctx) > 0 {
		j, _ := json.Marshal(ctx) // sorted keys: a canonical form
		b = append(b, j...)
	}
	return b
}

var errCiphertext = errorf("InvalidCiphertextException", "The ciphertext is invalid or was not encrypted under this key and encryption context.")

func (k *Key) encrypt(pt []byte, alg string, ctx map[string]string) ([]byte, error) {
	if k.Meta.KeySpec == "SYMMETRIC_DEFAULT" {
		gen := len(k.Material) - 1
		blob := make([]byte, hdr, hdr+12+len(pt)+16)
		blob[0] = 1
		copy(blob[1:], k.Meta.KeyId)
		binary.BigEndian.PutUint32(blob[37:], uint32(gen))
		nonce := random(12)
		blob = append(blob, nonce...)
		return gcm(k.Material[gen]).Seal(blob, nonce, pt, aad(blob[:hdr], ctx)), nil
	}
	ct, err := rsa.EncryptOAEP(oaepHash(alg), rand.Reader, k.signer().Public().(*rsa.PublicKey), pt, nil)
	if err != nil {
		return nil, errorf("ValidationException", "Plaintext is too long for %s with %s.", k.Meta.KeySpec, alg)
	}
	return ct, nil
}

func (k *Key) decrypt(blob []byte, alg string, ctx map[string]string) ([]byte, error) {
	if k.Meta.KeySpec == "SYMMETRIC_DEFAULT" {
		if len(blob) < hdr+12+16 {
			return nil, errCiphertext
		}
		gen := int(binary.BigEndian.Uint32(blob[37:hdr]))
		if gen >= len(k.Material) {
			return nil, errCiphertext
		}
		pt, err := gcm(k.Material[gen]).Open(nil, blob[hdr:hdr+12], blob[hdr+12:], aad(blob[:hdr], ctx))
		if err != nil {
			return nil, errCiphertext
		}
		return pt, nil
	}
	pt, err := rsa.DecryptOAEP(oaepHash(alg), nil, k.signer().(*rsa.PrivateKey), blob, nil)
	if err != nil {
		return nil, errCiphertext
	}
	return pt, nil
}

// open finds the key of a ciphertext and decrypts it; lock held. Symmetric
// blobs name their key; asymmetric ciphertexts need KeyId.
func (s *Service) open(c *core.Ctx, blob []byte, keyID, alg string, ctx map[string]string) (*Key, string, []byte, error) {
	id := ""
	if len(blob) > hdr && blob[0] == 1 {
		id = string(blob[1:37])
	}
	if keyID == "" {
		if id == "" || strings.ContainsAny(id, "/:") {
			return nil, "", nil, errCiphertext
		}
		keyID = id
	}
	k, err := s.cryptoKey(c, keyID, "ENCRYPT_DECRYPT")
	if err != nil {
		return nil, "", nil, err
	}
	alg = cmp.Or(alg, "SYMMETRIC_DEFAULT")
	if err := checkAlg(k, alg, k.Meta.EncryptionAlgorithms); err != nil {
		return nil, "", nil, err
	}
	if k.Meta.KeySpec == "SYMMETRIC_DEFAULT" && id != k.Meta.KeyId {
		if id == "" {
			return nil, "", nil, errCiphertext
		}
		return nil, "", nil, errorf("IncorrectKeyException", "The key ID in the request does not identify a KMS key that can perform this operation.")
	}
	pt, err := k.decrypt(blob, alg, ctx)
	return k, alg, pt, err
}

// ---- key management ----

func (s *Service) createKey(c *core.Ctx, in *struct {
	Policy, Description, KeyUsage, KeySpec, CustomerMasterKeySpec, Origin string
	Tags                                                                  []Tag
	MultiRegion                                                           bool
}) (any, error) {
	keySpec := cmp.Or(in.KeySpec, in.CustomerMasterKeySpec, "SYMMETRIC_DEFAULT")
	usage := cmp.Or(in.KeyUsage, "ENCRYPT_DECRYPT")
	sp, ok := specs[keySpec]
	switch {
	case keySpec == "SM2":
		return nil, errorf("UnsupportedOperationException", "KeySpec SM2 is not supported in this Region.")
	case !ok:
		return nil, errorf("ValidationException", "1 validation error detected: Value '%s' at 'keySpec' failed to satisfy constraint: Member must satisfy enum value set", keySpec)
	case !slices.Contains(sp.usages, usage):
		return nil, errorf("ValidationException", "KeyUsage %s is not compatible with KeySpec %s", usage, keySpec)
	case in.Origin != "" && in.Origin != "AWS_KMS":
		return nil, errorf("UnsupportedOperationException", "pogi only supports Origin AWS_KMS.")
	case in.MultiRegion:
		return nil, errorf("UnsupportedOperationException", "pogi does not support multi-Region keys.")
	case in.Policy != "" && !json.Valid([]byte(in.Policy)):
		return nil, errorf("MalformedPolicyDocumentException", "The key policy is not valid JSON.")
	}
	k, err := newKey(c.Account, c.Region, keySpec, usage) // outside the lock: RSA generation is slow
	if err != nil {
		return nil, err
	}
	k.Meta.Description, k.Tags = in.Description, in.Tags
	if in.Policy != "" {
		k.Policy = in.Policy
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st.Keys[c.Account+"/"+c.Region+"/"+k.Meta.KeyId] = k
	return map[string]any{"KeyMetadata": k.Meta}, nil
}

type keyInput struct {
	KeyId, Description, Marker string
	Limit                      int
}

func (s *Service) describeKey(c *core.Ctx, in *keyInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.key(c, in.KeyId)
	if err != nil {
		return nil, err
	}
	return map[string]any{"KeyMetadata": k.Meta}, nil
}

// page returns one page of items after an offset Marker.
func page[T any](key string, items []T, limit int, marker string) map[string]any {
	start, _ := strconv.Atoi(marker)
	start = min(max(start, 0), len(items))
	if limit <= 0 {
		limit = 100
	}
	end := min(start+limit, len(items))
	out := map[string]any{key: append([]T{}, items[start:end]...), "Truncated": end < len(items)}
	if end < len(items) {
		out["NextMarker"] = strconv.Itoa(end)
	}
	return out
}

func (s *Service) listKeys(c *core.Ctx, in *keyInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	type entry struct{ KeyId, KeyArn string }
	var out []entry
	for _, id := range core.SortedKeys(s.st.Keys) {
		if k := s.st.Keys[id]; strings.HasPrefix(id, c.Account+"/"+c.Region+"/") {
			out = append(out, entry{k.Meta.KeyId, k.Meta.Arn})
		}
	}
	return page("Keys", out, in.Limit, in.Marker), nil
}

func (s *Service) setEnabled(c *core.Ctx, in *keyInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.key(c, in.KeyId)
	if err != nil {
		return nil, err
	}
	if err := notPending(k); err != nil {
		return nil, err
	}
	k.Meta.Enabled = c.Op == "EnableKey"
	k.Meta.KeyState = map[bool]string{true: "Enabled", false: "Disabled"}[k.Meta.Enabled]
	return nil, nil
}

func (s *Service) scheduleKeyDeletion(c *core.Ctx, in *struct {
	KeyId               string
	PendingWindowInDays int
}) (any, error) {
	days := cmp.Or(in.PendingWindowInDays, 30)
	if days < 7 || days > 30 {
		return nil, errorf("ValidationException", "PendingWindowInDays must be between 7 and 30.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.key(c, in.KeyId)
	if err != nil {
		return nil, err
	}
	if err := notPending(k); err != nil {
		return nil, err
	}
	at := core.Time(time.Now().UTC().AddDate(0, 0, days))
	k.Meta.KeyState, k.Meta.Enabled, k.Meta.DeletionDate, k.Meta.PendingDeletionWindowInDays = "PendingDeletion", false, &at, days
	return map[string]any{"KeyId": k.Meta.Arn, "DeletionDate": at, "KeyState": "PendingDeletion", "PendingWindowInDays": days}, nil
}

func (s *Service) cancelKeyDeletion(c *core.Ctx, in *keyInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.key(c, in.KeyId)
	if err != nil {
		return nil, err
	}
	if k.Meta.KeyState != "PendingDeletion" {
		return nil, errorf("KMSInvalidStateException", "%s is not pending deletion.", k.Meta.Arn)
	}
	k.Meta.KeyState, k.Meta.DeletionDate, k.Meta.PendingDeletionWindowInDays = "Disabled", nil, 0
	return map[string]any{"KeyId": k.Meta.Arn}, nil
}

func (s *Service) updateKeyDescription(c *core.Ctx, in *keyInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.key(c, in.KeyId)
	if err != nil {
		return nil, err
	}
	if err := notPending(k); err != nil {
		return nil, err
	}
	k.Meta.Description = in.Description
	return nil, nil
}

func (s *Service) keyPolicy(c *core.Ctx, in *struct{ KeyId, PolicyName, Policy string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.key(c, in.KeyId)
	if err != nil {
		return nil, err
	}
	if in.PolicyName != "" && in.PolicyName != "default" {
		return nil, errorf("NotFoundException", "No such policy exists: %s", in.PolicyName)
	}
	switch c.Op {
	case "GetKeyPolicy":
		return map[string]any{"Policy": k.Policy, "PolicyName": "default"}, nil
	case "ListKeyPolicies":
		return map[string]any{"PolicyNames": []string{"default"}, "Truncated": false}, nil
	}
	if err := notPending(k); err != nil {
		return nil, err
	}
	if !json.Valid([]byte(in.Policy)) {
		return nil, errorf("MalformedPolicyDocumentException", "The key policy is not valid JSON.")
	}
	k.Policy = in.Policy
	return nil, nil
}

// rotation serves the rotation operations; only symmetric encryption keys rotate.
// ponytail: automatic rotation is recorded but never fires; RotateKeyOnDemand does rotate.
func (s *Service) rotation(c *core.Ctx, in *struct {
	KeyId                string
	RotationPeriodInDays int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.key(c, in.KeyId)
	if err != nil {
		return nil, err
	}
	if c.Op == "GetKeyRotationStatus" {
		out := map[string]any{"KeyId": k.Meta.Arn, "KeyRotationEnabled": k.Rotation}
		if k.Rotation {
			out["RotationPeriodInDays"] = k.RotationPeriod
			out["NextRotationDate"] = core.Time(time.Time(k.Meta.CreationDate).AddDate(0, 0, k.RotationPeriod))
		}
		return out, nil
	}
	if err := usable(k); err != nil {
		return nil, err
	}
	if k.Meta.KeySpec != "SYMMETRIC_DEFAULT" {
		return nil, errorf("UnsupportedOperationException", "%s does not support key rotation (%s).", k.Meta.Arn, k.Meta.KeySpec)
	}
	switch c.Op {
	case "EnableKeyRotation":
		days := cmp.Or(in.RotationPeriodInDays, 365)
		if days < 90 || days > 2560 {
			return nil, errorf("ValidationException", "RotationPeriodInDays must be between 90 and 2560.")
		}
		k.Rotation, k.RotationPeriod = true, days
	case "DisableKeyRotation":
		k.Rotation = false
	case "RotateKeyOnDemand":
		m, _ := generate("SYMMETRIC_DEFAULT")
		k.Material = append(k.Material, m)
		k.Rotations = append(k.Rotations, Rotation{KeyId: k.Meta.KeyId, RotationDate: now(), RotationType: "ON_DEMAND"})
		return map[string]any{"KeyId": k.Meta.KeyId}, nil
	}
	return nil, nil
}

func (s *Service) listKeyRotations(c *core.Ctx, in *keyInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.key(c, in.KeyId)
	if err != nil {
		return nil, err
	}
	if k.Meta.KeySpec != "SYMMETRIC_DEFAULT" {
		return nil, errorf("UnsupportedOperationException", "%s does not support key rotation (%s).", k.Meta.Arn, k.Meta.KeySpec)
	}
	return page("Rotations", k.Rotations, in.Limit, in.Marker), nil
}

var aliasName = regexp.MustCompile(`^alias/[a-zA-Z0-9/_-]{1,250}$`)

func (s *Service) alias(c *core.Ctx, in *struct{ AliasName, TargetKeyId string }) (any, error) {
	switch {
	case !strings.HasPrefix(in.AliasName, "alias/"):
		return nil, errorf("ValidationException", `Alias must start with the prefix "alias/".`)
	case strings.HasPrefix(in.AliasName, "alias/aws/"):
		return nil, errorf("NotAuthorizedException", "Alias names starting with alias/aws/ are reserved for AWS managed keys.")
	case !aliasName.MatchString(in.AliasName):
		return nil, errorf("ValidationException", "1 validation error detected: Value '%s' at 'aliasName' failed to satisfy constraint: Member must satisfy regular expression pattern: ^[a-zA-Z0-9/_-]+$", in.AliasName)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	name := c.Account + "/" + c.Region + "/" + in.AliasName
	al := s.st.Aliases[name]
	switch {
	case c.Op == "CreateAlias" && al != nil:
		return nil, errorf("AlreadyExistsException", "An alias with the name %s already exists", c.ARN("kms", in.AliasName))
	case c.Op != "CreateAlias" && al == nil:
		return nil, errorf("NotFoundException", "Alias %s is not found.", c.ARN("kms", in.AliasName))
	case c.Op == "DeleteAlias":
		delete(s.st.Aliases, name)
		return nil, nil
	}
	k, err := s.key(c, in.TargetKeyId)
	if err != nil {
		return nil, err
	}
	if err := notPending(k); err != nil {
		return nil, err
	}
	t := now()
	if al == nil {
		al = &Alias{AliasName: in.AliasName, AliasArn: c.ARN("kms", in.AliasName), CreationDate: t}
		s.st.Aliases[name] = al
	}
	al.TargetKeyId, al.LastUpdatedDate = k.Meta.KeyId, t
	return nil, nil
}

func (s *Service) listAliases(c *core.Ctx, in *keyInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	target := ""
	if in.KeyId != "" {
		k, err := s.key(c, in.KeyId)
		if err != nil {
			return nil, err
		}
		target = k.Meta.KeyId
	}
	var out []Alias
	for _, name := range core.SortedKeys(s.st.Aliases) {
		if a := s.st.Aliases[name]; strings.HasPrefix(name, c.Account+"/"+c.Region+"/") && (target == "" || a.TargetKeyId == target) {
			out = append(out, *a)
		}
	}
	return page("Aliases", out, in.Limit, in.Marker), nil
}

func (s *Service) tags(c *core.Ctx, in *struct {
	KeyId, Marker string
	Tags          []Tag
	TagKeys       []string
	Limit         int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.key(c, in.KeyId)
	if err != nil {
		return nil, err
	}
	if c.Op == "ListResourceTags" {
		return page("Tags", k.Tags, in.Limit, in.Marker), nil
	}
	if err := notPending(k); err != nil {
		return nil, err
	}
	tags := slices.DeleteFunc(slices.Clone(k.Tags), func(t Tag) bool {
		return slices.Contains(in.TagKeys, t.TagKey) || slices.ContainsFunc(in.Tags, func(n Tag) bool { return n.TagKey == t.TagKey })
	})
	if c.Op == "TagResource" {
		tags = append(tags, in.Tags...)
	}
	k.Tags = tags
	return nil, nil
}

// ---- cryptography ----

type cryptoInput struct {
	KeyId, EncryptionAlgorithm string
	Plaintext, CiphertextBlob  []byte
	EncryptionContext          map[string]string
}

func (s *Service) encrypt(c *core.Ctx, in *cryptoInput) (any, error) {
	if len(in.Plaintext) == 0 || len(in.Plaintext) > 4096 {
		return nil, errorf("ValidationException", "Plaintext must be between 1 and 4096 bytes.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.cryptoKey(c, in.KeyId, "ENCRYPT_DECRYPT")
	if err != nil {
		return nil, err
	}
	alg := cmp.Or(in.EncryptionAlgorithm, "SYMMETRIC_DEFAULT")
	if err := checkAlg(k, alg, k.Meta.EncryptionAlgorithms); err != nil {
		return nil, err
	}
	blob, err := k.encrypt(in.Plaintext, alg, in.EncryptionContext)
	if err != nil {
		return nil, err
	}
	return map[string]any{"KeyId": k.Meta.Arn, "CiphertextBlob": blob, "EncryptionAlgorithm": alg}, nil
}

func (s *Service) decrypt(c *core.Ctx, in *cryptoInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, alg, pt, err := s.open(c, in.CiphertextBlob, in.KeyId, in.EncryptionAlgorithm, in.EncryptionContext)
	if err != nil {
		return nil, err
	}
	return map[string]any{"KeyId": k.Meta.Arn, "Plaintext": pt, "EncryptionAlgorithm": alg}, nil
}

func (s *Service) reEncrypt(c *core.Ctx, in *struct {
	CiphertextBlob                                                                           []byte
	SourceKeyId, DestinationKeyId, SourceEncryptionAlgorithm, DestinationEncryptionAlgorithm string
	SourceEncryptionContext, DestinationEncryptionContext                                    map[string]string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src, srcAlg, pt, err := s.open(c, in.CiphertextBlob, in.SourceKeyId, in.SourceEncryptionAlgorithm, in.SourceEncryptionContext)
	if err != nil {
		return nil, err
	}
	dst, err := s.cryptoKey(c, in.DestinationKeyId, "ENCRYPT_DECRYPT")
	if err != nil {
		return nil, err
	}
	dstAlg := cmp.Or(in.DestinationEncryptionAlgorithm, "SYMMETRIC_DEFAULT")
	if err := checkAlg(dst, dstAlg, dst.Meta.EncryptionAlgorithms); err != nil {
		return nil, err
	}
	blob, err := dst.encrypt(pt, dstAlg, in.DestinationEncryptionContext)
	if err != nil {
		return nil, err
	}
	return map[string]any{"CiphertextBlob": blob, "SourceKeyId": src.Meta.Arn, "KeyId": dst.Meta.Arn,
		"SourceEncryptionAlgorithm": srcAlg, "DestinationEncryptionAlgorithm": dstAlg}, nil
}

type dataKeyInput struct {
	KeyId, KeySpec, KeyPairSpec string
	NumberOfBytes               int
	EncryptionContext           map[string]string
}

// dataKeyWrapper resolves the symmetric key that wraps data keys; lock held.
func (s *Service) dataKeyWrapper(c *core.Ctx, id string) (*Key, error) {
	k, err := s.cryptoKey(c, id, "ENCRYPT_DECRYPT")
	if err != nil {
		return nil, err
	}
	if k.Meta.KeySpec != "SYMMETRIC_DEFAULT" {
		return nil, errorf("InvalidKeyUsageException", "%s is not a symmetric encryption key.", k.Meta.Arn)
	}
	return k, nil
}

func (s *Service) generateDataKey(c *core.Ctx, in *dataKeyInput) (any, error) {
	n := in.NumberOfBytes
	switch {
	case in.KeySpec != "" && n != 0:
		return nil, errorf("ValidationException", "Please specify either number of bytes or key spec.")
	case in.KeySpec == "AES_256":
		n = 32
	case in.KeySpec == "AES_128":
		n = 16
	case in.KeySpec != "":
		return nil, errorf("ValidationException", "1 validation error detected: Value '%s' at 'keySpec' failed to satisfy constraint: Member must satisfy enum value set: [AES_256, AES_128]", in.KeySpec)
	case n < 1 || n > 1024:
		return nil, errorf("ValidationException", "Please specify either number of bytes (1-1024) or key spec.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.dataKeyWrapper(c, in.KeyId)
	if err != nil {
		return nil, err
	}
	pt := random(n)
	blob, _ := k.encrypt(pt, "SYMMETRIC_DEFAULT", in.EncryptionContext)
	out := map[string]any{"KeyId": k.Meta.Arn, "CiphertextBlob": blob}
	if c.Op == "GenerateDataKey" {
		out["Plaintext"] = pt
	}
	return out, nil
}

var dataKeyPairSpecs = []string{"RSA_2048", "RSA_3072", "RSA_4096", "ECC_NIST_P256", "ECC_NIST_P384", "ECC_NIST_P521", "ECC_SECG_P256K1"}

func (s *Service) generateDataKeyPair(c *core.Ctx, in *dataKeyInput) (any, error) {
	if !slices.Contains(dataKeyPairSpecs, in.KeyPairSpec) {
		return nil, errorf("ValidationException", "1 validation error detected: Value '%s' at 'keyPairSpec' failed to satisfy constraint: Member must satisfy enum value set", in.KeyPairSpec)
	}
	priv, err := generatePair(in.KeyPairSpec) // outside the lock: RSA generation is slow
	if err != nil {
		return nil, err
	}
	der, _ := privDER(priv)
	pub, _ := pubDER(priv.Public())
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.dataKeyWrapper(c, in.KeyId)
	if err != nil {
		return nil, err
	}
	blob, _ := k.encrypt(der, "SYMMETRIC_DEFAULT", in.EncryptionContext)
	out := map[string]any{"KeyId": k.Meta.Arn, "KeyPairSpec": in.KeyPairSpec, "PublicKey": pub, "PrivateKeyCiphertextBlob": blob}
	if c.Op == "GenerateDataKeyPair" {
		out["PrivateKeyPlaintext"] = der
	}
	return out, nil
}

func (s *Service) generateRandom(_ *core.Ctx, in *struct{ NumberOfBytes int }) (any, error) {
	if in.NumberOfBytes < 1 || in.NumberOfBytes > 1024 {
		return nil, errorf("ValidationException", "NumberOfBytes must be between 1 and 1024.")
	}
	return map[string]any{"Plaintext": random(in.NumberOfBytes)}, nil
}

// digestOf hashes a RAW message with the algorithm's hash; a DIGEST message
// must already have the hash's length.
func digestOf(alg, msgType string, msg []byte) ([]byte, crypto.Hash, error) {
	h := crypto.SHA256
	if strings.HasSuffix(alg, "_384") {
		h = crypto.SHA384
	} else if strings.HasSuffix(alg, "_512") {
		h = crypto.SHA512
	}
	switch {
	case len(msg) == 0 || len(msg) > 4096:
		return nil, 0, errorf("ValidationException", "Message must be between 1 and 4096 bytes.")
	case msgType == "DIGEST" && len(msg) != h.Size():
		return nil, 0, errorf("ValidationException", "Digest is invalid length for algorithm %s.", alg)
	case msgType == "DIGEST":
		return msg, h, nil
	case msgType != "" && msgType != "RAW":
		return nil, 0, errorf("ValidationException", "Invalid MessageType %s.", msgType)
	}
	d := h.New()
	d.Write(msg)
	return d.Sum(nil), h, nil
}

// hashless algorithms hash inside the signature scheme, so the message is passed as it is.
func hashless(alg string) bool {
	return strings.HasPrefix(alg, "ED25519") || strings.HasPrefix(alg, "ML_DSA")
}

// unhashedMessage checks the message and type: ED25519_SHA_512 and ML_DSA take
// RAW; ED25519_PH_SHA_512 takes a 64-byte DIGEST.
func unhashedMessage(alg, msgType string, msg []byte) ([]byte, error) {
	want := "RAW"
	if alg == "ED25519_PH_SHA_512" {
		want = "DIGEST"
	}
	switch {
	case len(msg) == 0 || len(msg) > 4096:
		return nil, errorf("ValidationException", "Message must be between 1 and 4096 bytes.")
	case cmp.Or(msgType, "RAW") != want:
		return nil, errorf("ValidationException", "MessageType %s is not valid for algorithm %s; use %s.", cmp.Or(msgType, "RAW"), alg, want)
	case want == "DIGEST" && len(msg) != 64:
		return nil, errorf("ValidationException", "Digest is invalid length for algorithm %s.", alg)
	}
	return msg, nil
}

func verify(pub crypto.PublicKey, alg string, h crypto.Hash, digest, sig []byte) bool {
	switch pub := pub.(type) {
	case *rsa.PublicKey:
		if strings.HasPrefix(alg, "RSASSA_PSS") {
			return rsa.VerifyPSS(pub, h, digest, sig, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthAuto}) == nil
		}
		return rsa.VerifyPKCS1v15(pub, h, digest, sig) == nil
	}
	return verifyECDSA(pub, digest, sig)
}

// sign serves Sign and Verify. ECDSA signatures are DER-encoded and PSS uses
// a salt as long as the hash, like AWS.
func (s *Service) sign(c *core.Ctx, in *struct {
	KeyId, MessageType, SigningAlgorithm string
	Message, Signature                   []byte
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.cryptoKey(c, in.KeyId, "SIGN_VERIFY")
	if err != nil {
		return nil, err
	}
	if err := checkAlg(k, in.SigningAlgorithm, k.Meta.SigningAlgorithms); err != nil {
		return nil, err
	}
	key := k.signer()
	out := map[string]any{"KeyId": k.Meta.Arn, "SigningAlgorithm": in.SigningAlgorithm}
	if hashless(in.SigningAlgorithm) {
		msg, err := unhashedMessage(in.SigningAlgorithm, in.MessageType, in.Message)
		if err != nil {
			return nil, err
		}
		if c.Op == "Verify" {
			if !verifyUnhashed(key.Public(), in.SigningAlgorithm, msg, in.Signature) {
				return nil, errorf("KMSInvalidSignatureException", "The signature is not valid.")
			}
			out["SignatureValid"] = true
			return out, nil
		}
		sig, err := signUnhashed(key, in.SigningAlgorithm, msg)
		if err != nil {
			return nil, err
		}
		out["Signature"] = sig
		return out, nil
	}
	digest, h, err := digestOf(in.SigningAlgorithm, in.MessageType, in.Message)
	if err != nil {
		return nil, err
	}
	if c.Op == "Verify" {
		if !verify(key.Public(), in.SigningAlgorithm, h, digest, in.Signature) {
			return nil, errorf("KMSInvalidSignatureException", "The signature is not valid.")
		}
		out["SignatureValid"] = true
		return out, nil
	}
	var opts crypto.SignerOpts = h
	if strings.HasPrefix(in.SigningAlgorithm, "RSASSA_PSS") {
		opts = &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: h}
	}
	sig, err := key.Sign(rand.Reader, digest, opts)
	if err != nil {
		return nil, err
	}
	out["Signature"] = sig
	return out, nil
}

func (s *Service) getPublicKey(c *core.Ctx, in *keyInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.key(c, in.KeyId)
	if err != nil {
		return nil, err
	}
	if err := usable(k); err != nil {
		return nil, err
	}
	if k.Meta.KeySpec == "SYMMETRIC_DEFAULT" || strings.HasPrefix(k.Meta.KeySpec, "HMAC") {
		return nil, errorf("UnsupportedOperationException", "%s is not an asymmetric key.", k.Meta.Arn)
	}
	der, _ := pubDER(k.signer().Public())
	return struct {
		KeyId                                    string
		PublicKey                                []byte
		KeySpec, CustomerMasterKeySpec, KeyUsage string
		EncryptionAlgorithms                     []string `json:",omitempty"`
		SigningAlgorithms                        []string `json:",omitempty"`
	}{k.Meta.Arn, der, k.Meta.KeySpec, k.Meta.KeySpec, k.Meta.KeyUsage, k.Meta.EncryptionAlgorithms, k.Meta.SigningAlgorithms}, nil
}

func macHash(alg string) func() hash.Hash {
	switch alg {
	case "HMAC_SHA_224":
		return sha256.New224
	case "HMAC_SHA_384":
		return sha512.New384
	case "HMAC_SHA_512":
		return sha512.New
	}
	return sha256.New
}

// mac serves GenerateMac and VerifyMac.
func (s *Service) mac(c *core.Ctx, in *struct {
	KeyId, MacAlgorithm string
	Message, Mac        []byte
}) (any, error) {
	if len(in.Message) == 0 || len(in.Message) > 4096 {
		return nil, errorf("ValidationException", "Message must be between 1 and 4096 bytes.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k, err := s.cryptoKey(c, in.KeyId, "GENERATE_VERIFY_MAC")
	if err != nil {
		return nil, err
	}
	if err := checkAlg(k, in.MacAlgorithm, k.Meta.MacAlgorithms); err != nil {
		return nil, err
	}
	m := hmac.New(macHash(in.MacAlgorithm), k.Material[len(k.Material)-1])
	m.Write(in.Message)
	sum := m.Sum(nil)
	if c.Op == "GenerateMac" {
		return map[string]any{"KeyId": k.Meta.Arn, "Mac": sum, "MacAlgorithm": in.MacAlgorithm}, nil
	}
	if !hmac.Equal(sum, in.Mac) {
		return nil, errorf("KMSInvalidMacException", "The MAC is not valid.")
	}
	return map[string]any{"KeyId": k.Meta.Arn, "MacValid": true, "MacAlgorithm": in.MacAlgorithm}, nil
}

// ---- console ----

// Resources lists keys and aliases.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := core.Table{Name: "Keys", Columns: []string{"KeyId", "Alias", "Region", "Account", "Spec", "Usage", "State"}}
	aliases := core.Table{Name: "Aliases", Columns: []string{"Alias", "Target key", "Region", "Account"}}
	first := map[string]string{} // key id → first alias
	for _, name := range core.SortedKeys(s.st.Aliases) {
		a := s.st.Aliases[name]
		p := strings.SplitN(name, "/", 3)
		aliases.Rows = append(aliases.Rows, []string{a.AliasName, a.TargetKeyId, p[1], p[0]})
		if first[a.TargetKeyId] == "" {
			first[a.TargetKeyId] = a.AliasName
		}
	}
	for _, id := range core.SortedKeys(s.st.Keys) {
		k := s.st.Keys[id]
		keys.Rows = append(keys.Rows, []string{k.Meta.KeyId, first[k.Meta.KeyId], k.Region, k.Meta.AWSAccountId, k.Meta.KeySpec, k.Meta.KeyUsage, k.Meta.KeyState})
	}
	return []core.Table{keys, aliases}
}

// Inspect returns a key's metadata, policy, tags and rotation settings (never
// its material), or an alias.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch table {
	case "Keys":
		for _, k := range s.st.Keys {
			if k.Meta.KeyId == id {
				return map[string]any{"metadata": k.Meta, "policy": k.Policy, "tags": slices.Clone(k.Tags),
					"rotationEnabled": k.Rotation, "rotationPeriodInDays": k.RotationPeriod, "rotations": slices.Clone(k.Rotations)}
			}
		}
	case "Aliases":
		for _, name := range core.SortedKeys(s.st.Aliases) {
			if a := s.st.Aliases[name]; a.AliasName == id {
				return *a
			}
		}
	}
	return nil
}
