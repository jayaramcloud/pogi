package kms_test

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"hash/crc32"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	gkms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/kms"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v, want %v", err, want)
	}
}

func crc(b []byte) *wrapperspb.Int64Value {
	return wrapperspb.Int64(int64(crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli))))
}

func setup(t *testing.T) (*kms.Service, *gkms.KeyManagementClient, string) {
	t.Helper()
	var svc *kms.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = kms.New(h); return svc })
	c := must(gkms.NewKeyManagementClient(ctx, option.WithEndpoint(strings.TrimPrefix(url, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	t.Cleanup(func() { c.Close() })
	return svc, c, url
}

const parent = "projects/proj/locations/global"

func ring(t *testing.T, c *gkms.KeyManagementClient) string {
	t.Helper()
	return must(c.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{Parent: parent, KeyRingId: "ring"})).Name
}

func newKey(t *testing.T, c *gkms.KeyManagementClient, rg, id string, p kmspb.CryptoKey_CryptoKeyPurpose, a kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm) *kmspb.CryptoKey {
	t.Helper()
	k := &kmspb.CryptoKey{Purpose: p}
	if a != 0 {
		k.VersionTemplate = &kmspb.CryptoKeyVersionTemplate{Algorithm: a}
	}
	return must(c.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: rg, CryptoKeyId: id, CryptoKey: k}))
}

func TestEncryptRotateDestroy(t *testing.T) {
	svc, c, _ := setup(t)
	rg := ring(t, c)
	k := newKey(t, c, rg, "sym", kmspb.CryptoKey_ENCRYPT_DECRYPT, 0)
	if k.Name != rg+"/cryptoKeys/sym" || k.Primary.GetName() != k.Name+"/cryptoKeyVersions/1" || k.Primary.State != kmspb.CryptoKeyVersion_ENABLED ||
		k.VersionTemplate.Algorithm != kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION {
		t.Fatalf("key = %v", k)
	}

	pt, aad := []byte("attack at dawn"), []byte("ctx")
	enc := must(c.Encrypt(ctx, &kmspb.EncryptRequest{Name: k.Name, Plaintext: pt, AdditionalAuthenticatedData: aad, PlaintextCrc32C: crc(pt),
		AdditionalAuthenticatedDataCrc32C: crc(aad)}))
	if enc.Name != k.Primary.Name || !enc.VerifiedPlaintextCrc32C || !enc.VerifiedAdditionalAuthenticatedDataCrc32C ||
		enc.CiphertextCrc32C.Value != crc(enc.Ciphertext).Value || bytes.Contains(enc.Ciphertext, pt) {
		t.Fatalf("encrypt = %v", enc)
	}
	dec := must(c.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.Name, Ciphertext: enc.Ciphertext, AdditionalAuthenticatedData: aad,
		CiphertextCrc32C: crc(enc.Ciphertext), AdditionalAuthenticatedDataCrc32C: crc(aad)}))
	if !bytes.Equal(dec.Plaintext, pt) || dec.PlaintextCrc32C.Value != crc(pt).Value || !dec.UsedPrimary {
		t.Fatalf("decrypt = %v", dec)
	}
	_, err := c.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.Name, Ciphertext: enc.Ciphertext, AdditionalAuthenticatedData: []byte("other")})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.Name, Ciphertext: enc.Ciphertext, CiphertextCrc32C: crc([]byte("x"))})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.Encrypt(ctx, &kmspb.EncryptRequest{Name: k.Name, Plaintext: pt, PlaintextCrc32C: crc([]byte("x"))})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.Name, Ciphertext: []byte("not a ciphertext at all, but long enough")})
	wantCode(t, err, codes.InvalidArgument)

	// Rotation: a new primary version; the old ciphertext still decrypts, on a non-primary version.
	v2 := must(c.CreateCryptoKeyVersion(ctx, &kmspb.CreateCryptoKeyVersionRequest{Parent: k.Name}))
	if got := must(c.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: k.Name})); got.Primary.Name != k.Primary.Name {
		t.Fatalf("a new version must not become primary by itself: %v", got.Primary)
	}
	rotated := must(c.UpdateCryptoKeyPrimaryVersion(ctx, &kmspb.UpdateCryptoKeyPrimaryVersionRequest{Name: k.Name, CryptoKeyVersionId: "2"}))
	if rotated.Primary.Name != v2.Name {
		t.Fatalf("rotated = %v", rotated.Primary)
	}
	enc2 := must(c.Encrypt(ctx, &kmspb.EncryptRequest{Name: k.Name, Plaintext: pt}))
	if enc2.Name != v2.Name {
		t.Fatalf("new ciphertext used %s", enc2.Name)
	}
	old := must(c.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.Name, Ciphertext: enc.Ciphertext, AdditionalAuthenticatedData: aad}))
	cur := must(c.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.Name, Ciphertext: enc2.Ciphertext}))
	if !bytes.Equal(old.Plaintext, pt) || old.UsedPrimary || !bytes.Equal(cur.Plaintext, pt) || !cur.UsedPrimary {
		t.Fatalf("old %v current %v", old, cur)
	}

	// Disabling, destroying and restoring a version.
	v1 := k.Primary.Name
	dis := must(c.UpdateCryptoKeyVersion(ctx, &kmspb.UpdateCryptoKeyVersionRequest{CryptoKeyVersion: &kmspb.CryptoKeyVersion{Name: v1, State: kmspb.CryptoKeyVersion_DISABLED},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}}}))
	if dis.State != kmspb.CryptoKeyVersion_DISABLED {
		t.Fatalf("disabled = %v", dis)
	}
	_, err = c.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.Name, Ciphertext: enc.Ciphertext, AdditionalAuthenticatedData: aad})
	wantCode(t, err, codes.FailedPrecondition)
	must(c.UpdateCryptoKeyVersion(ctx, &kmspb.UpdateCryptoKeyVersionRequest{CryptoKeyVersion: &kmspb.CryptoKeyVersion{Name: v1, State: kmspb.CryptoKeyVersion_ENABLED},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}}}))
	must(c.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.Name, Ciphertext: enc.Ciphertext, AdditionalAuthenticatedData: aad}))

	sched := must(c.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: v1}))
	if sched.State != kmspb.CryptoKeyVersion_DESTROY_SCHEDULED || sched.DestroyTime.AsTime().Before(time.Now().Add(29*24*time.Hour)) {
		t.Fatalf("scheduled = %v", sched)
	}
	_, err = c.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.Name, Ciphertext: enc.Ciphertext, AdditionalAuthenticatedData: aad})
	wantCode(t, err, codes.FailedPrecondition)
	restored := must(c.RestoreCryptoKeyVersion(ctx, &kmspb.RestoreCryptoKeyVersionRequest{Name: v1}))
	if restored.State != kmspb.CryptoKeyVersion_DISABLED || restored.DestroyTime != nil {
		t.Fatalf("restored = %v", restored)
	}
	must(c.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: v1}))
	svc.Tick(time.Now().Add(31 * 24 * time.Hour))
	gone := must(c.GetCryptoKeyVersion(ctx, &kmspb.GetCryptoKeyVersionRequest{Name: v1}))
	if gone.State != kmspb.CryptoKeyVersion_DESTROYED || gone.DestroyEventTime == nil {
		t.Fatalf("destroyed = %v", gone)
	}
	_, err = c.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.Name, Ciphertext: enc.Ciphertext, AdditionalAuthenticatedData: aad})
	wantCode(t, err, codes.FailedPrecondition)
	_, err = c.RestoreCryptoKeyVersion(ctx, &kmspb.RestoreCryptoKeyVersionRequest{Name: v1})
	wantCode(t, err, codes.FailedPrecondition)
	must(c.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.Name, Ciphertext: enc2.Ciphertext})) // version 2 is untouched

	var states []string
	for it := c.ListCryptoKeyVersions(ctx, &kmspb.ListCryptoKeyVersionsRequest{Parent: k.Name}); ; {
		v, err := it.Next()
		if err == iterator.Done {
			break
		}
		states = append(states, must(v, err).State.String())
	}
	if strings.Join(states, ",") != "DESTROYED,ENABLED" {
		t.Fatalf("states = %v", states)
	}
}

func TestAutoRotationAndDestroyDuration(t *testing.T) {
	svc, c, _ := setup(t)
	rg := ring(t, c)
	k := must(c.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: rg, CryptoKeyId: "auto", CryptoKey: &kmspb.CryptoKey{
		Purpose:                  kmspb.CryptoKey_ENCRYPT_DECRYPT,
		RotationSchedule:         &kmspb.CryptoKey_RotationPeriod{RotationPeriod: durationpb.New(24 * time.Hour)},
		DestroyScheduledDuration: durationpb.New(time.Hour),
		Labels:                   map[string]string{"team": "a"}}}))
	if k.NextRotationTime == nil || k.GetRotationPeriod().AsDuration() != 24*time.Hour || k.DestroyScheduledDuration.AsDuration() != time.Hour {
		t.Fatalf("key = %v", k)
	}
	enc := must(c.Encrypt(ctx, &kmspb.EncryptRequest{Name: k.Name, Plaintext: []byte("x")}))
	svc.Tick(k.NextRotationTime.AsTime().Add(time.Minute))
	got := must(c.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: k.Name}))
	if got.Primary.Name != k.Name+"/cryptoKeyVersions/2" || !got.NextRotationTime.AsTime().After(k.NextRotationTime.AsTime()) {
		t.Fatalf("after rotation: %v", got)
	}
	must(c.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.Name, Ciphertext: enc.Ciphertext}))

	// Patching labels and the rotation time through an update mask.
	next := timestamppb.New(time.Now().Add(100 * 24 * time.Hour))
	up := must(c.UpdateCryptoKey(ctx, &kmspb.UpdateCryptoKeyRequest{CryptoKey: &kmspb.CryptoKey{Name: k.Name, Labels: map[string]string{"team": "b"}, NextRotationTime: next},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels", "next_rotation_time"}}}))
	if up.Labels["team"] != "b" || !up.NextRotationTime.AsTime().Equal(next.AsTime()) || up.GetRotationPeriod() == nil {
		t.Fatalf("updated = %v", up)
	}
	_, err := c.UpdateCryptoKey(ctx, &kmspb.UpdateCryptoKeyRequest{CryptoKey: &kmspb.CryptoKey{Name: k.Name, Purpose: kmspb.CryptoKey_MAC},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"purpose"}}})
	wantCode(t, err, codes.InvalidArgument)

	// Destroy with the key's own (short) duration.
	v := k.Name + "/cryptoKeyVersions/2"
	must(c.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: v}))
	svc.Tick(time.Now().Add(2 * time.Hour))
	if got := must(c.GetCryptoKeyVersion(ctx, &kmspb.GetCryptoKeyVersionRequest{Name: v})); got.State != kmspb.CryptoKeyVersion_DESTROYED {
		t.Fatalf("destroyed = %v", got)
	}
}

func TestAsymmetric(t *testing.T) {
	_, c, _ := setup(t)
	rg := ring(t, c)
	pub := func(name string) any {
		pk := must(c.GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{Name: name}))
		if pk.PemCrc32C.Value != crc([]byte(pk.Pem)).Value || pk.Name != name {
			t.Fatalf("public key = %v", pk)
		}
		block, _ := pem.Decode([]byte(pk.Pem))
		return must(x509.ParsePKIXPublicKey(block.Bytes))
	}

	ec := newKey(t, c, rg, "ec", kmspb.CryptoKey_ASYMMETRIC_SIGN, kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256)
	if ec.Primary != nil {
		t.Fatalf("asymmetric keys have no primary: %v", ec.Primary)
	}
	ecv := ec.Name + "/cryptoKeyVersions/1"
	digest := sha256.Sum256([]byte("message"))
	sig := must(c.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{Name: ecv, Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest[:]}},
		DigestCrc32C: crc(digest[:])}))
	if !sig.VerifiedDigestCrc32C || sig.SignatureCrc32C.Value != crc(sig.Signature).Value || !ecdsa.VerifyASN1(pub(ecv).(*ecdsa.PublicKey), digest[:], sig.Signature) {
		t.Fatalf("EC signature does not verify: %v", sig)
	}
	_, err := c.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{Name: ecv, Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest[:5]}}})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{Name: ecv, Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest[:]}}, DigestCrc32C: crc([]byte("x"))})
	wantCode(t, err, codes.InvalidArgument)

	for algo, pss := range map[kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm]bool{
		kmspb.CryptoKeyVersion_RSA_SIGN_PSS_2048_SHA256: true, kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_2048_SHA256: false} {
		k := newKey(t, c, rg, strings.ToLower(algo.String()), kmspb.CryptoKey_ASYMMETRIC_SIGN, algo)
		v := k.Name + "/cryptoKeyVersions/1"
		sig := must(c.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{Name: v, Data: []byte("message"), DataCrc32C: crc([]byte("message"))})) // data: pogi hashes it
		rsaPub := pub(v).(*rsa.PublicKey)
		if pss {
			err = rsa.VerifyPSS(rsaPub, crypto.SHA256, digest[:], sig.Signature, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
		} else {
			err = rsa.VerifyPKCS1v15(rsaPub, crypto.SHA256, digest[:], sig.Signature)
		}
		if err != nil || !sig.VerifiedDataCrc32C {
			t.Fatalf("%s signature does not verify: %v", algo, err)
		}
	}

	dk := newKey(t, c, rg, "oaep", kmspb.CryptoKey_ASYMMETRIC_DECRYPT, kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_2048_SHA256)
	dv := dk.Name + "/cryptoKeyVersions/1"
	ct := must(rsa.EncryptOAEP(sha256.New(), rand.Reader, pub(dv).(*rsa.PublicKey), []byte("secret"), nil))
	got := must(c.AsymmetricDecrypt(ctx, &kmspb.AsymmetricDecryptRequest{Name: dv, Ciphertext: ct, CiphertextCrc32C: crc(ct)}))
	if string(got.Plaintext) != "secret" || !got.VerifiedCiphertextCrc32C || got.PlaintextCrc32C.Value != crc([]byte("secret")).Value {
		t.Fatalf("decrypted = %v", got)
	}
	_, err = c.AsymmetricDecrypt(ctx, &kmspb.AsymmetricDecryptRequest{Name: dv, Ciphertext: ct[:50]})
	wantCode(t, err, codes.InvalidArgument)

	// Purposes are enforced.
	_, err = c.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{Name: dv, Data: []byte("x")})
	wantCode(t, err, codes.FailedPrecondition)
	_, err = c.Encrypt(ctx, &kmspb.EncryptRequest{Name: ecv, Plaintext: []byte("x")})
	wantCode(t, err, codes.FailedPrecondition)
	_, err = c.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{Name: ec.Name, Data: []byte("x")})
	wantCode(t, err, codes.InvalidArgument)
}

func TestMAC(t *testing.T) {
	_, c, _ := setup(t)
	rg := ring(t, c)
	k := newKey(t, c, rg, "mac", kmspb.CryptoKey_MAC, kmspb.CryptoKeyVersion_HMAC_SHA256)
	v := k.Name + "/cryptoKeyVersions/1"
	data := []byte("payload")
	sig := must(c.MacSign(ctx, &kmspb.MacSignRequest{Name: v, Data: data, DataCrc32C: crc(data)}))
	if len(sig.Mac) != 32 || !sig.VerifiedDataCrc32C || sig.MacCrc32C.Value != crc(sig.Mac).Value {
		t.Fatalf("sign = %v", sig)
	}
	ok := must(c.MacVerify(ctx, &kmspb.MacVerifyRequest{Name: v, Data: data, Mac: sig.Mac, DataCrc32C: crc(data), MacCrc32C: crc(sig.Mac)}))
	if !ok.Success || !ok.VerifiedDataCrc32C || !ok.VerifiedMacCrc32C || !ok.VerifiedSuccessIntegrity {
		t.Fatalf("verify = %v", ok)
	}
	if bad := must(c.MacVerify(ctx, &kmspb.MacVerifyRequest{Name: v, Data: []byte("tampered"), Mac: sig.Mac})); bad.Success {
		t.Fatal("a MAC of other data verified")
	}
}

func TestResourcesAndErrors(t *testing.T) {
	svc, c, _ := setup(t)
	rg := ring(t, c)
	_, err := c.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{Parent: parent, KeyRingId: "ring"})
	wantCode(t, err, codes.AlreadyExists)
	_, err = c.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{Parent: parent, KeyRingId: "bad/id"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.GetKeyRing(ctx, &kmspb.GetKeyRingRequest{Name: parent + "/keyRings/none"})
	wantCode(t, err, codes.NotFound)
	_, err = c.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: parent + "/keyRings/none", CryptoKeyId: "k", CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT}})
	wantCode(t, err, codes.NotFound)
	_, err = c.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: rg, CryptoKeyId: "k", CryptoKey: &kmspb.CryptoKey{}})
	wantCode(t, err, codes.InvalidArgument) // no purpose
	_, err = c.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: rg, CryptoKeyId: "k", CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ASYMMETRIC_SIGN}})
	wantCode(t, err, codes.InvalidArgument) // no algorithm
	_, err = c.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: rg, CryptoKeyId: "k", CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_MAC,
		VersionTemplate: &kmspb.CryptoKeyVersionTemplate{Algorithm: kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256}}})
	wantCode(t, err, codes.InvalidArgument) // algorithm of another purpose
	newKey(t, c, rg, "k", kmspb.CryptoKey_ENCRYPT_DECRYPT, 0)
	_, err = c.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: rg, CryptoKeyId: "k", CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT}})
	wantCode(t, err, codes.AlreadyExists)
	_, err = c.Encrypt(ctx, &kmspb.EncryptRequest{Name: rg + "/cryptoKeys/none", Plaintext: []byte("x")})
	wantCode(t, err, codes.NotFound)

	// Without an initial version there is nothing to encrypt with, and no primary.
	empty := must(c.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: rg, CryptoKeyId: "empty", SkipInitialVersionCreation: true,
		CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT}}))
	if empty.Primary != nil {
		t.Fatalf("empty = %v", empty)
	}
	_, err = c.Encrypt(ctx, &kmspb.EncryptRequest{Name: empty.Name, Plaintext: []byte("x")})
	wantCode(t, err, codes.FailedPrecondition)
	_, err = c.UpdateCryptoKeyPrimaryVersion(ctx, &kmspb.UpdateCryptoKeyPrimaryVersionRequest{Name: empty.Name, CryptoKeyVersionId: "1"})
	wantCode(t, err, codes.NotFound)

	// Lists page.
	for _, id := range []string{"a", "b"} {
		must(c.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{Parent: parent, KeyRingId: id}))
	}
	var first []*kmspb.KeyRing
	token := must(iterator.NewPager(c.ListKeyRings(ctx, &kmspb.ListKeyRingsRequest{Parent: parent}), 2, "").NextPage(&first))
	var second []*kmspb.KeyRing
	must(iterator.NewPager(c.ListKeyRings(ctx, &kmspb.ListKeyRingsRequest{Parent: parent}), 2, token).NextPage(&second))
	if len(first) != 2 || token == "" || len(second) != 1 || second[0].Name != rg {
		t.Fatalf("pages = %v / %q / %v", first, token, second)
	}
	var keys []string
	for it := c.ListCryptoKeys(ctx, &kmspb.ListCryptoKeysRequest{Parent: rg}); ; {
		k, err := it.Next()
		if err == iterator.Done {
			break
		}
		keys = append(keys, must(k, err).Name[len(rg):])
	}
	if strings.Join(keys, ",") != "/cryptoKeys/empty,/cryptoKeys/k" {
		t.Fatalf("keys = %v", keys)
	}

	// Random bytes.
	r := must(c.GenerateRandomBytes(ctx, &kmspb.GenerateRandomBytesRequest{Location: parent, LengthBytes: 32}))
	if len(r.Data) != 32 || r.DataCrc32C.Value != crc(r.Data).Value {
		t.Fatalf("random = %v", r)
	}
	_, err = c.GenerateRandomBytes(ctx, &kmspb.GenerateRandomBytesRequest{Location: parent, LengthBytes: 1})
	wantCode(t, err, codes.InvalidArgument)

	// IAM on key rings and keys, stored as is.
	for _, res := range []string{rg, rg + "/cryptoKeys/k"} {
		if p := must(c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: res})); len(p.Bindings) != 0 {
			t.Fatalf("policy = %v", p)
		}
		pol := &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/cloudkms.cryptoKeyEncrypter", Members: []string{"user:dev@example.com"}}}}
		set := must(c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: res, Policy: pol}))
		got := must(c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: res}))
		if got.Bindings[0].Members[0] != "user:dev@example.com" || len(set.Etag) == 0 || string(got.Etag) != string(set.Etag) {
			t.Fatalf("policy = %v", got)
		}
		_, err = c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: res, Policy: &iampb.Policy{Etag: []byte("stale")}})
		wantCode(t, err, codes.Aborted)
		perms := must(c.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: res, Permissions: []string{"cloudkms.cryptoKeyVersions.useToEncrypt"}}))
		if len(perms.Permissions) != 1 {
			t.Fatalf("permissions = %v", perms)
		}
	}
	_, err = c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: rg + "/cryptoKeys/none"})
	wantCode(t, err, codes.NotFound)

	// Console: no key material.
	tables := svc.Resources()
	if len(tables) != 2 || len(tables[1].Rows) != 2 || tables[0].Rows[2][3] != "2" {
		t.Fatalf("tables = %v", tables)
	}
	b, _ := json.Marshal(svc.Inspect("Keys", rg+"/cryptoKeys/k"))
	if !strings.Contains(string(b), "cryptoKeyVersions/1") || strings.Contains(string(b), "mat") {
		t.Fatalf("inspect = %s", b)
	}
}

// TestPersistence: key material is saved, so old ciphertext and signatures survive a restart.
func TestPersistence(t *testing.T) {
	svc, c, _ := setup(t)
	rg := ring(t, c)
	k := newKey(t, c, rg, "sym", kmspb.CryptoKey_ENCRYPT_DECRYPT, 0)
	ec := newKey(t, c, rg, "ec", kmspb.CryptoKey_ASYMMETRIC_SIGN, kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256)
	enc := must(c.Encrypt(ctx, &kmspb.EncryptRequest{Name: k.Name, Plaintext: []byte("kept")}))
	must(c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: rg, Policy: &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/viewer", Members: []string{"user:a@b.c"}}}}}))
	saved := must(svc.Save())

	svc2, c2, _ := setup(t)
	if err := svc2.Load(saved); err != nil {
		t.Fatal(err)
	}
	if got := must(c2.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.Name, Ciphertext: enc.Ciphertext})); string(got.Plaintext) != "kept" {
		t.Fatalf("decrypted = %q", got.Plaintext)
	}
	digest := sha256.Sum256([]byte("m"))
	must(c2.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{Name: ec.Name + "/cryptoKeyVersions/1", Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest[:]}}}))
	if p := must(c2.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: rg})); len(p.Bindings) != 1 {
		t.Fatalf("policy = %v", p)
	}
	svc2.Reset()
	_, err := c2.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: k.Name})
	wantCode(t, err, codes.NotFound)
}

// TestREST drives the REST surface with the official REST client and with plain net/http.
func TestREST(t *testing.T) {
	_, _, url := setup(t)
	c := must(gkms.NewKeyManagementRESTClient(ctx, option.WithEndpoint(url), option.WithoutAuthentication()))
	t.Cleanup(func() { c.Close() })
	rg := ring(t, c)
	k := newKey(t, c, rg, "sym", kmspb.CryptoKey_ENCRYPT_DECRYPT, 0)
	enc := must(c.Encrypt(ctx, &kmspb.EncryptRequest{Name: k.Name, Plaintext: []byte("rest"), AdditionalAuthenticatedData: []byte("a"), PlaintextCrc32C: crc([]byte("rest"))}))
	dec := must(c.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.Name, Ciphertext: enc.Ciphertext, AdditionalAuthenticatedData: []byte("a")}))
	if string(dec.Plaintext) != "rest" || !enc.VerifiedPlaintextCrc32C {
		t.Fatalf("rest round trip = %v / %v", enc, dec)
	}
	_, err := c.Decrypt(ctx, &kmspb.DecryptRequest{Name: k.Name, Ciphertext: enc.Ciphertext})
	if err == nil || !strings.Contains(err.Error(), "Error 400") {
		t.Fatalf("wrong AAD over REST: %v", err)
	}
	v2 := must(c.CreateCryptoKeyVersion(ctx, &kmspb.CreateCryptoKeyVersionRequest{Parent: k.Name}))
	got := must(c.UpdateCryptoKeyPrimaryVersion(ctx, &kmspb.UpdateCryptoKeyPrimaryVersionRequest{Name: k.Name, CryptoKeyVersionId: "2"}))
	if got.Primary.Name != v2.Name {
		t.Fatalf("primary = %v", got.Primary)
	}
	must(c.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: k.Name + "/cryptoKeyVersions/1"}))
	if r := must(c.GenerateRandomBytes(ctx, &kmspb.GenerateRandomBytesRequest{Location: parent, LengthBytes: 16})); len(r.Data) != 16 {
		t.Fatalf("random = %v", r)
	}
	ec := must(c.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: rg, CryptoKeyId: "ec", CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ASYMMETRIC_SIGN,
		VersionTemplate: &kmspb.CryptoKeyVersionTemplate{Algorithm: kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256}}}))
	if pk := must(c.GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{Name: ec.Name + "/cryptoKeyVersions/1"})); !strings.HasPrefix(pk.Pem, "-----BEGIN PUBLIC KEY-----") {
		t.Fatalf("public key = %v", pk)
	}
	must(c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: rg, Policy: &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/viewer", Members: []string{"user:a@b.c"}}}}}))
	if p := must(c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: rg})); len(p.Bindings) != 1 {
		t.Fatalf("policy = %v", p)
	}

	// Plain HTTP.
	call := func(method, path, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(method, url+path, strings.NewReader(body))
		resp := must(http.DefaultClient.Do(req))
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var out map[string]any
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("%s %s: %d %s", method, path, resp.StatusCode, raw)
		}
		return resp.StatusCode, out
	}
	if code, out := call("POST", "/v1/projects/p2/locations/us/keyRings?keyRingId=web", ""); code != 200 || out["name"] != "projects/p2/locations/us/keyRings/web" {
		t.Fatalf("create ring: %d %v", code, out)
	}
	if code, out := call("POST", "/v1/projects/p2/locations/us/keyRings/web/cryptoKeys?cryptoKeyId=k", `{"purpose":"ENCRYPT_DECRYPT","labels":{"a":"b"}}`); code != 200 || out["primary"] == nil {
		t.Fatalf("create key: %d %v", code, out)
	}
	pt := base64.StdEncoding.EncodeToString([]byte("hello"))
	code, out := call("POST", "/v1/projects/p2/locations/us/keyRings/web/cryptoKeys/k:encrypt", `{"plaintext":"`+pt+`"}`)
	if code != 200 || out["ciphertext"] == nil {
		t.Fatalf("encrypt: %d %v", code, out)
	}
	code, out = call("POST", "/v1/projects/p2/locations/us/keyRings/web/cryptoKeys/k:decrypt", `{"ciphertext":"`+out["ciphertext"].(string)+`"}`)
	if code != 200 || out["plaintext"] != pt {
		t.Fatalf("decrypt: %d %v", code, out)
	}
	if code, out := call("GET", "/v1/projects/p2/locations/us/keyRings/web/cryptoKeys/nope", ""); code != 404 || out["error"].(map[string]any)["status"] != "NOT_FOUND" {
		t.Fatalf("missing key: %d %v", code, out)
	}
	if code, out := call("POST", "/v1/projects/p2/locations/us:generateRandomBytes", `{"lengthBytes":24}`); code != 200 || len(must(base64.StdEncoding.DecodeString(out["data"].(string)))) != 24 {
		t.Fatalf("random: %d %v", code, out)
	}
}
