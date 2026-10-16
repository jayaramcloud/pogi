package kms_test

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/mldsa"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/asn1"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/smithy-go"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	k256ecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	kmssvc "github.com/nrndev/pogi/internal/aws/kms"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

func serve(t *testing.T) (*kmssvc.Service, *kms.Client) {
	var svc *kmssvc.Service
	_, endpoint := core.ServeTest(t, func(h *core.Hub) core.Service { svc = kmssvc.New(h); return svc })
	return svc, kms.NewFromConfig(awstest.Config(t, endpoint))
}

func is[T error](err error) bool {
	var e T
	return errors.As(err, &e)
}

func code(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

func must[T any](t *testing.T) func(T, error) T {
	return func(v T, err error) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

func TestSymmetricKeys(t *testing.T) {
	ctx := context.Background()
	svc, c := serve(t)
	key, err := c.CreateKey(ctx, &kms.CreateKeyInput{Description: aws.String("app key"), Tags: []types.Tag{{TagKey: aws.String("env"), TagValue: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	m := key.KeyMetadata
	id := *m.KeyId
	if m.KeySpec != types.KeySpecSymmetricDefault || m.KeyUsage != types.KeyUsageTypeEncryptDecrypt || m.KeyState != types.KeyStateEnabled ||
		!m.Enabled || *m.Arn != "arn:aws:kms:us-east-1:000000000000:key/"+id || m.CreationDate == nil || len(m.EncryptionAlgorithms) != 1 {
		t.Fatalf("metadata = %+v", m)
	}
	if _, err := c.CreateAlias(ctx, &kms.CreateAliasInput{AliasName: aws.String("alias/app"), TargetKeyId: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	aliasArn := "arn:aws:kms:us-east-1:000000000000:alias/app"
	for _, ref := range []string{id, *m.Arn, "alias/app", aliasArn} {
		if d, err := c.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String(ref)}); err != nil || *d.KeyMetadata.KeyId != id {
			t.Fatalf("DescribeKey(%s) = %+v %v", ref, d, err)
		}
	}
	if _, err := c.CreateAlias(ctx, &kms.CreateAliasInput{AliasName: aws.String("alias/app"), TargetKeyId: aws.String(id)}); !is[*types.AlreadyExistsException](err) {
		t.Fatalf("duplicate alias err = %v", err)
	}
	if _, err := c.CreateAlias(ctx, &kms.CreateAliasInput{AliasName: aws.String("alias/aws/s3"), TargetKeyId: aws.String(id)}); code(err) != "NotAuthorizedException" {
		t.Fatalf("reserved alias err = %v", err)
	}
	if _, err := c.CreateAlias(ctx, &kms.CreateAliasInput{AliasName: aws.String("app"), TargetKeyId: aws.String(id)}); code(err) != "ValidationException" {
		t.Fatalf("alias without prefix err = %v", err)
	}
	if _, err := c.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String("alias/nope")}); !is[*types.NotFoundException](err) {
		t.Fatalf("unknown alias err = %v", err)
	}

	ectx := map[string]string{"purpose": "test"}
	enc, err := c.Encrypt(ctx, &kms.EncryptInput{KeyId: aws.String("alias/app"), Plaintext: []byte("secret"), EncryptionContext: ectx})
	if err != nil || *enc.KeyId != *m.Arn || enc.EncryptionAlgorithm != types.EncryptionAlgorithmSpecSymmetricDefault {
		t.Fatalf("Encrypt = %+v %v", enc, err)
	}
	dec, err := c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob, EncryptionContext: ectx})
	if err != nil || string(dec.Plaintext) != "secret" || *dec.KeyId != *m.Arn {
		t.Fatalf("Decrypt without KeyId = %+v %v", dec, err)
	}
	if dec, err := c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob, EncryptionContext: ectx, KeyId: aws.String(aliasArn)}); err != nil || string(dec.Plaintext) != "secret" {
		t.Fatalf("Decrypt with alias ARN = %+v %v", dec, err)
	}
	for _, bad := range []map[string]string{{"purpose": "other"}, nil} {
		if _, err := c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob, EncryptionContext: bad}); !is[*types.InvalidCiphertextException](err) {
			t.Fatalf("Decrypt with context %v err = %v", bad, err)
		}
	}
	if _, err := c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: []byte("garbage")}); !is[*types.InvalidCiphertextException](err) {
		t.Fatalf("Decrypt garbage err = %v", err)
	}
	other := must[*kms.CreateKeyOutput](t)(c.CreateKey(ctx, &kms.CreateKeyInput{})).KeyMetadata
	if _, err := c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob, EncryptionContext: ectx, KeyId: other.KeyId}); !is[*types.IncorrectKeyException](err) {
		t.Fatalf("Decrypt with another key err = %v", err)
	}

	dk, err := c.GenerateDataKey(ctx, &kms.GenerateDataKeyInput{KeyId: aws.String("alias/app"), KeySpec: types.DataKeySpecAes256})
	if err != nil || len(dk.Plaintext) != 32 {
		t.Fatalf("GenerateDataKey = %+v %v", dk, err)
	}
	if d, err := c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: dk.CiphertextBlob}); err != nil || !bytes.Equal(d.Plaintext, dk.Plaintext) {
		t.Fatalf("decrypt data key = %v", err)
	}
	wp, err := c.GenerateDataKeyWithoutPlaintext(ctx, &kms.GenerateDataKeyWithoutPlaintextInput{KeyId: aws.String(id), NumberOfBytes: aws.Int32(16)})
	if err != nil {
		t.Fatal(err)
	}
	if d, err := c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: wp.CiphertextBlob}); err != nil || len(d.Plaintext) != 16 {
		t.Fatalf("decrypt data key without plaintext = %v", err)
	}
	re, err := c.ReEncrypt(ctx, &kms.ReEncryptInput{CiphertextBlob: enc.CiphertextBlob, SourceEncryptionContext: ectx, DestinationKeyId: other.KeyId})
	if err != nil || *re.SourceKeyId != *m.Arn || *re.KeyId != *other.Arn {
		t.Fatalf("ReEncrypt = %+v %v", re, err)
	}
	if d, err := c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: re.CiphertextBlob}); err != nil || string(d.Plaintext) != "secret" || *d.KeyId != *other.Arn {
		t.Fatalf("decrypt re-encrypted = %+v %v", d, err)
	}

	// Rotation keeps old ciphertexts decryptable.
	must[*kms.EnableKeyRotationOutput](t)(c.EnableKeyRotation(ctx, &kms.EnableKeyRotationInput{KeyId: aws.String(id)}))
	if st, err := c.GetKeyRotationStatus(ctx, &kms.GetKeyRotationStatusInput{KeyId: aws.String(id)}); err != nil || !st.KeyRotationEnabled || *st.RotationPeriodInDays != 365 {
		t.Fatalf("GetKeyRotationStatus = %+v %v", st, err)
	}
	must[*kms.RotateKeyOnDemandOutput](t)(c.RotateKeyOnDemand(ctx, &kms.RotateKeyOnDemandInput{KeyId: aws.String(id)}))
	if rs, err := c.ListKeyRotations(ctx, &kms.ListKeyRotationsInput{KeyId: aws.String(id)}); err != nil || len(rs.Rotations) != 1 || rs.Rotations[0].RotationType != types.RotationTypeOnDemand {
		t.Fatalf("ListKeyRotations = %+v %v", rs, err)
	}
	if d, err := c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob, EncryptionContext: ectx}); err != nil || string(d.Plaintext) != "secret" {
		t.Fatalf("decrypt after rotation = %v", err)
	}
	enc2 := must[*kms.EncryptOutput](t)(c.Encrypt(ctx, &kms.EncryptInput{KeyId: aws.String(id), Plaintext: []byte("new")}))

	// Key material is persisted: ciphertexts decrypt on a fresh server.
	saved, err := svc.Save()
	if err != nil {
		t.Fatal(err)
	}
	svc2, c2 := serve(t)
	if err := svc2.Load(saved); err != nil {
		t.Fatal(err)
	}
	if d, err := c2.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob, EncryptionContext: ectx}); err != nil || string(d.Plaintext) != "secret" {
		t.Fatalf("decrypt original material after Load = %v", err)
	}
	if d, err := c2.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: enc2.CiphertextBlob}); err != nil || string(d.Plaintext) != "new" {
		t.Fatalf("decrypt rotated material after Load = %v", err)
	}

	// Disabled and pending-deletion keys refuse cryptography.
	must[*kms.DisableKeyOutput](t)(c.DisableKey(ctx, &kms.DisableKeyInput{KeyId: aws.String(id)}))
	if _, err := c.Encrypt(ctx, &kms.EncryptInput{KeyId: aws.String(id), Plaintext: []byte("x")}); !is[*types.DisabledException](err) {
		t.Fatalf("Encrypt with disabled key err = %v", err)
	}
	must[*kms.EnableKeyOutput](t)(c.EnableKey(ctx, &kms.EnableKeyInput{KeyId: aws.String(id)}))
	if _, err := c.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{KeyId: aws.String(id), PendingWindowInDays: aws.Int32(3)}); code(err) != "ValidationException" {
		t.Fatalf("3-day window err = %v", err)
	}
	sk, err := c.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{KeyId: aws.String(id), PendingWindowInDays: aws.Int32(7)})
	if err != nil || sk.KeyState != types.KeyStatePendingDeletion || sk.DeletionDate == nil || *sk.PendingWindowInDays != 7 {
		t.Fatalf("ScheduleKeyDeletion = %+v %v", sk, err)
	}
	if _, err := c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: enc2.CiphertextBlob}); !is[*types.KMSInvalidStateException](err) {
		t.Fatalf("Decrypt with key pending deletion err = %v", err)
	}
	if _, err := c.EnableKey(ctx, &kms.EnableKeyInput{KeyId: aws.String(id)}); !is[*types.KMSInvalidStateException](err) {
		t.Fatalf("EnableKey pending deletion err = %v", err)
	}
	must[*kms.CancelKeyDeletionOutput](t)(c.CancelKeyDeletion(ctx, &kms.CancelKeyDeletionInput{KeyId: aws.String(id)}))
	if d, err := c.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String(id)}); err != nil || d.KeyMetadata.KeyState != types.KeyStateDisabled || d.KeyMetadata.DeletionDate != nil {
		t.Fatalf("after CancelKeyDeletion = %+v %v", d, err)
	}

	if ks, err := c.ListKeys(ctx, &kms.ListKeysInput{}); err != nil || len(ks.Keys) != 2 {
		t.Fatalf("ListKeys = %+v %v", ks, err)
	}
	if as, err := c.ListAliases(ctx, &kms.ListAliasesInput{KeyId: aws.String(id)}); err != nil || len(as.Aliases) != 1 || *as.Aliases[0].AliasName != "alias/app" {
		t.Fatalf("ListAliases = %+v %v", as, err)
	}
	must[*kms.UpdateAliasOutput](t)(c.UpdateAlias(ctx, &kms.UpdateAliasInput{AliasName: aws.String("alias/app"), TargetKeyId: other.KeyId}))
	if d, err := c.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String("alias/app")}); err != nil || *d.KeyMetadata.KeyId != *other.KeyId {
		t.Fatalf("alias after UpdateAlias = %+v %v", d, err)
	}
	if tg, err := c.ListResourceTags(ctx, &kms.ListResourceTagsInput{KeyId: aws.String(id)}); err != nil || len(tg.Tags) != 1 || *tg.Tags[0].TagValue != "dev" {
		t.Fatalf("ListResourceTags = %+v %v", tg, err)
	}
	if p, err := c.GetKeyPolicy(ctx, &kms.GetKeyPolicyInput{KeyId: aws.String(id)}); err != nil || !strings.Contains(*p.Policy, "kms:*") {
		t.Fatalf("GetKeyPolicy = %+v %v", p, err)
	}
	if r, err := c.GenerateRandom(ctx, &kms.GenerateRandomInput{NumberOfBytes: aws.Int32(32)}); err != nil || len(r.Plaintext) != 32 {
		t.Fatalf("GenerateRandom = %+v %v", r, err)
	}
	if _, err := c.GenerateRandom(ctx, &kms.GenerateRandomInput{NumberOfBytes: aws.Int32(2000)}); code(err) != "ValidationException" {
		t.Fatalf("GenerateRandom(2000) err = %v", err)
	}
	if d, err := c.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String("alias/aws/s3")}); err != nil || d.KeyMetadata.KeyManager != types.KeyManagerTypeAws {
		t.Fatalf("AWS managed key = %+v %v", d, err)
	}
}

func TestAsymmetricKeys(t *testing.T) {
	ctx := context.Background()
	_, c := serve(t)
	msg := []byte("hello pogi")
	digest := sha256.Sum256(msg)

	rsaEnc := must[*kms.CreateKeyOutput](t)(c.CreateKey(ctx, &kms.CreateKeyInput{KeySpec: types.KeySpecRsa2048, KeyUsage: types.KeyUsageTypeEncryptDecrypt})).KeyMetadata
	if len(rsaEnc.EncryptionAlgorithms) != 2 || len(rsaEnc.SigningAlgorithms) != 0 {
		t.Fatalf("RSA encryption key = %+v", rsaEnc)
	}
	if _, err := c.Encrypt(ctx, &kms.EncryptInput{KeyId: rsaEnc.KeyId, Plaintext: msg}); !is[*types.InvalidKeyUsageException](err) {
		t.Fatalf("RSA encrypt with SYMMETRIC_DEFAULT err = %v", err)
	}
	enc, err := c.Encrypt(ctx, &kms.EncryptInput{KeyId: rsaEnc.KeyId, Plaintext: msg, EncryptionAlgorithm: types.EncryptionAlgorithmSpecRsaesOaepSha256})
	if err != nil {
		t.Fatal(err)
	}
	if d, err := c.Decrypt(ctx, &kms.DecryptInput{KeyId: rsaEnc.KeyId, CiphertextBlob: enc.CiphertextBlob, EncryptionAlgorithm: types.EncryptionAlgorithmSpecRsaesOaepSha256}); err != nil || !bytes.Equal(d.Plaintext, msg) {
		t.Fatalf("RSA decrypt = %v", err)
	}
	if _, err := c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob, EncryptionAlgorithm: types.EncryptionAlgorithmSpecRsaesOaepSha256}); err == nil {
		t.Fatal("RSA decrypt without KeyId must fail")
	}

	rsaSign := must[*kms.CreateKeyOutput](t)(c.CreateKey(ctx, &kms.CreateKeyInput{KeySpec: types.KeySpecRsa2048, KeyUsage: types.KeyUsageTypeSignVerify})).KeyMetadata
	pk := must[*kms.GetPublicKeyOutput](t)(c.GetPublicKey(ctx, &kms.GetPublicKeyInput{KeyId: rsaSign.KeyId}))
	pub, err := x509.ParsePKIXPublicKey(pk.PublicKey)
	if err != nil || pk.KeyUsage != types.KeyUsageTypeSignVerify || len(pk.SigningAlgorithms) != 6 {
		t.Fatalf("GetPublicKey = %+v %v", pk, err)
	}
	for _, alg := range []types.SigningAlgorithmSpec{types.SigningAlgorithmSpecRsassaPssSha256, types.SigningAlgorithmSpecRsassaPkcs1V15Sha256} {
		sig := must[*kms.SignOutput](t)(c.Sign(ctx, &kms.SignInput{KeyId: rsaSign.KeyId, Message: msg, MessageType: types.MessageTypeRaw, SigningAlgorithm: alg})).Signature
		if v, err := c.Verify(ctx, &kms.VerifyInput{KeyId: rsaSign.KeyId, Message: msg, Signature: sig, SigningAlgorithm: alg}); err != nil || !v.SignatureValid {
			t.Fatalf("Verify %s = %+v %v", alg, v, err)
		}
		local := rsa.VerifyPKCS1v15(pub.(*rsa.PublicKey), crypto.SHA256, digest[:], sig)
		if alg == types.SigningAlgorithmSpecRsassaPssSha256 {
			local = rsa.VerifyPSS(pub.(*rsa.PublicKey), crypto.SHA256, digest[:], sig, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
		}
		if local != nil {
			t.Fatalf("local verify %s: %v", alg, local)
		}
		sig[0] ^= 0xff
		if _, err := c.Verify(ctx, &kms.VerifyInput{KeyId: rsaSign.KeyId, Message: msg, Signature: sig, SigningAlgorithm: alg}); !is[*types.KMSInvalidSignatureException](err) {
			t.Fatalf("tampered %s signature err = %v", alg, err)
		}
	}

	ec := must[*kms.CreateKeyOutput](t)(c.CreateKey(ctx, &kms.CreateKeyInput{KeySpec: types.KeySpecEccNistP256, KeyUsage: types.KeyUsageTypeSignVerify})).KeyMetadata
	sig := must[*kms.SignOutput](t)(c.Sign(ctx, &kms.SignInput{KeyId: ec.KeyId, Message: digest[:], MessageType: types.MessageTypeDigest, SigningAlgorithm: types.SigningAlgorithmSpecEcdsaSha256})).Signature
	ecPub, err := x509.ParsePKIXPublicKey(must[*kms.GetPublicKeyOutput](t)(c.GetPublicKey(ctx, &kms.GetPublicKeyInput{KeyId: ec.KeyId})).PublicKey)
	if err != nil || !ecdsa.VerifyASN1(ecPub.(*ecdsa.PublicKey), digest[:], sig) {
		t.Fatalf("local ECDSA verify failed: %v", err)
	}
	if v, err := c.Verify(ctx, &kms.VerifyInput{KeyId: ec.KeyId, Message: msg, Signature: sig, SigningAlgorithm: types.SigningAlgorithmSpecEcdsaSha256}); err != nil || !v.SignatureValid {
		t.Fatalf("ECDSA Verify RAW = %+v %v", v, err)
	}
	if _, err := c.Sign(ctx, &kms.SignInput{KeyId: ec.KeyId, Message: msg, SigningAlgorithm: types.SigningAlgorithmSpecRsassaPssSha256}); !is[*types.InvalidKeyUsageException](err) {
		t.Fatalf("RSA algorithm on ECC key err = %v", err)
	}
	if _, err := c.Sign(ctx, &kms.SignInput{KeyId: ec.KeyId, Message: msg, MessageType: types.MessageTypeDigest, SigningAlgorithm: types.SigningAlgorithmSpecEcdsaSha256}); code(err) != "ValidationException" {
		t.Fatalf("short digest err = %v", err)
	}
	if _, err := c.CreateKey(ctx, &kms.CreateKeyInput{KeySpec: types.KeySpecEccNistP256}); code(err) != "ValidationException" {
		t.Fatalf("ECC key with ENCRYPT_DECRYPT err = %v", err)
	}
	if _, err := c.CreateKey(ctx, &kms.CreateKeyInput{KeySpec: types.KeySpecSm2, KeyUsage: types.KeyUsageTypeSignVerify}); !is[*types.UnsupportedOperationException](err) {
		t.Fatalf("SM2 err = %v", err)
	}

	sym := must[*kms.CreateKeyOutput](t)(c.CreateKey(ctx, &kms.CreateKeyInput{})).KeyMetadata
	if _, err := c.GetPublicKey(ctx, &kms.GetPublicKeyInput{KeyId: sym.KeyId}); !is[*types.UnsupportedOperationException](err) {
		t.Fatalf("GetPublicKey on a symmetric key err = %v", err)
	}
	pair, err := c.GenerateDataKeyPair(ctx, &kms.GenerateDataKeyPairInput{KeyId: sym.KeyId, KeyPairSpec: types.DataKeyPairSpecEccNistP256})
	if err != nil {
		t.Fatal(err)
	}
	priv, err := x509.ParsePKCS8PrivateKey(pair.PrivateKeyPlaintext)
	if err != nil {
		t.Fatal(err)
	}
	pairPub, err := x509.ParsePKIXPublicKey(pair.PublicKey)
	if err != nil || !priv.(*ecdsa.PrivateKey).PublicKey.Equal(pairPub) {
		t.Fatalf("data key pair public key mismatch: %v", err)
	}
	if d, err := c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: pair.PrivateKeyCiphertextBlob}); err != nil || !bytes.Equal(d.Plaintext, pair.PrivateKeyPlaintext) {
		t.Fatalf("decrypt private key = %v", err)
	}
	if np, err := c.GenerateDataKeyPairWithoutPlaintext(ctx, &kms.GenerateDataKeyPairWithoutPlaintextInput{KeyId: sym.KeyId, KeyPairSpec: types.DataKeyPairSpecRsa2048}); err != nil || len(np.PublicKey) == 0 || len(np.PrivateKeyCiphertextBlob) == 0 {
		t.Fatalf("GenerateDataKeyPairWithoutPlaintext = %+v %v", np, err)
	}
	if _, err := c.GenerateDataKeyPair(ctx, &kms.GenerateDataKeyPairInput{KeyId: rsaEnc.KeyId, KeyPairSpec: types.DataKeyPairSpecEccNistP256}); !is[*types.InvalidKeyUsageException](err) {
		t.Fatalf("data key pair under an RSA key err = %v", err)
	}
}

func TestHMACKeys(t *testing.T) {
	ctx := context.Background()
	_, c := serve(t)
	k := must[*kms.CreateKeyOutput](t)(c.CreateKey(ctx, &kms.CreateKeyInput{KeySpec: types.KeySpecHmac256, KeyUsage: types.KeyUsageTypeGenerateVerifyMac})).KeyMetadata
	if len(k.MacAlgorithms) != 1 || k.MacAlgorithms[0] != types.MacAlgorithmSpecHmacSha256 {
		t.Fatalf("HMAC key = %+v", k)
	}
	msg := []byte("payload")
	mac, err := c.GenerateMac(ctx, &kms.GenerateMacInput{KeyId: k.KeyId, Message: msg, MacAlgorithm: types.MacAlgorithmSpecHmacSha256})
	if err != nil || len(mac.Mac) != 32 {
		t.Fatalf("GenerateMac = %+v %v", mac, err)
	}
	if v, err := c.VerifyMac(ctx, &kms.VerifyMacInput{KeyId: k.KeyId, Message: msg, Mac: mac.Mac, MacAlgorithm: types.MacAlgorithmSpecHmacSha256}); err != nil || !v.MacValid {
		t.Fatalf("VerifyMac = %+v %v", v, err)
	}
	if _, err := c.VerifyMac(ctx, &kms.VerifyMacInput{KeyId: k.KeyId, Message: []byte("tampered"), Mac: mac.Mac, MacAlgorithm: types.MacAlgorithmSpecHmacSha256}); !is[*types.KMSInvalidMacException](err) {
		t.Fatalf("tampered MAC err = %v", err)
	}
	if _, err := c.GenerateMac(ctx, &kms.GenerateMacInput{KeyId: k.KeyId, Message: msg, MacAlgorithm: types.MacAlgorithmSpecHmacSha512}); !is[*types.InvalidKeyUsageException](err) {
		t.Fatalf("wrong MAC algorithm err = %v", err)
	}
	if _, err := c.Encrypt(ctx, &kms.EncryptInput{KeyId: k.KeyId, Plaintext: msg}); !is[*types.InvalidKeyUsageException](err) {
		t.Fatalf("Encrypt with an HMAC key err = %v", err)
	}
	if _, err := c.EnableKeyRotation(ctx, &kms.EnableKeyRotationInput{KeyId: k.KeyId}); !is[*types.UnsupportedOperationException](err) {
		t.Fatalf("rotation of an HMAC key err = %v", err)
	}
}

// floci #3025, #2924, #2787, #474: ML-DSA, Ed25519 and secp256k1 keys sign and
// verify, and the signatures check with an outside library.
func TestNewKeySpecs(t *testing.T) {
	ctx := context.Background()
	_, c := serve(t)
	create := func(spec string) (*string, []byte) {
		k, err := c.CreateKey(ctx, &kms.CreateKeyInput{KeySpec: types.KeySpec(spec), KeyUsage: types.KeyUsageTypeSignVerify})
		if err != nil {
			t.Fatalf("CreateKey %s: %v", spec, err)
		}
		pk, err := c.GetPublicKey(ctx, &kms.GetPublicKeyInput{KeyId: k.KeyMetadata.KeyId})
		if err != nil {
			t.Fatal(err)
		}
		return k.KeyMetadata.KeyId, pk.PublicKey
	}
	msg := []byte("hello")
	signAs := func(id *string, alg, msgType string, m []byte) *kms.SignOutput {
		t.Helper()
		out, err := c.Sign(ctx, &kms.SignInput{KeyId: id, Message: m, MessageType: types.MessageType(msgType), SigningAlgorithm: types.SigningAlgorithmSpec(alg)})
		if err != nil {
			t.Fatalf("Sign %s: %v", alg, err)
		}
		return out
	}
	verifyAs := func(id *string, alg, msgType string, m, sig []byte) error {
		_, err := c.Verify(ctx, &kms.VerifyInput{KeyId: id, Message: m, MessageType: types.MessageType(msgType), Signature: sig, SigningAlgorithm: types.SigningAlgorithmSpec(alg)})
		return err
	}

	for spec, sigLen := range map[string]int{"ML_DSA_44": 2420, "ML_DSA_65": 3309, "ML_DSA_87": 4627} {
		id, der := create(spec)
		s := signAs(id, "ML_DSA_SHAKE_256", "", msg)
		pub, err := x509.ParsePKIXPublicKey(der)
		if err != nil || len(s.Signature) != sigLen || mldsa.Verify(pub.(*mldsa.PublicKey), msg, s.Signature, &mldsa.Options{}) != nil {
			t.Errorf("%s: signature does not verify outside pogi (%v, len %d)", spec, err, len(s.Signature))
		}
		if err := verifyAs(id, "ML_DSA_SHAKE_256", "", msg, s.Signature); err != nil {
			t.Errorf("%s Verify: %v", spec, err)
		}
		if err := verifyAs(id, "ML_DSA_SHAKE_256", "", []byte("other"), s.Signature); code(err) != "KMSInvalidSignatureException" {
			t.Errorf("%s Verify wrong message: %q", spec, code(err))
		}
	}

	id, der := create("ECC_NIST_EDWARDS25519")
	pub, err := x509.ParsePKIXPublicKey(der)
	if err != nil || len(der) != 44 {
		t.Fatalf("Ed25519 public key: %v len %d", err, len(der))
	}
	s := signAs(id, "ED25519_SHA_512", "RAW", msg)
	if !ed25519.Verify(pub.(ed25519.PublicKey), msg, s.Signature) || verifyAs(id, "ED25519_SHA_512", "RAW", msg, s.Signature) != nil {
		t.Error("Ed25519 RAW signature does not verify")
	}
	digest := sha512.Sum512(msg)
	ph := signAs(id, "ED25519_PH_SHA_512", "DIGEST", digest[:])
	if ed25519.VerifyWithOptions(pub.(ed25519.PublicKey), digest[:], ph.Signature, &ed25519.Options{Hash: crypto.SHA512}) != nil ||
		verifyAs(id, "ED25519_PH_SHA_512", "DIGEST", digest[:], ph.Signature) != nil {
		t.Error("Ed25519ph signature does not verify")
	}
	if _, err := c.Sign(ctx, &kms.SignInput{KeyId: id, Message: msg, MessageType: types.MessageTypeRaw, SigningAlgorithm: "ED25519_PH_SHA_512"}); code(err) != "ValidationException" {
		t.Errorf("ED25519_PH_SHA_512 with RAW: %q", code(err))
	}
	if _, err := c.Sign(ctx, &kms.SignInput{KeyId: id, Message: msg, SigningAlgorithm: "ECDSA_SHA_256"}); !is[*types.InvalidKeyUsageException](err) {
		t.Errorf("ECDSA on an Ed25519 key: %v", err)
	}

	id, der = create("ECC_SECG_P256K1")
	var info struct {
		Algo struct{ A, P asn1.ObjectIdentifier }
		Key  asn1.BitString
	}
	if _, err := asn1.Unmarshal(der, &info); err != nil || !info.Algo.P.Equal(asn1.ObjectIdentifier{1, 3, 132, 0, 10}) {
		t.Fatalf("secp256k1 public key: %v %v", err, info.Algo)
	}
	kp, err := secp256k1.ParsePubKey(info.Key.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(msg)
	s = signAs(id, "ECDSA_SHA_256", "RAW", msg)
	if sig, err := k256ecdsa.ParseDERSignature(s.Signature); err != nil || !sig.Verify(h[:], kp) {
		t.Errorf("secp256k1 signature does not verify outside pogi: %v", err)
	}
	if err := verifyAs(id, "ECDSA_SHA_256", "RAW", msg, s.Signature); err != nil {
		t.Errorf("secp256k1 Verify: %v", err)
	}
	if err := verifyAs(id, "ECDSA_SHA_256", "RAW", []byte("other"), s.Signature); code(err) != "KMSInvalidSignatureException" {
		t.Errorf("secp256k1 Verify wrong message: %q", code(err))
	}
	// A data key pair on this curve comes back as PKCS #8 the outside library can read.
	sym, _ := c.CreateKey(ctx, &kms.CreateKeyInput{})
	pair, err := c.GenerateDataKeyPair(ctx, &kms.GenerateDataKeyPairInput{KeyId: sym.KeyMetadata.KeyId, KeyPairSpec: types.DataKeyPairSpecEccSecgP256k1})
	if err != nil || len(pair.PrivateKeyPlaintext) == 0 || len(pair.PublicKey) == 0 {
		t.Errorf("GenerateDataKeyPair ECC_SECG_P256K1: %v", err)
	}
	if _, err := c.GenerateDataKeyPair(ctx, &kms.GenerateDataKeyPairInput{KeyId: sym.KeyMetadata.KeyId, KeyPairSpec: "ML_DSA_44"}); code(err) != "ValidationException" {
		t.Errorf("data key pair ML_DSA_44: %q", code(err))
	}
}
