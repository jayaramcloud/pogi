package kms

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/mldsa"
	"crypto/rand"
	"crypto/x509"
	"encoding/asn1"
	"errors"
	"io"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	k256ecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
)

// Key specs the standard library cannot store in PKCS #8 (secp256k1) or that
// sign without a hash (Ed25519, ML-DSA). Keys are kept as PKCS #8 DER.

var (
	oidECPublicKey = asn1.ObjectIdentifier{1, 2, 840, 10045, 2, 1}
	oidSecp256k1   = asn1.ObjectIdentifier{1, 3, 132, 0, 10}
)

type algID struct {
	Algorithm  asn1.ObjectIdentifier
	Parameters asn1.ObjectIdentifier
}

type pkcs8 struct {
	Version    int
	Algo       algID
	PrivateKey []byte
}

type ecPrivateKey struct {
	Version    int
	PrivateKey []byte
	Params     asn1.ObjectIdentifier `asn1:"optional,explicit,tag:0"`
	PublicKey  asn1.BitString        `asn1:"optional,explicit,tag:1"`
}

type spki struct {
	Algo      algID
	PublicKey asn1.BitString
}

// k256 is a secp256k1 signing key. Sign takes a digest and returns a DER signature.
type k256 struct{ *secp256k1.PrivateKey }

func (k k256) Public() crypto.PublicKey { return k.PubKey() }

func (k k256) Sign(_ io.Reader, digest []byte, _ crypto.SignerOpts) ([]byte, error) {
	return k256ecdsa.Sign(k.PrivateKey, digest).Serialize(), nil
}

func privDER(priv crypto.Signer) ([]byte, error) {
	k, ok := priv.(k256)
	if !ok {
		return x509.MarshalPKCS8PrivateKey(priv)
	}
	inner, err := asn1.Marshal(ecPrivateKey{Version: 1, PrivateKey: k.Serialize(), Params: oidSecp256k1,
		PublicKey: asn1.BitString{Bytes: k.PubKey().SerializeUncompressed(), BitLength: 8 * 65}})
	if err != nil {
		return nil, err
	}
	return asn1.Marshal(pkcs8{Algo: algID{oidECPublicKey, oidSecp256k1}, PrivateKey: inner})
}

func parsePriv(der []byte) (crypto.Signer, error) {
	if priv, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		return priv.(crypto.Signer), nil
	}
	var p pkcs8
	var e ecPrivateKey
	if _, err := asn1.Unmarshal(der, &p); err != nil || !p.Algo.Parameters.Equal(oidSecp256k1) {
		return nil, errors.New("unsupported private key")
	}
	if _, err := asn1.Unmarshal(p.PrivateKey, &e); err != nil {
		return nil, err
	}
	return k256{secp256k1.PrivKeyFromBytes(e.PrivateKey)}, nil
}

func pubDER(pub crypto.PublicKey) ([]byte, error) {
	k, ok := pub.(*secp256k1.PublicKey)
	if !ok {
		return x509.MarshalPKIXPublicKey(pub)
	}
	return asn1.Marshal(spki{Algo: algID{oidECPublicKey, oidSecp256k1}, PublicKey: asn1.BitString{Bytes: k.SerializeUncompressed(), BitLength: 8 * 65}})
}

// verifyDigest checks an ECDSA signature over a digest (P-curves and secp256k1).
func verifyECDSA(pub crypto.PublicKey, digest, sig []byte) bool {
	switch pub := pub.(type) {
	case *ecdsa.PublicKey:
		return ecdsa.VerifyASN1(pub, digest, sig)
	case *secp256k1.PublicKey:
		s, err := k256ecdsa.ParseDERSignature(sig)
		return err == nil && s.Verify(digest, pub)
	}
	return false
}

// signUnhashed signs with the key specs whose algorithms do their own hashing:
// Ed25519 (pure on RAW, Ed25519ph on a 64-byte DIGEST) and ML-DSA (RAW only).
func signUnhashed(key crypto.Signer, alg string, msg []byte) ([]byte, error) {
	switch alg {
	case "ED25519_SHA_512":
		return ed25519.Sign(key.(ed25519.PrivateKey), msg), nil
	case "ED25519_PH_SHA_512":
		return key.Sign(rand.Reader, msg, &ed25519.Options{Hash: crypto.SHA512})
	}
	return key.Sign(rand.Reader, msg, &mldsa.Options{})
}

func verifyUnhashed(pub crypto.PublicKey, alg string, msg, sig []byte) bool {
	switch pub := pub.(type) {
	case ed25519.PublicKey:
		if alg == "ED25519_PH_SHA_512" {
			return ed25519.VerifyWithOptions(pub, msg, sig, &ed25519.Options{Hash: crypto.SHA512}) == nil
		}
		return ed25519.Verify(pub, msg, sig)
	case *mldsa.PublicKey:
		return mldsa.Verify(pub, msg, sig, &mldsa.Options{}) == nil
	}
	return false
}
