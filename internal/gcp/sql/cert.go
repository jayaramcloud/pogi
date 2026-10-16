package sql

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha1"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"time"

	sqladmin "google.golang.org/api/sqladmin/v1beta4"
)

// ca is the certificate authority of one instance (PEM): it signs the
// ephemeral certificates of generateEphemeralCert and the client certificates.
type ca struct {
	Cert string `json:"cert"`
	Key  string `json:"key"`
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	return n
}

func newCA() *ca {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{Country: []string{"US"}, Organization: []string{"Google, Inc"},
		CommonName: "Google Cloud SQL Server CA"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().AddDate(10, 0, 0),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	der, _ := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	kb, _ := x509.MarshalECPrivateKey(key)
	return &ca{Cert: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		Key: string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}))}
}

func (c *ca) parse() (*x509.Certificate, *ecdsa.PrivateKey) {
	cb, _ := pem.Decode([]byte(c.Cert))
	kb, _ := pem.Decode([]byte(c.Key))
	cert, _ := x509.ParseCertificate(cb.Bytes)
	key, _ := x509.ParseECPrivateKey(kb.Bytes)
	return cert, key
}

// issue signs a client certificate for pub, valid for ttl.
func (c *ca) issue(pub crypto.PublicKey, cn string, ttl time.Duration) (*sqladmin.SslCert, error) {
	parent, key := c.parse()
	tpl := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: cn}, NotBefore: time.Now().Add(-time.Minute),
		NotAfter: time.Now().Add(ttl), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tpl, parent, pub, key)
	if err != nil {
		return nil, err
	}
	return certInfo("", der), nil
}

// certInfo describes a DER certificate as a Cloud SQL SslCert.
func certInfo(instance string, der []byte) *sqladmin.SslCert {
	cert, _ := x509.ParseCertificate(der)
	sum := sha1.Sum(der)
	return &sqladmin.SslCert{Kind: "sql#sslCert", Cert: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		CertSerialNumber: cert.SerialNumber.String(), CommonName: cert.Subject.CommonName, Sha1Fingerprint: hex.EncodeToString(sum[:]),
		CreateTime: stamp(cert.NotBefore), ExpirationTime: stamp(cert.NotAfter), Instance: instance}
}

// info is the CA's own certificate, an instance's serverCaCert.
func (c *ca) info(instance string) *sqladmin.SslCert {
	b, _ := pem.Decode([]byte(c.Cert))
	return certInfo(instance, b.Bytes)
}

// publicKey reads a PEM public key (PKIX or PKCS #1) from a connector's request.
func publicKey(p string) (crypto.PublicKey, error) {
	b, _ := pem.Decode([]byte(p))
	if b == nil {
		return nil, fail(400, "invalidRequest", "Invalid request: public_key is not PEM.")
	}
	if k, err := x509.ParsePKIXPublicKey(b.Bytes); err == nil {
		return k, nil
	}
	if k, err := x509.ParsePKCS1PublicKey(b.Bytes); err == nil {
		return k, nil
	}
	return nil, fail(400, "invalidRequest", "Invalid request: public_key is not a public key.")
}
