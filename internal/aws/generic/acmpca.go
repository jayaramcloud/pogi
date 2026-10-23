package generic

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
)

// Private CA: authorities are stored and read back. Nothing is signed: no certificate is issued, and an
// authority stays PENDING_CERTIFICATE because no certificate can be imported for it. Its CSR is a real PEM
// request for the authority's subject, signed with a throwaway key that is not kept.
func init() {
	Derive("acm-pca", "CertificateAuthority", func(r *Resource) map[string]any {
		out := map[string]any{"Status": "PENDING_CERTIFICATE", "OwnerAccount": r.Account}
		if v := r.Data["CertificateAuthorityType"]; v != nil {
			out["Type"] = v
		}
		for k, def := range map[string]string{"UsageMode": "GENERAL_PURPOSE", "KeyStorageSecurityStandard": "FIPS_140_2_LEVEL_3_OR_HIGHER"} {
			if r.Data[k] == nil {
				out[k] = def
			}
		}
		return out
	})
	Override("acm-pca", "GetCertificateAuthorityCsr", func(c *Call) (map[string]any, error) {
		r := c.resolve(c.m.types["certificateauthority"])
		if r == nil {
			return nil, c.NotFound("The certificate authority %s was not found.", str(c.In["CertificateAuthorityArn"]))
		}
		subj := pkix.Name{}
		cfg, _ := r.Data["CertificateAuthorityConfiguration"].(map[string]any)
		if s, ok := cfg["Subject"].(map[string]any); ok {
			subj.CommonName = str(s["CommonName"])
			if v := str(s["Organization"]); v != "" {
				subj.Organization = []string{v}
			}
			if v := str(s["Country"]); v != "" {
				subj.Country = []string{v}
			}
		}
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, err
		}
		der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: subj}, key)
		if err != nil {
			return nil, err
		}
		return map[string]any{"Csr": string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}))}, nil
	})
}
