// Package acm emulates AWS Certificate Manager (JSON 1.1). Requested
// certificates are issued at once, with DNS validation records for IaC to
// create; imported ones are parsed from PEM.
package acm

import (
	"cmp"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"maps"
	"math/big"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the ACM emulator.
type Service struct {
	hub *core.Hub
	rpc *core.RPC
	mu  sync.Mutex
	st  state
}

type state struct {
	Certs map[string]*Cert `json:"certs"` // ARN
}

// Cert is a certificate with its tags and PEM bodies.
type Cert struct {
	Detail Detail            `json:"detail"`
	Tags   map[string]string `json:"tags,omitempty"`
	PEM    string            `json:"pem,omitempty"` // made on first GetCertificate for requested certificates
	Chain  string            `json:"chain,omitempty"`
}

// Detail is the CertificateDetail wire shape.
type Detail struct {
	CertificateArn          string
	DomainName              string
	SubjectAlternativeNames []string
	DomainValidationOptions []validation `json:",omitempty"`
	Serial                  string
	Subject                 string
	Issuer                  string
	CreatedAt               *core.Time `json:",omitempty"`
	IssuedAt                *core.Time `json:",omitempty"`
	ImportedAt              *core.Time `json:",omitempty"`
	Status                  string
	NotBefore               core.Time
	NotAfter                core.Time
	KeyAlgorithm            string
	SignatureAlgorithm      string
	InUseBy                 []string
	Type                    string
	RenewalEligibility      string
	Options                 *options `json:",omitempty"`
	CertificateAuthorityArn string   `json:",omitempty"`
}

type options struct{ CertificateTransparencyLoggingPreference string }

type record struct{ Name, Type, Value string }

type validation struct {
	DomainName       string
	ValidationDomain string
	ValidationStatus string
	ValidationMethod string
	ResourceRecord   *record  `json:",omitempty"`
	ValidationEmails []string `json:",omitempty"`
}

type tag struct {
	Key   string
	Value *string `json:",omitempty"`
}

// New creates the ACM service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"RequestCertificate":        core.H(s.requestCertificate),
		"ImportCertificate":         core.H(s.importCertificate),
		"DescribeCertificate":       core.H(s.describeCertificate),
		"ListCertificates":          core.H(s.listCertificates),
		"GetCertificate":            core.H(s.getCertificate),
		"DeleteCertificate":         core.H(s.deleteCertificate),
		"AddTagsToCertificate":      core.H(s.addTags),
		"RemoveTagsFromCertificate": core.H(s.removeTags),
		"ListTagsForCertificate":    core.H(s.listTags),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "acm", Cloud: core.AWS, Title: "Certificate Manager", Targets: []string{"CertificateManager"}, Signing: []string{"acm"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Certs: map[string]*Cert{}}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := state{Certs: map[string]*Cert{}}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}

// cert finds a certificate of the caller; lock held.
func (s *Service) cert(c *core.Ctx, arn string) (*Cert, error) {
	_, region, account, res, ok := core.ParseARN(arn)
	if !ok || !strings.HasPrefix(res, "certificate/") {
		return nil, core.Errorf(400, "InvalidArnException", "String '%s' is not a valid ARN.", arn)
	}
	if ct := s.st.Certs[arn]; ct != nil && account == c.Account && region == c.Region {
		return ct, nil
	}
	return nil, core.Errorf(400, "ResourceNotFoundException", "Certificate with arn '%s' not found", arn)
}

var domainRe = regexp.MustCompile(`^(\*\.)?([A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)

func (s *Service) requestCertificate(c *core.Ctx, in *struct {
	DomainName              string
	SubjectAlternativeNames []string
	ValidationMethod        string
	DomainValidationOptions []struct{ DomainName, ValidationDomain string }
	KeyAlgorithm            string
	CertificateAuthorityArn string
	Options                 *options
	Tags                    []tag
}) (any, error) {
	if in.SubjectAlternativeNames != nil && len(in.SubjectAlternativeNames) == 0 {
		return nil, invalid("1 validation error detected: Value '[]' at 'subjectAlternativeNames' failed to satisfy constraint: Member must have length greater than or equal to 1")
	}
	names := []string{in.DomainName}
	for _, n := range in.SubjectAlternativeNames {
		if !slices.Contains(names, n) {
			names = append(names, n)
		}
	}
	for _, n := range names {
		if !domainRe.MatchString(n) {
			return nil, invalid("1 validation error detected: Value '%s' at 'domainName' failed to satisfy constraint: Member must satisfy regular expression pattern", n)
		}
	}
	method, alg := cmp.Or(in.ValidationMethod, "EMAIL"), cmp.Or(in.KeyAlgorithm, "RSA_2048")
	now := core.Time(time.Now().UTC())
	d := Detail{CertificateArn: c.ARN("acm", "certificate/"+core.UUID()), DomainName: in.DomainName, SubjectAlternativeNames: names,
		Serial: colonHex(randomSerial().Bytes()), Subject: "CN=" + in.DomainName, Issuer: "Amazon", CreatedAt: &now, IssuedAt: &now,
		Status: "ISSUED", NotBefore: now, NotAfter: core.Time(time.Time(now).AddDate(0, 13, 0)), KeyAlgorithm: alg,
		SignatureAlgorithm: "SHA256WITHRSA", InUseBy: []string{}, Type: "AMAZON_ISSUED", RenewalEligibility: "INELIGIBLE",
		Options: cmp.Or(in.Options, &options{"ENABLED"}), CertificateAuthorityArn: in.CertificateAuthorityArn}
	if strings.HasPrefix(alg, "EC_") {
		d.SignatureAlgorithm = "SHA256WITHECDSA"
	}
	if in.CertificateAuthorityArn != "" {
		d.Type, d.Issuer = "PRIVATE", "Private CA"
	} else {
		for _, n := range names {
			base := strings.TrimPrefix(n, "*.") // a wildcard and its apex share one record
			vd := base
			for _, o := range in.DomainValidationOptions { // the caller may pick the domain that gets the emails
				if o.DomainName == n && o.ValidationDomain != "" {
					vd = o.ValidationDomain
				}
			}
			v := validation{DomainName: n, ValidationDomain: vd, ValidationStatus: "SUCCESS", ValidationMethod: method}
			if method == "DNS" {
				h := sha256.Sum256([]byte(d.CertificateArn + base))
				x := hex.EncodeToString(h[:])
				v.ResourceRecord = &record{"_" + x[:32] + "." + base + ".", "CNAME", "_" + x[32:] + ".acm-validations.aws."}
			} else {
				for _, u := range []string{"admin", "administrator", "hostmaster", "postmaster", "webmaster"} {
					v.ValidationEmails = append(v.ValidationEmails, u+"@"+vd)
				}
			}
			d.DomainValidationOptions = append(d.DomainValidationOptions, v)
		}
	}
	s.mu.Lock()
	s.st.Certs[d.CertificateArn] = &Cert{Detail: d, Tags: tagMap(in.Tags)}
	s.mu.Unlock()
	return map[string]string{"CertificateArn": d.CertificateArn}, nil
}

func randomSerial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	return n
}

func colonHex(b []byte) string {
	h := hex.EncodeToString(b)
	var parts []string
	for i := 0; i < len(h); i += 2 {
		parts = append(parts, h[i:i+2])
	}
	return strings.Join(parts, ":")
}

func tagMap(tags []tag) map[string]string {
	m := map[string]string{}
	for _, t := range tags {
		m[t.Key] = ""
		if t.Value != nil {
			m[t.Key] = *t.Value
		}
	}
	return m
}

func (s *Service) importCertificate(c *core.Ctx, in *struct {
	CertificateArn   string
	Certificate      []byte
	PrivateKey       []byte
	CertificateChain []byte
	Tags             []tag
}) (any, error) {
	block, _ := pem.Decode(in.Certificate)
	if block == nil {
		return nil, invalid("Unable to parse certificate. Please ensure the certificate is in PEM format.")
	}
	x, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, invalid("Unable to parse certificate. Please ensure the certificate is in PEM format.")
	}
	if key, _ := pem.Decode(in.PrivateKey); key == nil {
		return nil, invalid("Unable to parse key; the key must be in PEM format.")
	}
	domain := x.Subject.CommonName
	if domain == "" && len(x.DNSNames) > 0 {
		domain = x.DNSNames[0]
	}
	now := core.Time(time.Now().UTC())
	d := Detail{CertificateArn: in.CertificateArn, DomainName: domain, SubjectAlternativeNames: orSlice(x.DNSNames, []string{domain}),
		Serial: colonHex(x.SerialNumber.Bytes()), Subject: x.Subject.String(), Issuer: cmp.Or(x.Issuer.CommonName, x.Issuer.String()),
		ImportedAt: &now, Status: "ISSUED", NotBefore: core.Time(x.NotBefore), NotAfter: core.Time(x.NotAfter),
		KeyAlgorithm: keyAlgorithm(x.PublicKey), SignatureAlgorithm: sigName(x.SignatureAlgorithm.String()), InUseBy: []string{},
		Type: "IMPORTED", RenewalEligibility: "INELIGIBLE"}
	if x.NotAfter.Before(time.Now()) {
		d.Status = "EXPIRED"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tags := tagMap(in.Tags)
	if d.CertificateArn != "" { // re-import keeps the ARN and tags
		old, err := s.cert(c, d.CertificateArn)
		if err != nil {
			return nil, err
		}
		tags = old.Tags
	} else {
		d.CertificateArn = c.ARN("acm", "certificate/"+core.UUID())
	}
	s.st.Certs[d.CertificateArn] = &Cert{Detail: d, Tags: tags, PEM: string(in.Certificate), Chain: string(in.CertificateChain)}
	return map[string]string{"CertificateArn": d.CertificateArn}, nil
}

func orSlice(v, def []string) []string {
	if len(v) == 0 {
		return def
	}
	return v
}

func keyAlgorithm(pub any) string {
	switch k := pub.(type) {
	case *rsa.PublicKey:
		return "RSA_" + strconv.Itoa(k.N.BitLen())
	case *ecdsa.PublicKey:
		return map[string]string{"P-256": "EC_prime256v1", "P-384": "EC_secp384r1", "P-521": "EC_secp521r1"}[k.Curve.Params().Name]
	}
	return "RSA_2048"
}

// sigName turns Go's "SHA256-RSA" / "ECDSA-SHA256" into ACM's "SHA256WITHRSA" / "SHA256WITHECDSA".
func sigName(s string) string {
	a, b, _ := strings.Cut(s, "-")
	if a == "ECDSA" {
		a, b = b, a
	}
	return a + "WITH" + b
}

func (s *Service) describeCertificate(c *core.Ctx, in *struct{ CertificateArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ct, err := s.cert(c, in.CertificateArn)
	if err != nil {
		return nil, err
	}
	return map[string]Detail{"Certificate": ct.Detail}, nil
}

type summary struct {
	CertificateArn                       string
	DomainName                           string
	SubjectAlternativeNameSummaries      []string
	HasAdditionalSubjectAlternativeNames bool
	Status                               string
	Type                                 string
	KeyAlgorithm                         string
	InUse                                bool
	RenewalEligibility                   string
	NotBefore                            core.Time
	NotAfter                             core.Time
	CreatedAt                            *core.Time `json:",omitempty"`
	IssuedAt                             *core.Time `json:",omitempty"`
	ImportedAt                           *core.Time `json:",omitempty"`
}

func (s *Service) listCertificates(c *core.Ctx, in *struct {
	CertificateStatuses []string
	Includes            struct{ KeyTypes []string }
}) (any, error) {
	keyTypes := orSlice(in.Includes.KeyTypes, []string{"RSA_1024", "RSA_2048"}) // ACM's default filter
	out := &struct{ CertificateSummaryList []summary }{[]summary{}}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, arn := range core.SortedKeys(s.st.Certs) {
		d := s.st.Certs[arn].Detail
		if _, err := s.cert(c, arn); err != nil || !slices.Contains(keyTypes, d.KeyAlgorithm) ||
			len(in.CertificateStatuses) > 0 && !slices.Contains(in.CertificateStatuses, d.Status) {
			continue
		}
		out.CertificateSummaryList = append(out.CertificateSummaryList, summary{d.CertificateArn, d.DomainName, d.SubjectAlternativeNames,
			false, d.Status, d.Type, d.KeyAlgorithm, false, d.RenewalEligibility, d.NotBefore, d.NotAfter, d.CreatedAt, d.IssuedAt, d.ImportedAt})
	}
	return out, nil
}

// getCertificate returns the PEM; for requested certificates it is a
// self-signed certificate made on first use, outside the lock.
func (s *Service) getCertificate(c *core.Ctx, in *struct{ CertificateArn string }) (any, error) {
	s.mu.Lock()
	ct, err := s.cert(c, in.CertificateArn)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	pemText, chain, d := ct.PEM, ct.Chain, ct.Detail
	s.mu.Unlock()
	if pemText == "" {
		if pemText, err = selfSigned(d); err != nil {
			return nil, err
		}
		s.mu.Lock()
		if ct.PEM == "" {
			ct.PEM = pemText
		}
		pemText = ct.PEM
		s.mu.Unlock()
	}
	out := map[string]string{"Certificate": pemText}
	if chain != "" {
		out["CertificateChain"] = chain
	}
	return out, nil
}

func selfSigned(d Detail) (string, error) {
	var key crypto.Signer
	var err error
	switch d.KeyAlgorithm {
	case "EC_prime256v1":
		key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case "EC_secp384r1":
		key, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	case "EC_secp521r1":
		key, err = ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	default:
		bits, _ := strconv.Atoi(strings.TrimPrefix(d.KeyAlgorithm, "RSA_"))
		key, err = rsa.GenerateKey(rand.Reader, cmp.Or(bits, 2048))
	}
	if err != nil {
		return "", err
	}
	serial, _ := new(big.Int).SetString(strings.ReplaceAll(d.Serial, ":", ""), 16)
	tmpl := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: d.DomainName}, DNSNames: d.SubjectAlternativeNames,
		NotBefore: time.Time(d.NotBefore), NotAfter: time.Time(d.NotAfter), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return "", err
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), nil
}

func (s *Service) deleteCertificate(c *core.Ctx, in *struct{ CertificateArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.cert(c, in.CertificateArn); err != nil {
		return nil, err
	}
	delete(s.st.Certs, in.CertificateArn)
	return nil, nil
}

type tagsInput struct {
	CertificateArn string
	Tags           []tag
}

func (s *Service) addTags(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ct, err := s.cert(c, in.CertificateArn)
	if err != nil {
		return nil, err
	}
	if ct.Tags == nil {
		ct.Tags = map[string]string{}
	}
	maps.Copy(ct.Tags, tagMap(in.Tags))
	return nil, nil
}

func (s *Service) removeTags(c *core.Ctx, in *tagsInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ct, err := s.cert(c, in.CertificateArn)
	if err != nil {
		return nil, err
	}
	for _, t := range in.Tags {
		if v, ok := ct.Tags[t.Key]; ok && (t.Value == nil || *t.Value == v) {
			delete(ct.Tags, t.Key)
		}
	}
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, in *struct{ CertificateArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ct, err := s.cert(c, in.CertificateArn)
	if err != nil {
		return nil, err
	}
	out := &struct{ Tags []tag }{[]tag{}}
	for _, k := range core.SortedKeys(ct.Tags) {
		out.Tags = append(out.Tags, tag{k, new(ct.Tags[k])})
	}
	return out, nil
}

// TaggedResources lists certificates (ARN → tags) for the tagging API and Resource Explorer.
func (s *Service) TaggedResources(account, region string) map[string]map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]map[string]string{}
	for arn, ct := range s.st.Certs {
		if _, r, a, _, _ := core.ParseARN(arn); a == account && r == region {
			out[arn] = maps.Clone(ct.Tags)
		}
	}
	return out
}

// Resources lists certificates.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Certificates", Columns: []string{"Certificate", "Domain", "Status", "Type", "Region"}}
	for _, arn := range core.SortedKeys(s.st.Certs) {
		d := s.st.Certs[arn].Detail
		_, region, _, res, _ := core.ParseARN(arn)
		t.Rows = append(t.Rows, []string{strings.TrimPrefix(res, "certificate/"), d.DomainName, d.Status, d.Type, region})
	}
	return []core.Table{t}
}

// Inspect returns a certificate's detail and tags by id (the ARN's last part).
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for arn, ct := range s.st.Certs {
		if strings.HasSuffix(arn, "certificate/"+id) {
			return map[string]any{"certificate": ct.Detail, "tags": maps.Clone(ct.Tags), "pem": ct.PEM}
		}
	}
	return nil
}
