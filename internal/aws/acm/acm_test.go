package acm_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/acm"
	"github.com/aws/aws-sdk-go-v2/service/acm/types"
	acmsvc "github.com/nrndev/pogi/internal/aws/acm"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

// roundTrip snapshots, resets and restores the hub: state must survive Save and Load.
func roundTrip(t *testing.T, h *core.Hub) {
	t.Helper()
	b, err := h.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	h.Reset()
	if err := h.Restore(b); err != nil {
		t.Fatal(err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestCertificates(t *testing.T) {
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { return acmsvc.New(h) })
	c := acm.NewFromConfig(awstest.Config(t, url))

	arn := must(c.RequestCertificate(ctx, &acm.RequestCertificateInput{DomainName: aws.String("example.com"),
		SubjectAlternativeNames: []string{"example.com", "*.example.com"}, ValidationMethod: types.ValidationMethodDns,
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})).CertificateArn
	d := must(c.DescribeCertificate(ctx, &acm.DescribeCertificateInput{CertificateArn: arn})).Certificate
	if d.Status != types.CertificateStatusIssued || d.Type != types.CertificateTypeAmazonIssued || len(d.SubjectAlternativeNames) != 2 ||
		len(d.DomainValidationOptions) != 2 || d.DomainValidationOptions[0].ResourceRecord.Type != types.RecordTypeCname {
		t.Fatalf("certificate = %+v", d)
	}
	if a, b := d.DomainValidationOptions[0].ResourceRecord, d.DomainValidationOptions[1].ResourceRecord; *a.Name != *b.Name || *a.Value != *b.Value {
		t.Fatalf("a wildcard and its apex must share one validation record: %+v %+v", *a, *b)
	}

	got := must(c.GetCertificate(ctx, &acm.GetCertificateInput{CertificateArn: arn}))
	block, _ := pem.Decode([]byte(*got.Certificate))
	x := must(x509.ParseCertificate(block.Bytes))
	if x.Subject.CommonName != "example.com" || x.DNSNames[1] != "*.example.com" {
		t.Fatalf("issued PEM = %+v", x.Subject)
	}

	// Import an EC certificate: listing hides it unless its key type is asked for, as in ACM.
	key := must(ecdsa.GenerateKey(elliptic.P256(), rand.Reader))
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: "api.local"}, DNSNames: []string{"api.local"},
		NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour)}
	der := must(x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key))
	keyDER := must(x509.MarshalPKCS8PrivateKey(key))
	imported := must(c.ImportCertificate(ctx, &acm.ImportCertificateInput{Certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		PrivateKey: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})})).CertificateArn
	roundTrip(t, hub)
	id := must(c.DescribeCertificate(ctx, &acm.DescribeCertificateInput{CertificateArn: imported})).Certificate
	if id.Type != types.CertificateTypeImported || *id.DomainName != "api.local" || id.KeyAlgorithm != types.KeyAlgorithmEcPrime256v1 || *id.SignatureAlgorithm != "SHA256WITHECDSA" {
		t.Fatalf("imported = %+v", id)
	}
	if l := must(c.ListCertificates(ctx, &acm.ListCertificatesInput{})); len(l.CertificateSummaryList) != 1 || *l.CertificateSummaryList[0].CertificateArn != *arn {
		t.Fatalf("default list = %+v", l.CertificateSummaryList)
	}
	if l := must(c.ListCertificates(ctx, &acm.ListCertificatesInput{Includes: &types.Filters{KeyTypes: []types.KeyAlgorithm{types.KeyAlgorithmEcPrime256v1, types.KeyAlgorithmRsa2048}},
		CertificateStatuses: []types.CertificateStatus{types.CertificateStatusIssued}})); len(l.CertificateSummaryList) != 2 {
		t.Fatalf("list with key types = %+v", l.CertificateSummaryList)
	}
	_, err := c.ImportCertificate(ctx, &acm.ImportCertificateInput{Certificate: []byte("nope"), PrivateKey: []byte("nope")})
	var ve *types.ValidationException
	if !errors.As(err, &ve) {
		t.Fatalf("want ValidationException, got %v", err)
	}

	must(c.AddTagsToCertificate(ctx, &acm.AddTagsToCertificateInput{CertificateArn: arn, Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("web")}}}))
	must(c.RemoveTagsFromCertificate(ctx, &acm.RemoveTagsFromCertificateInput{CertificateArn: arn, Tags: []types.Tag{{Key: aws.String("env")}}}))
	if tags := must(c.ListTagsForCertificate(ctx, &acm.ListTagsForCertificateInput{CertificateArn: arn})).Tags; len(tags) != 1 || *tags[0].Value != "web" {
		t.Fatalf("tags = %+v", tags)
	}

	must(c.DeleteCertificate(ctx, &acm.DeleteCertificateInput{CertificateArn: arn}))
	_, err = c.DescribeCertificate(ctx, &acm.DescribeCertificateInput{CertificateArn: arn})
	var nf *types.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("want ResourceNotFoundException, got %v", err)
	}
}

// floci #5674: an empty SubjectAlternativeNames list is a ValidationException.
// floci #3251: ValidationDomain from DomainValidationOptions shows up in DescribeCertificate.
func TestRequestCertificateOptions(t *testing.T) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return acmsvc.New(h) })
	c := acm.NewFromConfig(awstest.Config(t, url))

	_, err := c.RequestCertificate(ctx, &acm.RequestCertificateInput{DomainName: aws.String("example.com"), SubjectAlternativeNames: []string{}})
	var ve *types.ValidationException
	if !errors.As(err, &ve) {
		t.Fatalf("empty SubjectAlternativeNames: want ValidationException, got %v", err)
	}

	arn := must(c.RequestCertificate(ctx, &acm.RequestCertificateInput{DomainName: aws.String("probe.example.com"), ValidationMethod: types.ValidationMethodEmail,
		DomainValidationOptions: []types.DomainValidationOption{{DomainName: aws.String("probe.example.com"), ValidationDomain: aws.String("example.com")}}})).CertificateArn
	v := must(c.DescribeCertificate(ctx, &acm.DescribeCertificateInput{CertificateArn: arn})).Certificate.DomainValidationOptions[0]
	if aws.ToString(v.ValidationDomain) != "example.com" || v.ValidationEmails[0] != "admin@example.com" {
		t.Errorf("ValidationDomain = %q, emails %v", aws.ToString(v.ValidationDomain), v.ValidationEmails)
	}
}
