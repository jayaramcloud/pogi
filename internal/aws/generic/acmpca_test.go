package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/acmpca"
	"github.com/aws/aws-sdk-go-v2/service/acmpca/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestACMPCA(t *testing.T) {
	_, url := start(t, "acm-pca")
	ctx := context.Background()
	c := acmpca.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateCertificateAuthority(ctx, &acmpca.CreateCertificateAuthorityInput{CertificateAuthorityType: types.CertificateAuthorityTypeRoot,
		CertificateAuthorityConfiguration: &types.CertificateAuthorityConfiguration{KeyAlgorithm: types.KeyAlgorithmRsa2048, SigningAlgorithm: types.SigningAlgorithmSha256withrsa,
			Subject: &types.ASN1Subject{CommonName: aws.String("example.com")}},
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	arn := o.CertificateAuthorityArn
	if !strings.HasPrefix(aws.ToString(arn), "arn:aws:acm-pca:us-east-1:000000000000:certificate-authority/") {
		t.Fatalf("arn: %q", aws.ToString(arn))
	}
	g, err := c.DescribeCertificateAuthority(ctx, &acmpca.DescribeCertificateAuthorityInput{CertificateAuthorityArn: arn})
	ca := g.CertificateAuthority
	if err != nil || ca.Type != types.CertificateAuthorityTypeRoot || ca.Status != types.CertificateAuthorityStatusPendingCertificate || ca.CertificateAuthorityConfiguration == nil || aws.ToString(ca.CertificateAuthorityConfiguration.Subject.CommonName) != "example.com" {
		t.Fatalf("describe: %v %+v", err, ca)
	}
	csr, err := c.GetCertificateAuthorityCsr(ctx, &acmpca.GetCertificateAuthorityCsrInput{CertificateAuthorityArn: arn})
	if err != nil || !strings.Contains(aws.ToString(csr.Csr), "CERTIFICATE REQUEST") {
		t.Fatalf("csr: %v %+v", err, csr)
	}
	l, err := c.ListCertificateAuthorities(ctx, &acmpca.ListCertificateAuthoritiesInput{})
	if err != nil || len(l.CertificateAuthorities) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	tg, err := c.ListTags(ctx, &acmpca.ListTagsInput{CertificateAuthorityArn: arn})
	if err != nil || len(tg.Tags) != 1 {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.DeleteCertificateAuthority(ctx, &acmpca.DeleteCertificateAuthorityInput{CertificateAuthorityArn: arn}); err != nil {
		t.Fatal(err)
	}
	bad := aws.String("arn:aws:acm-pca:us-east-1:000000000000:certificate-authority/nosuch")
	var nf *types.ResourceNotFoundException
	if _, err := c.DescribeCertificateAuthority(ctx, &acmpca.DescribeCertificateAuthorityInput{CertificateAuthorityArn: bad}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.CreateCertificateAuthority(ctx, &acmpca.CreateCertificateAuthorityInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}
