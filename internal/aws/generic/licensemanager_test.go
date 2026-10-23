package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/licensemanager"
	"github.com/aws/aws-sdk-go-v2/service/licensemanager/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestLicenseManager(t *testing.T) {
	_, url := start(t, "license-manager")
	ctx := context.Background()
	c := licensemanager.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateLicenseConfiguration(ctx, &licensemanager.CreateLicenseConfigurationInput{
		Name: aws.String("cores"), LicenseCountingType: types.LicenseCountingTypeCore, LicenseCount: aws.Int64(10), LicenseCountHardLimit: aws.Bool(true),
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	if err != nil || aws.ToString(o.LicenseConfigurationArn) == "" {
		t.Fatalf("create: %v %+v", err, o)
	}
	arn := o.LicenseConfigurationArn
	g, err := c.GetLicenseConfiguration(ctx, &licensemanager.GetLicenseConfigurationInput{LicenseConfigurationArn: arn})
	if err != nil || aws.ToString(g.Name) != "cores" || aws.ToInt64(g.LicenseCount) != 10 || g.LicenseCountingType != types.LicenseCountingTypeCore {
		t.Fatalf("get: %v %+v", err, g)
	}
	if _, err := c.UpdateLicenseConfiguration(ctx, &licensemanager.UpdateLicenseConfigurationInput{LicenseConfigurationArn: arn, LicenseCount: aws.Int64(20)}); err != nil {
		t.Fatal(err)
	}
	if g, _ := c.GetLicenseConfiguration(ctx, &licensemanager.GetLicenseConfigurationInput{LicenseConfigurationArn: arn}); aws.ToInt64(g.LicenseCount) != 20 {
		t.Fatalf("update not kept: %+v", g)
	}
	l, err := c.ListLicenseConfigurations(ctx, &licensemanager.ListLicenseConfigurationsInput{})
	if err != nil || len(l.LicenseConfigurations) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.CreateLicenseConfiguration(ctx, &licensemanager.CreateLicenseConfigurationInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
	if _, err := c.GetLicenseConfiguration(ctx, &licensemanager.GetLicenseConfigurationInput{LicenseConfigurationArn: aws.String("arn:aws:license-manager:us-east-1:000000000000:license-configuration:lic-nope")}); err == nil {
		t.Fatal("unknown configuration found")
	}
	if _, err := c.DeleteLicenseConfiguration(ctx, &licensemanager.DeleteLicenseConfigurationInput{LicenseConfigurationArn: arn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetLicenseConfiguration(ctx, &licensemanager.GetLicenseConfigurationInput{LicenseConfigurationArn: arn}); err == nil {
		t.Fatal("deleted configuration found")
	}
}
