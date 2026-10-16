package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/account"
	"github.com/aws/aws-sdk-go-v2/service/account/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestAccount(t *testing.T) {
	_, base := start(t, "account")
	ctx := context.Background()
	c := account.NewFromConfig(awstest.Config(t, base))

	if _, err := c.GetContactInformation(ctx, &account.GetContactInformationInput{}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("contact before put: %T %v", err, err)
	}
	ci := &types.ContactInformation{FullName: aws.String("Jane Doe"), AddressLine1: aws.String("1 Main St"), City: aws.String("Pune"), PostalCode: aws.String("411001"), CountryCode: aws.String("IN"), PhoneNumber: aws.String("+911234567890")}
	if _, err := c.PutContactInformation(ctx, &account.PutContactInformationInput{ContactInformation: ci}); err != nil {
		t.Fatal(err)
	}
	g, err := c.GetContactInformation(ctx, &account.GetContactInformationInput{})
	if err != nil || aws.ToString(g.ContactInformation.FullName) != "Jane Doe" || aws.ToString(g.ContactInformation.CountryCode) != "IN" {
		t.Fatalf("get contact: %v %+v", err, g)
	}
	ci.City = aws.String("Mumbai")
	c.PutContactInformation(ctx, &account.PutContactInformationInput{ContactInformation: ci})
	if g, _ = c.GetContactInformation(ctx, &account.GetContactInformationInput{}); aws.ToString(g.ContactInformation.City) != "Mumbai" {
		t.Fatalf("update contact: %+v", g.ContactInformation)
	}

	// Alternate contacts are one per type.
	put := func(email string) {
		t.Helper()
		if _, err := c.PutAlternateContact(ctx, &account.PutAlternateContactInput{AlternateContactType: types.AlternateContactTypeSecurity, Name: aws.String("Sec"), Title: aws.String("CISO"),
			EmailAddress: aws.String(email), PhoneNumber: aws.String("+911234567890")}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.GetAlternateContact(ctx, &account.GetAlternateContactInput{AlternateContactType: types.AlternateContactTypeSecurity}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("alternate contact before put: %T %v", err, err)
	}
	put("sec@example.com")
	ac, err := c.GetAlternateContact(ctx, &account.GetAlternateContactInput{AlternateContactType: types.AlternateContactTypeSecurity})
	if err != nil || aws.ToString(ac.AlternateContact.EmailAddress) != "sec@example.com" || ac.AlternateContact.AlternateContactType != types.AlternateContactTypeSecurity {
		t.Fatalf("alternate contact: %v %+v", err, ac)
	}
	put("ciso@example.com")
	if ac, _ = c.GetAlternateContact(ctx, &account.GetAlternateContactInput{AlternateContactType: types.AlternateContactTypeSecurity}); aws.ToString(ac.AlternateContact.EmailAddress) != "ciso@example.com" {
		t.Fatalf("alternate contact update: %+v", ac.AlternateContact)
	}
	if _, err := c.GetAlternateContact(ctx, &account.GetAlternateContactInput{AlternateContactType: types.AlternateContactTypeBilling}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("other type: %T %v", err, err)
	}
	if _, err := c.DeleteAlternateContact(ctx, &account.DeleteAlternateContactInput{AlternateContactType: types.AlternateContactTypeSecurity}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetAlternateContact(ctx, &account.GetAlternateContactInput{AlternateContactType: types.AlternateContactTypeSecurity}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("after delete: %T %v", err, err)
	}

	// Regions: opt-in ones start disabled.
	rs, err := c.ListRegions(ctx, &account.ListRegionsInput{RegionOptStatusContains: []types.RegionOptStatus{types.RegionOptStatusDisabled}})
	if err != nil || len(rs.Regions) == 0 {
		t.Fatalf("disabled regions: %v %+v", err, rs)
	}
	if st, err := c.GetRegionOptStatus(ctx, &account.GetRegionOptStatusInput{RegionName: aws.String("af-south-1")}); err != nil || st.RegionOptStatus != types.RegionOptStatusDisabled {
		t.Fatalf("af-south-1: %v %+v", err, st)
	}
	if _, err := c.EnableRegion(ctx, &account.EnableRegionInput{RegionName: aws.String("af-south-1")}); err != nil {
		t.Fatal(err)
	}
	if st, _ := c.GetRegionOptStatus(ctx, &account.GetRegionOptStatusInput{RegionName: aws.String("af-south-1")}); st.RegionOptStatus != types.RegionOptStatusEnabled {
		t.Fatalf("af-south-1 after enable: %+v", st)
	}
	if _, err := c.EnableRegion(ctx, &account.EnableRegionInput{RegionName: aws.String("us-east-1")}); err == nil {
		t.Fatal("a default region was enabled")
	}
	if _, err := c.DisableRegion(ctx, &account.DisableRegionInput{RegionName: aws.String("af-south-1")}); err != nil {
		t.Fatal(err)
	}

	// Account name and information.
	if _, err := c.PutAccountName(ctx, &account.PutAccountNameInput{AccountName: aws.String("sandbox")}); err != nil {
		t.Fatal(err)
	}
	info, err := c.GetAccountInformation(ctx, &account.GetAccountInformationInput{})
	if err != nil || aws.ToString(info.AccountName) != "sandbox" || aws.ToString(info.AccountId) != "000000000000" || info.AccountState != types.AccountStateActive {
		t.Fatalf("account information: %v %+v", err, info)
	}
}
