package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/organizations"
	"github.com/aws/aws-sdk-go-v2/service/ram"
	"github.com/aws/aws-sdk-go-v2/service/ram/types"
	awsorgs "github.com/nrndev/pogi/internal/aws/organizations"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

func TestRAM(t *testing.T) {
	h, base := startWith(t, "ram")
	h.Register(awsorgs.New(h))
	ctx := context.Background()
	cfg := awstest.Config(t, base)
	c := ram.NewFromConfig(cfg)

	// A member account of the organization gets shares without an invitation; a stranger needs one.
	orgs := organizations.NewFromConfig(cfg)
	if _, err := orgs.CreateOrganization(ctx, &organizations.CreateOrganizationInput{}); err != nil {
		t.Fatal(err)
	}
	acc, err := orgs.CreateAccount(ctx, &organizations.CreateAccountInput{AccountName: aws.String("member"), Email: aws.String("member@example.com")})
	if err != nil {
		t.Fatal(err)
	}
	member := aws.ToString(acc.CreateAccountStatus.AccountId)
	if member == "" {
		list, _ := orgs.ListAccounts(ctx, &organizations.ListAccountsInput{})
		for _, a := range list.Accounts {
			if aws.ToString(a.Name) == "member" {
				member = aws.ToString(a.Id)
			}
		}
	}
	const stranger = "999988887777"
	const subnet = "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-0abc"

	cr, err := c.CreateResourceShare(ctx, &ram.CreateResourceShareInput{Name: aws.String("net"), ResourceArns: []string{subnet}, Principals: []string{member, stranger},
		AllowExternalPrincipals: aws.Bool(true), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	rs := cr.ResourceShare
	arn := aws.ToString(rs.ResourceShareArn)
	if aws.ToString(rs.Name) != "net" || rs.Status != types.ResourceShareStatusActive || aws.ToString(rs.OwningAccountId) != core.DefaultAccount || arn == "" || len(rs.Tags) != 1 {
		t.Fatalf("create: %+v", rs)
	}
	if _, err := c.CreateResourceShare(ctx, &ram.CreateResourceShareInput{Name: aws.String("net")}); err == nil {
		t.Fatal("duplicate share name accepted")
	}

	shares, err := c.GetResourceShares(ctx, &ram.GetResourceSharesInput{ResourceOwner: types.ResourceOwnerSelf, ResourceShareArns: []string{arn}})
	if err != nil || len(shares.ResourceShares) != 1 || aws.ToString(shares.ResourceShares[0].Name) != "net" {
		t.Fatalf("get shares: %v %+v", err, shares)
	}
	if other, _ := c.GetResourceShares(ctx, &ram.GetResourceSharesInput{ResourceOwner: types.ResourceOwnerOtherAccounts}); len(other.ResourceShares) != 0 {
		t.Fatalf("other-accounts shares: %+v", other.ResourceShares)
	}
	pa, err := c.GetResourceShareAssociations(ctx, &ram.GetResourceShareAssociationsInput{AssociationType: types.ResourceShareAssociationTypePrincipal, ResourceShareArns: []string{arn}})
	if err != nil || len(pa.ResourceShareAssociations) != 2 {
		t.Fatalf("principal associations: %v %+v", err, pa)
	}
	for _, a := range pa.ResourceShareAssociations {
		want := types.ResourceShareAssociationStatusAssociated // inside the organization: no invitation
		if aws.ToString(a.AssociatedEntity) == stranger {
			want = types.ResourceShareAssociationStatusAssociating
		}
		if a.Status != want {
			t.Fatalf("association of %s is %s, want %s", aws.ToString(a.AssociatedEntity), a.Status, want)
		}
	}
	ra, err := c.GetResourceShareAssociations(ctx, &ram.GetResourceShareAssociationsInput{AssociationType: types.ResourceShareAssociationTypeResource, ResourceArn: aws.String(subnet)})
	if err != nil || len(ra.ResourceShareAssociations) != 1 || ra.ResourceShareAssociations[0].Status != types.ResourceShareAssociationStatusAssociated {
		t.Fatalf("resource associations: %v %+v", err, ra)
	}
	rl, err := c.ListResources(ctx, &ram.ListResourcesInput{ResourceOwner: types.ResourceOwnerSelf})
	if err != nil || len(rl.Resources) != 1 || aws.ToString(rl.Resources[0].Type) != "ec2:Subnet" {
		t.Fatalf("resources: %v %+v", err, rl)
	}
	pl, err := c.ListPrincipals(ctx, &ram.ListPrincipalsInput{ResourceOwner: types.ResourceOwnerSelf})
	if err != nil || len(pl.Principals) != 2 {
		t.Fatalf("principals: %v %+v", err, pl)
	}
	perms, err := c.ListResourceSharePermissions(ctx, &ram.ListResourceSharePermissionsInput{ResourceShareArn: aws.String(arn)})
	if err != nil || len(perms.Permissions) != 1 || aws.ToString(perms.Permissions[0].Name) != "AWSRAMDefaultPermissionSubnet" {
		t.Fatalf("share permissions: %v %+v", err, perms)
	}
	if p, err := c.GetPermission(ctx, &ram.GetPermissionInput{PermissionArn: perms.Permissions[0].Arn}); err != nil || aws.ToString(p.Permission.ResourceType) != "ec2:Subnet" {
		t.Fatalf("permission: %v %+v", err, p)
	}
	if lp, err := c.ListPermissions(ctx, &ram.ListPermissionsInput{}); err != nil || len(lp.Permissions) < 5 {
		t.Fatalf("permissions: %v %+v", err, lp)
	}

	// The stranger's invitation is accepted by the stranger; the share then shows for it.
	inv, err := c.GetResourceShareInvitations(ctx, &ram.GetResourceShareInvitationsInput{ResourceShareArns: []string{arn}})
	if err != nil || len(inv.ResourceShareInvitations) != 1 || inv.ResourceShareInvitations[0].Status != types.ResourceShareInvitationStatusPending ||
		aws.ToString(inv.ResourceShareInvitations[0].ReceiverAccountId) != stranger {
		t.Fatalf("invitations: %v %+v", err, inv)
	}
	invArn := inv.ResourceShareInvitations[0].ResourceShareInvitationArn
	guest := ram.NewFromConfig(awstest.ConfigAs(t, base, stranger, "x", ""))
	if pr, err := guest.ListPendingInvitationResources(ctx, &ram.ListPendingInvitationResourcesInput{ResourceShareInvitationArn: invArn}); err != nil || len(pr.Resources) != 1 {
		t.Fatalf("pending resources: %v %+v", err, pr)
	}
	acceptedIn, err := guest.AcceptResourceShareInvitation(ctx, &ram.AcceptResourceShareInvitationInput{ResourceShareInvitationArn: invArn})
	if err != nil || acceptedIn.ResourceShareInvitation.Status != types.ResourceShareInvitationStatusAccepted {
		t.Fatalf("accept: %v %+v", err, acceptedIn)
	}
	if _, err := guest.AcceptResourceShareInvitation(ctx, &ram.AcceptResourceShareInvitationInput{ResourceShareInvitationArn: invArn}); !errors.As(err, new(*types.ResourceShareInvitationAlreadyAcceptedException)) {
		t.Fatalf("accept twice: %T %v", err, err)
	}
	if sh, err := guest.GetResourceShares(ctx, &ram.GetResourceSharesInput{ResourceOwner: types.ResourceOwnerOtherAccounts}); err != nil || len(sh.ResourceShares) != 1 {
		t.Fatalf("shares of the guest: %v %+v", err, sh)
	}
	pa, _ = c.GetResourceShareAssociations(ctx, &ram.GetResourceShareAssociationsInput{AssociationType: types.ResourceShareAssociationTypePrincipal, Principal: aws.String(stranger)})
	if len(pa.ResourceShareAssociations) != 1 || pa.ResourceShareAssociations[0].Status != types.ResourceShareAssociationStatusAssociated {
		t.Fatalf("association after accept: %+v", pa)
	}

	// Update, tag, dissociate.
	up, err := c.UpdateResourceShare(ctx, &ram.UpdateResourceShareInput{ResourceShareArn: aws.String(arn), Name: aws.String("network"), AllowExternalPrincipals: aws.Bool(false)})
	if err != nil || aws.ToString(up.ResourceShare.Name) != "network" || aws.ToBool(up.ResourceShare.AllowExternalPrincipals) {
		t.Fatalf("update: %v %+v", err, up)
	}
	if _, err := c.TagResource(ctx, &ram.TagResourceInput{ResourceShareArn: aws.String(arn), Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("net")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &ram.UntagResourceInput{ResourceShareArn: aws.String(arn), TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	shares, _ = c.GetResourceShares(ctx, &ram.GetResourceSharesInput{ResourceOwner: types.ResourceOwnerSelf, Name: aws.String("network")})
	if len(shares.ResourceShares) != 1 || len(shares.ResourceShares[0].Tags) != 1 || aws.ToString(shares.ResourceShares[0].Tags[0].Key) != "team" {
		t.Fatalf("tags: %+v", shares.ResourceShares)
	}
	if _, err := c.DisassociateResourceShare(ctx, &ram.DisassociateResourceShareInput{ResourceShareArn: aws.String(arn), ResourceArns: []string{subnet}}); err != nil {
		t.Fatal(err)
	}
	if rl, _ := c.ListResources(ctx, &ram.ListResourcesInput{ResourceOwner: types.ResourceOwnerSelf}); len(rl.Resources) != 0 {
		t.Fatalf("resource left after disassociate: %+v", rl.Resources)
	}
	if _, err := c.AssociateResourceShare(ctx, &ram.AssociateResourceShareInput{ResourceShareArn: aws.String(arn), ResourceArns: []string{subnet}}); err != nil {
		t.Fatal(err)
	}

	// Delete.
	if _, err := c.DeleteResourceShare(ctx, &ram.DeleteResourceShareInput{ResourceShareArn: aws.String(arn)}); err != nil {
		t.Fatal(err)
	}
	if shares, _ = c.GetResourceShares(ctx, &ram.GetResourceSharesInput{ResourceOwner: types.ResourceOwnerSelf}); len(shares.ResourceShares) != 0 {
		t.Fatalf("share left: %+v", shares.ResourceShares)
	}
	_, err = c.UpdateResourceShare(ctx, &ram.UpdateResourceShareInput{ResourceShareArn: aws.String(arn), Name: aws.String("x")})
	if !errors.As(err, new(*types.UnknownResourceException)) {
		t.Fatalf("update after delete: %T %v", err, err)
	}
}
