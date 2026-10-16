package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/identitystore"
	"github.com/aws/aws-sdk-go-v2/service/identitystore/document"
	"github.com/aws/aws-sdk-go-v2/service/identitystore/types"
	"github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestIdentityStore(t *testing.T) {
	_, url := start(t, "identitystore", "sso-admin")
	ctx := context.Background()
	cfg := awstest.Config(t, url)
	c := identitystore.NewFromConfig(cfg)
	ins, err := ssoadmin.NewFromConfig(cfg).ListInstances(ctx, &ssoadmin.ListInstancesInput{})
	if err != nil || len(ins.Instances) != 1 {
		t.Fatalf("instances: %v %+v", err, ins)
	}
	store := ins.Instances[0].IdentityStoreId
	d, err := c.DescribeUser(ctx, &identitystore.DescribeUserInput{IdentityStoreId: store, UserId: aws.String("nope")})
	var nf *types.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("describe before create: %T %v %v", err, err, d)
	}

	u, err := c.CreateUser(ctx, &identitystore.CreateUserInput{IdentityStoreId: store, UserName: aws.String("jdoe"), DisplayName: aws.String("Jane"),
		Name:   &types.Name{GivenName: aws.String("Jane"), FamilyName: aws.String("Doe")},
		Emails: []types.Email{{Value: aws.String("jane@example.com"), Primary: true, Type: aws.String("work")}}})
	if err != nil || aws.ToString(u.UserId) == "" {
		t.Fatalf("create user: %v %+v", err, u)
	}
	if _, err := c.CreateUser(ctx, &identitystore.CreateUserInput{IdentityStoreId: store, UserName: aws.String("jdoe")}); err == nil {
		t.Fatal("duplicate user name accepted")
	}
	g, err := c.DescribeUser(ctx, &identitystore.DescribeUserInput{IdentityStoreId: store, UserId: u.UserId})
	if err != nil || aws.ToString(g.UserName) != "jdoe" || aws.ToString(g.DisplayName) != "Jane" || len(g.Emails) != 1 {
		t.Fatalf("describe user: %v %+v", err, g)
	}
	id, err := c.GetUserId(ctx, &identitystore.GetUserIdInput{IdentityStoreId: store,
		AlternateIdentifier: &types.AlternateIdentifierMemberUniqueAttribute{Value: types.UniqueAttribute{AttributePath: aws.String("userName"), AttributeValue: document.NewLazyDocument("jdoe")}}})
	if err != nil || aws.ToString(id.UserId) != aws.ToString(u.UserId) {
		t.Fatalf("get user id: %v %+v", err, id)
	}
	if _, err := c.UpdateUser(ctx, &identitystore.UpdateUserInput{IdentityStoreId: store, UserId: u.UserId,
		Operations: []types.AttributeOperation{{AttributePath: aws.String("displayName"), AttributeValue: document.NewLazyDocument("Jane D")}}}); err != nil {
		t.Fatal(err)
	}
	g, _ = c.DescribeUser(ctx, &identitystore.DescribeUserInput{IdentityStoreId: store, UserId: u.UserId})
	if aws.ToString(g.DisplayName) != "Jane D" {
		t.Fatalf("update user: %+v", g)
	}

	grp, err := c.CreateGroup(ctx, &identitystore.CreateGroupInput{IdentityStoreId: store, DisplayName: aws.String("admins"), Description: aws.String("all admins")})
	if err != nil {
		t.Fatal(err)
	}
	m, err := c.CreateGroupMembership(ctx, &identitystore.CreateGroupMembershipInput{IdentityStoreId: store, GroupId: grp.GroupId, MemberId: &types.MemberIdMemberUserId{Value: aws.ToString(u.UserId)}})
	if err != nil || aws.ToString(m.MembershipId) == "" {
		t.Fatalf("membership: %v %+v", err, m)
	}
	in, err := c.IsMemberInGroups(ctx, &identitystore.IsMemberInGroupsInput{IdentityStoreId: store, MemberId: &types.MemberIdMemberUserId{Value: aws.ToString(u.UserId)}, GroupIds: []string{aws.ToString(grp.GroupId), "other"}})
	if err != nil || len(in.Results) != 2 || !in.Results[0].MembershipExists || in.Results[1].MembershipExists {
		t.Fatalf("is member: %v %+v", err, in)
	}

	us, err := c.ListUsers(ctx, &identitystore.ListUsersInput{IdentityStoreId: store})
	if err != nil || len(us.Users) != 1 {
		t.Fatalf("list users: %v %+v", err, us)
	}
	us, _ = c.ListUsers(ctx, &identitystore.ListUsersInput{IdentityStoreId: store, Filters: []types.Filter{{AttributePath: aws.String("UserName"), AttributeValue: aws.String("nobody")}}})
	if len(us.Users) != 0 {
		t.Fatalf("filtered users: %+v", us)
	}
	gs, err := c.ListGroups(ctx, &identitystore.ListGroupsInput{IdentityStoreId: store})
	if err != nil || len(gs.Groups) != 1 || aws.ToString(gs.Groups[0].DisplayName) != "admins" {
		t.Fatalf("list groups: %v %+v", err, gs)
	}
	ms, err := c.ListGroupMemberships(ctx, &identitystore.ListGroupMembershipsInput{IdentityStoreId: store, GroupId: grp.GroupId})
	if err != nil || len(ms.GroupMemberships) != 1 {
		t.Fatalf("list memberships: %v %+v", err, ms)
	}
	ms2, err := c.ListGroupMembershipsForMember(ctx, &identitystore.ListGroupMembershipsForMemberInput{IdentityStoreId: store, MemberId: &types.MemberIdMemberUserId{Value: aws.ToString(u.UserId)}})
	if err != nil || len(ms2.GroupMemberships) != 1 {
		t.Fatalf("memberships for member: %v %+v", err, ms2)
	}

	if _, err := c.DeleteGroupMembership(ctx, &identitystore.DeleteGroupMembershipInput{IdentityStoreId: store, MembershipId: m.MembershipId}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteGroup(ctx, &identitystore.DeleteGroupInput{IdentityStoreId: store, GroupId: grp.GroupId}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteUser(ctx, &identitystore.DeleteUserInput{IdentityStoreId: store, UserId: u.UserId}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeUser(ctx, &identitystore.DescribeUserInput{IdentityStoreId: store, UserId: u.UserId})
	if !errors.As(err, &nf) {
		t.Fatalf("describe after delete: %T %v", err, err)
	}
}
