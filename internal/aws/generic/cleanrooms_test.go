package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	"github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestCleanRooms(t *testing.T) {
	_, url := start(t, "cleanrooms")
	ctx := context.Background()
	c := cleanrooms.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateCollaboration(ctx, &cleanrooms.CreateCollaborationInput{Name: aws.String("collab"), Description: aws.String("d"),
		CreatorDisplayName: aws.String("me"), CreatorMemberAbilities: []types.MemberAbility{types.MemberAbilityCanQuery, types.MemberAbilityCanReceiveResults},
		Members:        []types.MemberSpecification{{AccountId: aws.String("111122223333"), DisplayName: aws.String("them"), MemberAbilities: []types.MemberAbility{}}},
		QueryLogStatus: types.CollaborationQueryLogStatusEnabled, Tags: map[string]string{"env": "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	id := o.Collaboration.Id
	g, err := c.GetCollaboration(ctx, &cleanrooms.GetCollaborationInput{CollaborationIdentifier: id})
	if err != nil || aws.ToString(g.Collaboration.Name) != "collab" || aws.ToString(g.Collaboration.CreatorAccountId) != "000000000000" {
		t.Fatalf("get: %v %+v", err, g)
	}
	ms, err := c.ListMembers(ctx, &cleanrooms.ListMembersInput{CollaborationIdentifier: id})
	if err != nil || len(ms.MemberSummaries) != 2 || ms.MemberSummaries[0].Status != types.MemberStatusActive || ms.MemberSummaries[1].Status != types.MemberStatusInvited ||
		aws.ToString(ms.MemberSummaries[1].AccountId) != "111122223333" || len(ms.MemberSummaries[0].Abilities) != 2 {
		t.Fatalf("members: %v %+v", err, ms)
	}
	l, err := c.ListCollaborations(ctx, &cleanrooms.ListCollaborationsInput{})
	if err != nil || len(l.CollaborationList) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.DeleteCollaboration(ctx, &cleanrooms.DeleteCollaborationInput{CollaborationIdentifier: id}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetCollaboration(ctx, &cleanrooms.GetCollaborationInput{CollaborationIdentifier: id}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.DeleteCollaboration(ctx, &cleanrooms.DeleteCollaborationInput{CollaborationIdentifier: id}); !errors.As(err, &nf) {
		t.Fatalf("second delete: %T %v", err, err)
	}
	if _, err := c.CreateCollaboration(ctx, &cleanrooms.CreateCollaborationInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}
