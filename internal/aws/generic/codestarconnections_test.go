package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/codestarconnections"
	"github.com/aws/aws-sdk-go-v2/service/codestarconnections/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestCodeStarConnections(t *testing.T) {
	_, url := start(t, "codestar-connections")
	ctx := context.Background()
	c := codestarconnections.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateConnection(ctx, &codestarconnections.CreateConnectionInput{ConnectionName: aws.String("gh"), ProviderType: types.ProviderTypeGithub,
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	arn := aws.ToString(o.ConnectionArn)
	if !strings.HasPrefix(arn, "arn:aws:codestar-connections:us-east-1:000000000000:connection/") {
		t.Fatalf("arn: %q", arn)
	}
	g, err := c.GetConnection(ctx, &codestarconnections.GetConnectionInput{ConnectionArn: aws.String(arn)})
	if err != nil {
		t.Fatal(err)
	}
	cn := g.Connection
	if cn.ConnectionStatus != types.ConnectionStatusAvailable || aws.ToString(cn.ConnectionName) != "gh" || cn.ProviderType != types.ProviderTypeGithub || aws.ToString(cn.OwnerAccountId) != "000000000000" {
		t.Fatalf("connection: %+v", cn)
	}
	if _, err := c.CreateConnection(ctx, &codestarconnections.CreateConnectionInput{ConnectionName: aws.String("bb"), ProviderType: types.ProviderTypeBitbucket}); err != nil {
		t.Fatal(err)
	}
	l, err := c.ListConnections(ctx, &codestarconnections.ListConnectionsInput{ProviderTypeFilter: types.ProviderTypeGithub})
	if err != nil || len(l.Connections) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	tg, err := c.ListTagsForResource(ctx, &codestarconnections.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	if err != nil || len(tg.Tags) != 1 {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.DeleteConnection(ctx, &codestarconnections.DeleteConnectionInput{ConnectionArn: aws.String(arn)}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetConnection(ctx, &codestarconnections.GetConnectionInput{ConnectionArn: aws.String(arn)}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.DeleteConnection(ctx, &codestarconnections.DeleteConnectionInput{ConnectionArn: aws.String(arn)}); !errors.As(err, &nf) {
		t.Fatalf("second delete: %T %v", err, err)
	}
	if _, err := c.CreateConnection(ctx, &codestarconnections.CreateConnectionInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}
