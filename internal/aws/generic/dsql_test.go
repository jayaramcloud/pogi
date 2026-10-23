package generic_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dsql"
	"github.com/aws/aws-sdk-go-v2/service/dsql/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestDSQL(t *testing.T) {
	_, url := start(t, "dsql")
	ctx := context.Background()
	c := dsql.NewFromConfig(awstest.Config(t, url))

	_, err := c.GetCluster(ctx, &dsql.GetClusterInput{Identifier: aws.String("abcdefghijklmnopqrstuvwxyz")})
	kCode(t, err, "ResourceNotFoundException")
	_, err = c.DeleteCluster(ctx, &dsql.DeleteClusterInput{Identifier: aws.String("abcdefghijklmnopqrstuvwxyz")})
	kCode(t, err, "ResourceNotFoundException")
	if _, err := c.GetCluster(ctx, &dsql.GetClusterInput{Identifier: aws.String("x")}); err == nil {
		t.Fatal("a bad id was accepted")
	}

	n, err := c.CreateCluster(ctx, &dsql.CreateClusterInput{DeletionProtectionEnabled: aws.Bool(false), Tags: map[string]string{"env": "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	id := aws.ToString(n.Identifier)
	if len(id) != 26 || n.Status != types.ClusterStatusActive && n.Status != types.ClusterStatusCreating {
		t.Fatalf("create: %+v", n)
	}
	g, err := c.GetCluster(ctx, &dsql.GetClusterInput{Identifier: aws.String(id)})
	if err != nil || g.Status != types.ClusterStatusActive || aws.ToString(g.Arn) != aws.ToString(n.Arn) ||
		!strings.HasPrefix(aws.ToString(g.Endpoint), id+".dsql.") || g.DeletionProtectionEnabled == nil || *g.DeletionProtectionEnabled ||
		g.EncryptionDetails == nil || g.EncryptionDetails.EncryptionType != types.EncryptionTypeAwsOwnedKmsKey || aws.ToString(g.EncryptionDetails.KmsKeyArn) != "AWS_OWNED_KMS_KEY" {
		t.Fatalf("get: %v %+v", err, g)
	}
	sn, err := c.GetVpcEndpointServiceName(ctx, &dsql.GetVpcEndpointServiceNameInput{Identifier: aws.String(id)})
	if err != nil || !strings.HasPrefix(aws.ToString(sn.ServiceName), "com.amazonaws.us-east-1.dsql-") {
		t.Fatalf("service name: %v %+v", err, sn)
	}
	_, err = c.GetVpcEndpointServiceName(ctx, &dsql.GetVpcEndpointServiceNameInput{Identifier: aws.String("abcdefghijklmnopqrstuvwxyz")})
	kCode(t, err, "ResourceNotFoundException")
	l, err := c.ListClusters(ctx, &dsql.ListClustersInput{})
	if err != nil || len(l.Clusters) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	tg, err := c.ListTagsForResource(ctx, &dsql.ListTagsForResourceInput{ResourceArn: n.Arn})
	if err != nil || tg.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.DeleteCluster(ctx, &dsql.DeleteClusterInput{Identifier: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetCluster(ctx, &dsql.GetClusterInput{Identifier: aws.String(id)})
	kCode(t, err, "ResourceNotFoundException")
}
