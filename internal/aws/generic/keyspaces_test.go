package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/keyspaces"
	"github.com/aws/aws-sdk-go-v2/service/keyspaces/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestKeyspaces(t *testing.T) {
	_, url := start(t, "keyspaces")
	ctx := context.Background()
	c := keyspaces.NewFromConfig(awstest.Config(t, url))
	const arnp = "arn:aws:cassandra:us-east-1:000000000000:/keyspace/"

	k, err := c.CreateKeyspace(ctx, &keyspaces.CreateKeyspaceInput{KeyspaceName: aws.String("shop"), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil || aws.ToString(k.ResourceArn) != arnp+"shop/" {
		t.Fatalf("keyspace: %v %+v", err, k)
	}
	var cf *types.ConflictException
	if _, err := c.CreateKeyspace(ctx, &keyspaces.CreateKeyspaceInput{KeyspaceName: aws.String("shop")}); !errors.As(err, &cf) {
		t.Fatalf("duplicate keyspace: %T %v", err, err)
	}
	gk, err := c.GetKeyspace(ctx, &keyspaces.GetKeyspaceInput{KeyspaceName: aws.String("shop")})
	if err != nil || gk.ReplicationStrategy != types.RsSingleRegion || aws.ToString(gk.ResourceArn) != arnp+"shop/" {
		t.Fatalf("get keyspace: %v %+v", err, gk)
	}

	schema := &types.SchemaDefinition{
		AllColumns:    []types.ColumnDefinition{{Name: aws.String("id"), Type: aws.String("uuid")}, {Name: aws.String("total"), Type: aws.String("decimal")}},
		PartitionKeys: []types.PartitionKey{{Name: aws.String("id")}},
	}
	tb, err := c.CreateTable(ctx, &keyspaces.CreateTableInput{KeyspaceName: aws.String("shop"), TableName: aws.String("orders"), SchemaDefinition: schema})
	if err != nil || aws.ToString(tb.ResourceArn) != arnp+"shop/table/orders" {
		t.Fatalf("table: %v %+v", err, tb)
	}
	if _, err := c.CreateTable(ctx, &keyspaces.CreateTableInput{KeyspaceName: aws.String("shop"), TableName: aws.String("orders"), SchemaDefinition: schema}); !errors.As(err, &cf) {
		t.Fatalf("duplicate table: %T %v", err, err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.CreateTable(ctx, &keyspaces.CreateTableInput{KeyspaceName: aws.String("nope"), TableName: aws.String("t"), SchemaDefinition: schema}); !errors.As(err, &nf) {
		t.Fatalf("table in a missing keyspace: %T %v", err, err)
	}
	gt, err := c.GetTable(ctx, &keyspaces.GetTableInput{KeyspaceName: aws.String("shop"), TableName: aws.String("orders")})
	if err != nil {
		t.Fatal(err)
	}
	if gt.Status != types.TableStatusActive || len(gt.SchemaDefinition.AllColumns) != 2 || gt.CreationTimestamp == nil ||
		gt.CapacitySpecification == nil || gt.CapacitySpecification.ThroughputMode != types.ThroughputModePayPerRequest ||
		gt.EncryptionSpecification == nil || gt.EncryptionSpecification.Type != types.EncryptionTypeAwsOwnedKmsKey ||
		gt.PointInTimeRecovery == nil || gt.PointInTimeRecovery.Status != types.PointInTimeRecoveryStatusDisabled {
		t.Fatalf("get table: %+v", gt)
	}
	if _, err := c.UpdateTable(ctx, &keyspaces.UpdateTableInput{KeyspaceName: aws.String("shop"), TableName: aws.String("orders"),
		PointInTimeRecovery: &types.PointInTimeRecovery{Status: types.PointInTimeRecoveryStatusEnabled}}); err != nil {
		t.Fatal(err)
	}
	gt, _ = c.GetTable(ctx, &keyspaces.GetTableInput{KeyspaceName: aws.String("shop"), TableName: aws.String("orders")})
	if gt.PointInTimeRecovery.Status != types.PointInTimeRecoveryStatusEnabled {
		t.Fatalf("pitr: %+v", gt.PointInTimeRecovery)
	}
	tl, err := c.ListTables(ctx, &keyspaces.ListTablesInput{KeyspaceName: aws.String("shop")})
	if err != nil || len(tl.Tables) != 1 || aws.ToString(tl.Tables[0].TableName) != "orders" {
		t.Fatalf("list tables: %v %+v", err, tl)
	}
	kl, err := c.ListKeyspaces(ctx, &keyspaces.ListKeyspacesInput{})
	if err != nil || len(kl.Keyspaces) != 1 {
		t.Fatalf("list keyspaces: %v %+v", err, kl)
	}
	tg, err := c.ListTagsForResource(ctx, &keyspaces.ListTagsForResourceInput{ResourceArn: k.ResourceArn})
	if err != nil || len(tg.Tags) != 1 {
		t.Fatalf("tags: %v %+v", err, tg)
	}

	if _, err := c.DeleteTable(ctx, &keyspaces.DeleteTableInput{KeyspaceName: aws.String("shop"), TableName: aws.String("orders")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetTable(ctx, &keyspaces.GetTableInput{KeyspaceName: aws.String("shop"), TableName: aws.String("orders")}); !errors.As(err, &nf) {
		t.Fatalf("table not found: %T %v", err, err)
	}
	if _, err := c.CreateTable(ctx, &keyspaces.CreateTableInput{KeyspaceName: aws.String("shop"), TableName: aws.String("left"), SchemaDefinition: schema}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteKeyspace(ctx, &keyspaces.DeleteKeyspaceInput{KeyspaceName: aws.String("shop")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetKeyspace(ctx, &keyspaces.GetKeyspaceInput{KeyspaceName: aws.String("shop")}); !errors.As(err, &nf) {
		t.Fatalf("keyspace not found: %T %v", err, err)
	}
	// its tables went with it
	if _, err := c.GetTable(ctx, &keyspaces.GetTableInput{KeyspaceName: aws.String("shop"), TableName: aws.String("left")}); !errors.As(err, &nf) {
		t.Fatalf("table of a deleted keyspace: %T %v", err, err)
	}
	if _, err := c.DeleteKeyspace(ctx, &keyspaces.DeleteKeyspaceInput{KeyspaceName: aws.String("shop")}); !errors.As(err, &nf) {
		t.Fatalf("second delete: %T %v", err, err)
	}
	if _, err := c.CreateKeyspace(ctx, &keyspaces.CreateKeyspaceInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
	if _, err := c.CreateKeyspace(ctx, &keyspaces.CreateKeyspaceInput{KeyspaceName: aws.String("bad-name")}); err == nil {
		t.Fatal("bad name accepted")
	}
}
