package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dax"
	"github.com/aws/aws-sdk-go-v2/service/dax/types"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	pogiddb "github.com/nrndev/pogi/internal/aws/dynamodb"
	"github.com/nrndev/pogi/internal/aws/generic"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

func daxFault(t *testing.T, err error, code string) {
	t.Helper()
	var ae interface{ ErrorCode() string }
	if !errors.As(err, &ae) || ae.ErrorCode() != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func TestDAX(t *testing.T) {
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return generic.New(h, "dax") },
		func(h *core.Hub) core.Service { return pogiddb.New(h) })
	ctx := context.Background()
	cfg := awstest.Config(t, url)
	c := dax.NewFromConfig(cfg)
	role := aws.String("arn:aws:iam::000000000000:role/dax")

	// failures first
	_, err := c.CreateCluster(ctx, &dax.CreateClusterInput{ClusterName: aws.String("x"), NodeType: aws.String("dax.t3.small"), ReplicationFactor: 1})
	if err == nil {
		t.Fatal("create without a role succeeded")
	}
	_, err = c.CreateCluster(ctx, &dax.CreateClusterInput{ClusterName: aws.String("x"), NodeType: aws.String("dax.t3.small"), ReplicationFactor: 0, IamRoleArn: role})
	if err == nil {
		t.Fatal("create with 0 nodes succeeded")
	}
	_, err = c.CreateCluster(ctx, &dax.CreateClusterInput{ClusterName: aws.String("x"), NodeType: aws.String("dax.t3.small"), ReplicationFactor: 1, IamRoleArn: role, ParameterGroupName: aws.String("nope")})
	daxFault(t, err, "ParameterGroupNotFoundFault")
	_, err = c.CreateCluster(ctx, &dax.CreateClusterInput{ClusterName: aws.String("x"), NodeType: aws.String("dax.t3.small"), ReplicationFactor: 1, IamRoleArn: role, SubnetGroupName: aws.String("nope")})
	daxFault(t, err, "SubnetGroupNotFoundFault")

	// parameters and groups
	d, err := c.DescribeDefaultParameters(ctx, &dax.DescribeDefaultParametersInput{})
	if err != nil || len(d.Parameters) != 2 || aws.ToString(d.Parameters[0].ParameterName) != "query-ttl-millis" || aws.ToString(d.Parameters[0].ParameterValue) != "300000" {
		t.Fatalf("defaults: %v %+v", err, d)
	}
	if _, err := c.CreateParameterGroup(ctx, &dax.CreateParameterGroupInput{ParameterGroupName: aws.String("pg"), Description: aws.String("ttl")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UpdateParameterGroup(ctx, &dax.UpdateParameterGroupInput{ParameterGroupName: aws.String("pg"),
		ParameterNameValues: []types.ParameterNameValue{{ParameterName: aws.String("record-ttl-millis"), ParameterValue: aws.String("1000")}}}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []types.ParameterNameValue{
		{ParameterName: aws.String("nope"), ParameterValue: aws.String("1")},
		{ParameterName: aws.String("query-ttl-millis"), ParameterValue: aws.String("soon")},
		{ParameterName: aws.String("query-ttl-millis"), ParameterValue: aws.String("-5")},
	} {
		if _, err := c.UpdateParameterGroup(ctx, &dax.UpdateParameterGroupInput{ParameterGroupName: aws.String("pg"), ParameterNameValues: []types.ParameterNameValue{bad}}); err == nil {
			t.Fatalf("bad parameter accepted: %+v", bad)
		}
	}
	_, err = c.UpdateParameterGroup(ctx, &dax.UpdateParameterGroupInput{ParameterGroupName: aws.String("ghost"),
		ParameterNameValues: []types.ParameterNameValue{{ParameterName: aws.String("query-ttl-millis"), ParameterValue: aws.String("1")}}})
	daxFault(t, err, "ParameterGroupNotFoundFault")
	p, err := c.DescribeParameters(ctx, &dax.DescribeParametersInput{ParameterGroupName: aws.String("pg")})
	if err != nil || len(p.Parameters) != 2 || aws.ToString(p.Parameters[0].ParameterValue) != "300000" || aws.ToString(p.Parameters[1].ParameterValue) != "1000" {
		t.Fatalf("parameters: %v %+v", err, p)
	}
	_, err = c.DescribeParameters(ctx, &dax.DescribeParametersInput{ParameterGroupName: aws.String("ghost")})
	daxFault(t, err, "ParameterGroupNotFoundFault")

	// the subnet group takes subnets as they are (no EC2 here)
	if _, err := c.CreateSubnetGroup(ctx, &dax.CreateSubnetGroupInput{SubnetGroupName: aws.String("sg"), SubnetIds: []string{"subnet-1"}}); err != nil {
		t.Fatal(err)
	}

	// create
	out, err := c.CreateCluster(ctx, &dax.CreateClusterInput{ClusterName: aws.String("cache"), NodeType: aws.String("dax.t3.small"), ReplicationFactor: 2, IamRoleArn: role,
		ParameterGroupName: aws.String("pg"), SubnetGroupName: aws.String("sg"), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	cl := out.Cluster
	if aws.ToString(cl.Status) != "available" || aws.ToInt32(cl.TotalNodes) != 2 || aws.ToInt32(cl.ActiveNodes) != 2 || len(cl.Nodes) != 2 ||
		aws.ToString(cl.ClusterArn) == "" || aws.ToString(cl.ParameterGroup.ParameterGroupName) != "pg" || aws.ToString(cl.SubnetGroup) != "sg" {
		t.Fatalf("cluster: %+v", cl)
	}
	n := cl.Nodes[0]
	if aws.ToString(n.NodeId) != "cache-a" || aws.ToString(n.NodeStatus) != "available" || aws.ToString(n.ParameterGroupStatus) != "in-sync" ||
		aws.ToString(n.AvailabilityZone) == "" || aws.ToString(n.Endpoint.URL) != aws.ToString(cl.ClusterDiscoveryEndpoint.URL) || aws.ToString(cl.Nodes[1].NodeId) != "cache-b" {
		t.Fatalf("node: %+v", n)
	}
	if _, err := c.CreateCluster(ctx, &dax.CreateClusterInput{ClusterName: aws.String("cache"), NodeType: aws.String("dax.t3.small"), ReplicationFactor: 1, IamRoleArn: role}); err == nil {
		t.Fatal("duplicate create succeeded")
	} else {
		daxFault(t, err, "ClusterAlreadyExistsFault")
	}
	if _, err := c.DeleteSubnetGroup(ctx, &dax.DeleteSubnetGroupInput{SubnetGroupName: aws.String("sg")}); err == nil {
		t.Fatal("deleted a subnet group in use")
	}
	if _, err := c.DeleteParameterGroup(ctx, &dax.DeleteParameterGroupInput{ParameterGroupName: aws.String("pg")}); err == nil {
		t.Fatal("deleted a parameter group in use")
	}

	// end to end: a plain DynamoDB client on the cluster's endpoint uses pogi's DynamoDB
	dc := ddb.NewFromConfig(awstest.Config(t, aws.ToString(cl.ClusterDiscoveryEndpoint.URL)))
	if _, err := dc.CreateTable(ctx, &ddb.CreateTableInput{TableName: aws.String("items"), BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := dc.PutItem(ctx, &ddb.PutItemInput{TableName: aws.String("items"), Item: map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "1"}, "v": &ddbtypes.AttributeValueMemberS{Value: "hi"}}}); err != nil {
		t.Fatal(err)
	}
	g, err := dc.GetItem(ctx, &ddb.GetItemInput{TableName: aws.String("items"), Key: map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "1"}}})
	if err != nil || g.Item["v"].(*ddbtypes.AttributeValueMemberS).Value != "hi" {
		t.Fatalf("get: %v %+v", err, g)
	}

	// describe, tags
	ds, err := c.DescribeClusters(ctx, &dax.DescribeClustersInput{ClusterNames: []string{"cache"}})
	if err != nil || len(ds.Clusters) != 1 || len(ds.Clusters[0].Nodes) != 2 {
		t.Fatalf("describe: %v %+v", err, ds)
	}
	_, err = c.DescribeClusters(ctx, &dax.DescribeClustersInput{ClusterNames: []string{"ghost"}})
	daxFault(t, err, "ClusterNotFoundFault")
	tags, err := c.ListTags(ctx, &dax.ListTagsInput{ResourceName: cl.ClusterArn})
	if err != nil || len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Value) != "dev" {
		t.Fatalf("tags: %v %+v", err, tags)
	}

	// scale
	up, err := c.IncreaseReplicationFactor(ctx, &dax.IncreaseReplicationFactorInput{ClusterName: aws.String("cache"), NewReplicationFactor: 4})
	if err != nil || len(up.Cluster.Nodes) != 4 || aws.ToInt32(up.Cluster.TotalNodes) != 4 || aws.ToString(up.Cluster.Nodes[3].NodeId) != "cache-d" {
		t.Fatalf("increase: %v %+v", err, up)
	}
	if _, err := c.IncreaseReplicationFactor(ctx, &dax.IncreaseReplicationFactorInput{ClusterName: aws.String("cache"), NewReplicationFactor: 4}); err == nil {
		t.Fatal("increase to the same size succeeded")
	}
	down, err := c.DecreaseReplicationFactor(ctx, &dax.DecreaseReplicationFactorInput{ClusterName: aws.String("cache"), NewReplicationFactor: 3, NodeIdsToRemove: []string{"cache-b"}})
	if err != nil || len(down.Cluster.Nodes) != 3 || aws.ToString(down.Cluster.Nodes[1].NodeId) != "cache-c" {
		t.Fatalf("decrease: %v %+v", err, down)
	}
	// the freed letter is used again
	up, err = c.IncreaseReplicationFactor(ctx, &dax.IncreaseReplicationFactorInput{ClusterName: aws.String("cache"), NewReplicationFactor: 4})
	if err != nil || aws.ToString(up.Cluster.Nodes[3].NodeId) != "cache-b" {
		t.Fatalf("increase again: %v %+v", err, up)
	}
	for _, bad := range []*dax.DecreaseReplicationFactorInput{
		{ClusterName: aws.String("cache"), NewReplicationFactor: 0},
		{ClusterName: aws.String("cache"), NewReplicationFactor: 4},
	} {
		if _, err := c.DecreaseReplicationFactor(ctx, bad); err == nil {
			t.Fatalf("bad decrease accepted: %+v", bad)
		}
	}
	_, err = c.DecreaseReplicationFactor(ctx, &dax.DecreaseReplicationFactorInput{ClusterName: aws.String("cache"), NewReplicationFactor: 3, NodeIdsToRemove: []string{"cache-z"}})
	daxFault(t, err, "NodeNotFoundFault")
	_, err = c.DecreaseReplicationFactor(ctx, &dax.DecreaseReplicationFactorInput{ClusterName: aws.String("ghost"), NewReplicationFactor: 1})
	daxFault(t, err, "ClusterNotFoundFault")

	// reboot
	if _, err := c.RebootNode(ctx, &dax.RebootNodeInput{ClusterName: aws.String("cache"), NodeId: aws.String("cache-a")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.RebootNode(ctx, &dax.RebootNodeInput{ClusterName: aws.String("cache"), NodeId: aws.String("cache-z")})
	daxFault(t, err, "NodeNotFoundFault")

	// delete
	if _, err := c.DeleteCluster(ctx, &dax.DeleteClusterInput{ClusterName: aws.String("cache")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeClusters(ctx, &dax.DescribeClustersInput{ClusterNames: []string{"cache"}})
	daxFault(t, err, "ClusterNotFoundFault")
	_, err = c.DeleteCluster(ctx, &dax.DeleteClusterInput{ClusterName: aws.String("cache")})
	daxFault(t, err, "ClusterNotFoundFault")
	if _, err := c.DeleteSubnetGroup(ctx, &dax.DeleteSubnetGroupInput{SubnetGroupName: aws.String("sg")}); err != nil {
		t.Fatal(err)
	}
}
