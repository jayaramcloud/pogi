package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestDirectConnect(t *testing.T) {
	_, url := start(t, "directconnect")
	ctx := context.Background()
	c := directconnect.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateConnection(ctx, &directconnect.CreateConnectionInput{
		ConnectionName: aws.String("link"), Bandwidth: aws.String("1Gbps"), Location: aws.String("EqDC2"),
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := aws.ToString(o.ConnectionId)
	if id == "" || aws.ToString(o.ConnectionName) != "link" || aws.ToString(o.Bandwidth) != "1Gbps" || o.ConnectionState == "" {
		t.Fatalf("connection: %+v", o)
	}
	d, err := c.DescribeConnections(ctx, &directconnect.DescribeConnectionsInput{ConnectionId: aws.String(id)})
	if err != nil || len(d.Connections) != 1 || aws.ToString(d.Connections[0].Location) != "EqDC2" {
		t.Fatalf("describe: %v %+v", err, d)
	}
	if _, err := c.DescribeConnections(ctx, &directconnect.DescribeConnectionsInput{ConnectionId: aws.String("dxcon-nope")}); err == nil {
		t.Fatal("unknown connection found")
	}
	gw, err := c.CreateDirectConnectGateway(ctx, &directconnect.CreateDirectConnectGatewayInput{DirectConnectGatewayName: aws.String("gw"), AmazonSideAsn: aws.Int64(64512)})
	if err != nil || gw.DirectConnectGateway == nil || aws.ToString(gw.DirectConnectGateway.DirectConnectGatewayId) == "" {
		t.Fatalf("gateway: %v %+v", err, gw)
	}
	gl, err := c.DescribeDirectConnectGateways(ctx, &directconnect.DescribeDirectConnectGatewaysInput{})
	if err != nil || len(gl.DirectConnectGateways) != 1 {
		t.Fatalf("gateways: %v %+v", err, gl)
	}
	if _, err := c.DeleteDirectConnectGateway(ctx, &directconnect.DeleteDirectConnectGatewayInput{DirectConnectGatewayId: gw.DirectConnectGateway.DirectConnectGatewayId}); err != nil {
		t.Fatal(err)
	}
	if gl, err := c.DescribeDirectConnectGateways(ctx, &directconnect.DescribeDirectConnectGatewaysInput{DirectConnectGatewayId: gw.DirectConnectGateway.DirectConnectGatewayId}); err != nil || len(gl.DirectConnectGateways) != 0 {
		t.Fatalf("deleted gateway by id: %v %+v", err, gl)
	}
	if _, err := c.DeleteConnection(ctx, &directconnect.DeleteConnectionInput{ConnectionId: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	// as in AWS, a deleted connection can still be read, in the state "deleted"
	if d, err := c.DescribeConnections(ctx, &directconnect.DescribeConnectionsInput{ConnectionId: aws.String(id)}); err != nil || len(d.Connections) != 1 || d.Connections[0].ConnectionState != types.ConnectionStateDeleted {
		t.Fatalf("after delete: %v %+v", err, d)
	}
	if _, err := c.DeleteConnection(ctx, &directconnect.DeleteConnectionInput{ConnectionId: aws.String("dxcon-nope")}); err == nil {
		t.Fatal("delete of an unknown connection succeeded")
	}
	if _, err := c.CreateConnection(ctx, &directconnect.CreateConnectionInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}

func TestDirectConnectTags(t *testing.T) {
	_, url := start(t, "directconnect")
	ctx := context.Background()
	c := directconnect.NewFromConfig(awstest.Config(t, url))
	o, err := c.CreateConnection(ctx, &directconnect.CreateConnectionInput{ConnectionName: aws.String("link"), Bandwidth: aws.String("1Gbps"), Location: aws.String("EqDC2"),
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	arn := "arn:aws:directconnect:us-east-1:000000000000:dxcon/" + aws.ToString(o.ConnectionId)
	d, err := c.DescribeTags(ctx, &directconnect.DescribeTagsInput{ResourceArns: []string{arn}})
	if err != nil || len(d.ResourceTags) != 1 || len(d.ResourceTags[0].Tags) != 1 {
		t.Fatalf("tags: %v %+v", err, d)
	}
}
