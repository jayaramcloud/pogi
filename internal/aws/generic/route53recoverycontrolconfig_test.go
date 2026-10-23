package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rc "github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig"
	"github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestRoute53RecoveryControlConfig(t *testing.T) {
	_, url := start(t, "route53-recovery-control-config")
	ctx := context.Background()
	c := rc.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateCluster(ctx, &rc.CreateClusterInput{ClusterName: aws.String("cl"), Tags: map[string]string{"env": "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	cl := o.Cluster
	if aws.ToString(cl.ClusterArn) == "" || cl.Status != types.StatusDeployed {
		t.Fatalf("cluster: %+v", cl)
	}
	g, err := c.DescribeCluster(ctx, &rc.DescribeClusterInput{ClusterArn: cl.ClusterArn})
	if err != nil || aws.ToString(g.Cluster.Name) != "cl" {
		t.Fatalf("describe: %v %+v", err, g)
	}
	cp, err := c.CreateControlPanel(ctx, &rc.CreateControlPanelInput{ClusterArn: cl.ClusterArn, ControlPanelName: aws.String("panel")})
	if err != nil || cp.ControlPanel.Status != types.StatusDeployed {
		t.Fatalf("panel: %v %+v", err, cp)
	}
	rcn, err := c.CreateRoutingControl(ctx, &rc.CreateRoutingControlInput{ClusterArn: cl.ClusterArn, ControlPanelArn: cp.ControlPanel.ControlPanelArn, RoutingControlName: aws.String("ctl")})
	if err != nil || aws.ToString(rcn.RoutingControl.Name) != "ctl" {
		t.Fatalf("routing control: %v %+v", err, rcn)
	}
	l, err := c.ListRoutingControls(ctx, &rc.ListRoutingControlsInput{ControlPanelArn: cp.ControlPanel.ControlPanelArn})
	if err != nil || len(l.RoutingControls) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.DeleteCluster(ctx, &rc.DeleteClusterInput{ClusterArn: cl.ClusterArn}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.DescribeCluster(ctx, &rc.DescribeClusterInput{ClusterArn: cl.ClusterArn}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.CreateCluster(ctx, &rc.CreateClusterInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}
